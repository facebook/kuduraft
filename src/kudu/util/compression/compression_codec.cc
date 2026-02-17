// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// For LZ4 dictionary compression (LZ4F_decompress_usingDict, LZ4F_createCDict
// etc.)
#define LZ4F_STATIC_LINKING_ONLY

#include "kudu/util/compression/compression_codec.h"

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <lz4.h>
#include <lz4frame.h>
#include <snappy-sinksource.h> // @manual
#include <snappy.h> // @manual
#include <zlib.h>
#include <zstd.h>

#include <folly/compression/CompressionContextPoolSingletons.h>

#include <fmt/core.h>
#include "kudu/util/faststring.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"

namespace kudu {

using std::vector;

CompressionCodec::CompressionCodec() {}

CompressionCodec::~CompressionCodec() {}

std::string CompressionCodec::Stats() const {
  try {
    std::ostringstream s;
    JsonWriter jw(&s, JsonWriter::kCompact);
    jw.StartObject();

    jw.String("codec");
    jw.String(CompressionType_Name(type()));

    jw.String("dict_id");
    jw.Int(CompressionCodecManager::GetCurrentDictionaryID());

    jw.String("level");
    jw.Int(compressionLevel_);

    jw.String("total_bytes_before_compression");
    jw.Int64(totalBytesBeforeCompression_);

    jw.String("total_bytes_after_compression");
    jw.Int64(totalBytesAfterCompression_);

    jw.String("total_compressions");
    jw.Int64(totalCompressions_);

    jw.String("total_bytes_before_decompression");
    jw.Int64(totalBytesBeforeDecompression_);

    jw.String("total_bytes_after_decompression");
    jw.Int64(totalBytesAfterDecompression_);

    jw.String("total_decompressions");
    jw.Int64(totalDecompressions_);

    jw.String("total_compression_errors");
    jw.Int64(totalCompressionErrors_);

    jw.String("total_decompression_errors");
    jw.Int64(totalDecompressionErrors_);

    jw.EndObject();
    return s.str();
  } catch (...) {
    return {};
  }
}

class SlicesSource : public snappy::Source {
 public:
  explicit SlicesSource(const std::vector<Slice>& slices)
      : sliceIndex_(0), sliceOffset_(0), slices_(slices) {
    available_ = totalSize();
  }

  size_t Available() const override {
    return available_;
  }

  const char* Peek(size_t* len) override {
    if (available_ == 0) {
      *len = 0;
      return nullptr;
    }

    const Slice& data = slices_[sliceIndex_];
    *len = data.size() - sliceOffset_;
    return reinterpret_cast<const char*>(data.data()) + sliceOffset_;
  }

  void Skip(size_t n) override {
    DCHECK_LE(n, Available());
    if (n == 0) {
      return;
    }

    available_ -= n;
    if ((n + sliceOffset_) < slices_[sliceIndex_].size()) {
      sliceOffset_ += n;
    } else {
      n -= slices_[sliceIndex_].size() - sliceOffset_;
      sliceIndex_++;
      while (n > 0 && n >= slices_[sliceIndex_].size()) {
        n -= slices_[sliceIndex_].size();
        sliceIndex_++;
      }
      sliceOffset_ = n;
    }
  }

  void dump(faststring* buffer) {
    buffer->reserve(buffer->size() + totalSize());
    for (const Slice& block : slices_) {
      buffer->append(block.data(), block.size());
    }
  }

 private:
  size_t totalSize(void) const {
    size_t size = 0;
    for (const Slice& data : slices_) {
      size += data.size();
    }
    return size;
  }

 private:
  size_t available_;
  size_t sliceIndex_;
  size_t sliceOffset_;
  const vector<Slice>& slices_;
};

class SnappyCodec : public CompressionCodec {
 public:
  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    snappy::RawCompress(
        reinterpret_cast<const char*>(input.data()),
        input.size(),
        reinterpret_cast<char*>(compressed),
        compressed_length);
    return Status::OK();
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    SlicesSource source(input_slices);
    snappy::UncheckedByteArraySink sink(reinterpret_cast<char*>(compressed));
    if ((*compressed_length = snappy::Compress(&source, &sink)) <= 0) {
      return Status::Corruption("unable to compress the buffer");
    }
    return Status::OK();
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t /* uncompressed_length */) override {
    bool success = snappy::RawUncompress(
        reinterpret_cast<const char*>(compressed.data()),
        compressed.size(),
        reinterpret_cast<char*>(uncompressed));
    return success ? Status::OK()
                   : Status::Corruption("unable to uncompress the buffer");
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return snappy::MaxCompressedLength(source_bytes);
  }

  CompressionType type() const override {
    return SNAPPY;
  }
};

class Lz4Codec : public CompressionCodec {
 public:
  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    int n = LZ4_compress(
        reinterpret_cast<const char*>(input.data()),
        reinterpret_cast<char*>(compressed),
        input.size());
    *compressed_length = n;
    return Status::OK();
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    SlicesSource source(input_slices);
    faststring buffer;
    source.dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    int n = LZ4_decompress_safe(
        reinterpret_cast<const char*>(compressed.data()),
        reinterpret_cast<char*>(uncompressed),
        compressed.size(),
        uncompressed_length);
    if (n != uncompressed_length) {
      return Status::Corruption(
          fmt::format(
              "unable to uncompress the buffer. error near {}, buffer", -n),
          KUDU_REDACT(compressed.ToDebugString(1000)));
    }
    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return LZ4_compressBound(source_bytes);
  }

  Status SetCompressionLevel(int level) override {
    if (level < 0) {
      const std::string& msg =
          fmt::format("Compression level {} not supported by LZ4", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compressionLevel_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return LZ4;
  }
};

class Lz4DictCodec : public CompressionCodec {
 public:
  Lz4DictCodec() {
    compressionLevel_ = 1;
  }

  ~Lz4DictCodec() {
    LZ4F_freeCompressionContext(compressionCtx_);
    LZ4F_freeDecompressionContext(decompressionCtx_);
    LZ4F_freeCDict(dictCtx_);
  }

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (!compressionCtx_ &&
        LZ4F_createCompressionContext(&compressionCtx_, LZ4F_VERSION)) {
      return Status::RuntimeError("Could not create LZ4 compression context");
    }

    const size_t maxCompSize = MaxCompressedLength(input.size());

    LZ4F_preferences_t prefs{};
    prefs.compressionLevel = compressionLevel_;
    prefs.frameInfo.dictID = CompressionCodecManager::GetDictionaryID(dict_);
    prefs.frameInfo.contentSize = input.size();

    size_t ret = LZ4F_compressFrame_usingCDict(
        compressionCtx_,
        compressed,
        maxCompSize,
        input.data(),
        input.size(),
        dictCtx_,
        &prefs);

    if (LZ4F_isError(ret)) {
      return Status::Corruption(
          fmt::format(
              "Unable to compress the buffer: {}", LZ4F_getErrorName(ret)));
    }

    *compressed_length = ret;
    return Status::OK();
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    SlicesSource source(input_slices);
    faststring buffer;
    source.dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    if (!decompressionCtx_ &&
        LZ4F_createDecompressionContext(&decompressionCtx_, LZ4F_VERSION)) {
      return Status::RuntimeError("Could not create LZ4 decompression context");
    }

    size_t frameInfoSize = compressed.size();

    LZ4F_frameInfo_t frameInfo;
    size_t ret = LZ4F_getFrameInfo(
        decompressionCtx_, &frameInfo, compressed.data(), &frameInfoSize);
    if (LZ4F_isError(ret)) {
      LZ4F_resetDecompressionContext(decompressionCtx_);
      return Status::Corruption(
          fmt::format(
              "Could not extract LZ4 frame info: {}", LZ4F_getErrorName(ret)));
    }

    const unsigned actualDictId = frameInfo.dictID;
    const unsigned expectedDictId =
        CompressionCodecManager::GetDictionaryID(dict_);

    if (expectedDictId != actualDictId) {
      return Status::CompressionDictMismatch("Dictionary ID mismatch");
    }

    LZ4F_decompressOptions_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.stableDst = 0;

    size_t compressedSize = compressed.size() - frameInfoSize;
    const uint8_t* compressedBuf = compressed.data() + frameInfoSize;

    ret = LZ4F_decompress_usingDict(
        decompressionCtx_,
        uncompressed,
        &uncompressed_length,
        compressedBuf,
        &compressedSize,
        dict_.data(),
        dict_.size(),
        &opts);
    if (LZ4F_isError(ret)) {
      LZ4F_resetDecompressionContext(decompressionCtx_);
      return Status::Corruption(
          fmt::format(
              "Unable to decompress the buffer: {}", LZ4F_getErrorName(ret)));
    }

    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return LZ4F_compressBound(source_bytes, nullptr) + LZ4F_HEADER_SIZE_MAX;
  }

  Status SetDictionary(const std::string& dict) override {
    dict_ = dict;
    dictCtx_ = LZ4F_createCDict(dict_.data(), dict_.size());
    return Status::OK();
  }

  std::string GetDictionary() const override {
    return dict_;
  }

  Status SetCompressionLevel(int level) override {
    if (level < 0) {
      const std::string& msg =
          fmt::format("Compression level {} not supported by LZ4", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compressionLevel_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return LZ4_DICT;
  }

 private:
  LZ4F_cctx* compressionCtx_ = nullptr;
  LZ4F_dctx* decompressionCtx_ = nullptr;

  LZ4F_CDict* dictCtx_ = nullptr;
  std::string dict_;
};

/**
 * TODO: use a instance-local Arena and pass alloc/free into zlib
 * so that it allocates from the arena.
 */
class ZlibCodec : public CompressionCodec {
 public:
  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    *compressed_length = MaxCompressedLength(input.size());
    int err =
        ::compress(compressed, compressed_length, input.data(), input.size());
    return err == Z_OK ? Status::OK()
                       : Status::IOError("unable to compress the buffer");
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    // TODO: use z_stream
    SlicesSource source(input_slices);
    faststring buffer;
    source.dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    int err = ::uncompress(
        uncompressed,
        &uncompressed_length,
        compressed.data(),
        compressed.size());
    return err == Z_OK ? Status::OK()
                       : Status::Corruption("unable to uncompress the buffer");
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    // one-time overhead of six bytes for the entire stream plus five bytes per
    // 16 KB block
    return source_bytes + (6 + (5 * ((source_bytes + 16383) >> 14)));
  }

  CompressionType type() const override {
    return ZLIB;
  }
};

class ZstdCodec : public CompressionCodec {
 public:
  ZstdCodec() {
    compressionLevel_ = 1;
  }

  ~ZstdCodec() {}

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    const size_t maxCompSize = MaxCompressedLength(input.size());
    const auto ctxRef = folly::compression::contexts::getZSTD_CCtx();
    auto* ctx = ctxRef.get();
    const size_t ret = ZSTD_compressCCtx(
        ctx,
        compressed,
        maxCompSize,
        input.data(),
        input.size(),
        compressionLevel_);
    if (ZSTD_isError(ret)) {
      return Status::Corruption(
          fmt::format(
              "unable to compress the buffer: {}", ZSTD_getErrorName(ret)));
    }
    *compressed_length = ret;
    return Status::OK();
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    SlicesSource source(input_slices);
    faststring buffer;
    source.dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    const auto ctxRef = folly::compression::contexts::getZSTD_DCtx();
    auto* ctx = ctxRef.get();
    size_t ret = ZSTD_decompressDCtx(
        ctx,
        uncompressed,
        uncompressed_length,
        compressed.data(),
        compressed.size());
    if (ZSTD_isError(ret)) {
      return Status::Corruption(
          fmt::format(
              "unable to uncompress the buffer: {}", ZSTD_getErrorName(ret)));
    }
    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return ZSTD_compressBound(source_bytes);
  }

  Status SetCompressionLevel(int level) override {
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel()) {
      const std::string& msg =
          fmt::format("Compression level {} not supported by ZSTD", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compressionLevel_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return ZSTD;
  }
};

class ZstdDictCodec : public CompressionCodec {
 public:
  ZstdDictCodec() {
    compressionLevel_ = 1;
    SetDictionary("");
  }

  ~ZstdDictCodec() {
    ZSTD_freeCDict(compressionDict_);
    ZSTD_freeDDict(decompressionDict_);
  }

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (!compressionDict_) {
      return Status::CompressionDictMismatch("Compression dictionary is empty");
    }

    auto ctxRef = folly::compression::contexts::getZSTD_CCtx();
    auto ctx = ctxRef.get();

    const size_t maxCompSize = MaxCompressedLength(input.size());
    const size_t ret = ZSTD_compress_usingCDict(
        ctx,
        compressed,
        maxCompSize,
        input.data(),
        input.size(),
        compressionDict_);

    if (ZSTD_isError(ret)) {
      return Status::Corruption(
          fmt::format(
              "unable to compress the buffer: {}", ZSTD_getErrorName(ret)));
    }

    *compressed_length = ret;
    return Status::OK();
  }

  Status Compress(
      const vector<Slice>& input_slices,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (input_slices.size() == 1) {
      return Compress(input_slices[0], compressed, compressed_length);
    }

    SlicesSource source(input_slices);
    faststring buffer;
    source.dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    if (!decompressionDict_) {
      return Status::CompressionDictMismatch("Compression dictionary is empty");
    }

    const unsigned expectedDictId =
        CompressionCodecManager::GetDictionaryID(dict_);
    const unsigned actualDictId =
        ZSTD_getDictID_fromFrame(compressed.data(), compressed.size());

    if (expectedDictId != actualDictId) {
      return Status::CompressionDictMismatch("Dictionary ID mismatch");
    }

    auto ctxRef = folly::compression::contexts::getZSTD_DCtx();
    auto ctx = ctxRef.get();

    size_t ret = ZSTD_decompress_usingDDict(
        ctx,
        uncompressed,
        uncompressed_length,
        compressed.data(),
        compressed.size(),
        decompressionDict_);
    if (ZSTD_isError(ret)) {
      return Status::Corruption(
          fmt::format(
              "unable to uncompress the buffer: {}", ZSTD_getErrorName(ret)));
    }

    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return ZSTD_compressBound(source_bytes);
  }

  Status SetDictionary(const std::string& dict) override {
    ZSTD_freeCDict(compressionDict_);
    ZSTD_freeDDict(decompressionDict_);

    dict_.clear();
    compressionDict_ = nullptr;
    decompressionDict_ = nullptr;

    compressionDict_ =
        ZSTD_createCDict(dict.c_str(), dict.size(), compressionLevel_);
    decompressionDict_ = ZSTD_createDDict(dict.c_str(), dict.size());

    if (!compressionDict_ || !decompressionDict_) {
      return Status::RuntimeError("Could not create compression dict objects");
    }

    dict_ = dict;
    return Status::OK();
  }

  std::string GetDictionary() const override {
    return dict_;
  }

  Status SetCompressionLevel(int level) override {
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel()) {
      const std::string& msg =
          fmt::format("Compression level {} not supported by ZSTD", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compressionLevel_ = level;
    std::string dict = dict_;
    return SetDictionary(dict);
  }

  CompressionType type() const override {
    return ZSTD_DICT;
  }

 private:
  std::string dict_;

  ZSTD_CDict* compressionDict_ = nullptr;
  ZSTD_DDict* decompressionDict_ = nullptr;
};

folly::Synchronized<CompressionCodecManager::CodecData, folly::SpinLock>
    CompressionCodecManager::codecData;

std::atomic_int CompressionCodecManager::level;

Status CompressionCodecManager::GetCodec(
    CompressionType type,
    std::shared_ptr<CompressionCodec>* codec) {
  switch (type) {
    case NO_COMPRESSION:
      *codec = nullptr;
      break;
    case SNAPPY:
      *codec = std::make_shared<SnappyCodec>();
      break;
    case LZ4:
      *codec = std::make_shared<Lz4Codec>();
      break;
    case LZ4_DICT:
      *codec = std::make_shared<Lz4DictCodec>();
      break;
    case ZLIB:
      *codec = std::make_shared<ZlibCodec>();
      break;
    case ZSTD:
      *codec = std::make_shared<ZstdCodec>();
      break;
    case ZSTD_DICT:
      *codec = std::make_shared<ZstdDictCodec>();
      break;
    default:
      return Status::NotFound("bad compression type");
  }
  return Status::OK();
}

Status CompressionCodecManager::SetCurrentCodec(CompressionType type) {
  auto dataLocked = codecData.lock();
  auto& codec = dataLocked->first;
  auto& dictionary = dataLocked->second;

  if (codec && type == codec->type()) {
    return Status::OK();
  }
  // codec can be nullptr if type = NO_COMPRESSION
  RETURN_NOT_OK(GetCodec(type, &codec));
  if (codec) {
    RETURN_NOT_OK(codec->SetDictionary(dictionary));
    if (!codec->SetCompressionLevel(CompressionCodecManager::level).ok()) {
      int codecLevel = codec->CompressionLevel();
      LOG(WARNING) << "Could not set compression level to "
                   << CompressionCodecManager::level << ". "
                   << "Using the default compression level " << codecLevel
                   << " instead";
      CompressionCodecManager::level = codecLevel;
    }
  }
  LOG(INFO) << "Set compression codec to: "
            << GetCodecName(codec ? codec->type() : NO_COMPRESSION);
  return Status::OK();
}

Status CompressionCodecManager::SetDictionary(const std::string& dict) {
  auto dataLocked = codecData.lock();
  auto& codec = dataLocked->first;
  auto& dictionary = dataLocked->second;

  if (!codec) {
    dictionary = dict;
    return Status::OK();
  }
  RETURN_NOT_OK(codec->SetDictionary(dict));
  dictionary = dict;
  LOG(INFO) << "Updating compression dict to id "
            << GetDictionaryID(dictionary);
  return Status::OK();
}

unsigned int CompressionCodecManager::GetCurrentDictionaryID() {
  return GetDictionaryID(codecData.lock()->second);
}

unsigned int CompressionCodecManager::GetDictionaryID(const std::string& dict) {
  // LZ4 also uses ZSTD dict format
  return ZSTD_getDictID_fromDict(dict.data(), dict.size());
}

Status CompressionCodecManager::SetCurrentCompressionLevel(
    int compressionLevel) {
  auto dataLocked = codecData.lock();
  const auto& codec = dataLocked->first;

  if (!codec) {
    CompressionCodecManager::level = compressionLevel;
    return Status::OK();
  }
  RETURN_NOT_OK(codec->SetCompressionLevel(compressionLevel));
  CompressionCodecManager::level = compressionLevel;
  return Status::OK();
}

} // namespace kudu
