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

#include <folly/compression/CompressionContextPoolSingletons.h>
#include <glog/logging.h>
#include <lz4.h>
#include <lz4frame.h>
#include <snappy-sinksource.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/scoped_cleanup.h"

DEFINE_string(compression_dict_filename, "", "Compression dictionary filename");

namespace kudu {

using std::vector;

CompressionCodec::CompressionCodec() {}

CompressionCodec::~CompressionCodec() {}

std::string CompressionCodec::Stats() const {
  try {
    std::ostringstream s;
    JsonWriter jw(&s, JsonWriter::COMPACT);
    jw.StartObject();

    jw.String("codec");
    jw.String(CompressionType_Name(type()));

    jw.String("dict_id");
    jw.Int(CompressionCodecManager::GetDictionaryID(GetDictionary()));

    jw.String("level");
    jw.Int(compression_level_);

    jw.String("total_bytes_before_compression");
    jw.Int64(total_bytes_before_compression_);

    jw.String("total_bytes_after_compression");
    jw.Int64(total_bytes_after_compression_);

    jw.String("total_compressions");
    jw.Int64(total_compressions_);

    jw.String("total_bytes_before_decompression");
    jw.Int64(total_bytes_before_decompression_);

    jw.String("total_bytes_after_decompression");
    jw.Int64(total_bytes_after_decompression_);

    jw.String("total_decompressions");
    jw.Int64(total_decompressions_);

    jw.String("total_compression_errors");
    jw.Int64(total_compression_errors_);

    jw.String("total_decompression_errors");
    jw.Int64(total_decompression_errors_);

    jw.EndObject();
    return s.str();
  } catch (...) {
    return {};
  }
}

class SlicesSource : public snappy::Source {
 public:
  explicit SlicesSource(const std::vector<Slice>& slices)
      : slice_index_(0), slice_offset_(0), slices_(slices) {
    available_ = TotalSize();
  }

  size_t Available() const override {
    return available_;
  }

  const char* Peek(size_t* len) override {
    if (available_ == 0) {
      *len = 0;
      return nullptr;
    }

    const Slice& data = slices_[slice_index_];
    *len = data.size() - slice_offset_;
    return reinterpret_cast<const char*>(data.data()) + slice_offset_;
  }

  void Skip(size_t n) override {
    DCHECK_LE(n, Available());
    if (n == 0)
      return;

    available_ -= n;
    if ((n + slice_offset_) < slices_[slice_index_].size()) {
      slice_offset_ += n;
    } else {
      n -= slices_[slice_index_].size() - slice_offset_;
      slice_index_++;
      while (n > 0 && n >= slices_[slice_index_].size()) {
        n -= slices_[slice_index_].size();
        slice_index_++;
      }
      slice_offset_ = n;
    }
  }

  void Dump(faststring* buffer) {
    buffer->reserve(buffer->size() + TotalSize());
    for (const Slice& block : slices_) {
      buffer->append(block.data(), block.size());
    }
  }

 private:
  size_t TotalSize(void) const {
    size_t size = 0;
    for (const Slice& data : slices_) {
      size += data.size();
    }
    return size;
  }

 private:
  size_t available_;
  size_t slice_index_;
  size_t slice_offset_;
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
    source.Dump(&buffer);
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
          StringPrintf(
              "unable to uncompress the buffer. error near %d, buffer", -n),
          KUDU_REDACT(compressed.ToDebugString(1000)));
    }
    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return LZ4_compressBound(source_bytes);
  }

  Status SetCompressionLevel(int level) override {
    if (level < 0) {
      const std::string& msg = strings::Substitute(
          "Compression level $0 not supported by LZ4", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compression_level_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return LZ4;
  }
};

class Lz4DictCodec : public CompressionCodec {
 public:
  Lz4DictCodec() {
    compression_level_ = 1;
  }

  ~Lz4DictCodec() {
    LZ4F_freeCompressionContext(compression_ctx_);
    LZ4F_freeDecompressionContext(decompression_ctx_);
    LZ4F_freeCDict(dict_ctx_);
  }

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (!compression_ctx_ &&
        LZ4F_createCompressionContext(&compression_ctx_, LZ4F_VERSION)) {
      return Status::RuntimeError("Could not create LZ4 compression context");
    }

    const size_t max_comp_size = MaxCompressedLength(input.size());

    LZ4F_preferences_t prefs{};
    prefs.compressionLevel = compression_level_;
    prefs.frameInfo.dictID = CompressionCodecManager::GetDictionaryID(dict_);
    prefs.frameInfo.contentSize = input.size();

    size_t ret = LZ4F_compressFrame_usingCDict(
        compression_ctx_,
        compressed,
        max_comp_size,
        input.data(),
        input.size(),
        dict_ctx_,
        &prefs);

    if (LZ4F_isError(ret)) {
      return Status::Corruption(strings::Substitute(
          "Unable to compress the buffer: $0", LZ4F_getErrorName(ret)));
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
    source.Dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    if (!decompression_ctx_ &&
        LZ4F_createDecompressionContext(&decompression_ctx_, LZ4F_VERSION)) {
      return Status::RuntimeError("Could not create LZ4 decompression context");
    }

    size_t frame_info_size = compressed.size();

    LZ4F_frameInfo_t frame_info;
    size_t ret = LZ4F_getFrameInfo(
        decompression_ctx_, &frame_info, compressed.data(), &frame_info_size);
    if (LZ4F_isError(ret)) {
      LZ4F_resetDecompressionContext(decompression_ctx_);
      return Status::Corruption(strings::Substitute(
          "Could not extract LZ4 frame info: $0", LZ4F_getErrorName(ret)));
    }

    const unsigned actual_dict_id = frame_info.dictID;
    const unsigned expected_dict_id =
        CompressionCodecManager::GetDictionaryID(dict_);

    if (expected_dict_id != actual_dict_id) {
      return Status::CompressionDictMismatch("Dictionary ID mismatch");
    }

    LZ4F_decompressOptions_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.stableDst = 0;

    size_t compressed_size = compressed.size() - frame_info_size;
    const uint8_t* compressed_buf = compressed.data() + frame_info_size;

    ret = LZ4F_decompress_usingDict(
        decompression_ctx_,
        uncompressed,
        &uncompressed_length,
        compressed_buf,
        &compressed_size,
        dict_.data(),
        dict_.size(),
        &opts);
    if (LZ4F_isError(ret)) {
      LZ4F_resetDecompressionContext(decompression_ctx_);
      return Status::Corruption(strings::Substitute(
          "Unable to decompress the buffer: $0", LZ4F_getErrorName(ret)));
    }

    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return LZ4F_compressBound(source_bytes, nullptr) + LZ4F_HEADER_SIZE_MAX;
  }

  Status SetDictionary(const std::string& dict) override {
    dict_ = dict;
    dict_ctx_ = LZ4F_createCDict(dict_.data(), dict_.size());
    return Status::OK();
  }

  std::string GetDictionary() const override {
    return dict_;
  }

  Status SetCompressionLevel(int level) override {
    if (level < 0) {
      const std::string& msg = strings::Substitute(
          "Compression level $0 not supported by LZ4", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compression_level_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return LZ4_DICT;
  }

 private:
  LZ4F_cctx* compression_ctx_ = nullptr;
  LZ4F_dctx* decompression_ctx_ = nullptr;

  LZ4F_CDict* dict_ctx_ = nullptr;
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
    source.Dump(&buffer);
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
    compression_level_ = 1;
  }

  ~ZstdCodec() {}

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    const size_t max_comp_size = MaxCompressedLength(input.size());
    const auto ctx_ref = folly::compression::contexts::getZSTD_CCtx();
    auto* ctx = ctx_ref.get();
    const size_t ret = ZSTD_compressCCtx(
        ctx,
        compressed,
        max_comp_size,
        input.data(),
        input.size(),
        compression_level_);
    if (ZSTD_isError(ret)) {
      return Status::Corruption(strings::Substitute(
          "unable to compress the buffer: $0", ZSTD_getErrorName(ret)));
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
    source.Dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    const auto ctx_ref = folly::compression::contexts::getZSTD_DCtx();
    auto* ctx = ctx_ref.get();
    size_t ret = ZSTD_decompressDCtx(
        ctx,
        uncompressed,
        uncompressed_length,
        compressed.data(),
        compressed.size());
    if (ZSTD_isError(ret)) {
      return Status::Corruption(strings::Substitute(
          "unable to uncompress the buffer: $0", ZSTD_getErrorName(ret)));
    }
    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return ZSTD_compressBound(source_bytes);
  }

  Status SetCompressionLevel(int level) override {
    if (level < ZSTD_minCLevel() || level > ZSTD_maxCLevel()) {
      const std::string& msg = strings::Substitute(
          "Compression level $0 not supported by ZSTD", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compression_level_ = level;
    return Status::OK();
  }

  CompressionType type() const override {
    return ZSTD;
  }
};

class ZstdDictCodec : public CompressionCodec {
 public:
  ZstdDictCodec() {
    compression_level_ = 1;
    SetDictionary("");
  }

  ~ZstdDictCodec() {
    ZSTD_freeCDict(compression_dict_);
    ZSTD_freeDDict(decompression_dict_);
  }

  Status Compress(
      const Slice& input,
      uint8_t* compressed,
      size_t* compressed_length) override {
    if (!compression_dict_) {
      return Status::CompressionDictMismatch("Compression dictionary is empty");
    }

    auto ctx_ref = folly::compression::contexts::getZSTD_CCtx();
    auto ctx = ctx_ref.get();

    const size_t max_comp_size = MaxCompressedLength(input.size());
    const size_t ret = ZSTD_compress_usingCDict(
        ctx,
        compressed,
        max_comp_size,
        input.data(),
        input.size(),
        compression_dict_);

    if (ZSTD_isError(ret)) {
      return Status::Corruption(strings::Substitute(
          "unable to compress the buffer: $0", ZSTD_getErrorName(ret)));
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
    source.Dump(&buffer);
    return Compress(
        Slice(buffer.data(), buffer.size()), compressed, compressed_length);
  }

  Status Uncompress(
      const Slice& compressed,
      uint8_t* uncompressed,
      size_t uncompressed_length) override {
    if (!decompression_dict_) {
      return Status::CompressionDictMismatch("Compression dictionary is empty");
    }

    const unsigned expected_dict_id =
        CompressionCodecManager::GetDictionaryID(dict_);
    const unsigned actual_dict_id =
        ZSTD_getDictID_fromFrame(compressed.data(), compressed.size());

    if (expected_dict_id != actual_dict_id) {
      return Status::CompressionDictMismatch("Dictionary ID mismatch");
    }

    auto ctx_ref = folly::compression::contexts::getZSTD_DCtx();
    auto ctx = ctx_ref.get();

    size_t ret = ZSTD_decompress_usingDDict(
        ctx,
        uncompressed,
        uncompressed_length,
        compressed.data(),
        compressed.size(),
        decompression_dict_);
    if (ZSTD_isError(ret)) {
      return Status::Corruption(strings::Substitute(
          "unable to uncompress the buffer: $0", ZSTD_getErrorName(ret)));
    }

    return Status::OK();
  }

  size_t MaxCompressedLength(size_t source_bytes) const override {
    return ZSTD_compressBound(source_bytes);
  }

  Status SetDictionary(const std::string& dict) override {
    ZSTD_freeCDict(compression_dict_);
    ZSTD_freeDDict(decompression_dict_);

    dict_.clear();
    compression_dict_ = nullptr;
    decompression_dict_ = nullptr;

    compression_dict_ =
        ZSTD_createCDict(dict.c_str(), dict.size(), compression_level_);
    decompression_dict_ = ZSTD_createDDict(dict.c_str(), dict.size());

    if (!compression_dict_ || !decompression_dict_) {
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
      const std::string& msg = strings::Substitute(
          "Compression level $0 not supported by ZSTD", level);
      LOG(ERROR) << msg;
      return Status::NotSupported(msg);
    }
    compression_level_ = level;
    std::string dict = dict_;
    return SetDictionary(dict);
  }

  CompressionType type() const override {
    return ZSTD_DICT;
  }

 private:
  std::string dict_;

  ZSTD_CDict* compression_dict_ = nullptr;
  ZSTD_DDict* decompression_dict_ = nullptr;
};

std::string CompressionCodecManager::dictionary_;
std::shared_ptr<CompressionCodec> CompressionCodecManager::codec_;
int CompressionCodecManager::level_;

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
  if (codec_ && type == codec_->type()) {
    return Status::OK();
  }
  std::shared_ptr<CompressionCodec> codec = nullptr;
  // codec can be nullptr if type = NO_COMPRESSION
  RETURN_NOT_OK(GetCodec(type, &codec));
  if (codec) {
    RETURN_NOT_OK(codec->SetDictionary(dictionary_));
    if (!codec->SetCompressionLevel(level_).ok()) {
      int codec_level = codec->CompressionLevel();
      LOG(WARNING) << "Could not set compression level to " << level_ << ". "
                   << "Using the default compression level " << codec_level
                   << " instead";
      level_ = codec_level;
    }
  }
  codec_ = codec;
  LOG(INFO) << "Set compression codec to: "
            << GetCodecName(codec_ ? codec_->type() : NO_COMPRESSION);
  return Status::OK();
}

Status CompressionCodecManager::SetDictionary(const std::string& dict) {
  if (!codec_) {
    dictionary_ = dict;
    return Status::OK();
  }
  RETURN_NOT_OK(codec_->SetDictionary(dict));
  dictionary_ = dict;
  LOG(INFO) << "Updating compression dict to id " << GetCurrentDictionaryID();
  return Status::OK();
}

unsigned int CompressionCodecManager::GetCurrentDictionaryID() {
  return GetDictionaryID(dictionary_);
}

unsigned int CompressionCodecManager::GetDictionaryID(const std::string& dict) {
  // LZ4 also uses ZSTD dict format
  return ZSTD_getDictID_fromDict(dict.data(), dict.size());
}

Status CompressionCodecManager::SetCurrentCompressionLevel(int level) {
  if (!codec_) {
    level_ = level;
    return Status::OK();
  }
  RETURN_NOT_OK(codec_->SetCompressionLevel(level));
  level_ = level;
  return Status::OK();
}

} // namespace kudu
