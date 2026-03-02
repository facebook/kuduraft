#pragma once

#include <fmt/core.h>
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/faststring.h"

namespace kudu::consensus {

/**
 * Thin wrapper to handle compression/decompression of replicate msg
 *
 * Pass any msg (compressed or uncompressed) to the constructor and then call
 * init() to populate both the compressed and uncompressed msgs.
 */
class ReplicateMsgWrapper {
 public:
  explicit ReplicateMsgWrapper(
      const ReplicateRefPtr& msg,
      const bool shouldCompress = true) {
    origMsg_ = msg;
    auto codecHint = CompressionCodecManager::getCurrentCodec();
    const CompressionType msgCodecType =
        origMsg_->get()->write_payload().compression_codec();
    if (msgCodecType == NO_COMPRESSION) {
      msg_ = origMsg_;
      codec_ = codecHint;
      shouldCompress_ = shouldCompress &&
          msg_->get()->op_type() == WRITE_OP_EXT && codec_ != nullptr;
    } else {
      compressedMsg_ = origMsg_;
      CHECK_OK(CompressionCodecManager::setCurrentCodec(msgCodecType));
      codec_ = CompressionCodecManager::getCurrentCodec();
    }
    DCHECK(msg_ || compressedMsg_);
  }

  /**
   * Tries to populate both compressed and uncompressed msgs
   *
   * The msg passed to the ctor is either a compressed msg or an uncompressed
   * msg. In this method we'll uncompress the compressed msg or compress the
   * uncompressed msg.
   *
   * @param compressionBuffer Buffer to use for compression/uncompression
   *
   * @return    Status::OK() if everthing is good, error otherwise
   */
  Status init(faststring* compressionBuffer) {
    if (!msg_ && !compressedMsg_) {
      return Status::IllegalState(
          "Both compressed and uncompressed msg are not populated!");
    }
    if (!compressionBuffer) {
      compressionBuffer_ = std::make_shared<faststring>();
      compressionBuffer = compressionBuffer_.get();
    }
    if (compressedMsg_ && !msg_) {
      return uncompressMsg(compressionBuffer);
    }
    if (shouldCompress_ && msg_ && !compressedMsg_) {
      return compressMsg(compressionBuffer);
    }
    return Status::OK();
  }

  /** Returns the msg that was originally passed to the ctor **/
  ReplicateRefPtr getOrigMsg() const {
    return origMsg_;
  }

  /** Returns the uncompressed msg **/
  ReplicateRefPtr getUncompressedMsg() const {
    return msg_;
  }

  /** Returns the compressed msg **/
  ReplicateRefPtr getCompressedMsg() const {
    return compressedMsg_;
  }

  std::shared_ptr<CompressionCodec> getCodec() const {
    return codec_;
  }

 private:
  Status uncompressMsg(faststring* buffer) {
    DCHECK(!msg_ && compressedMsg_);
    DCHECK(buffer);

    if (!codec_) {
      return Status::IllegalState(
          "Codec not populated while uncompressing msg");
    }

    const OperationType opType = compressedMsg_->get()->op_type();
    const WritePayloadPB& payload = compressedMsg_->get()->write_payload();
    const CompressionType compressionCodec = payload.compression_codec();
    const int64_t uncompressedSize = payload.uncompressed_size();
    const int64_t compressedSize = payload.payload().size();

    DCHECK(codec_->type() == compressionCodec);

    // Resize buffer to hold uncompressed payload.
    // TODO: needs perf testing and maybe implement streaming (un)compression
    buffer->resize(uncompressedSize);

    VLOG(2) << "Uncompressing message"
            << " opid: " << compressedMsg_->get()->id().ShortDebugString()
            << " codec: " << compressionCodec << " op_type: " << opType
            << " compressed payload size: " << compressedSize
            << " uncompressed payload size: " << uncompressedSize;

    Slice compressedSlice(payload.payload().c_str(), compressedSize);

    Status status = codec_->uncompressWithStats(
        compressedSlice, buffer->data(), uncompressedSize);

    // Return early if uncompression failed
    RETURN_NOT_OK_PREPEND(
        status,
        fmt::format(
            "Failed to uncompress OpId {}. Compression codec used: {}, "
            "Operation type: {}, Compressed payload size: {} "
            "Uncompressed payload size: {}",
            compressedMsg_->get()->id().ShortDebugString(),
            compressionCodec,
            opType,
            compressedSize,
            uncompressedSize));

    // Now create a new ReplicateMsg and copy over the contents from the
    // original msg and the uncompressed payload
    std::unique_ptr<ReplicateMsg> repMsg(new ReplicateMsg);
    *(repMsg->mutable_id()) = compressedMsg_->get()->id();
    repMsg->set_timestamp(compressedMsg_->get()->timestamp());
    repMsg->set_op_type(compressedMsg_->get()->op_type());

    WritePayloadPB* writePayload = repMsg->mutable_write_payload();
    writePayload->set_payload(buffer->ToString());

    msg_ =
        makeScopedRefptrReplicate(repMsg.release(), compressedMsg_->source());
    return Status::OK();
  }

  Status compressMsg(faststring* buffer) {
    DCHECK(msg_ && !compressedMsg_);
    DCHECK(buffer);

    if (!shouldCompress_) {
      return Status::OK();
    }

    if (!codec_) {
      return Status::IllegalState("Codec not populated while compressing msg");
    }

    // Grab the reference to the payload that needs to be compressed
    const std::string& payloadStr = msg_->get()->write_payload().payload();
    DCHECK(msg_->get()->write_payload().compression_codec() == NO_COMPRESSION);

    Slice uncompressedSlice(payloadStr.c_str(), payloadStr.size());

    // Resize buffer to hold max possible compressed payload size
    // TODO: Needs perf testing and maybe add support for streaming compression
    buffer->resize(codec_->maxCompressedLength(uncompressedSlice.size()));

    size_t compressedLen = 0;
    auto status = codec_->compressWithStats(
        uncompressedSlice,
        reinterpret_cast<unsigned char*>(buffer->data()),
        &compressedLen);

    if (!status.ok()) {
      LOG(ERROR) << "Compression failed for OpId: "
                 << msg_->get()->id().ShortDebugString();
      return status;
    }

    // Resize buffer to the actual compressed length
    buffer->resize(compressedLen);
    VLOG(2) << "Compressed OpId: " << msg_->get()->id().ShortDebugString()
            << " original payload size: " << uncompressedSlice.size()
            << " compressed payload size: " << compressedLen;

    // Now create a new replicate message and copy contents from original
    // message and compressed payload
    std::unique_ptr<ReplicateMsg> repMsg(new ReplicateMsg);
    *(repMsg->mutable_id()) = msg_->get()->id();
    repMsg->set_timestamp(msg_->get()->timestamp());
    repMsg->set_op_type(msg_->get()->op_type());

    WritePayloadPB* writePayload = repMsg->mutable_write_payload();
    writePayload->set_payload(buffer->ToString());
    writePayload->set_compression_codec(codec_->type());
    writePayload->set_uncompressed_size(payloadStr.size());

    compressedMsg_ =
        makeScopedRefptrReplicate(repMsg.release(), msg_->source());
    return Status::OK();
  }

  // The original replicate that's passed in to the wrapper
  ReplicateRefPtr origMsg_ = nullptr;
  // The uncompressed replicate
  ReplicateRefPtr msg_ = nullptr;
  // The compressed replicate
  ReplicateRefPtr compressedMsg_ = nullptr;
  // Should we compress?
  bool shouldCompress_ = false;
  // The compression codec to use
  std::shared_ptr<CompressionCodec> codec_ = nullptr;
  // Buffer used for compression if user hasn't provided one
  std::shared_ptr<faststring> compressionBuffer_;
};

} // namespace kudu::consensus
