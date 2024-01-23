#pragma once

#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/faststring.h"

namespace kudu::consensus {

/**
 * Thin wrapper to handle compression/decompression of replicate msg
 *
 * Pass any msg (compressed or uncompressed) to the constructor and then call
 * Init() to populate both the compressed and uncompressed msgs.
 */
class ReplicateMsgWrapper {
 public:
  explicit ReplicateMsgWrapper(
      const ReplicateRefPtr& msg,
      const bool should_compress = true) {
    orig_msg_ = msg;
    auto codec_hint = CompressionCodecManager::GetCurrentCodec();
    const CompressionType msg_codec_type =
        orig_msg_->get()->write_payload().compression_codec();
    if (msg_codec_type == NO_COMPRESSION) {
      msg_ = orig_msg_;
      codec_ = codec_hint;
      should_compress_ = should_compress &&
          msg_->get()->op_type() == WRITE_OP_EXT && codec_ != nullptr;
    } else {
      compressed_msg_ = orig_msg_;
      CHECK_OK(CompressionCodecManager::SetCurrentCodec(msg_codec_type));
      codec_ = CompressionCodecManager::GetCurrentCodec();
    }
    DCHECK(msg_ || compressed_msg_);
  }

  /**
   * Tries to populate both compressed and uncompressed msgs
   *
   * The msg passed to the ctor is either a compressed msg or an uncompressed
   * msg. In this method we'll uncompress the compressed msg or compress the
   * uncompressed msg.
   *
   * @param compression_buffer Buffer to use for compression/uncompression
   *
   * @return    Status::OK() if everthing is good, error otherwise
   */
  Status Init(faststring* compression_buffer) {
    if (!msg_ && !compressed_msg_) {
      return Status::IllegalState(
          "Both compressed and uncompressed msg are not populated!");
    }
    if (!compression_buffer) {
      compression_buffer_ = std::make_shared<faststring>();
      compression_buffer = compression_buffer_.get();
    }
    if (compressed_msg_ && !msg_) {
      return UncompressMsg(compression_buffer);
    }
    if (should_compress_ && msg_ && !compressed_msg_) {
      return CompressMsg(compression_buffer);
    }
    return Status::OK();
  }

  /** Returns the msg that was originally passed to the ctor **/
  ReplicateRefPtr GetOrigMsg() const {
    return orig_msg_;
  }

  /** Returns the uncompressed msg **/
  ReplicateRefPtr GetUncompressedMsg() const {
    return msg_;
  }

  /** Returns the compressed msg **/
  ReplicateRefPtr GetCompressedMsg() const {
    return compressed_msg_;
  }

  std::shared_ptr<CompressionCodec> GetCodec() const {
    return codec_;
  }

 private:
  Status UncompressMsg(faststring* buffer) {
    DCHECK(!msg_ && compressed_msg_);
    DCHECK(buffer);

    if (!codec_) {
      return Status::IllegalState(
          "Codec not populated while uncompressing msg");
    }

    const OperationType op_type = compressed_msg_->get()->op_type();
    const WritePayloadPB& payload = compressed_msg_->get()->write_payload();
    const CompressionType compression_codec = payload.compression_codec();
    const int64_t uncompressed_size = payload.uncompressed_size();
    const int64_t compressed_size = payload.payload().size();

    DCHECK(codec_->type() == compression_codec);

    // Resize buffer to hold uncompressed payload.
    // TODO: needs perf testing and maybe implement streaming (un)compression
    buffer->resize(uncompressed_size);

    VLOG(2) << "Uncompressing message"
            << " opid: " << compressed_msg_->get()->id().ShortDebugString()
            << " codec: " << compression_codec << " op_type: " << op_type
            << " compressed payload size: " << compressed_size
            << " uncompressed payload size: " << uncompressed_size;

    Slice compressed_slice(payload.payload().c_str(), compressed_size);

    Status status = codec_->UncompressWithStats(
        compressed_slice, buffer->data(), uncompressed_size);

    // Return early if uncompression failed
    RETURN_NOT_OK_PREPEND(
        status,
        strings::Substitute(
            "Failed to uncompress OpId $0. Compression codec used: $1, "
            "Operation type: $2, Compressed payload size: $3 "
            "Uncompressed payload size: $4",
            compressed_msg_->get()->id().ShortDebugString(),
            compression_codec,
            op_type,
            compressed_size,
            uncompressed_size));

    // Now create a new ReplicateMsg and copy over the contents from the
    // original msg and the uncompressed payload
    std::unique_ptr<ReplicateMsg> rep_msg(new ReplicateMsg);
    *(rep_msg->mutable_id()) = compressed_msg_->get()->id();
    rep_msg->set_timestamp(compressed_msg_->get()->timestamp());
    rep_msg->set_op_type(compressed_msg_->get()->op_type());

    WritePayloadPB* write_payload = rep_msg->mutable_write_payload();
    write_payload->set_payload(buffer->ToString());

    msg_ = make_scoped_refptr_replicate(rep_msg.release());
    return Status::OK();
  }

  Status CompressMsg(faststring* buffer) {
    DCHECK(msg_ && !compressed_msg_);
    DCHECK(buffer);

    if (!should_compress_) {
      return Status::OK();
    }

    if (!codec_) {
      return Status::IllegalState("Codec not populated while compressing msg");
    }

    // Grab the reference to the payload that needs to be compressed
    const std::string& payload_str = msg_->get()->write_payload().payload();
    DCHECK(msg_->get()->write_payload().compression_codec() == NO_COMPRESSION);

    Slice uncompressed_slice(payload_str.c_str(), payload_str.size());

    // Resize buffer to hold max possible compressed payload size
    // TODO: Needs perf testing and maybe add support for streaming compression
    buffer->resize(codec_->MaxCompressedLength(uncompressed_slice.size()));

    size_t compressed_len = 0;
    auto status = codec_->CompressWithStats(
        uncompressed_slice,
        reinterpret_cast<unsigned char*>(buffer->data()),
        &compressed_len);

    if (!status.ok()) {
      LOG(ERROR) << "Compression failed for OpId: "
                 << msg_->get()->id().ShortDebugString();
      return status;
    }

    // Resize buffer to the actual compressed length
    buffer->resize(compressed_len);
    VLOG(2) << "Compressed OpId: " << msg_->get()->id().ShortDebugString()
            << " original payload size: " << uncompressed_slice.size()
            << " compressed payload size: " << compressed_len;

    // Now create a new replicate message and copy contents from original
    // message and compressed payload
    std::unique_ptr<ReplicateMsg> rep_msg(new ReplicateMsg);
    *(rep_msg->mutable_id()) = msg_->get()->id();
    rep_msg->set_timestamp(msg_->get()->timestamp());
    rep_msg->set_op_type(msg_->get()->op_type());

    WritePayloadPB* write_payload = rep_msg->mutable_write_payload();
    write_payload->set_payload(buffer->ToString());
    write_payload->set_compression_codec(codec_->type());
    write_payload->set_uncompressed_size(payload_str.size());

    compressed_msg_ = make_scoped_refptr_replicate(rep_msg.release());
    return Status::OK();
  }

  // The original replicate that's passed in to the wrapper
  ReplicateRefPtr orig_msg_ = nullptr;
  // The uncompressed replicate
  ReplicateRefPtr msg_ = nullptr;
  // The compressed replicate
  ReplicateRefPtr compressed_msg_ = nullptr;
  // Should we compress?
  bool should_compress_ = false;
  // The compression codec to use
  std::shared_ptr<CompressionCodec> codec_ = nullptr;
  // Buffer used for compression if user hasn't provided one
  std::shared_ptr<faststring> compression_buffer_;
};

} // namespace kudu::consensus
