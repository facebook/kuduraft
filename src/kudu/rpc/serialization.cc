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

#include "kudu/rpc/serialization.h"

#include <limits>
#include <ostream>
#include <string>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message_lite.h>

#include <fmt/core.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/constants.h"
#include "kudu/util/faststring.h"
#include "kudu/util/logging.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DECLARE_int64(rpc_max_message_size);

using google::protobuf::MessageLite;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

namespace kudu {
namespace rpc {
namespace serialization {

enum {
  kHeaderPosVersion = 0,
  kHeaderPosServiceClass = 1,
  kHeaderPosAuthProto = 2
};

void serializeMessage(
    const MessageLite& message,
    faststring* paramBuf,
    int additionalSize,
    bool useCachedSize) {
  DCHECK_GE(additionalSize, 0);
  int pbSize = useCachedSize ? message.GetCachedSize() : message.ByteSize();
  DCHECK_EQ(message.ByteSize(), pbSize);
  // Use 8-byte integers to avoid overflowing when additionalSize approaches
  // INT_MAX.
  int64_t recordedSize =
      static_cast<int64_t>(pbSize) + static_cast<int64_t>(additionalSize);
  int64_t sizeWithDelim = static_cast<int64_t>(pbSize) +
      static_cast<int64_t>(CodedOutputStream::VarintSize32(recordedSize));
  int64_t totalSize = sizeWithDelim + static_cast<int64_t>(additionalSize);
  // The message format relies on an unsigned 32-bit integer to express the
  // size, so the message must not exceed this size. Since additionalSize is
  // limited to INT_MAX, this is a safe limitation.
  CHECK_LE(totalSize, std::numeric_limits<uint32_t>::max());

  if (totalSize > FLAGS_rpc_max_message_size) {
    LOG(WARNING) << fmt::format(
        "Serialized {} ({} bytes) is larger than the maximum configured "
        "RPC message size ({} bytes). "
        "Sending anyway, but peer may reject the data.",
        message.GetTypeName(),
        totalSize,
        FLAGS_rpc_max_message_size);
  }

  paramBuf->resize(sizeWithDelim);
  uint8_t* dst = paramBuf->data();
  dst = CodedOutputStream::WriteVarint32ToArray(recordedSize, dst);
  dst = message.SerializeWithCachedSizesToArray(dst);
  CHECK_EQ(dst, paramBuf->data() + sizeWithDelim);
}

void serializeHeader(
    const MessageLite& header,
    size_t paramLen,
    faststring* headerBuf) {
  CHECK(header.IsInitialized())
      << "RPC header missing fields: " << header.InitializationErrorString();

  // Compute all the lengths for the packet.
  size_t headerPbLen = header.ByteSize();
  size_t headerTotLen =
      kMsgLengthPrefixLength // Int prefix for the total length.
      + CodedOutputStream::VarintSize32(
            headerPbLen) // Varint delimiter for header PB.
      + headerPbLen; // Length for the header PB itself.
  size_t totalSize = headerTotLen + paramLen;

  headerBuf->resize(headerTotLen);
  uint8_t* dst = headerBuf->data();

  // 1. The length for the whole request, not including the 4-byte
  // length prefix.
  NetworkByteOrder::store32(dst, totalSize - kMsgLengthPrefixLength);
  dst += sizeof(uint32_t);

  // 2. The varint-prefixed RequestHeader PB
  dst = CodedOutputStream::WriteVarint32ToArray(headerPbLen, dst);
  dst = header.SerializeWithCachedSizesToArray(dst);

  // We should have used the whole buffer we allocated.
  CHECK_EQ(dst, headerBuf->data() + headerTotLen);
}

Status parseTotalLength(const Slice& buf, uint32_t* totalLen) {
  if (PREDICT_FALSE(buf.size() < kMsgLengthPrefixLength)) {
    return Status::Corruption(
        "Invalid packet: not enough bytes for length header",
        KUDU_REDACT(buf.toDebugString()));
  }

  *totalLen = NetworkByteOrder::load32(buf.data());
  return Status::OK();
}

Status
parseHeader(const Slice& buf, CodedInputStream& in, MessageLite* parsedHeader) {
  uint32_t headerLen;
  if (PREDICT_FALSE(!in.ReadVarint32(&headerLen))) {
    return Status::Corruption(
        "Invalid packet: missing header delimiter",
        KUDU_REDACT(buf.toDebugString()));
  }

  CodedInputStream::Limit l;
  l = in.PushLimit(headerLen);
  if (PREDICT_FALSE(!parsedHeader->ParseFromCodedStream(&in))) {
    return Status::Corruption(
        "Invalid packet: header too short", KUDU_REDACT(buf.toDebugString()));
  }
  in.PopLimit(l);
  return Status::OK();
}

Status tryParseRpcHeader(
    const Slice& buf,
    uint32_t* totalLen,
    google::protobuf::MessageLite* parsedHeader) {
  RETURN_NOT_OK(parseTotalLength(buf, totalLen));
  CodedInputStream in(buf.data(), buf.size());
  // Protobuf enforces a 64MB total bytes limit on CodedInputStream by default.
  // Override this default with the actual size of the buffer to allow messages
  // larger than 64MB.
  in.SetTotalBytesLimit(buf.size());
  in.Skip(kMsgLengthPrefixLength);

  RETURN_NOT_OK(parseHeader(buf, in, parsedHeader));
  return Status::OK();
}

Status parseMessage(
    const Slice& buf,
    MessageLite* parsedHeader,
    Slice* parsedMainMessage) {
  // First grab the total length
  uint32_t totalLen;
  RETURN_NOT_OK(parseTotalLength(buf, &totalLen));
  DCHECK_EQ(totalLen, buf.size() - kMsgLengthPrefixLength)
      << "Got mis-sized buffer: " << KUDU_REDACT(buf.toDebugString());

  if (totalLen > std::numeric_limits<int32_t>::max()) {
    return Status::Corruption(
        fmt::format(
            "Invalid packet: message had a length of {},"
            "but we only support messages up to {} bytes\n",
            totalLen,
            std::numeric_limits<int32_t>::max()));
  }

  CodedInputStream in(buf.data(), buf.size());
  // Protobuf enforces a 64MB total bytes limit on CodedInputStream by default.
  // Override this default with the actual size of the buffer to allow messages
  // larger than 64MB.
  in.SetTotalBytesLimit(buf.size());
  in.Skip(kMsgLengthPrefixLength);

  RETURN_NOT_OK(parseHeader(buf, in, parsedHeader));

  uint32_t mainMsgLen;
  if (PREDICT_FALSE(!in.ReadVarint32(&mainMsgLen))) {
    return Status::Corruption(
        "Invalid packet: missing main msg length",
        KUDU_REDACT(buf.toDebugString()));
  }

  if (PREDICT_FALSE(!in.Skip(mainMsgLen))) {
    return Status::Corruption(
        fmt::format(
            "Invalid packet: data too short, expected {} byte main_msg",
            mainMsgLen),
        KUDU_REDACT(buf.toDebugString()));
  }

  if (PREDICT_FALSE(in.BytesUntilLimit() > 0)) {
    return Status::Corruption(
        fmt::format(
            "Invalid packet: {} extra bytes at end of packet",
            in.BytesUntilLimit()),
        KUDU_REDACT(buf.toDebugString()));
  }

  *parsedMainMessage = Slice(buf.data() + buf.size() - mainMsgLen, mainMsgLen);
  return Status::OK();
}

void serializeConnHeader(uint8_t* buf) {
  memcpy(reinterpret_cast<char*>(buf), kMagicNumber, kMagicNumberLength);
  buf += kMagicNumberLength;
  buf[kHeaderPosVersion] = kCurrentRpcVersion;
  buf[kHeaderPosServiceClass] = 0; // TODO: implement
  buf[kHeaderPosAuthProto] = 0; // TODO: implement
}

// validate the entire rpc header (magic number + flags)
Status validateConnHeader(const Slice& slice) {
  DCHECK_EQ(kMagicNumberLength + kHeaderFlagsLength, slice.size())
      << "Invalid RPC header length";

  // validate actual magic
  if (!slice.startsWith(kMagicNumber)) {
    if (slice.startsWith("GET ") || slice.startsWith("POST") ||
        slice.startsWith("HEAD")) {
      return Status::InvalidArgument(
          "invalid negotation, appears to be an HTTP client on "
          "the RPC port");
    }
    return Status::InvalidArgument(
        "connection must begin with magic number", kMagicNumber);
  }

  const uint8_t* data = slice.data();
  data += kMagicNumberLength;

  // validate version
  if (data[kHeaderPosVersion] != kCurrentRpcVersion) {
    return Status::InvalidArgument(
        "Unsupported RPC version",
        fmt::format(
            "Received: {}, Supported: {}",
            data[kHeaderPosVersion],
            kCurrentRpcVersion));
  }

  // TODO: validate additional header flags:
  // RPC_SERVICE_CLASS
  // RPC_AUTH_PROTOCOL

  return Status::OK();
}

} // namespace serialization
} // namespace rpc
} // namespace kudu
