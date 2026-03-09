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

#include "kudu/rpc/blocking_ops.h"

#include <cstdint>
#include <cstring>
#include <ostream>

#include <glog/logging.h>
#include <google/protobuf/message_lite.h>

#include <fmt/core.h>
#include "kudu/gutil/endian.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

using google::protobuf::MessageLite;

const char kHttpHeader[] = "HTTP";

Status checkInBlockingMode(const Socket* sock) {
  bool isNonblocking;
  RETURN_NOT_OK(sock->IsNonBlocking(&isNonblocking));
  if (isNonblocking) {
    static const char* const kErrMsg = "socket is not in blocking mode";
    LOG(DFATAL) << kErrMsg;
    return Status::IllegalState(kErrMsg);
  }
  return Status::OK();
}

Status sendFramedMessageBlocking(
    Socket* sock,
    const MessageLite& header,
    const MessageLite& msg,
    const MonoTime& deadline) {
  DCHECK(sock != nullptr);
  DCHECK(header.IsInitialized()) << "header protobuf must be initialized";
  DCHECK(msg.IsInitialized()) << "msg protobuf must be initialized";

  // Ensure we are in blocking mode.
  // These blocking calls are typically not in the fast path, so doing this for
  // all build types.
  RETURN_NOT_OK(checkInBlockingMode(sock));

  // Serialize message
  faststring paramBuf;
  serialization::SerializeMessage(msg, &paramBuf);

  // Serialize header and initial length
  faststring headerBuf;
  serialization::SerializeHeader(header, paramBuf.size(), &headerBuf);

  // Write header & param to stream
  size_t nsent;
  RETURN_NOT_OK(sock->BlockingWrite(
      headerBuf.data(), headerBuf.size(), &nsent, deadline));
  RETURN_NOT_OK(
      sock->BlockingWrite(paramBuf.data(), paramBuf.size(), &nsent, deadline));

  return Status::OK();
}

Status receiveFramedMessageBlocking(
    Socket* sock,
    faststring* recvBuf,
    MessageLite* header,
    Slice* paramBuf,
    const MonoTime& deadline) {
  DCHECK(sock != nullptr);
  DCHECK(recvBuf != nullptr);
  DCHECK(header != nullptr);
  DCHECK(paramBuf != nullptr);

  RETURN_NOT_OK(checkInBlockingMode(sock));

  // Read the message prefix, which specifies the length of the payload.
  recvBuf->clear();
  recvBuf->resize(kMsgLengthPrefixLength);
  size_t recvd = 0;
  RETURN_NOT_OK(sock->BlockingRecv(
      recvBuf->data(), kMsgLengthPrefixLength, &recvd, deadline));
  uint32_t payloadLen = NetworkByteOrder::load32(recvBuf->data());

  // Verify that the payload size isn't out of bounds.
  // This can happen because of network corruption, or a naughty client.
  if (PREDICT_FALSE(payloadLen > FLAGS_rpc_max_message_size)) {
    // A common user mistake is to try to speak the Kudu RPC protocol to an
    // HTTP endpoint, or vice versa.
    if (memcmp(recvBuf->data(), kHttpHeader, strlen(kHttpHeader)) == 0) {
      return Status::IOError(
          "received invalid RPC message which appears to be an HTTP response. "
          "Verify that you have specified a valid RPC port and not an HTTP port.");
    }

    return Status::IOError(
        fmt::format(
            "received invalid message of size {} which exceeds"
            " the rpc_max_message_size of {} bytes",
            payloadLen,
            FLAGS_rpc_max_message_size));
  }

  // Read the message payload.
  recvd = 0;
  recvBuf->resize(payloadLen + kMsgLengthPrefixLength);
  RETURN_NOT_OK(sock->BlockingRecv(
      recvBuf->data() + kMsgLengthPrefixLength, payloadLen, &recvd, deadline));
  RETURN_NOT_OK(serialization::ParseMessage(Slice(*recvBuf), header, paramBuf));
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
