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

#include "kudu/rpc/transfer.h"

#include <sys/uio.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/endian.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/constants.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/socket.h"

DEFINE_int64(
    rpc_max_message_size,
    (600 * 1024 * 1024),
    "The maximum size of a message that any RPC that the server will accept. "
    "Must be at least 1MB.");
TAG_FLAG(rpc_max_message_size, advanced);
TAG_FLAG(rpc_max_message_size, runtime);

DEFINE_int64(
    rpc_long_message_size,
    (80 * 1024 * 1024),
    "The size of a message that we consider a 'long' message, and will inform "
    "the services about before and after loading it");
TAG_FLAG(rpc_long_message_size, advanced);
TAG_FLAG(rpc_long_message_size, runtime);

static bool ValidateMaxMessageSize(const char* flagname, int64_t value) {
  if (value < 1 * 1024 * 1024) {
    LOG(ERROR) << flagname << " must be at least 1MB.";
    return false;
  }
  if (value > std::numeric_limits<int32_t>::max()) {
    LOG(ERROR) << flagname << " must be less than "
               << std::numeric_limits<int32_t>::max() << " bytes.";
  }

  return true;
}
static bool dummy = gflags::RegisterFlagValidator(
    &FLAGS_rpc_max_message_size,
    &ValidateMaxMessageSize);

namespace kudu {
namespace rpc {

using std::string;

#define RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status)         \
  do {                                                      \
    Status _s = (status);                                   \
    if (PREDICT_FALSE(!_s.ok())) {                          \
      if (Socket::IsTemporarySocketError(_s.posixCode())) { \
        return Status::OK(); /* EAGAIN, etc. */             \
      }                                                     \
      return _s;                                            \
    }                                                       \
  } while (0)

TransferCallbacks::~TransferCallbacks() {}

InboundTransfer::InboundTransfer()
    : totalLength_(kMsgLengthPrefixLength), curOffset_(0) {
  buf_.resize(kMsgLengthPrefixLength);
}

Status InboundTransfer::receiveBuffer(Socket& socket) {
  if (curOffset_ < kMsgLengthPrefixLength) {
    // receive uint32 length prefix
    int32_t rem = kMsgLengthPrefixLength - curOffset_;
    int32_t nread;
    Status status = socket.Recv(&buf_[curOffset_], rem, &nread);
    RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);
    if (nread == 0) {
      return Status::OK();
    }
    DCHECK_GE(nread, 0);
    curOffset_ += nread;
    if (curOffset_ < kMsgLengthPrefixLength) {
      // If we still don't have the full length prefix, we can't continue
      // reading yet.
      return Status::OK();
    }
    // Since we only read 'rem' bytes above, we should now have exactly
    // the length prefix in our buffer and no more.
    DCHECK_EQ(curOffset_, kMsgLengthPrefixLength);

    // The length prefix doesn't include its own 4 bytes, so we have to
    // add that back in.
    totalLength_ = NetworkByteOrder::load32(&buf_[0]) + kMsgLengthPrefixLength;
    if (totalLength_ > FLAGS_rpc_max_message_size) {
      return Status::NetworkError(
          fmt::format(
              "RPC frame had a length of {}, but we only support messages up to {} bytes "
              "long.",
              totalLength_,
              FLAGS_rpc_max_message_size));
    }
    if (totalLength_ <= kMsgLengthPrefixLength) {
      return Status::NetworkError(
          fmt::format("RPC frame had invalid length of {}", totalLength_));
    }
    buf_.resize(totalLength_);

    // Fall through to receive the message body, which is likely to be already
    // available on the socket.
  }

  // receive message body
  int32_t nread;

  // Socket::Recv() handles at most INT_MAX at a time, so cap the remainder at
  // INT_MAX. The message will be split across multiple Recv() calls.
  // Note that this is only needed when rpc_max_message_size > INT_MAX, which is
  // currently only used for unit tests.
  int32_t rem = std::min(
      totalLength_ - curOffset_,
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));
  Status status = socket.Recv(&buf_[curOffset_], rem, &nread);
  RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);
  curOffset_ += nread;

  return Status::OK();
}

bool InboundTransfer::transferStarted() const {
  return curOffset_ != 0;
}

bool InboundTransfer::transferFinished() const {
  return curOffset_ == totalLength_;
}

string InboundTransfer::statusAsString() const {
  return fmt::format("{}/{} bytes received", curOffset_, totalLength_);
}

bool InboundTransfer::isLongTransfer() const {
  return totalLength_ > FLAGS_rpc_long_message_size;
}

OutboundTransfer* OutboundTransfer::createForCallRequest(
    int32_t callId,
    const TransferPayload& payload,
    size_t nPayloadSlices,
    TransferCallbacks* callbacks) {
  return new OutboundTransfer(callId, payload, nPayloadSlices, callbacks);
}

OutboundTransfer* OutboundTransfer::createForCallResponse(
    const TransferPayload& payload,
    size_t nPayloadSlices,
    TransferCallbacks* callbacks) {
  return new OutboundTransfer(
      kInvalidCallId, payload, nPayloadSlices, callbacks);
}

OutboundTransfer::OutboundTransfer(
    int32_t callId,
    const TransferPayload& payload,
    size_t nPayloadSlices,
    TransferCallbacks* callbacks)
    : curSliceIdx_(0),
      curOffsetInSlice_(0),
      callbacks_(callbacks),
      callId_(callId),
      started_(false),
      aborted_(false) {
  nPayloadSlices_ = nPayloadSlices;
  CHECK_LE(nPayloadSlices_, payloadSlices_.size());
  for (int i = 0; i < nPayloadSlices; i++) {
    payloadSlices_[i] = payload[i];
  }
}

OutboundTransfer::~OutboundTransfer() {
  if (!transferFinished() && !aborted_) {
    callbacks_->notifyTransferAborted(
        Status::RuntimeError(
            "RPC transfer destroyed before it finished sending"));
  }
}

void OutboundTransfer::abort(const Status& status) {
  CHECK(!aborted_) << "Already aborted";
  CHECK(!transferFinished()) << "Cannot abort a finished transfer";
  callbacks_->notifyTransferAborted(status);
  aborted_ = true;
}

Status OutboundTransfer::sendBuffer(Socket& socket) {
  CHECK_LT(curSliceIdx_, nPayloadSlices_);

  started_ = true;
  int nIovecs = nPayloadSlices_ - curSliceIdx_;
  struct iovec iovec[nIovecs];
  {
    int offsetInSlice = curOffsetInSlice_;
    for (int i = 0; i < nIovecs; i++) {
      Slice& slice = payloadSlices_[curSliceIdx_ + i];
      iovec[i].iov_base = slice.mutableData() + offsetInSlice;
      iovec[i].iov_len = slice.size() - offsetInSlice;

      offsetInSlice = 0;
    }
  }

  int64_t written;
  Status status = socket.Writev(iovec, nIovecs, &written);
  RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);

  // Adjust our accounting of current writer position.
  for (int i = curSliceIdx_; i < nPayloadSlices_; i++) {
    Slice& slice = payloadSlices_[i];
    int remInSlice = slice.size() - curOffsetInSlice_;
    DCHECK_GE(remInSlice, 0);

    if (written >= remInSlice) {
      // Used up this entire slice, advance to the next slice.
      curSliceIdx_++;
      curOffsetInSlice_ = 0;
      written -= remInSlice;
    } else {
      // Partially used up this slice, just advance the offset within it.
      curOffsetInSlice_ += written;
      break;
    }
  }

  if (curSliceIdx_ == nPayloadSlices_) {
    callbacks_->notifyTransferFinished();
    DCHECK_EQ(0, curOffsetInSlice_);
  } else {
    DCHECK_LT(curSliceIdx_, nPayloadSlices_);
    DCHECK_LT(curOffsetInSlice_, payloadSlices_[curSliceIdx_].size());
  }

  return Status::OK();
}

bool OutboundTransfer::transferStarted() const {
  return started_;
}

bool OutboundTransfer::transferFinished() const {
  if (curSliceIdx_ == nPayloadSlices_) {
    DCHECK_EQ(0, curOffsetInSlice_); // sanity check
    return true;
  }
  return false;
}

string OutboundTransfer::hexDump() const {
  if (KUDU_SHOULD_REDACT()) {
    return kRedactionMessage;
  }

  string ret;
  for (int i = 0; i < nPayloadSlices_; i++) {
    ret.append(payloadSlices_[i].ToDebugString());
  }
  return ret;
}

int32_t OutboundTransfer::totalLength() const {
  int32_t ret = 0;
  for (int i = 0; i < nPayloadSlices_; i++) {
    ret += payloadSlices_[i].size();
  }
  return ret;
}

} // namespace rpc
} // namespace kudu
