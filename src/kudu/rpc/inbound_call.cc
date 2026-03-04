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

#include "kudu/rpc/inbound_call.h"

#include <cstdint>
#include <memory>
#include <ostream>

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>

#include <fmt/core.h>
#include "kudu/gutil/port.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/trace.h"

namespace google {
namespace protobuf {
class FieldDescriptor;
}
} // namespace google

using google::protobuf::FieldDescriptor;
using google::protobuf::MessageLite;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

InboundCall::InboundCall(std::shared_ptr<Connection> conn)
    : conn_(std::move(conn)),
      trace_(std::make_shared<Trace>()),
      methodInfo_(nullptr),
      deadline_(MonoTime::Max()) {
  recordCallReceived();
}

InboundCall::~InboundCall() {}

Status InboundCall::parseFrom(unique_ptr<InboundTransfer> transfer) {
  TRACE_EVENT_FLOW_BEGIN0("rpc", "InboundCall", this);
  TRACE_EVENT0("rpc", "InboundCall::parseFrom");
  RETURN_NOT_OK(
      serialization::ParseMessage(
          transfer->data(), &header_, &serializedRequest_));

  // Adopt the service/method info from the header as soon as it's available.
  if (PREDICT_FALSE(!header_.has_remote_method())) {
    return Status::Corruption(
        "Non-connection context request header must specify remote_method");
  }
  if (PREDICT_FALSE(!header_.remote_method().IsInitialized())) {
    return Status::Corruption(
        "remote_method in request header is not initialized",
        header_.remote_method().InitializationErrorString());
  }
  remoteMethod_.fromPb(header_.remote_method());

  // Compute and cache the call deadline.
  if (header_.has_timeout_millis() && header_.timeout_millis() != 0) {
    deadline_ = timing_.timeReceived +
        MonoDelta::FromMilliseconds(header_.timeout_millis());
  }

  if (header_.sidecar_offsets_size() > TransferLimits::kMaxSidecars) {
    return Status::Corruption(
        fmt::format(
            "Received {} additional payload slices, expected at most {}",
            header_.sidecar_offsets_size(),
            TransferLimits::kMaxSidecars));
  }

  RETURN_NOT_OK(
      RpcSidecar::parseSidecars(
          header_.sidecar_offsets(),
          serializedRequest_,
          inboundSidecarSlices_));
  if (header_.sidecar_offsets_size() > 0) {
    // Trim the request to just the message
    serializedRequest_ =
        Slice(serializedRequest_.data(), header_.sidecar_offsets(0));
  }

  // Retain the buffer that we have a view into.
  transfer_.swap(transfer);
  return Status::OK();
}

void InboundCall::respondSuccess(const MessageLite& response) {
  TRACE_EVENT0("rpc", "InboundCall::respondSuccess");
  respond(response, true);
}

void InboundCall::respondUnsupportedFeature(
    const vector<uint32_t>& unsupportedFeatures) {
  TRACE_EVENT0("rpc", "InboundCall::respondUnsupportedFeature");
  ErrorStatusPB err;
  err.set_message("unsupported feature flags");
  err.set_code(ErrorStatusPB::ERROR_INVALID_REQUEST);
  for (uint32_t feature : unsupportedFeatures) {
    err.add_unsupported_feature_flags(feature);
  }

  respond(err, false);
}

void InboundCall::respondFailure(
    ErrorStatusPB::RpcErrorCodePB errorCode,
    const Status& status) {
  TRACE_EVENT0("rpc", "InboundCall::respondFailure");
  ErrorStatusPB err;
  err.set_message(status.ToString());
  err.set_code(errorCode);

  respond(err, false);
}

void InboundCall::respondApplicationError(
    int errorExtId,
    const std::string& message,
    const MessageLite& appErrorPb) {
  ErrorStatusPB err;
  applicationErrorToPb(errorExtId, message, appErrorPb, &err);
  respond(err, false);
}

void InboundCall::applicationErrorToPb(
    int errorExtId,
    const std::string& message,
    const google::protobuf::MessageLite& appErrorPb,
    ErrorStatusPB* err) {
  err->set_message(message);
  const FieldDescriptor* appErrorField =
      err->GetReflection()->FindKnownExtensionByNumber(errorExtId);
  if (appErrorField != nullptr) {
    err->GetReflection()
        ->MutableMessage(err, appErrorField)
        ->CheckTypeAndMergeFrom(appErrorPb);
  } else {
    LOG(DFATAL) << "Unable to find application error extension ID "
                << errorExtId << " (message=" << message << ")";
  }
}

void InboundCall::respond(const MessageLite& response, bool isSuccess) {
  TRACE_EVENT_FLOW_END0("rpc", "InboundCall", this);
  serializeResponseBuffer(response, isSuccess);

  TRACE_EVENT_ASYNC_END1(
      "rpc", "InboundCall", this, "method", remoteMethod_.methodName());
  TRACE_TO(trace_, "Queueing $0 response", isSuccess ? "success" : "failure");
  recordHandlingCompleted();
  conn_->rpczStore()->logTrace(this);
  conn_->queueResponseForCall(unique_ptr<InboundCall>(this));
}

void InboundCall::serializeResponseBuffer(
    const MessageLite& response,
    bool isSuccess) {
  if (PREDICT_FALSE(!response.IsInitialized())) {
    LOG(ERROR) << "Invalid RPC response for " << toString()
               << ": protobuf missing required fields: "
               << response.InitializationErrorString();
    // Send it along anyway -- the client will also notice the missing fields
    // and produce an error on the other side, but this will at least
    // make it clear on both sides of the RPC connection what kind of error
    // happened.
  }

  uint32_t protobufMsgSize = response.ByteSize();

  ResponseHeader respHdr;
  respHdr.set_call_id(header_.call_id());
  respHdr.set_is_error(!isSuccess);
  int32_t sidecarByteSize = 0;
  for (const unique_ptr<RpcSidecar>& car : outboundSidecars_) {
    respHdr.add_sidecar_offsets(sidecarByteSize + protobufMsgSize);
    int32_t sidecarBytes = car->asSlice().size();
    DCHECK_LE(
        sidecarByteSize, TransferLimits::kMaxTotalSidecarBytes - sidecarBytes);
    sidecarByteSize += sidecarBytes;
  }

  serialization::SerializeMessage(
      response, &responseMsgBuf_, sidecarByteSize, true);
  int64_t mainMsgSize = sidecarByteSize + responseMsgBuf_.size();
  serialization::SerializeHeader(respHdr, mainMsgSize, &responseHdrBuf_);
}

size_t InboundCall::serializeResponseTo(TransferPayload* slices) const {
  TRACE_EVENT0("rpc", "InboundCall::serializeResponseTo");
  DCHECK_GT(responseHdrBuf_.size(), 0);
  DCHECK_GT(responseMsgBuf_.size(), 0);
  size_t nSlices = 2 + outboundSidecars_.size();
  DCHECK_LE(nSlices, slices->size());
  auto sliceIter = slices->begin();
  *sliceIter++ = Slice(responseHdrBuf_);
  *sliceIter++ = Slice(responseMsgBuf_);
  for (auto& sidecar : outboundSidecars_) {
    *sliceIter++ = sidecar->asSlice();
  }
  DCHECK_EQ(sliceIter - slices->begin(), nSlices);
  return nSlices;
}

Status InboundCall::addOutboundSidecar(unique_ptr<RpcSidecar> car, int* idx) {
  // Check that the number of sidecars does not exceed the number of payload
  // slices that are free (two are used up by the header and main message
  // protobufs).
  if (outboundSidecars_.size() > TransferLimits::kMaxSidecars) {
    return Status::ServiceUnavailable("All available sidecars already used");
  }
  int64_t sidecarBytes = car->asSlice().size();
  if (outboundSidecarsTotalBytes_ >
      TransferLimits::kMaxTotalSidecarBytes - sidecarBytes) {
    return Status::RuntimeError(
        fmt::format(
            "Total size of sidecars {} would exceed limit {}",
            static_cast<int64_t>(outboundSidecarsTotalBytes_) + sidecarBytes,
            TransferLimits::kMaxTotalSidecarBytes));
  }

  outboundSidecars_.emplace_back(std::move(car));
  outboundSidecarsTotalBytes_ += sidecarBytes;
  DCHECK_GE(outboundSidecarsTotalBytes_, 0);
  *idx = outboundSidecars_.size() - 1;
  return Status::OK();
}

string InboundCall::toString() const {
  if (header_.has_request_id()) {
    return fmt::format(
        "Call {} from {} (ReqId={{client: {}, seq_no={}, attempt_no={}}}) recv: {} handled: {} comp: {}",
        remoteMethod_.toString(),
        conn_->remote().ToString(),
        header_.request_id().client_id(),
        header_.request_id().seq_no(),
        header_.request_id().attempt_no(),
        timing_.timeReceived.ToString(),
        (timing_.timeHandled.Initialized() ? timing_.timeHandled.ToString()
                                           : "NOT_HANDLED"),
        (timing_.timeCompleted.Initialized() ? timing_.timeCompleted.ToString()
                                             : "NOT_COMPLETED"));
  }
  return fmt::format(
      "Call {} from {} (request call id {}) recv: {} handled: {} comp: {}",
      remoteMethod_.toString(),
      conn_->remote().ToString(),
      header_.call_id(),
      timing_.timeReceived.ToString(),
      (timing_.timeHandled.Initialized() ? timing_.timeHandled.ToString()
                                         : "NOT_HANDLED"),
      (timing_.timeCompleted.Initialized() ? timing_.timeCompleted.ToString()
                                           : "NOT_COMPLETED"));
}

void InboundCall::dumpPb(
    const DumpRunningRpcsRequestPB& req,
    RpcCallInProgressPB* resp) {
  resp->mutable_header()->CopyFrom(header_);
  if (req.include_traces() && trace_) {
    resp->set_trace_buffer(trace_->dumpToString());
  }
  resp->set_micros_elapsed(
      (MonoTime::Now() - timing_.timeReceived).ToMicroseconds());
}

const RemoteUser& InboundCall::remoteUser() const {
  return conn_->remote_user();
}

const Sockaddr& InboundCall::remoteAddress() const {
  return conn_->remote();
}

const std::shared_ptr<Connection>& InboundCall::connection() const {
  return conn_;
}

std::shared_ptr<Trace> InboundCall::trace() {
  return trace_;
}

void InboundCall::recordCallReceived() {
  TRACE_EVENT_ASYNC_BEGIN0("rpc", "InboundCall", this);
  DCHECK(
      !timing_.timeReceived.Initialized()); // Protect against multiple calls.
  timing_.timeReceived = MonoTime::Now();
}

void InboundCall::recordHandlingStarted(Histogram* incomingQueueTime) {
  DCHECK(incomingQueueTime != nullptr);
  DCHECK(!timing_.timeHandled.Initialized()); // Protect against multiple calls.
  timing_.timeHandled = MonoTime::Now();
  incomingQueueTime->Increment(
      (timing_.timeHandled - timing_.timeReceived).ToMicroseconds());
}

void InboundCall::recordHandlingCompleted() {
  DCHECK(
      !timing_.timeCompleted.Initialized()); // Protect against multiple calls.
  timing_.timeCompleted = MonoTime::Now();

  if (!timing_.timeHandled.Initialized()) {
    // Sometimes we respond to a call before we begin handling it (e.g. due to
    // queue overflow, etc). These cases should not be counted against the
    // histogram.
    return;
  }

  if (methodInfo_) {
    methodInfo_->handlerLatencyHistogram->Increment(
        (timing_.timeCompleted - timing_.timeHandled).ToMicroseconds());
  }
}

bool InboundCall::clientTimedOut() const {
  return MonoTime::Now() >= deadline_;
}

MonoTime InboundCall::getTimeReceived() const {
  return timing_.timeReceived;
}

vector<uint32_t> InboundCall::getRequiredFeatures() const {
  vector<uint32_t> features;
  for (uint32_t feature : header_.required_feature_flags()) {
    features.push_back(feature);
  }
  return features;
}

Status InboundCall::getInboundSidecar(int idx, Slice* sidecar) const {
  DCHECK(transfer_) << "Sidecars have been discarded";
  if (idx < 0 || idx >= header_.sidecar_offsets_size()) {
    return Status::InvalidArgument(
        fmt::format("Index {} does not reference a valid sidecar", idx));
  }
  *sidecar = inboundSidecarSlices_[idx];
  return Status::OK();
}

void InboundCall::discardTransfer() {
  transfer_.reset();
}

size_t InboundCall::getTransferSize() {
  if (!transfer_) {
    return 0;
  }
  return transfer_->data().size();
}

} // namespace rpc
} // namespace kudu
