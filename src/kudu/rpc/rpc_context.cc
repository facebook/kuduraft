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

#include "kudu/rpc/rpc_context.h"

#include <memory>
#include <ostream>
#include <utility>

#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/rpc/connection.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

using google::protobuf::Message;
using kudu::pb_util::SecureDebugString;
using std::string;
using std::unique_ptr;

namespace kudu {

class Slice;

namespace rpc {

RpcContext::RpcContext(
    InboundCall* call,
    const google::protobuf::Message* requestPb,
    google::protobuf::Message* responsePb)
    : call_(CHECK_NOTNULL(call)),
      request_pb_(requestPb),
      response_pb_(responsePb) {
  VLOG(4) << call_->remoteMethod().serviceName()
          << ": Received RPC request for " << call_->toString() << ":"
          << std::endl
          << SecureDebugString(*request_pb_);
  TRACE_EVENT_ASYNC_BEGIN2(
      "rpc_call",
      "RPC",
      this,
      "call",
      call_->toString(),
      "request",
      pb_util::PbTracer::TracePb(*request_pb_));
}

RpcContext::~RpcContext() {}

void RpcContext::setResultTracker(
    std::shared_ptr<ResultTracker> resultTracker) {
  DCHECK(!result_tracker_);
  result_tracker_ = std::move(resultTracker);
}

void RpcContext::respondSuccess() {
  if (areResultsTracked()) {
    result_tracker_->recordCompletionAndRespond(
        call_->header().request_id(), response_pb_.get());
  } else {
    VLOG(4) << call_->remoteMethod().serviceName()
            << ": Sending RPC success response for " << call_->toString() << ":"
            << std::endl
            << SecureDebugString(*response_pb_);
    TRACE_EVENT_ASYNC_END2(
        "rpc_call",
        "RPC",
        this,
        "response",
        pb_util::PbTracer::TracePb(*response_pb_),
        "trace",
        trace()->dumpToString());
    call_->respondSuccess(*response_pb_);
    delete this;
  }
}

void RpcContext::respondNoCache() {
  if (areResultsTracked()) {
    result_tracker_->failAndRespond(
        call_->header().request_id(), response_pb_.get());
  } else {
    VLOG(4) << call_->remoteMethod().serviceName()
            << ": Sending RPC failure response for " << call_->toString()
            << ": " << SecureDebugString(*response_pb_);
    TRACE_EVENT_ASYNC_END2(
        "rpc_call",
        "RPC",
        this,
        "response",
        pb_util::PbTracer::TracePb(*response_pb_),
        "trace",
        trace()->dumpToString());
    // This is a bit counter intuitive, but when we get the failure but set the
    // error on the call's response we call respondSuccess() instead of
    // respondFailure().
    call_->respondSuccess(*response_pb_);
    delete this;
  }
}

void RpcContext::respondFailure(const Status& status) {
  return respondRpcFailure(ErrorStatusPB::ERROR_APPLICATION, status);
}

void RpcContext::respondRpcFailure(
    ErrorStatusPB_RpcErrorCodePB err,
    const Status& status) {
  if (areResultsTracked()) {
    result_tracker_->failAndRespond(call_->header().request_id(), err, status);
  } else {
    VLOG(4) << call_->remoteMethod().serviceName()
            << ": Sending RPC failure response for " << call_->toString()
            << ": " << status.ToString();
    TRACE_EVENT_ASYNC_END2(
        "rpc_call",
        "RPC",
        this,
        "status",
        status.ToString(),
        "trace",
        trace()->dumpToString());
    call_->respondFailure(err, status);
    delete this;
  }
}

void RpcContext::respondApplicationError(
    int errorExtId,
    const std::string& message,
    const Message& appErrorPb) {
  if (areResultsTracked()) {
    result_tracker_->failAndRespond(
        call_->header().request_id(), errorExtId, message, appErrorPb);
  } else {
    if (VLOG_IS_ON(4)) {
      ErrorStatusPB err;
      InboundCall::applicationErrorToPb(errorExtId, message, appErrorPb, &err);
      VLOG(4) << call_->remoteMethod().serviceName()
              << ": Sending application error response for "
              << call_->toString() << ":" << std::endl
              << SecureDebugString(err);
    }
    TRACE_EVENT_ASYNC_END2(
        "rpc_call",
        "RPC",
        this,
        "response",
        pb_util::PbTracer::TracePb(appErrorPb),
        "trace",
        trace()->dumpToString());
    call_->respondApplicationError(errorExtId, message, appErrorPb);
    delete this;
  }
}

const rpc::RequestIdPB* RpcContext::requestId() const {
  return call_->header().has_request_id() ? &call_->header().request_id()
                                          : nullptr;
}

size_t RpcContext::getTransferSize() const {
  return call_->getTransferSize();
}

Status RpcContext::addOutboundSidecar(unique_ptr<RpcSidecar> car, int* idx) {
  return call_->addOutboundSidecar(std::move(car), idx);
}

Status RpcContext::getInboundSidecar(int idx, Slice* slice) const {
  return call_->getInboundSidecar(idx, slice);
}

const RemoteUser& RpcContext::remoteUser() const {
  return call_->remoteUser();
}

bool RpcContext::isConfidential() const {
  return call_->connection()->isConfidential();
}

void RpcContext::discardTransfer() {
  call_->discardTransfer();
}

const Sockaddr& RpcContext::remoteAddress() const {
  return call_->remoteAddress();
}

std::string RpcContext::requestorString() const {
  return call_->remoteUser().toString() + " at " +
      call_->remoteAddress().ToString();
}

std::string RpcContext::methodName() const {
  return call_->remoteMethod().methodName();
}

std::string RpcContext::serviceName() const {
  return call_->remoteMethod().serviceName();
}

MonoTime RpcContext::getClientDeadline() const {
  return call_->getClientDeadline();
}

MonoTime RpcContext::getTimeReceived() const {
  return call_->getTimeReceived();
}

std::shared_ptr<Trace> RpcContext::trace() {
  return call_->trace();
}

void RpcContext::panic(
    const char* filePath,
    int lineNumber,
    const string& message) {
  // Use the LogMessage class directly so that the log messages appear to come
  // from the line of code which caused the panic, not this code.
#define MY_ERROR \
  google::LogMessage(filePath, lineNumber, google::GLOG_ERROR).stream()
#define MY_FATAL google::LogMessageFatal(filePath, lineNumber).stream()

  MY_ERROR << "Panic handling " << call_->toString() << ": " << message;
  MY_ERROR << "Request:\n" << SecureDebugString(*request_pb_);
  auto t = trace();
  if (t) {
    MY_ERROR << "RPC trace:";
    t->dump(&MY_ERROR, true);
  }
  MY_FATAL << "Exiting due to panic.";

#undef MY_ERROR
#undef MY_FATAL
}

} // namespace rpc
} // namespace kudu
