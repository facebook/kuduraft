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

#include "kudu/rpc/service_if.h"

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

// TODO remove this once we have fully cluster-tested this.
// Despite being on by default, this is left in in case we discover
// any issues in 0.10.0, we'll have an easy workaround to disable the feature.
DEFINE_bool(
    enable_exactly_once,
    true,
    "Whether to enable exactly once semantics.");
TAG_FLAG(enable_exactly_once, hidden);

using google::protobuf::Message;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace rpc {

ServiceIf::~ServiceIf() {}

void ServiceIf::Shutdown() {}

bool ServiceIf::supportsFeature(uint32_t feature) const {
  return false;
}

RpcMethodInfo* ServiceIf::lookupMethod(const RemoteMethod& /*method*/) {
  return nullptr;
}

bool ServiceIf::parseParam(
    InboundCall* call,
    google::protobuf::Message* message) {
  Slice param(call->serialized_request());
  if (PREDICT_FALSE(!message->ParseFromArray(param.data(), param.size()))) {
    string err = fmt::format(
        "invalid parameter for call {}: missing fields: {}",
        call->remote_method().toString(),
        message->InitializationErrorString().c_str());
    LOG(WARNING) << err;
    call->RespondFailure(
        ErrorStatusPB::ERROR_INVALID_REQUEST, Status::InvalidArgument(err));
    return false;
  }
  return true;
}

void ServiceIf::respondBadMethod(InboundCall* call) {
  Sockaddr localAddr, remoteAddr;

  CHECK_OK(call->connection()->socket()->GetSocketAddress(&localAddr));
  CHECK_OK(call->connection()->socket()->GetPeerAddress(&remoteAddr));
  string err = fmt::format(
      "Call on service {} received at {} from {} with an "
      "invalid method name: {}",
      call->remote_method().serviceName(),
      localAddr.ToString(),
      remoteAddr.ToString(),
      call->remote_method().methodName());
  LOG(WARNING) << err;
  call->RespondFailure(
      ErrorStatusPB::ERROR_NO_SUCH_METHOD, Status::InvalidArgument(err));
}

GeneratedServiceIf::~GeneratedServiceIf() {}

void GeneratedServiceIf::Handle(InboundCall* call) {
  const RpcMethodInfo* methodInfo = call->method_info();
  if (!methodInfo) {
    respondBadMethod(call);
    return;
  }
  unique_ptr<Message> req(methodInfo->reqPrototype->New());
  if (PREDICT_FALSE(!parseParam(call, req.get()))) {
    return;
  }
  Message* resp = methodInfo->respPrototype->New();

  RpcContext* ctx = new RpcContext(call, req.release(), resp);
  if (!methodInfo->authzMethod(ctx->request_pb(), resp, ctx)) {
    // The authzMethod itself should have responded to the RPC.
    return;
  }

  if (call->header().has_request_id() && methodInfo->trackResult &&
      FLAGS_enable_exactly_once) {
    ctx->setResultTracker(resultTracker_);
    ResultTracker::RpcState state =
        ctx->result_tracker()->TrackRpc(call->header().request_id(), resp, ctx);
    switch (state) {
      case ResultTracker::NEW:
        // Fall out of the 'if' statement to the normal path.
        break;
      case ResultTracker::COMPLETED:
      case ResultTracker::IN_PROGRESS:
      case ResultTracker::STALE:
        // ResultTracker has already responded to the RPC and deleted
        // 'ctx'.
        return;
      default:
        LOG(FATAL) << "Unknown state: " << state;
    }
  }
  methodInfo->func(ctx->request_pb(), resp, ctx);
}

RpcMethodInfo* GeneratedServiceIf::lookupMethod(const RemoteMethod& method) {
  DCHECK_EQ(method.serviceName(), service_name());
  const auto& it = methodsByName_.find(method.methodName());
  if (PREDICT_FALSE(it == methodsByName_.end())) {
    return nullptr;
  }
  return it->second.get();
}

void GeneratedServiceIf::NotifyLongCallLoading(const RemoteMethod& method) {
  RpcMethodInfo* methodInfo = lookupMethod(method);
  if (!methodInfo) {
    VLOG(2) << "[NotifyLongCallLoading] No method found for "
            << method.toString();
    return;
  }
  methodInfo->longCallLoadingHook();
}

void GeneratedServiceIf::NotifyLongCallLoaded(const RemoteMethod& method) {
  RpcMethodInfo* methodInfo = lookupMethod(method);
  if (!methodInfo) {
    VLOG(2) << "[NotifyLongCallLoading] No method found for "
            << method.toString();
    return;
  }
  methodInfo->longCallLoadedHook();
}

} // namespace rpc
} // namespace kudu
