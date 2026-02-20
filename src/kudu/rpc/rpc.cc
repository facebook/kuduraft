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

#include "kudu/rpc/rpc.h"

#include <cstdlib>
#include <string>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"

using std::string;

namespace kudu {

namespace rpc {

bool RpcRetrier::handleResponse(Rpc* rpc, Status* outStatus) {
  DCHECK(rpc);
  DCHECK(outStatus);

  // Always retry TOO_BUSY and UNAVAILABLE errors.
  const Status controllerStatus = controller_.status();
  if (controllerStatus.IsRemoteError()) {
    const ErrorStatusPB* err = controller_.error_response();
    if (err && err->has_code() &&
        (err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY ||
         err->code() == ErrorStatusPB::ERROR_UNAVAILABLE)) {
      // The UNAVAILABLE code is a broader counterpart of the
      // SERVER_TOO_BUSY. In both cases it's necessary to retry a bit later.
      delayedRetry(rpc, controllerStatus);
      return true;
    }
  }

  *outStatus = controllerStatus;
  return false;
}

void RpcRetrier::delayedRetry(Rpc* rpc, const Status& whyStatus) {
  if (!whyStatus.ok() && (lastError_.ok() || lastError_.IsTimedOut())) {
    lastError_ = whyStatus;
  }
  // Add some jitter to the retry delay.
  //
  // If the delay causes us to miss our deadline, RetryCb will fail the
  // RPC on our behalf.
  int numMs = ++attemptNum_ + ((rand() % 5));
  messenger_->ScheduleOnReactor(
      boost::bind(&RpcRetrier::delayedRetryCb, this, rpc, _1),
      MonoDelta::FromMilliseconds(numMs));
}

void RpcRetrier::delayedRetryCb(Rpc* rpc, const Status& status) {
  Status newStatus = status;
  if (newStatus.ok()) {
    // Has this RPC timed out?
    if (deadline_.Initialized()) {
      if (MonoTime::Now() > deadline_) {
        string errStr = fmt::format("{} passed its deadline", rpc->toString());
        if (!lastError_.ok()) {
          errStr += fmt::format(": {}", lastError_.ToString());
        }
        newStatus = Status::TimedOut(errStr);
      }
    }
  }
  if (newStatus.ok()) {
    controller_.Reset();
    rpc->sendRpc();
  } else {
    rpc->sendRpcCb(newStatus);
  }
}

} // namespace rpc
} // namespace kudu
