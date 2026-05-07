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

#include "kudu/rpc/proxy.h"

#include <iostream>
#include <memory>
#include <utility>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/core/ref.hpp>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/user.h"

using std::string;

namespace kudu {
namespace rpc {

Proxy::Proxy(
    std::shared_ptr<Messenger> messenger,
    const Sockaddr& remote,
    string hostname,
    string serviceName)
    : serviceName_(std::move(serviceName)),
      messenger_(std::move(messenger)),
      isStarted_(false) {
  CHECK(messenger_ != nullptr);
  DCHECK(!serviceName_.empty()) << "Proxy service name must not be blank";

  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string realUser;
  Status s = getLoggedInUser(&realUser);
  if (!s.ok()) {
    LOG(WARNING) << "Proxy for " << serviceName_
                 << ": Unable to get logged-in user name: " << s.ToString()
                 << " before connecting to remote: " << remote.ToString();
  }

  UserCredentials creds;
  creds.setRealUser(std::move(realUser));
  connId_ = ConnectionId(remote, std::move(hostname), std::move(creds));
}

Proxy::~Proxy() {}

void Proxy::asyncRequest(
    const string& method,
    const google::protobuf::Message& req,
    google::protobuf::Message* response,
    RpcController* controller,
    const ResponseCallback& callback) const {
  CHECK(!controller->call_) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&isStarted_, true);
  RemoteMethod remoteMethod(serviceName_, method);
  controller->call_.reset(
      new OutboundCall(connId_, remoteMethod, response, controller, callback));
  controller->setRequestParam(req);
  controller->setMessenger(messenger_.get());

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->queueOutboundCall(controller->call_);
}

Status Proxy::syncRequest(
    const string& method,
    const google::protobuf::Message& req,
    google::protobuf::Message* resp,
    RpcController* controller) const {
  CountDownLatch latch(1);
  asyncRequest(
      method,
      req,
      DCHECK_NOTNULL(resp),
      controller,
      boost::bind(&CountDownLatch::countDown, boost::ref(latch)));

  latch.wait();
  return controller->status();
}

void Proxy::setUserCredentials(const UserCredentials& userCredentials) {
  CHECK(base::subtle::NoBarrier_Load(&isStarted_) == false)
      << "It is illegal to call setUserCredentials() after request processing has started";
  connId_.setUserCredentials(userCredentials);
}

std::string Proxy::toString() const {
  return fmt::format("{}@{}", serviceName_, connId_.ToString());
}

} // namespace rpc
} // namespace kudu
