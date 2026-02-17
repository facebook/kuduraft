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

#pragma once

#include <memory>

#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

class RemoteMethod;
struct RpcMethodInfo;
class InboundCall;

class RpcService {
 public:
  RpcService() = default;
  virtual ~RpcService() = default;
  RpcService(const RpcService&) = delete;
  RpcService& operator=(const RpcService&) = delete;
  RpcService(RpcService&&) = delete;
  RpcService& operator=(RpcService&&) = delete;

  // Enqueue a call for processing.
  // On failure, the RpcService::QueueInboundCall() implementation is
  // responsible for responding to the client with a failure message.
  virtual Status QueueInboundCall(std::unique_ptr<InboundCall> call) = 0;

  // Look up the method being requested by the remote call.
  // Returns a raw pointer to the RpcMethodInfo. The lifetime is guaranteed
  // by the Service, which owns the method info and outlives all InboundCalls.
  // Returns nullptr if the method is not found.
  virtual RpcMethodInfo* lookupMethod(const RemoteMethod& method) {
    return nullptr;
  }

  virtual void NotifyLongCallLoading(const RemoteMethod& method) = 0;

  virtual void NotifyLongCallLoaded(const RemoteMethod& method) = 0;
};

} // namespace rpc
} // namespace kudu
