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

namespace kudu {
namespace rpc {

class DumpRpczStoreRequestPB;
class DumpRpczStoreResponsePB;
class InboundCall;
class MethodSampler;
struct RpcMethodInfo;

// Responsible for storing sampled traces associated with completed calls.
// Before each call is responded to, it is added to this store.
class RpczStore {
 public:
  RpczStore() = default;
  ~RpczStore() = default;

  RpczStore(const RpczStore&) = delete;
  RpczStore& operator=(const RpczStore&) = delete;
  RpczStore(RpczStore&&) = delete;
  RpczStore& operator=(RpczStore&&) = delete;

  // Log a WARNING message if the RPC response was slow enough that the
  // client likely timed out. This is based on the client-provided timeout
  // value.
  // Also can be configured to log _all_ RPC traces for help debugging.
  void logTrace(InboundCall* call);
};

} // namespace rpc
} // namespace kudu
