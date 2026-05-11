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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

template <class T>
class scoped_refptr;

namespace kudu {

namespace rpc {
class AcceptorPool;
class Messenger;
class ServiceIf;
class ServicePool;
} // namespace rpc

struct RpcServerOptions {
  RpcServerOptions();

  std::string rpcBindAddresses;
  std::string rpcAdvertisedAddresses;
  uint32_t numAcceptorsPerAddress;
  uint32_t numServiceThreads;
  uint16_t defaultPort;
  size_t serviceQueueLength;
  uint32_t numReactorThreads;
};

class RpcServer {
 public:
  explicit RpcServer(RpcServerOptions opts);
  ~RpcServer();

  // Set a hook which will be called by any registered service when
  // its queue overflows. The service pool itself will be passed
  // as a parameter.
  //
  // REQUIRES: must be set before the server is started.
  void setTooBusyHook(std::function<void(rpc::ServicePool*)> hook) {
    CHECK_NE(serverState_, kStarted);
    tooBusyHook_ = std::move(hook);
  }

  Status init(const std::shared_ptr<rpc::Messenger>& messenger)
      WARN_UNUSED_RESULT;
  // Services need to be registered after init'ing, but before start'ing.
  // The service's ownership will be given to a ServicePool.
  Status registerService(std::unique_ptr<rpc::ServiceIf> service)
      WARN_UNUSED_RESULT;
  Status bind() WARN_UNUSED_RESULT;
  Status start() WARN_UNUSED_RESULT;
  void shutdown();

  std::string toString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been start()ed.
  Status getBoundAddresses(std::vector<Sockaddr>* addresses) const
      WARN_UNUSED_RESULT;

  // Return the addresses that this server is advertising externally
  // to the world. Requires that the server has been start()ed.
  Status getAdvertisedAddresses(std::vector<Sockaddr>* addresses) const
      WARN_UNUSED_RESULT;

  const rpc::ServicePool* servicePool(const std::string& serviceName) const;

  // Return all of the currently-registered service pools.
  //
  // This is not thread-safe against concurrent calls to registerService().
  std::vector<std::shared_ptr<rpc::ServicePool>> servicePools() const;

 private:
  enum ServerState {
    // Default state when the rpc server is constructed.
    kUninitialized,
    // State after init() was called.
    kInitialized,
    // State after bind().
    kBound,
    // State after start() was called.
    kStarted
  };
  ServerState serverState_;

  const RpcServerOptions options_;
  std::shared_ptr<rpc::Messenger> messenger_;

  // Parsed addresses to bind RPC to. Set by init()
  std::vector<Sockaddr> rpcBindAddresses_;

  // Parsed addresses to advertise. Set by init(). Empty if rpcBindAddresses_
  // should be advertised.
  std::vector<Sockaddr> rpcAdvertisedAddresses_;

  std::vector<std::shared_ptr<rpc::AcceptorPool>> acceptorPools_;

  // Function called when one of this server's pools rejects an RPC due to queue
  // overflow.
  std::function<void(rpc::ServicePool*)> tooBusyHook_;

  DISALLOW_COPY_AND_ASSIGN(RpcServer);
};

} // namespace kudu
