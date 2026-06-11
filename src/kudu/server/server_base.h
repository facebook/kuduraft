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
#ifndef KUDU_SERVER_SERVER_BASE_H
#define KUDU_SERVER_SERVER_BASE_H

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/security/simple_acl.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;
class MemTracker;
class MetricEntity;
class MetricRegistry;
class NodeInstancePB;
class RpcServer;
class ScopedGLogMetrics;
class Sockaddr;
class Thread;

namespace clock {
class Clock;
} // namespace clock

namespace rpc {
class Messenger;
class ResultTracker;
class RpcContext;
class ServiceIf;
class ServicePool;
} // namespace rpc

namespace security {
class TlsContext;
class TokenVerifier;
} // namespace security

namespace server {
class DiagnosticsLog;
class ServerStatusPB;

// Base class for tablet server and master.
// Handles starting and stopping the RPC server and web server,
// and provides a common interface for server-type-agnostic functions.
class ServerBase {
 public:
  const RpcServer* rpcServer() const {
    return rpcServer_.get();
  }

  const std::shared_ptr<rpc::Messenger>& messenger() const {
    return messenger_;
  }

  // Return the first RPC address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr firstRpcAddress() const;

  FsManager* fsManager() {
    return fsManager_.get();
  }

  const security::TlsContext& tlsContext() const;
  security::TlsContext* mutableTlsContext();

  const security::TokenVerifier& tokenVerifier() const;
  security::TokenVerifier* mutableTokenVerifier();

  // Return the instance identifier of this server.
  // This may not be called until after the server is Started.
  const NodeInstancePB& instancePb() const;

  const std::shared_ptr<MemTracker>& memTracker() const {
    return memTracker_;
  }

  const std::shared_ptr<MetricEntity>& metricEntity() const {
    return metricEntity_;
  }

  MetricRegistry* metricRegistry() {
    return metricRegistry_.get();
  }

  const std::shared_ptr<rpc::ResultTracker>& resultTracker() const {
    return resultTracker_;
  }

  // Returns this server's clock.
  clock::Clock* clock() {
    return clock_.get();
  }

  // Return a PB describing the status of the server (version info, bound ports,
  // etc)
  Status getStatusPb(ServerStatusPB* status) const;

  enum { kSuperUser = 1, kUser = 1 << 1, kServiceUser = 1 << 2 };

  // Authorize an RPC. 'allowed_roles' is a bitset of which roles from the above
  // enum should be allowed to make hthe RPC.
  //
  // If authorization fails, return false and respond to the RPC.
  bool authorize(rpc::RpcContext* rpc, uint32_t allowed_roles);

 protected:
  ServerBase(
      std::string name,
      const ServerBaseOptions& options,
      const std::string& metric_namespace);
  virtual ~ServerBase();

  virtual Status Init();

  // Starts the server, including activating its RPC and HTTP endpoints such
  // that incoming requests get processed.
  virtual Status Start();

  // Shuts down the server.
  virtual void Shutdown();

  // Registers a new RPC service. Once Start() is called, the server will
  // process and dispatch incoming RPCs belonging to this service.
  Status registerService(std::unique_ptr<rpc::ServiceIf> rpc_impl);

  // Unregisters all RPC services. After this function returns, any subsequent
  // incoming RPCs will be rejected.
  //
  // When shutting down, this function should be called before shutting down
  // higher-level subsystems. For example:
  // 1. ServerBase::unregisterAllServices()
  // 2. <shut down other subsystems>
  // 3. ServerBase::Shutdown()
  //
  // TODO(adar): this should also wait on all outstanding RPCs to finish via
  // Messenger::Shutdown, but doing that causes too many other shutdown-related
  // issues. Here are a few that I observed:
  // - tserver heartbeater threads access acceptor pool socket state.
  // - Shutting down TabletReplicas invokes RPC callbacks for aborted
  //   transactions, but Messenger::Shutdown has already destroyed too much
  //   necessary RPC state.
  //
  // TODO(adar): this should also shutdown the webserver, but it isn't safe to
  // do that before before shutting down the tserver heartbeater.
  void unregisterAllServices();

  void logUnauthorizedAccess(rpc::RpcContext* rpc) const;

  const std::string name_;

  std::shared_ptr<MemTracker> memTracker_;
  std::unique_ptr<MetricRegistry> metricRegistry_;
  std::shared_ptr<MetricEntity> metricEntity_;
  std::unique_ptr<FsManager> fsManager_;
  std::unique_ptr<RpcServer> rpcServer_;

  std::shared_ptr<rpc::Messenger> messenger_;
  std::shared_ptr<rpc::ResultTracker> resultTracker_;
  bool isFirstRun_;

  std::shared_ptr<clock::Clock> clock_;

  // The instance identifier of this server.
  std::unique_ptr<NodeInstancePB> instancePb_;

  // The ACL of users who are allowed to act as superusers.
  security::SimpleAcl superuserAcl_;

  // The ACL of users who are allowed to access the cluster.
  security::SimpleAcl userAcl_;

  // The ACL of users who may act as part of the Kudu service.
  security::SimpleAcl serviceAcl_;

 private:
  Status initAcls();
  void generateInstanceId();
  Status dumpServerInfo(const std::string& path, const std::string& format)
      const;
  Status startMetricsLogging();
  void metricsLoggingThread();

  // Callback from the RPC system when a service queue has overflowed.
  void serviceQueueOverflowed(rpc::ServicePool* service);

  // Start thread to remove excess glog and minidump files.
  Status startExcessLogFileDeleterThread();
  void excessLogFileDeleterThread();

  ServerBaseOptions options_;

  std::unique_ptr<DiagnosticsLog> diagLog_;
  std::shared_ptr<Thread> excessLogDeleterThread_;
  CountDownLatch stopBackgroundThreadsLatch_;

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
