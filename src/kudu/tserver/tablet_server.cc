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

#include "kudu/tserver/tablet_server.h"

#include <cstddef>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include "kudu/consensus/consensus.service.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/tserver/consensus_service.h"
#include "kudu/tserver/simple_tablet_manager.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using kudu::rpc::ServiceIf;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace tserver {

std::string RaftConsensusServerIf::ConsensusServiceRpcQueueToString() const {
  const kudu::rpc::ServicePool* pool = rpc_server_->service_pool(
      kudu::consensus::ConsensusServiceIf::static_service_name());
  if (pool) {
    return pool->RpcServiceQueueToString();
  }
  return "";
}

/*static*/ Status RaftConsensusServerIf::ShowKuduThreadStatus(
    std::vector<ThreadDescriptor>* threads) {
  return GlobalShowThreadStatus(threads);
}

// Change thread priority for a particular category, this not only changes the
// current threads belong to that category, but also future threads spawned in
// that category.
//
// @param category In the other words, thread pool name
// @param priority thread priority based on nice. Should be -20 to 19
// @return Status:OK if succeed
/*static*/ Status RaftConsensusServerIf::ChangeKuduThreadPriority(
    const std::string& pool,
    int priority) {
  return GlobalChangeThreadPriority(pool, priority);
}

RaftConsensusServerIf::RaftConsensusServerIf(
    const std::string& name,
    const server::ServerBaseOptions& opts,
    const std::string& metrics_namespace)
    : kserver::KuduServer(name, opts, metrics_namespace) {}

TabletServer::TabletServer(const TabletServerOptions& opts)
    : RaftConsensusServerIf("TabletServer", opts, "kudu.tabletserver"),
      initted_(false),
      opts_(opts),
      tablet_manager_(new TSTabletManager(this)) {}

TabletServer::TabletServer(
    const TabletServerOptions& opts,
    const std::function<std::unique_ptr<TabletManagerIf>(TabletServer&)>&
        factory)
    : RaftConsensusServerIf("TabletServer", opts, "kudu.tabletserver"),
      initted_(false),
      opts_(opts),
      tablet_manager_(factory(*this)) {}

TabletServer::~TabletServer() {
  Shutdown();
}

string TabletServer::ToString() const {
  // TODO: include port numbers, etc.
  return "TabletServer";
}

Status TabletServer::Init() {
  CHECK(!initted_);

  // This pool will be used to wait for Raft
  RETURN_NOT_OK(
      ThreadPoolBuilder("init").set_max_threads(1).Build(&init_pool_));

  // Initialize FS, rpc_server, rpc messenger and Raft pool
  RETURN_NOT_OK(KuduServer::Init());

  // Moving registration of consensus service and RPC server
  // start to Init. This allows us to create a barebones Raft
  // distributed config. We need the service to be here, because
  // Raft::create makes remote GetNodeInstance RPC calls.
  unique_ptr<ServiceIf> consensus_service(
      new ConsensusServiceImpl(this, *tablet_manager_));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(KuduServer::Start());

  // Moving tablet manager initialization to Init phase of
  // tablet server
  if (tablet_manager_->IsInitialized()) {
    return Status::IllegalState("Catalog manager is already initialized");
  }
  RETURN_NOT_OK_PREPEND(
      tablet_manager_->Init(is_first_run_),
      "Unable to initialize catalog manager");

  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

  if (!tablet_manager_->IsInitialized()) {
    return Status::IllegalState("Tablet manager is not initialized");
  }

  RETURN_NOT_OK_PREPEND(
      tablet_manager_->Start(is_first_run_),
      "Unable to start raft in tablet manager");
  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  return Status::OK();
}

void TabletServer::Shutdown() {
  if (initted_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    // 1. Stop accepting new RPCs.
    UnregisterAllServices();

    tablet_manager_->Shutdown();

    // 3. Shut down generic subsystems.
    KuduServer::Shutdown();
    LOG(INFO) << name << " shutdown complete.";
    initted_ = false;
  }
}

} // namespace tserver
} // namespace kudu
