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
#ifndef KUDU_TSERVER_TABLET_SERVER_H
#define KUDU_TSERVER_TABLET_SERVER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/kserver/kserver.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"

namespace kudu {

class ThreadPool;

namespace tserver {

class TSTabletManager;
class TabletManagerIf;

class RaftConsensusServerIf : public kserver::KuduServer {
 public:
  RaftConsensusServerIf(
      const std::string& name,
      const server::ServerBaseOptions& opts,
      const std::string& metrics_namespace);

  virtual Status Init() override = 0;

  virtual Status Start() override = 0;

  virtual void Shutdown() override = 0;

  virtual std::string ToString() const = 0;

  virtual TabletManagerIf* tablet_manager() = 0;

  /*
   * Capture a snapshot of the RPC service queue in the server log file.
   */
  std::string ConsensusServiceRpcQueueToString() const;

  static Status ShowKuduThreadStatus(std::vector<ThreadDescriptor>* threads);

  // Change thread priority for a particular category, this not only changes the
  // current threads belong to that category, but also future threads spawned in
  // that category.
  //
  // @param category In the other words, thread pool name
  // @param priority thread priority based on nice. Should be -20 to 19
  // @return Status:OK if succeed
  static Status ChangeKuduThreadPriority(const std::string& pool, int priority);
};

class TabletServer : public RaftConsensusServerIf {
 public:
  // TODO(unknown): move this out of this header, since clients want to use
  // this constant as well.
  static const uint16_t kDefaultPort = 7050;

  explicit TabletServer(const TabletServerOptions& opts);

  TabletServer(
      const TabletServerOptions& opts,
      const std::function<std::unique_ptr<TabletManagerIf>(TabletServer&)>&
          factory);

  virtual ~TabletServer() override;

  // Initializes the tablet server, including the bootstrapping of all
  // existing tablets.
  // Some initialization tasks are asynchronous, such as the bootstrapping
  // of tablets. Caller can block, waiting for the initialization to fully
  // complete by calling WaitInited().
  // RPC server is created and addresses bound
  // Tablet manager is initialized
  // Raft is created.
  // Raft log is created.
  virtual Status Init() override;

  virtual Status Start() override;

  virtual void Shutdown() override;

  std::string ToString() const override;

  TabletManagerIf* tablet_manager() override {
    return tablet_manager_.get();
  }

  const TabletServerOptions& opts() {
    return opts_;
  }

 private:
  friend class TabletServerTestBase;
  friend class TSTabletManager;

  bool initted_;

  // For initializing the catalog manager.
  std::unique_ptr<ThreadPool> init_pool_;

  // The options passed at construction time.
  const TabletServerOptions opts_;

  // Manager for tablets which are available on this server.
  std::unique_ptr<TabletManagerIf> tablet_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
