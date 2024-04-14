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
#ifndef KUDU_CONSENSUS_SERVER_H
#define KUDU_CONSENSUS_SERVER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <folly/SharedMutex.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/tserver/simple_tablet_manager.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"

namespace kudu {

class ThreadPool;

namespace tserver {

class TabletManagerIf;
class RaftConsensusServer;

/**
 * This is basically a wrapper around a map where key is the ID of the ring and
 * value is an obj of TabletServerOptions
 */
struct ConsensusServerOptions : public kudu::server::ServerBaseOptions {
  void AddOptions(
      const std::string& id,
      const std::shared_ptr<TabletServerOptions>& opts) {
    map_[id] = opts;
  }

  void RemoveOptions(const std::string& id) {
    map_.erase(id);
  }

  std::shared_ptr<TabletServerOptions> GetOptions(const std::string& id) const {
    auto itr = map_.find(id);
    if (itr == map_.end()) {
      return nullptr;
    }

    return itr->second;
  }

  void GetIds(std::vector<std::string>& ids) const {
    DCHECK(ids.empty());
    ids.clear();
    for (const auto& entry : map_) {
      ids.push_back(entry.first);
    }
    std::sort(ids.begin(), ids.end());
  }

  size_t Size() const {
    return map_.size();
  }

 private:
  std::unordered_map<std::string, std::shared_ptr<TabletServerOptions>> map_;
};

/**
 * Consensus server that can handle multiple raft rings
 */
class RaftConsensusServer : public RaftConsensusServerIf {
 public:
  explicit RaftConsensusServer(const ConsensusServerOptions& opts);

  ~RaftConsensusServer();

  virtual Status Init() override;

  virtual Status Start() override;

  virtual void Shutdown() override;

  std::string ToString() const override;

  TabletManagerIf* tablet_manager() override {
    return consensus_manager_.get();
  }

  const TabletServerOptions& opts(const std::string& id) {
    return *(opts_.GetOptions(id));
  }

 private:
  friend class RaftConsensusManager;
  friend class RaftConsensusInstance;

  bool initted_;

  bool started_;

  // For initializing the catalog manager.
  std::unique_ptr<ThreadPool> init_pool_;

  // The options passed at construction time.
  const ConsensusServerOptions opts_;

  // Manager for tablets which are available on this server.
  std::unique_ptr<TabletManagerIf> consensus_manager_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensusServer);
};

/**
 * Represents a single consensus ring
 */
class RaftConsensusInstance {
 public:
  RaftConsensusInstance(
      const std::string& id,
      RaftConsensusServer* server,
      scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
      scoped_refptr<consensus::PersistentVarsManager> persistent_vars_manager);

  ~RaftConsensusInstance();

  Status Init(bool is_first_run);

  Status Start(bool is_first_run);

  bool IsInitialized() const;

  void Shutdown();

  std::shared_ptr<consensus::RaftConsensus> shared_consensus() const;

  scoped_refptr<kudu::log::Log> GetLog() const {
    return log_;
  }

  std::string LogPrefix() const;

 private:
  Status CreateNew(FsManager* fs_manager);

  Status Load(FsManager* /* fs_manager */);

  Status CreateDistributedConfig(
      const TabletServerOptions& options,
      consensus::RaftConfigPB* committed_config);

  Status WaitUntilConsensusRunning(const MonoDelta& timeout);

  Status WaitUntilRunning();

  Status SetupRaft();

  void InitLocalRaftPeerPB();

  const TSTabletManagerStatePB& state() const {
    return state_;
  }

  void set_state(const TSTabletManagerStatePB& state) {
    state_ = state;
  }

  void MarkTabletDirty(const std::string& reason) {};

  const std::string id_;

  TSTabletManagerStatePB state_;

  RaftConsensusServer* server_;

  FsManager* const fs_manager_;

  scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  scoped_refptr<consensus::PersistentVarsManager> persistent_vars_manager_;

  consensus::RaftPeerPB local_peer_pb_;

  mutable RWMutex lock_;

  std::shared_ptr<consensus::RaftConsensus> consensus_;

  scoped_refptr<kudu::log::Log> log_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensusInstance);
};

/**
 * Manager used by RaftConsensusServer to manage consensus rings
 */
class RaftConsensusManager : public TabletManagerIf {
 public:
  explicit RaftConsensusManager(RaftConsensusServer* server);

  Status Init(bool is_first_run) override;

  Status Start(bool is_first_run) override;

  bool IsInitialized() const override;

  void Shutdown() override;

  const NodeInstancePB& NodeInstance() const override;

  std::shared_ptr<consensus::RaftConsensus> shared_consensus(
      const std::string& id) const override;

 private:
  // TODO (abhinavsharma): Consider making the map const and getting rid of
  // map_lock_
  std::unordered_map<std::string, std::shared_ptr<RaftConsensusInstance>> map_;
  mutable folly::SharedMutexReadPriority map_lock_;

  FsManager* const fs_manager_;

  scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  scoped_refptr<consensus::PersistentVarsManager> persistent_vars_manager_;

  RaftConsensusServer* server_;

  TSTabletManagerStatePB state_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensusManager);
};

} // namespace tserver
} // namespace kudu
#endif
