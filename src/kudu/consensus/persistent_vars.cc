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
#include "kudu/consensus/persistent_vars.h"

#include <glog/logging.h>
#include <atomic>

#include <fmt/core.h>
#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

namespace kudu::consensus {

using std::string;

bool PersistentVars::isStartElectionAllowed() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  // allow_start_election is optional with default = true
  // So if it not present, we will allow start elections by default
  return pb_.allow_start_election();
}

void PersistentVars::setAllowStartElection(bool val) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  pb_.set_allow_start_election(val);
}

std::shared_ptr<const std::string> PersistentVars::raftRpcToken() const {
  return std::atomic_load_explicit(
      &raft_rpc_token_cache_, std::memory_order_relaxed);
}

void PersistentVars::setRaftRpcToken(std::optional<std::string> rpc_token) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (rpc_token) {
    std::atomic_store_explicit(
        &raft_rpc_token_cache_,
        std::make_shared<const std::string>(*rpc_token),
        std::memory_order_relaxed);
    pb_.set_raft_rpc_token(*std::move(rpc_token));
  } else {
    std::atomic_store_explicit(
        &raft_rpc_token_cache_, {}, std::memory_order_relaxed);
    pb_.clear_raft_rpc_token();
  }
}

const std::string& PersistentVars::compressionDictionary() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return pb_.compression_dictionary();
}

void PersistentVars::setCompressionDictionary(const std::string& dict) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  pb_.set_compression_dictionary(dict);
}

Status PersistentVars::flush(FlushMode flush_mode) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(
      WARNING, 500, logPrefix(), "flushing persistent variables");

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::CreateDirIfMissing(fs_manager_->env(), dir, &created_dir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parent_dir),
        "Unable to fsync consensus parent dir " + parent_dir);
  }

  string persistent_vars_file_path =
      fs_manager_->GetPersistentVarsPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fs_manager_->env(),
          persistent_vars_file_path,
          pb_,
          flush_mode == OVERWRITE ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write persistent vars file for tablet {} to path {}",
          tablet_id_,
          persistent_vars_file_path));
  return Status::OK();
}

PersistentVars::PersistentVars(
    FsManager* fs_manager,
    std::string tablet_id,
    std::string peer_uuid)
    : fs_manager_(CHECK_NOTNULL(fs_manager)),
      tablet_id_(std::move(tablet_id)),
      peer_uuid_(std::move(peer_uuid)) {}

Status PersistentVars::create(
    FsManager* fs_manager,
    const string& tablet_id,
    const std::string& peer_uuid,
    std::shared_ptr<PersistentVars>* persistent_vars_out) {
  std::shared_ptr<PersistentVars> persistent_vars(
      new PersistentVars(fs_manager, tablet_id, peer_uuid));

  RETURN_NOT_OK(
      persistent_vars->flush(NO_OVERWRITE)); // create() should not clobber.

  if (persistent_vars_out) {
    *persistent_vars_out = std::move(persistent_vars);
  }
  return Status::OK();
}

Status PersistentVars::load(
    FsManager* fs_manager,
    const std::string& tablet_id,
    const std::string& peer_uuid,
    std::shared_ptr<PersistentVars>* persistent_vars_out) {
  std::shared_ptr<PersistentVars> persistent_vars(
      new PersistentVars(fs_manager, tablet_id, peer_uuid));
  RETURN_NOT_OK(
      pb_util::ReadPBContainerFromPath(
          fs_manager->env(),
          fs_manager->GetPersistentVarsPath(tablet_id),
          &persistent_vars->pb_));
  if (persistent_vars->pb_.has_raft_rpc_token()) {
    persistent_vars->raft_rpc_token_cache_ =
        std::make_shared<const std::string>(
            persistent_vars->pb_.raft_rpc_token());
  }
  if (persistent_vars_out) {
    *persistent_vars_out = std::move(persistent_vars);
  }
  return Status::OK();
}

bool PersistentVars::fileExists(
    FsManager* fs_manager,
    const std::string& tablet_id) {
  return fs_manager->env()->FileExists(
      fs_manager->GetPersistentVarsPath(tablet_id));
}

std::string PersistentVars::logPrefix() const {
  // No need to lock to read const members.
  return fmt::format("T {} P {}: ", tablet_id_, peer_uuid_);
}

} // namespace kudu::consensus
