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

void PersistentVars::setRaftRpcToken(std::optional<std::string> rpcToken) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (rpcToken) {
    std::atomic_store_explicit(
        &raft_rpc_token_cache_,
        std::make_shared<const std::string>(*rpcToken),
        std::memory_order_relaxed);
    pb_.set_raft_rpc_token(*std::move(rpcToken));
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

Status PersistentVars::flush(FlushMode flushMode) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(
      WARNING, 500, logPrefix(), "flushing persistent variables");

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool createdDir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::createDirIfMissing(fs_manager_->env(), dir, &createdDir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(createdDir)) {
    string parentDir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parentDir),
        "Unable to fsync consensus parent dir " + parentDir);
  }

  string persistentVarsFilePath =
      fs_manager_->GetPersistentVarsPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fs_manager_->env(),
          persistentVarsFilePath,
          pb_,
          flushMode == OVERWRITE ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write persistent vars file for tablet {} to path {}",
          tablet_id_,
          persistentVarsFilePath));
  return Status::OK();
}

PersistentVars::PersistentVars(
    FsManager* fsManager,
    std::string tabletId,
    std::string peerUuid)
    : fs_manager_(CHECK_NOTNULL(fsManager)),
      tablet_id_(std::move(tabletId)),
      peer_uuid_(std::move(peerUuid)) {}

Status PersistentVars::create(
    FsManager* fsManager,
    const string& tabletId,
    const std::string& peerUuid,
    std::shared_ptr<PersistentVars>* persistentVarsOut) {
  std::shared_ptr<PersistentVars> persistentVars(
      new PersistentVars(fsManager, tabletId, peerUuid));

  RETURN_NOT_OK(
      persistentVars->flush(NO_OVERWRITE)); // create() should not clobber.

  if (persistentVarsOut) {
    *persistentVarsOut = std::move(persistentVars);
  }
  return Status::OK();
}

Status PersistentVars::load(
    FsManager* fsManager,
    const std::string& tabletId,
    const std::string& peerUuid,
    std::shared_ptr<PersistentVars>* persistentVarsOut) {
  std::shared_ptr<PersistentVars> persistentVars(
      new PersistentVars(fsManager, tabletId, peerUuid));
  RETURN_NOT_OK(
      pb_util::ReadPBContainerFromPath(
          fsManager->env(),
          fsManager->GetPersistentVarsPath(tabletId),
          &persistentVars->pb_));
  if (persistentVars->pb_.has_raft_rpc_token()) {
    persistentVars->raft_rpc_token_cache_ = std::make_shared<const std::string>(
        persistentVars->pb_.raft_rpc_token());
  }
  if (persistentVarsOut) {
    *persistentVarsOut = std::move(persistentVars);
  }
  return Status::OK();
}

bool PersistentVars::fileExists(
    FsManager* fsManager,
    const std::string& tabletId) {
  return fsManager->env()->FileExists(
      fsManager->GetPersistentVarsPath(tabletId));
}

std::string PersistentVars::logPrefix() const {
  // No need to lock to read const members.
  return fmt::format("T {} P {}: ", tablet_id_, peer_uuid_);
}

} // namespace kudu::consensus
