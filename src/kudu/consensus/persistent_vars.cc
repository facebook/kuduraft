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

#include <atomic>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

namespace kudu {
namespace consensus {

using std::lock_guard;
using std::string;
using strings::Substitute;

bool PersistentVars::is_start_election_allowed() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  // allow_start_election is optional with default = true
  // So if it not present, we will allow start elections by default
  return pb_.allow_start_election();
}

void PersistentVars::set_allow_start_election(bool val) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  pb_.set_allow_start_election(val);
}

std::shared_ptr<const std::string> PersistentVars::raft_rpc_token() const {
  return std::atomic_load_explicit(&raft_rpc_token_cache_,
                                   std::memory_order_relaxed);
}

void PersistentVars::set_raft_rpc_token(
    boost::optional<std::string> rpc_token) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (rpc_token) {
    std::atomic_store_explicit(&raft_rpc_token_cache_,
                               std::make_shared<const std::string>(*rpc_token),
                               std::memory_order_relaxed);
    pb_.set_raft_rpc_token(*std::move(rpc_token));
  } else {
    std::atomic_store_explicit(&raft_rpc_token_cache_, {},
                               std::memory_order_relaxed);
    pb_.clear_raft_rpc_token();
  }
}

Status PersistentVars::Flush(FlushMode flush_mode) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(WARNING, 500, LogPrefix(), "flushing persistent variables");

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(
      fs_manager_->env(), dir, &created_dir),
                        "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(Env::Default()->SyncDir(parent_dir),
                          "Unable to fsync consensus parent dir " + parent_dir);
  }

  string persistent_vars_file_path = fs_manager_->GetPersistentVarsPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
      fs_manager_->env(), persistent_vars_file_path, pb_,
      flush_mode == OVERWRITE ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
      pb_util::SYNC),
          Substitute("Unable to write persistent vars file for tablet $0 to path $1",
                     tablet_id_, persistent_vars_file_path));
  return Status::OK();
}

PersistentVars::PersistentVars(FsManager* fs_manager,
                                     std::string tablet_id,
                                     std::string peer_uuid)
    : fs_manager_(CHECK_NOTNULL(fs_manager)),
      tablet_id_(std::move(tablet_id)),
      peer_uuid_(std::move(peer_uuid)) {}

Status PersistentVars::Create(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const std::string& peer_uuid,
                                 scoped_refptr<PersistentVars>* persistent_vars_out) {

  scoped_refptr<PersistentVars> persistent_vars(new PersistentVars(fs_manager, tablet_id, peer_uuid));

  RETURN_NOT_OK(persistent_vars->Flush(NO_OVERWRITE)); // Create() should not clobber.

  if (persistent_vars_out) *persistent_vars_out = std::move(persistent_vars);
  return Status::OK();
}

Status PersistentVars::Load(FsManager* fs_manager,
                               const std::string& tablet_id,
                               const std::string& peer_uuid,
                               scoped_refptr<PersistentVars>* persistent_vars_out) {
  scoped_refptr<PersistentVars> persistent_vars(new PersistentVars(fs_manager, tablet_id, peer_uuid));
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(fs_manager->env(),
                                                 fs_manager->GetPersistentVarsPath(tablet_id),
                                                 &persistent_vars->pb_));
  if (persistent_vars->pb_.has_raft_rpc_token()) {
    persistent_vars->raft_rpc_token_cache_ =
        std::make_shared<const std::string>(
            persistent_vars->pb_.raft_rpc_token());
  }
  if (persistent_vars_out) *persistent_vars_out = std::move(persistent_vars);
  return Status::OK();
}

bool PersistentVars::FileExists(FsManager* fs_manager, const std::string& tablet_id) {
  return fs_manager->env()->FileExists(fs_manager->GetPersistentVarsPath(tablet_id));
}

std::string PersistentVars::LogPrefix() const {
  // No need to lock to read const members.
  return Substitute("T $0 P $1: ", tablet_id_, peer_uuid_);
}

} // namespace consensus
} // namespace kudu
