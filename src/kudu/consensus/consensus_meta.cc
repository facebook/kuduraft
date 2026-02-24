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
#include "kudu/consensus/consensus_meta.h"

#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

DEFINE_double(
    fault_crash_before_cmeta_flush,
    0.0,
    "Fraction of the time when the server will crash just before flushing "
    "consensus metadata. (For testing only!)");
TAG_FLAG(fault_crash_before_cmeta_flush, unsafe);

namespace kudu::consensus {

using std::string;

int64_t ConsensusMetadata::currentTerm() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  DCHECK(pb_.has_current_term());
  return pb_.current_term();
}

void ConsensusMetadata::setCurrentTerm(int64_t term) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  DCHECK_GE(term, kMinimumTerm);
  pb_.set_current_term(term);
}

bool ConsensusMetadata::hasVotedFor() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return pb_.has_voted_for();
}

const string& ConsensusMetadata::votedFor() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  DCHECK(pb_.has_voted_for());
  return pb_.voted_for();
}

void ConsensusMetadata::clearVotedFor() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  pb_.clear_voted_for();
}

void ConsensusMetadata::populatePreviousVoteHistory(
    const PreviousVotePB& prev_vote) {
  google::protobuf::Map<int64_t, PreviousVotePB>* previous_vote_history =
      pb_.mutable_previous_vote_history();
  previous_vote_history->insert({prev_vote.election_term(), prev_vote});

  int64_t term_to_prune_to = pb_.last_known_leader().election_term();

  if (previous_vote_history->size() > kVoteHistoryMaxSize) {
    std::vector<int64_t> terms;
    terms.reserve(previous_vote_history->size());
    for (const auto& [term, _] : *previous_vote_history) {
      terms.push_back(term);
    }
    std::sort(terms.begin(), terms.end(), std::greater<int64_t>());
    term_to_prune_to = std::max(term_to_prune_to, terms[kVoteHistoryMaxSize]);
  }

  if (term_to_prune_to <= pb_.last_pruned_term()) {
    return;
  }
  VLOG_WITH_PREFIX(2) << "Pruning history older than: " << term_to_prune_to;

  pb_.set_last_pruned_term(term_to_prune_to);
  for (google::protobuf::Map<int64_t, PreviousVotePB>::iterator it =
           previous_vote_history->begin();
       it != previous_vote_history->end();) {
    if (it->first <= term_to_prune_to) {
      it = previous_vote_history->erase(it);
    } else {
      it++;
    }
  }
}

void ConsensusMetadata::setVotedFor(const string& uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  DCHECK(!uuid.empty());
  pb_.set_voted_for(uuid);

  // Populate previous vote information.
  DCHECK(pb_.has_current_term());
  PreviousVotePB prev_vote;
  prev_vote.set_candidate_uuid(uuid);
  prev_vote.set_election_term(pb_.current_term());
  populatePreviousVoteHistory(prev_vote);
}

bool ConsensusMetadata::isVoterInConfig(
    const string& uuid,
    RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return isRaftConfigVoter(uuid, GetConfig(type));
}

bool ConsensusMetadata::isMemberInConfig(
    const string& uuid,
    RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return isRaftConfigMember(uuid, GetConfig(type));
}

bool ConsensusMetadata::isMemberInConfigWithDetail(
    const std::string& uuid,
    RaftConfigState type,
    std::string* hostname_port,
    bool* is_voter,
    std::string* quorum_id) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return isRaftConfigMemberWithDetail(
      uuid, GetConfig(type), hostname_port, is_voter, quorum_id);
}

int ConsensusMetadata::countVotersInConfig(RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return countVoters(GetConfig(type));
}

int64_t ConsensusMetadata::getConfigOpIdIndex(RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return GetConfig(type).opid_index();
}

const RaftConfigPB& ConsensusMetadata::committedConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return GetConfig(COMMITTED_CONFIG);
}

const RaftConfigPB& ConsensusMetadata::GetConfig(RaftConfigState type) const {
  switch (type) {
    case ACTIVE_CONFIG:
      if (hasPendingConfig_) {
        return pendingConfig_;
      }
      DCHECK(pb_.has_committed_config());
      return pb_.committed_config();
    case COMMITTED_CONFIG:
      DCHECK(pb_.has_committed_config());
      return pb_.committed_config();
    case PENDING_CONFIG:
      CHECK(hasPendingConfig_) << LogPrefix() << "There is no pending config";
      return pendingConfig_;
    default:
      LOG(FATAL) << "Unknown RaftConfigState type: " << type;
  }
}

void ConsensusMetadata::setCommittedConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  *pb_.mutable_committed_config() = config;
  if (!hasPendingConfig_) {
    UpdateActiveRole();
  }
}

void ConsensusMetadata::setCommittedConfigRaw(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  *pb_.mutable_committed_config() = config;
}

kudu::Status ConsensusMetadata::voterDistribution(
    std::map<std::string, int32_t>* vd) const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (!pb_.has_committed_config()) {
    return kudu::Status::NotFound(
        "Committed config not present to get voter distribution");
  }
  vd->insert(
      pb_.committed_config().voter_distribution().begin(),
      pb_.committed_config().voter_distribution().end());
  return kudu::Status::OK();
}

bool ConsensusMetadata::hasPendingConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return hasPendingConfig_;
}

const RaftConfigPB& ConsensusMetadata::PendingConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return GetConfig(PENDING_CONFIG);
  ;
}

void ConsensusMetadata::clearPendingConfig() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  hasPendingConfig_ = false;
  pendingConfig_.Clear();
  UpdateActiveRole();
}

void ConsensusMetadata::setPendingConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  hasPendingConfig_ = true;
  pendingConfig_ = config;
  UpdateActiveRole();
}

void ConsensusMetadata::setActiveConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (hasPendingConfig_) {
    setPendingConfig(config);
  } else {
    setCommittedConfig(config);
  }
}

const RaftConfigPB& ConsensusMetadata::ActiveConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return GetConfig(ACTIVE_CONFIG);
}

const string& ConsensusMetadata::leaderUuid() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return leaderUuid_;
}

LastKnownLeaderPB ConsensusMetadata::lastKnownLeader() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return pb_.last_known_leader();
}

std::map<int64_t, PreviousVotePB> ConsensusMetadata::previousVoteHistory()
    const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  std::map<int64_t, PreviousVotePB> pvh;
  pvh.insert(
      pb_.previous_vote_history().begin(), pb_.previous_vote_history().end());
  return pvh;
}

int64_t ConsensusMetadata::lastPrunedTerm() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return pb_.last_pruned_term();
}

void ConsensusMetadata::setLeaderUuid(string uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  leaderUuid_ = std::move(uuid);
  UpdateActiveRole();
  // cmeta not persisted untill we sync to LKL
}

Status ConsensusMetadata::syncLastKnownLeader(std::optional<int64_t> cas_term) {
  // Only update last_known_leader when the current node
  // 1) has won a leader election (LEADER)
  // 2) receives AppendEntries from a legitimate leader (FOLLOWER)
  if (leaderUuid_.empty()) {
    return Status::OK();
  }
  DCHECK(pb_.has_current_term());
  int64_t current_term = pb_.current_term();
  if (cas_term && current_term != *cas_term) {
    LOG(INFO) << "Compare and swap on LKL term mismatch. Supplied term: "
              << *cas_term << ", current term: " << current_term
              << ". Will not update LKL";
    return Status::OK();
  }
  LOG(INFO) << "LKL updated to " << leaderUuid_
            << " for term: " << current_term;
  pb_.mutable_last_known_leader()->set_uuid(leaderUuid_);
  pb_.mutable_last_known_leader()->set_election_term(current_term);
  return Flush();
}

std::pair<string, unsigned int> ConsensusMetadata::leaderHostport() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  for (const RaftPeerPB& peer : ActiveConfig().peers()) {
    if (peer.permanent_uuid() == leaderUuid_ && peer.has_last_known_addr()) {
      const ::kudu::HostPortPB& host_port = peer.last_known_addr();
      return std::make_pair(host_port.host(), host_port.port());
    }
  }
  return {};
}

Status ConsensusMetadata::GetConfigMemberCopy(
    const std::string& uuid,
    RaftPeerPB* member) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  for (const RaftPeerPB& peer : ActiveConfig().peers()) {
    if (peer.permanent_uuid() == uuid) {
      *member = peer;
      return Status::OK();
    }
  }
  return Status::NotFound(
      fmt::format("Peer with uuid {} not found in consensus config", uuid));
}

RaftPeerPB::Role ConsensusMetadata::activeRole() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  return activeRole_;
}

ConsensusStatePB ConsensusMetadata::ToConsensusStatePB() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  ConsensusStatePB cstate;
  cstate.set_current_term(pb_.current_term());
  if (!leaderUuid_.empty()) {
    cstate.set_leader_uuid(leaderUuid_);
  }
  *cstate.mutable_committed_config() = committedConfig();
  if (hasPendingConfig_) {
    *cstate.mutable_pending_config() = pendingConfig_;
  }
  return cstate;
}

void ConsensusMetadata::MergeCommittedConsensusStatePB(
    const ConsensusStatePB& cstate) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  if (cstate.current_term() > currentTerm()) {
    setCurrentTerm(cstate.current_term());
    clearVotedFor();
  }

  setLeaderUuid("");
  setCommittedConfig(cstate.committed_config());
  clearPendingConfig();
}

Status ConsensusMetadata::Flush(FlushMode flush_mode) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  MAYBE_FAULT(FLAGS_fault_crash_before_cmeta_flush);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(
      WARNING, 500, LogPrefix(), "flushing consensus metadata");

  flush_count_for_tests_++;
  // Sanity test to ensure we never write out a bad configuration.
  RETURN_NOT_OK_PREPEND(
      verifyRaftConfig(pb_.committed_config()),
      "Invalid config in ConsensusMetadata, cannot flush to disk");

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::createDirIfMissing(fs_manager_->env(), dir, &created_dir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parent_dir),
        "Unable to fsync consensus parent dir " + parent_dir);
  }

  string meta_file_path = fs_manager_->GetConsensusMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fs_manager_->env(),
          meta_file_path,
          pb_,
          flush_mode == kOverwrite ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write consensus meta file for tablet {} to path {}",
          tablet_id_,
          meta_file_path));
  RETURN_NOT_OK(UpdateOnDiskSize());
  return Status::OK();
}

ConsensusMetadata::ConsensusMetadata(
    FsManager* fs_manager,
    std::string tablet_id,
    std::string peer_uuid)
    : fs_manager_(CHECK_NOTNULL(fs_manager)),
      tablet_id_(std::move(tablet_id)),
      peer_uuid_(std::move(peer_uuid)),
      hasPendingConfig_(false),
      flush_count_for_tests_(0),
      on_disk_size_(0) {
  // This is not really required as default values but specifying explicitly
  // since correctness is dependent on it.
  pb_.mutable_last_known_leader()->set_uuid("");
  pb_.mutable_last_known_leader()->set_election_term(0);
  pb_.set_last_pruned_term(-1);
}

Status ConsensusMetadata::Create(
    FsManager* fs_manager,
    const string& tablet_id,
    const std::string& peer_uuid,
    const RaftConfigPB& config,
    int64_t current_term,
    ConsensusMetadataCreateMode create_mode,
    std::shared_ptr<ConsensusMetadata>* cmeta_out) {
  std::shared_ptr<ConsensusMetadata> cmeta(
      new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  cmeta->setCommittedConfig(config);
  cmeta->setCurrentTerm(current_term);

  if (create_mode == ConsensusMetadataCreateMode::FlushOnCreate) {
    RETURN_NOT_OK(cmeta->Flush(kNoOverwrite)); // Create() should not clobber.
  } else {
    // Sanity check: ensure that there is no cmeta file currently on disk.
    const string& path = fs_manager->GetConsensusMetadataPath(tablet_id);
    if (fs_manager->env()->FileExists(path)) {
      return Status::AlreadyPresent(
          fmt::format("File {} already exists", path));
    }
  }
  if (cmeta_out) {
    *cmeta_out = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadata::Load(
    FsManager* fs_manager,
    const std::string& tablet_id,
    const std::string& peer_uuid,
    std::shared_ptr<ConsensusMetadata>* cmeta_out) {
  std::shared_ptr<ConsensusMetadata> cmeta(
      new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  RETURN_NOT_OK(
      pb_util::ReadPBContainerFromPath(
          fs_manager->env(),
          fs_manager->GetConsensusMetadataPath(tablet_id),
          &cmeta->pb_));
  cmeta->UpdateActiveRole(); // Needs to happen here as we sidestep the accessor
                             // APIs.

  RETURN_NOT_OK(cmeta->UpdateOnDiskSize());
  if (cmeta_out) {
    *cmeta_out = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadata::DeleteOnDiskData(
    FsManager* fs_manager,
    const string& tablet_id) {
  string cmeta_path = fs_manager->GetConsensusMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(
      fs_manager->env()->DeleteFile(cmeta_path),
      fmt::format(
          "Unable to delete consensus metadata file for tablet {}", tablet_id));
  return Status::OK();
}

std::string ConsensusMetadata::LogPrefix() const {
  // No need to lock to read const members.
  return fmt::format("T {} P {}: ", tablet_id_, peer_uuid_);
}

void ConsensusMetadata::UpdateActiveRole() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  activeRole_ = getConsensusRole(peer_uuid_, leaderUuid_, ActiveConfig());
  VLOG_WITH_PREFIX(1) << "Updating active role to "
                      << RaftPeerPB::Role_Name(activeRole_)
                      << ". Consensus state: "
                      << pb_util::SecureShortDebugString(ToConsensusStatePB());
}

Status ConsensusMetadata::UpdateOnDiskSize() {
  string path = fs_manager_->GetConsensusMetadataPath(tablet_id_);
  uint64_t on_disk_size;
  RETURN_NOT_OK(fs_manager_->env()->GetFileSize(path, &on_disk_size));
  on_disk_size_ = on_disk_size;
  return Status::OK();
}

void ConsensusMetadata::InsertIntoRemovedPeersList(
    const std::vector<std::string>& removed_peers) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);

  for (const auto& peer_uuid : removed_peers) {
    // Sanity check again to ensure that the peer is not in active config
    if (!isMemberInConfig(peer_uuid, ACTIVE_CONFIG)) {
      if (removed_peers_.size() == kMaxRemovedPeers) {
        removed_peers_.pop_front();
      }
      removed_peers_.push_back(peer_uuid);
    }
  }
}

bool ConsensusMetadata::IsPeerRemoved(const std::string& peer_uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);

  // Sanity check in active config too
  if (isMemberInConfig(peer_uuid, ACTIVE_CONFIG)) {
    return false;
  }

  auto removed = std::find(
      std::begin(removed_peers_), std::end(removed_peers_), peer_uuid);

  return (removed != std::end(removed_peers_));
}

void ConsensusMetadata::DeleteFromRemovedPeersList(
    const std::string& peer_uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);

  for (auto it = removed_peers_.begin(); it != removed_peers_.end();) {
    if (peer_uuid == *it) {
      removed_peers_.erase(it);
    } else {
      it++;
    }
  }
}

void ConsensusMetadata::DeleteFromRemovedPeersList(
    const std::vector<std::string>& peer_uuids) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);

  for (const auto& peer_uuid : peer_uuids) {
    DeleteFromRemovedPeersList(peer_uuid);
  }
}

void ConsensusMetadata::ClearRemovedPeersList() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  removed_peers_.clear();
}

std::vector<std::string> ConsensusMetadata::RemovedPeersList() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fake_lock_);
  std::vector<std::string> removed_peers(
      removed_peers_.begin(), removed_peers_.end());
  return removed_peers;
}

} // namespace kudu::consensus
