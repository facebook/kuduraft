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
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  DCHECK(pb_.has_current_term());
  return pb_.current_term();
}

void ConsensusMetadata::setCurrentTerm(int64_t term) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  DCHECK_GE(term, kMinimumTerm);
  pb_.set_current_term(term);
}

bool ConsensusMetadata::hasVotedFor() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return pb_.has_voted_for();
}

const string& ConsensusMetadata::votedFor() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  DCHECK(pb_.has_voted_for());
  return pb_.voted_for();
}

void ConsensusMetadata::clearVotedFor() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  pb_.clear_voted_for();
}

void ConsensusMetadata::populatePreviousVoteHistory(
    const PreviousVotePB& prevVote) {
  google::protobuf::Map<int64_t, PreviousVotePB>* previousVoteHistory =
      pb_.mutable_previous_vote_history();
  previousVoteHistory->insert({prevVote.election_term(), prevVote});

  int64_t termToPruneTo = pb_.last_known_leader().election_term();

  if (previousVoteHistory->size() > kVoteHistoryMaxSize) {
    std::vector<int64_t> terms;
    terms.reserve(previousVoteHistory->size());
    for (const auto& [term, _] : *previousVoteHistory) {
      terms.push_back(term);
    }
    std::sort(terms.begin(), terms.end(), std::greater<int64_t>());
    termToPruneTo = std::max(termToPruneTo, terms[kVoteHistoryMaxSize]);
  }

  if (termToPruneTo <= pb_.last_pruned_term()) {
    return;
  }
  VLOG_WITH_PREFIX(2) << "Pruning history older than: " << termToPruneTo;

  pb_.set_last_pruned_term(termToPruneTo);
  for (google::protobuf::Map<int64_t, PreviousVotePB>::iterator it =
           previousVoteHistory->begin();
       it != previousVoteHistory->end();) {
    if (it->first <= termToPruneTo) {
      it = previousVoteHistory->erase(it);
    } else {
      it++;
    }
  }
}

void ConsensusMetadata::setVotedFor(const string& uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  DCHECK(!uuid.empty());
  pb_.set_voted_for(uuid);

  // Populate previous vote information.
  DCHECK(pb_.has_current_term());
  PreviousVotePB prevVote;
  prevVote.set_candidate_uuid(uuid);
  prevVote.set_election_term(pb_.current_term());
  populatePreviousVoteHistory(prevVote);
}

bool ConsensusMetadata::isVoterInConfig(
    const string& uuid,
    RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return isRaftConfigVoter(uuid, getConfig(type));
}

bool ConsensusMetadata::isMemberInConfig(
    const string& uuid,
    RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return isRaftConfigMember(uuid, getConfig(type));
}

bool ConsensusMetadata::isMemberInConfigWithDetail(
    const std::string& uuid,
    RaftConfigState type,
    std::string* hostnamePort,
    bool* isVoter,
    std::string* quorumId) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return isRaftConfigMemberWithDetail(
      uuid, getConfig(type), hostnamePort, isVoter, quorumId);
}

int ConsensusMetadata::countVotersInConfig(RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return countVoters(getConfig(type));
}

int64_t ConsensusMetadata::getConfigOpIdIndex(RaftConfigState type) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return getConfig(type).opid_index();
}

const RaftConfigPB& ConsensusMetadata::committedConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return getConfig(kCommittedConfig);
}

const RaftConfigPB& ConsensusMetadata::getConfig(RaftConfigState type) const {
  switch (type) {
    case kActiveConfig:
      if (hasPendingConfig_) {
        return pendingConfig_;
      }
      DCHECK(pb_.has_committed_config());
      return pb_.committed_config();
    case kCommittedConfig:
      DCHECK(pb_.has_committed_config());
      return pb_.committed_config();
    case kPendingConfig:
      CHECK(hasPendingConfig_) << logPrefix() << "There is no pending config";
      return pendingConfig_;
    default:
      LOG(FATAL) << "Unknown RaftConfigState type: " << type;
  }
}

void ConsensusMetadata::setCommittedConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  *pb_.mutable_committed_config() = config;
  if (!hasPendingConfig_) {
    updateActiveRole();
  }
}

void ConsensusMetadata::setCommittedConfigRaw(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  *pb_.mutable_committed_config() = config;
}

kudu::Status ConsensusMetadata::voterDistribution(
    std::map<std::string, int32_t>* vd) const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
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
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return hasPendingConfig_;
}

const RaftConfigPB& ConsensusMetadata::pendingConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return getConfig(kPendingConfig);
  ;
}

void ConsensusMetadata::clearPendingConfig() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  hasPendingConfig_ = false;
  pendingConfig_.Clear();
  updateActiveRole();
}

void ConsensusMetadata::setPendingConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  hasPendingConfig_ = true;
  pendingConfig_ = config;
  updateActiveRole();
}

void ConsensusMetadata::setActiveConfig(const RaftConfigPB& config) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  if (hasPendingConfig_) {
    setPendingConfig(config);
  } else {
    setCommittedConfig(config);
  }
}

const RaftConfigPB& ConsensusMetadata::activeConfig() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return getConfig(kActiveConfig);
}

const string& ConsensusMetadata::leaderUuid() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return leaderUuid_;
}

LastKnownLeaderPB ConsensusMetadata::lastKnownLeader() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return pb_.last_known_leader();
}

std::map<int64_t, PreviousVotePB> ConsensusMetadata::previousVoteHistory()
    const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  std::map<int64_t, PreviousVotePB> pvh;
  pvh.insert(
      pb_.previous_vote_history().begin(), pb_.previous_vote_history().end());
  return pvh;
}

int64_t ConsensusMetadata::lastPrunedTerm() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return pb_.last_pruned_term();
}

void ConsensusMetadata::setLeaderUuid(string uuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  leaderUuid_ = std::move(uuid);
  updateActiveRole();
  // cmeta not persisted untill we sync to LKL
}

Status ConsensusMetadata::syncLastKnownLeader(std::optional<int64_t> casTerm) {
  // Only update last_known_leader when the current node
  // 1) has won a leader election (LEADER)
  // 2) receives AppendEntries from a legitimate leader (FOLLOWER)
  if (leaderUuid_.empty()) {
    return Status::OK();
  }
  DCHECK(pb_.has_current_term());
  int64_t curTerm = pb_.current_term();
  if (casTerm && curTerm != *casTerm) {
    LOG(INFO) << "Compare and swap on LKL term mismatch. Supplied term: "
              << *casTerm << ", current term: " << curTerm
              << ". Will not update LKL";
    return Status::OK();
  }
  LOG(INFO) << "LKL updated to " << leaderUuid_ << " for term: " << curTerm;
  pb_.mutable_last_known_leader()->set_uuid(leaderUuid_);
  pb_.mutable_last_known_leader()->set_election_term(curTerm);
  return flush();
}

std::pair<string, unsigned int> ConsensusMetadata::leaderHostport() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  for (const RaftPeerPB& peer : activeConfig().peers()) {
    if (peer.permanent_uuid() == leaderUuid_ && peer.has_last_known_addr()) {
      const ::kudu::HostPortPB& hostPort = peer.last_known_addr();
      return std::make_pair(hostPort.host(), hostPort.port());
    }
  }
  return {};
}

Status ConsensusMetadata::getConfigMemberCopy(
    const std::string& uuid,
    RaftPeerPB* member) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  for (const RaftPeerPB& peer : activeConfig().peers()) {
    if (peer.permanent_uuid() == uuid) {
      *member = peer;
      return Status::OK();
    }
  }
  return Status::NotFound(
      fmt::format("Peer with uuid {} not found in consensus config", uuid));
}

RaftPeerPB::Role ConsensusMetadata::activeRole() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  return activeRole_;
}

ConsensusStatePB ConsensusMetadata::toConsensusStatePB() const {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
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

void ConsensusMetadata::mergeCommittedConsensusStatePB(
    const ConsensusStatePB& cstate) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  if (cstate.current_term() > currentTerm()) {
    setCurrentTerm(cstate.current_term());
    clearVotedFor();
  }

  setLeaderUuid("");
  setCommittedConfig(cstate.committed_config());
  clearPendingConfig();
}

Status ConsensusMetadata::flush(FlushMode flushMode) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  MAYBE_FAULT(FLAGS_fault_crash_before_cmeta_flush);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(
      WARNING, 500, logPrefix(), "flushing consensus metadata");

  flushCountForTests_++;
  // Sanity test to ensure we never write out a bad configuration.
  RETURN_NOT_OK_PREPEND(
      verifyRaftConfig(pb_.committed_config()),
      "Invalid config in ConsensusMetadata, cannot flush to disk");

  // Create directories if needed.
  string dir = fsManager_->GetConsensusMetadataDir();
  bool createdDir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::createDirIfMissing(fsManager_->env(), dir, &createdDir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(createdDir)) {
    string parentDir = dirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parentDir),
        "Unable to fsync consensus parent dir " + parentDir);
  }

  string metaFilePath = fsManager_->GetConsensusMetadataPath(tabletId_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fsManager_->env(),
          metaFilePath,
          pb_,
          flushMode == kOverwrite ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write consensus meta file for tablet {} to path {}",
          tabletId_,
          metaFilePath));
  RETURN_NOT_OK(updateOnDiskSize());
  return Status::OK();
}

ConsensusMetadata::ConsensusMetadata(
    FsManager* fsManager,
    std::string tabletId,
    std::string peerUuid)
    : fsManager_(CHECK_NOTNULL(fsManager)),
      tabletId_(std::move(tabletId)),
      peerUuid_(std::move(peerUuid)),
      hasPendingConfig_(false),
      flushCountForTests_(0),
      onDiskSize_(0) {
  // This is not really required as default values but specifying explicitly
  // since correctness is dependent on it.
  pb_.mutable_last_known_leader()->set_uuid("");
  pb_.mutable_last_known_leader()->set_election_term(0);
  pb_.set_last_pruned_term(-1);
}

Status ConsensusMetadata::create(
    FsManager* fsManager,
    const string& tabletId,
    const std::string& peerUuid,
    const RaftConfigPB& config,
    int64_t currentTerm,
    ConsensusMetadataCreateMode createMode,
    std::shared_ptr<ConsensusMetadata>* cmetaOut) {
  std::shared_ptr<ConsensusMetadata> cmeta(
      new ConsensusMetadata(fsManager, tabletId, peerUuid));
  cmeta->setCommittedConfig(config);
  cmeta->setCurrentTerm(currentTerm);

  if (createMode == ConsensusMetadataCreateMode::FlushOnCreate) {
    RETURN_NOT_OK(cmeta->flush(kNoOverwrite)); // create() should not clobber.
  } else {
    // Sanity check: ensure that there is no cmeta file currently on disk.
    const string& path = fsManager->GetConsensusMetadataPath(tabletId);
    if (fsManager->env()->FileExists(path)) {
      return Status::AlreadyPresent(
          fmt::format("File {} already exists", path));
    }
  }
  if (cmetaOut) {
    *cmetaOut = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadata::load(
    FsManager* fsManager,
    const std::string& tabletId,
    const std::string& peerUuid,
    std::shared_ptr<ConsensusMetadata>* cmetaOut) {
  std::shared_ptr<ConsensusMetadata> cmeta(
      new ConsensusMetadata(fsManager, tabletId, peerUuid));
  RETURN_NOT_OK(
      pb_util::ReadPBContainerFromPath(
          fsManager->env(),
          fsManager->GetConsensusMetadataPath(tabletId),
          &cmeta->pb_));
  cmeta->updateActiveRole(); // Needs to happen here as we sidestep the accessor
                             // APIs.

  RETURN_NOT_OK(cmeta->updateOnDiskSize());
  if (cmetaOut) {
    *cmetaOut = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadata::deleteOnDiskData(
    FsManager* fsManager,
    const string& tabletId) {
  string cmetaPath = fsManager->GetConsensusMetadataPath(tabletId);
  RETURN_NOT_OK_PREPEND(
      fsManager->env()->DeleteFile(cmetaPath),
      fmt::format(
          "Unable to delete consensus metadata file for tablet {}", tabletId));
  return Status::OK();
}

std::string ConsensusMetadata::logPrefix() const {
  // No need to lock to read const members.
  return fmt::format("T {} P {}: ", tabletId_, peerUuid_);
}

void ConsensusMetadata::updateActiveRole() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  activeRole_ = getConsensusRole(peerUuid_, leaderUuid_, activeConfig());
  VLOG_WITH_PREFIX(1) << "Updating active role to "
                      << RaftPeerPB::Role_Name(activeRole_)
                      << ". Consensus state: "
                      << pb_util::SecureShortDebugString(toConsensusStatePB());
}

Status ConsensusMetadata::updateOnDiskSize() {
  string path = fsManager_->GetConsensusMetadataPath(tabletId_);
  uint64_t diskSize;
  RETURN_NOT_OK(fsManager_->env()->GetFileSize(path, &diskSize));
  onDiskSize_ = diskSize;
  return Status::OK();
}

void ConsensusMetadata::insertIntoRemovedPeersList(
    const std::vector<std::string>& removedPeers) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);

  for (const auto& peerUuid : removedPeers) {
    // Sanity check again to ensure that the peer is not in active config
    if (!isMemberInConfig(peerUuid, kActiveConfig)) {
      if (removedPeers_.size() == kMaxRemovedPeers) {
        removedPeers_.pop_front();
      }
      removedPeers_.push_back(peerUuid);
    }
  }
}

bool ConsensusMetadata::isPeerRemoved(const std::string& peerUuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);

  // Sanity check in active config too
  if (isMemberInConfig(peerUuid, kActiveConfig)) {
    return false;
  }

  auto removed =
      std::find(std::begin(removedPeers_), std::end(removedPeers_), peerUuid);

  return (removed != std::end(removedPeers_));
}

void ConsensusMetadata::deleteFromRemovedPeersList(
    const std::string& peerUuid) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);

  for (auto it = removedPeers_.begin(); it != removedPeers_.end();) {
    if (peerUuid == *it) {
      removedPeers_.erase(it);
    } else {
      it++;
    }
  }
}

void ConsensusMetadata::deleteFromRemovedPeersList(
    const std::vector<std::string>& peerUuids) {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);

  for (const auto& peerUuid : peerUuids) {
    deleteFromRemovedPeersList(peerUuid);
  }
}

void ConsensusMetadata::clearRemovedPeersList() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  removedPeers_.clear();
}

std::vector<std::string> ConsensusMetadata::removedPeersList() {
  DFAKE_SCOPED_RECURSIVE_LOCK(fakeLock_);
  std::vector<std::string> removedPeers(
      removedPeers_.begin(), removedPeers_.end());
  return removedPeers;
}

} // namespace kudu::consensus
