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

#include <atomic>
#include <cstdint>
#include <deque>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/threading/thread_collision_warner.h"

namespace kudu {

class FsManager;
class Status;

namespace consensus {

class ConsensusMetadataManager; // IWYU pragma: keep
class ConsensusMetadataTest; // IWYU pragma: keep

enum class ConsensusMetadataCreateMode {
  FlushOnCreate,
  NoFlushOnCreate,
};

// Provides methods to read, write, and persist consensus-related metadata.
// This partly corresponds to Raft Figure 2's "Persistent state on all servers".
//
// In addition to the persistent state, this class also provides access to some
// transient state. This includes the peer that this node considers to be the
// leader of the configuration, as well as the "pending" configuration, if any.
//
// Conceptually, a pending configuration is one that has been proposed via a
// config change operation (AddServer or RemoveServer from Chapter 4 of Diego
// Ongaro's Raft thesis) but has not yet been committed. According to the above
// spec, as soon as a server hears of a new cluster membership configuration, it
// must be adopted (even prior to be committed).
//
// The data structure difference between a committed configuration and a pending
// one is that opid_index (the index in the log of the committed config change
// operation) is always set in a committed configuration, while it is always
// unset in a pending configuration.
//
// Finally, this class exposes the concept of an "active" configuration, which
// means the pending configuration if a pending configuration is set, otherwise
// the committed configuration.
//
// This class is not thread-safe and requires external synchronization.
class ConsensusMetadata {
 public:
  // Specify whether we are allowed to overwrite an existing file when flushing.
  enum FlushMode { kOverwrite, kNoOverwrite };

  // Accessors for current term.
  int64_t currentTerm() const;
  void setCurrentTerm(int64_t term);

  // Accessors for voted_for.
  bool hasVotedFor() const;
  const std::string& votedFor() const;
  void clearVotedFor();
  void setVotedFor(const std::string& uuid);

  // Returns true iff peer with specified uuid is a voter in the specified
  // local Raft config.
  bool isVoterInConfig(const std::string& uuid, RaftConfigState type);

  // Returns true iff peer with specified uuid is a member of the specified
  // local Raft config.
  bool isMemberInConfig(const std::string& uuid, RaftConfigState type);

  // Check that the member is in config and if it is part of the config,
  // retrieve some key information about the member
  bool isMemberInConfigWithDetail(
      const std::string& uuid,
      RaftConfigState type,
      std::string* hostnamePort,
      bool* isVoter,
      std::string* quorumId);

  // Returns a count of the number of voters in the specified local Raft
  // config.
  int countVotersInConfig(RaftConfigState type);

  // Returns the opid_index of the specified local Raft config.
  int64_t getConfigOpIdIndex(RaftConfigState type);

  // Accessors for committed configuration.
  const RaftConfigPB& committedConfig() const;
  void setCommittedConfig(const RaftConfigPB& config);

  // Same as above but dont update active role
  void setCommittedConfigRaw(const RaftConfigPB& config);

  // Getter for Voter Distribution map
  Status voterDistribution(std::map<std::string, int32_t>* vd) const;

  // Returns whether a pending configuration is set.
  bool hasPendingConfig() const;

  // Returns the pending configuration if one is set. Otherwise, fires a DCHECK.
  const RaftConfigPB& pendingConfig() const;

  // Set & clear the pending configuration.
  void clearPendingConfig();
  void setPendingConfig(const RaftConfigPB& config);

  void setActiveConfig(const RaftConfigPB& config);

  // If a pending configuration is set, return it.
  // Otherwise, return the committed configuration.
  const RaftConfigPB& activeConfig() const;

  // Accessors for setting the active leader.
  const std::string& leaderUuid() const;
  void setLeaderUuid(std::string uuid);
  Status syncLastKnownLeader(std::optional<int64_t> casTerm = {});

  // Accessor for last known leader. It's not necessarily an active leader.
  // Used for computation of quorums for flexiraft leader elections.
  LastKnownLeaderPB lastKnownLeader() const;

  // Getter for PreviousVote.
  std::map<int64_t, PreviousVotePB> previousVoteHistory() const;

  // Getter for the last term that was pruned from the voting history.
  // Returns -1 if no term was pruned.
  int64_t lastPrunedTerm() const;

  std::pair<std::string, unsigned int> leaderHostport() const;

  // Returns the currently active role of the current node.
  RaftPeerPB::Role activeRole() const;

  Status getConfigMemberCopy(const std::string& uuid, RaftPeerPB* member);

  // Copy the stored state into a ConsensusStatePB object.
  // To get the active configuration, specify 'type' = ACTIVE.
  // Otherwise, 'type' = COMMITTED will return a version of the
  // ConsensusStatePB using only the committed configuration. In this case, if
  // the current leader is not a member of the committed configuration, then the
  // leader_uuid field of the returned ConsensusStatePB will be cleared.
  ConsensusStatePB toConsensusStatePB() const;

  // Merge the committed portion of the consensus state from the source node
  // during tablet copy.
  //
  // This method will clear any pending config change, replace the committed
  // consensus config with the one in 'cstate', and clear the currently
  // tracked leader.
  //
  // It will also check whether the current term passed in 'cstate'
  // is greater than the currently recorded one. If so, it will update the
  // local current term to match the passed one and it will clear the voting
  // record for this node. If the current term in 'cstate' is less
  // than the locally recorded term, the locally recorded term and voting
  // record are not changed.
  void mergeCommittedConsensusStatePB(const ConsensusStatePB& cstate);

  // Persist current state of the protobuf to disk.
  Status flush(FlushMode flushMode = kOverwrite);

  int64_t flushCountForTests() const {
    return flushCountForTests_;
  }

  // The on-disk size of the consensus metadata, as of the last call to
  // load() or flush(). This method is thread-safe.
  int64_t onDiskSize() const {
    return onDiskSize_.load(std::memory_order_relaxed);
  }

  // Adds all the peerUuid's in 'removedPeers' to the internal list
  // (removedPeers_) tracking peers that have been removed from the active
  // config. 'removedPeers_' can only track 'max_removed_peers' peers. So, the
  // earliest peers are evicted from the list (if needed)
  void insertIntoRemovedPeersList(const std::vector<std::string>& removedPeers);

  // Returns true if 'peerUuid' is present in 'removedPeers_' list
  bool isPeerRemoved(const std::string& peerUuid);

  // Deletes all the uuids in 'peerUuids' from 'removedPeers_' list
  void deleteFromRemovedPeersList(const std::vector<std::string>& peerUuids);

  // Deletes 'peerUuid' frpm 'removedPeers_' list
  void deleteFromRemovedPeersList(const std::string& peerUuid);

  // Clears the 'removedPeers_' list
  void clearRemovedPeersList();

  // Returns a copy of 'removedPeers_' list
  std::vector<std::string> removedPeersList();

 private:
  friend class ConsensusMetadataManager;

  FRIEND_TEST(ConsensusMetadataTest, TestCreateLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestDeferredCreateLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestCreateNoOverwrite);
  FRIEND_TEST(ConsensusMetadataTest, TestFailedLoad);
  FRIEND_TEST(ConsensusMetadataTest, TestFlush);
  FRIEND_TEST(ConsensusMetadataTest, TestActiveRole);
  FRIEND_TEST(ConsensusMetadataTest, TestToConsensusStatePB);
  FRIEND_TEST(ConsensusMetadataTest, TestMergeCommittedConsensusStatePB);

  static const int32_t kVoteHistoryMaxSize = 100;

  ConsensusMetadata(
      FsManager* fsManager,
      std::string tabletId,
      std::string peerUuid);

  // Create a ConsensusMetadata object with provided initial state.
  // If 'createMode' is set to FlushOnCreate, the encoded PB is flushed to
  // disk before returning. Otherwise, if 'createMode' is set to
  // NoFlushOnCreate, the caller must explicitly call flush() on the
  // returned object to get the bytes onto disk.
  static Status create(
      FsManager* fsManager,
      const std::string& tabletId,
      const std::string& peerUuid,
      const RaftConfigPB& config,
      int64_t currentTerm,
      ConsensusMetadataCreateMode createMode =
          ConsensusMetadataCreateMode::FlushOnCreate,
      std::shared_ptr<ConsensusMetadata>* cmetaOut = nullptr);

  // Load a ConsensusMetadata object from disk.
  // Returns Status::NotFound if the file could not be found. May return other
  // Status codes if unable to read the file.
  static Status load(
      FsManager* fsManager,
      const std::string& tabletId,
      const std::string& peerUuid,
      std::shared_ptr<ConsensusMetadata>* cmetaOut = nullptr);

  // Delete the ConsensusMetadata file associated with the given tablet from
  // disk. Returns Status::NotFound if the on-disk data is not found.
  static Status deleteOnDiskData(
      FsManager* fsManager,
      const std::string& tabletId);

  // Return the specified config.
  const RaftConfigPB& getConfig(RaftConfigState type) const;

  // Helper function to extend previousVoteHistory_
  void populatePreviousVoteHistory(const PreviousVotePB& prevVote);

  std::string LogPrefix() const;

  // Updates the cached active role.
  void updateActiveRole();

  // Updates the cached on-disk size of the consensus metadata.
  Status updateOnDiskSize();

  FsManager* const fsManager_;
  const std::string tabletId_;
  const std::string peerUuid_;

  // This fake mutex helps ensure that this ConsensusMetadata object stays
  // externally synchronized.
  DFAKE_MUTEX(fakeLock_);

  std::string
      leaderUuid_; // Leader of the current term (term == pb_.current_term).

  bool hasPendingConfig_; // Indicates whether there is an as-yet uncommitted
                          // configuration change pending.
  // RaftConfig used by the peers when there is a pending config change
  // operation.
  RaftConfigPB pendingConfig_;

  // Cached role of the peerUuid_ within the active configuration.
  RaftPeerPB::Role activeRole_;

  // The number of times the metadata has been flushed to disk.
  int64_t flushCountForTests_;

  // Durable fields.
  ConsensusMetadataPB pb_;

  // The on-disk size of the consensus metadata, as of the last call to
  // load() or flush().
  // The type is int64_t for consistency with other on-disk size metrics,
  // as opposed to uint64_t, which is the return type of the underlying function
  // used to populate this value.
  std::atomic<int64_t> onDiskSize_;

  // Tracks the last 'kMaxRemovedPeers' peers that have been removed
  // from the config
  static const int kMaxRemovedPeers = 30;
  std::deque<std::string> removedPeers_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadata);
};

} // namespace consensus
} // namespace kudu
