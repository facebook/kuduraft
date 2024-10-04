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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#pragma once

#include <atomic>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <optional>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h" // IWYU pragma: keep
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/leader_election.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/persistent_vars.h"
#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/consensus/proxy_policy.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/routing.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"

#include "kudu/consensus/flags_layering.h"
#include "kudu/util/atomic.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/status_callback.h"

DECLARE_int32(lag_threshold_for_request_vote);

namespace facebook::datashuttle {
class KuduRingManager;
} // namespace facebook::datashuttle

namespace kudu {

using Lock = std::lock_guard<simple_mutexlock>;
using ScopedLock = std::unique_ptr<Lock>;

class Status;
class ThreadPool;
class ThreadPoolToken;
template <typename Sig>
class Callback;

namespace rpc {
class PeriodicTimer;
class RpcContext;
} // namespace rpc

namespace tserver {
class TSTabletManager;
class RaftConsensusManager;
class RaftConsensusInstance;
} // namespace tserver

namespace consensus {

class ConsensusMetadataManager;
class ConsensusRound;
class ConsensusRoundHandler;
class PeerManager;
class PeerProxyFactory;
class PersistentVarsManager;
class PendingRounds;
struct ConsensusBootstrapInfo;

struct ConsensusOptions {
  std::string tablet_id;
  ProxyPolicy proxy_policy;
  std::vector<std::unordered_set<std::string>> proxy_region_groups = {};
  std::optional<std::string> initial_raft_rpc_token;
};

struct TabletVotingState {
  std::optional<OpId> tombstone_last_logged_opid_;

  explicit TabletVotingState(std::optional<OpId> tombstone_last_logged_opid)
      : tombstone_last_logged_opid_(std::move(tombstone_last_logged_opid)) {}
};

using ConsensusReplicatedCallback = StdStatusCallback;

// Reasons for StartElection().
enum ElectionReason {
  // The election is being called because the Raft configuration has only
  // a single node and has just started up.
  INITIAL_SINGLE_NODE_ELECTION,

  // The election is being called because the timeout expired. In other
  // words, the previous leader probably failed (or there was no leader
  // in this term)
  ELECTION_TIMEOUT_EXPIRED,

  // The election is being started because of an explicit external request.
  EXTERNAL_REQUEST,

  // There was a failure to commit to quorum of nodes from the leader.
  FAILED_CHECK_QUORUM
};

struct ElectionContext {
  using Timepoint = const std::chrono::system_clock::time_point;

  ElectionContext(
      ElectionReason reason,
      Timepoint chained_start_time,
      std::optional<OpId> mock_election_snapshot_op_id = {})
      : reason_(reason),
        chained_start_time_(chained_start_time),
        is_origin_dead_promotion_(
            reason == ElectionReason::ELECTION_TIMEOUT_EXPIRED),
        mock_election_snapshot_op_id_(std::move(mock_election_snapshot_op_id)) {
  }

  ElectionContext(
      ElectionReason reason,
      Timepoint chained_start_time,
      std::optional<OpId> mock_election_snapshot_op_id,
      std::string source_uuid,
      bool is_origin_dead_promotion)
      : reason_(reason),
        chained_start_time_(chained_start_time),
        source_uuid_(std::move(source_uuid)),
        is_origin_dead_promotion_(is_origin_dead_promotion),
        mock_election_snapshot_op_id_(std::move(mock_election_snapshot_op_id)) {
  }

  PeerMessageQueue::TransferContext TransferContext() const;

  const ElectionReason reason_;

  // The time the current election started at
  const Timepoint start_time_ = std::chrono::system_clock::now();

  // If this election is preceeded by other elections considered as a single
  // event. E.g. Multiple chained promotions
  bool is_chained_election_ = false;

  // The time the first election in the  started
  const Timepoint chained_start_time_;

  // The UUID of the leader at the start of the election or election chain
  std::string source_uuid_;

  // The UUID of the leader at the start of the election. If election is not
  // a chain, this should be equal to source_uuid
  std::string current_leader_uuid_;

  // True if the start of the election is a dead promotion
  const bool is_origin_dead_promotion_;

  std::optional<OpId> mock_election_snapshot_op_id_;
};

class RaftConsensus : public std::enable_shared_from_this<RaftConsensus>,
                      public enable_make_shared<RaftConsensus>,
                      public PeerMessageQueueObserver {
 public:
  using ElectionDecisionCallback =
      std::function<void(const ElectionResult&, const ElectionContext&)>;
  using TermAdvancementCallback = std::function<void(int64_t)>;
  using NoOpReceivedCallback =
      std::function<void(const OpId&, const RaftPeerPB&)>;
  using LeaderDetectedCallback =
      std::function<void(int64_t, const RaftPeerPB&)>;
  using CheckQuorumFailureCallback = std::function<void()>;

  ~RaftConsensus();

  // Factory method to construct and initialize a RaftConsensus instance.
  static Status Create(
      ConsensusOptions options,
      RaftPeerPB local_peer_pb,
      scoped_refptr<ConsensusMetadataManager> cmeta_manager,
      scoped_refptr<PersistentVarsManager> persistent_vars_manager,
      ThreadPool* raft_pool,
      std::shared_ptr<RaftConsensus>* consensus_out);

  void DisableNoOpEntries() {
    disable_noop_ = true;
  }

  std::shared_ptr<LogCache> GetLogCache() {
    if (queue_) {
      return queue_->log_cache();
    }
    return nullptr;
  }

  // Starts running the Raft consensus algorithm.
  // Start() is not thread-safe. Calls to Start() should be externally
  // synchronized with calls accessing non-const members of this class.
  Status Start(
      const std::shared_ptr<ConsensusBootstrapInfo>& info,
      std::unique_ptr<PeerProxyFactory> peer_proxy_factory,
      scoped_refptr<log::Log> log,
      scoped_refptr<ITimeManager> time_manager,
      ConsensusRoundHandler* round_handler,
      const scoped_refptr<MetricEntity>& metric_entity,
      Callback<void(const std::string& reason)> mark_dirty_clbk);

  // Returns true if RaftConsensus is running.
  bool IsRunning() const;

  // Allow (or disallow) starting elections on the peer. If disallowed, no type
  // of election will be started on the peer - even if there are heartbeat
  // failures from the leader. The setting is persisted to disk and respected
  // even after the node restarts - this means that if starting elections was
  // disabled on a leader before it crashes, it will not become leader again on
  // restart until starting elections is manually re-allowed
  //
  // In the future, we can probably have a TTL on this to protect against
  // accidental prolonged blockage of starting elections
  void SetAllowStartElection(bool val);

  // Check if starting elections is allowed
  bool IsStartElectionAllowed() const;

  // Sets a RPC token to be sent with Raft RPCs to prove we're in a certain ring
  Status SetRaftRpcToken(std::optional<std::string> token);

  // Returns the rpc token
  std::shared_ptr<const std::string> GetRaftRpcToken() const;

  // If we should be enforcing incoming consensus RPCs to have a token
  bool ShouldEnforceRaftRpcToken() const;

  // Start tracking the leader for failures. This typically occurs at startup
  // and when the local peer steps down as leader.
  //
  // If 'delta' is set, it is used as the initial period for leader failure
  // detection. Otherwise, the minimum election timeout is used.
  //
  // If the failure detector is already registered, has no effect.
  void EnableFailureDetector(std::optional<MonoDelta> delta);

  // Stop tracking the current leader for failures. This typically occurs when
  // the local peer becomes leader.
  //
  // If the failure detector is already disabled, has no effect.
  void DisableFailureDetector();

  // Pauses outgoing votes from this server during elections, if set to true.
  void SetWithholdVotesForTests(bool withhold_votes);

  // Rejects AppendEntries RPCs, if set to true.
  void SetRejectAppendEntriesForTests(bool reject_append_entries);

  // If set to false we won't adjust voter distribution based on current config
  void SetAdjustVoterDistribution(bool val);

  // Update the proxy policy used to route entries
  Status SetProxyPolicy(const ProxyPolicy& proxy_policy);

  // Returns the current proxy policy in use
  void GetProxyPolicy(std::string* proxy_policy);

  // Set the failure threshold (in milliseconds) beyond which the leader will
  // mark a 'proxy_peer' as being unhealthy (for acting as a proxy)
  void SetProxyFailureThreshold(int32_t proxy_failure_threshold_ms);

  // Set the failure threshold lag (in term of #ops as compared to destination
  // peer) beyond which the leader will mark a 'proxy_peer' as being unhealthy
  // (for acting as a proxy for a given destination peer)
  void SetProxyFailureThresholdLag(int64_t proxy_failure_threshold_lag);

  // Emulates an election by increasing the term number and asserting leadership
  // in the configuration by sending a NO_OP to other peers.
  // This is NOT safe to use in a distributed configuration with failure
  // detection enabled, as it could result in a split-brain scenario.
  Status EmulateElection();

  // Triggers a leader election.
  Status StartElection(
      ElectionMode mode,
      ElectionContext context,
      std::function<void(const ElectionResult&)> callback = {});

  // Wait until the node has LEADER role.
  // Returns Status::TimedOut if the role is not LEADER within 'timeout'.
  Status WaitUntilLeaderForTests(const MonoDelta& timeout);

  // Return a copy of the failure detector instance. Only for use in tests.
  std::shared_ptr<rpc::PeriodicTimer> GetFailureDetectorForTests() const {
    return failure_detector_;
  }

  // Performs an abrupt leader step down. This node, if the leader, becomes a
  // follower immediately and sleeps its failure detector for an extra election
  // timeout to decrease its chances of being reelected.
  Status StepDown(LeaderStepDownResponsePB* resp);

  // Attempts to gracefully transfer leadership to the peer with uuid
  // 'new_leader_uuid' or to the next up-to-date peer the leader gets
  // a response from if 'new_leader_uuid' is {}. To allow peers time
  // to catch up, the leader will not accept write or config change requests
  // during a 'transfer period' that lasts one election timeout. If no
  // successor is eligible by the end of the transfer period, leadership
  // transfer fails and the leader resumes normal operation. The transfer is
  // asynchronous: once the transfer period is started the method returns
  // success.
  // Additional calls to this method during the transfer period prolong it.
  Status TransferLeadership(
      const std::optional<std::string>& new_leader_uuid,
      const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
      const ElectionContext& election_ctx,
      LeaderStepDownResponsePB* resp);

  // Runs a mock version of TransferLeadership to see if the operation can be
  // successfuly or not. It is mock in the sense that election votes are not
  // durably stored, much like a pre-election.
  //
  // The process of a mock election is as follows:
  //
  // 1. Leader takes a snapshot opid, this is the point where we pretend we
  // stopped writes.
  // 2. We wait a few heartbeats.
  // 3. Candidate starts election with own state as min(own opid, snapshot opid)
  // and sends voters the snapshot op id.
  // 4. Voter's own state is min(own opid, snapshot opid) and votes based on
  // that.
  //
  // Case 1: Candidate is replicating properly.
  // In this case, the candidate's current opid will exceed the snapshot and due
  // to min(own opid, snapshot opid), they will request votes with snapshot opid
  // as current state.
  //
  // Voter cases:
  // a) Voters are at or ahead of snapshot opid. They will vote yes since they
  // will be voting with their own state as snapshot opid which is equal to
  // candidate's state.
  // b) Voters are behind snapshot opid and not in the candidate region. They
  // will vote yes.
  // c) Voters are behind snapshot opid and in the candidate region. They will
  // vote no since they themselves are not caught up to the snapshot and won't
  // be able to commit a no-op should a real election elect the candidate
  // (lag threshold).
  //
  // Case 2: Candidate is not replicating properly and is behind.
  // In this case, it'll start election with an outdated snapshot opid.
  //
  // Voter cases:
  // a) Voters are at or ahead of candidates opid. They will vote no since they
  // will be voting with their own state as snapshot op id which is greater than
  // candidate's state.
  // b) Voters are behind candidate's opid. They will vote yes.
  Status MockTransferLeadership(
      const std::string& new_leader_uuid,
      const ElectionContext& election_ctx,
      const std::chrono::milliseconds& wait_time,
      RunLeaderElectionResponsePB* resp);

  // Attempts to cancel the leadership transfer. This stops any leadership
  // transfers, then checks if we are past the point where we notified anyone to
  // start a election. Returns OK if we have not (safe to assume
  // TransferLeadership have not happened), and IllegalState if we have.
  // This method cancels transfer initiated by the last TransferLeadership call
  // and users are responsible for controling races to multiple calls of
  // TransferLeadership to ensure the right one is cancelled.
  Status CancelTransferLeadership();

  // Begin or end a leadership transfer period. During a transfer period, a
  // leader will not accept writes or config changes, but will continue updating
  // followers. If a leader transfer period is already in progress,
  // BeginLeaderTransferPeriodUnlocked returns ServiceUnavailable.
  Status BeginLeaderTransferPeriodUnlocked(
      const std::optional<std::string>& successor_uuid,
      const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
      const ElectionContext& election_ctx);
  void EndLeaderTransferPeriod();

  // Creates a new ConsensusRound, the entity that owns all the data
  // structures required for a consensus round, such as the ReplicateMsg
  // (and later on the CommitMsg). ConsensusRound will also point to and
  // increase the reference count for the provided callbacks.
  scoped_refptr<ConsensusRound> NewRound(
      std::unique_ptr<ReplicateMsg> replicate_msg,
      ConsensusReplicatedCallback replicated_cb);

  // Creates a new ConsensusRound, the entity that owns all the data
  // structures required for a consensus round, such as the ReplicateMsg
  scoped_refptr<ConsensusRound> NewRound(
      std::unique_ptr<ReplicateMsg> replicate_msg);

  // Called by a Leader to replicate an entry to the state machine.
  //
  // From the leader instance perspective execution proceeds as follows:
  //
  //           Leader                               RaftConfig
  //             +                                     +
  //     1) Req->| Replicate()                         |
  //             |                                     |
  //     2)      +-------------replicate-------------->|
  //             |<---------------ACK------------------+
  //             |                                     |
  //     3)      +--+                                  |
  //           <----+ round.NotifyReplicationFinished()|
  //             |                                     |
  //     3a)     |  +------ update commitIndex ------->|
  //             |                                     |
  //
  // 1) Caller calls Replicate(), method returns immediately to the caller and
  //    runs asynchronously.
  //
  // 2) Leader replicates the entry to the peers using the consensus
  //    algorithm, proceeds as soon as a majority of voters acknowledges the
  //    entry.
  //
  // 3) Leader defers to the caller by calling
  // ConsensusRound::NotifyReplicationFinished,
  //    which calls the ConsensusReplicatedCallback.
  //
  // 3a) The leader asynchronously notifies other peers of the new
  //     commit index, which tells them to apply the operation.
  //
  // This method can only be called on the leader, i.e. role() == LEADER
  Status Replicate(const scoped_refptr<ConsensusRound>& round);

  // Ensures that the consensus implementation is currently acting as LEADER,
  // and thus is allowed to submit operations to be prepared before they are
  // replicated. To avoid a time-of-check-to-time-of-use (TOCTOU) race, the
  // implementation also stores the current term inside the round's "bound_term"
  // member. When we eventually are about to replicate the transaction, we
  // verify that the term has not changed in the meantime.
  Status CheckLeadershipAndBindTerm(const scoped_refptr<ConsensusRound>& round);

  // Messages sent from LEADER to FOLLOWERS and LEARNERS to update their
  // state machines. This is equivalent to "AppendEntries()" in Raft
  // terminology.
  //
  // ConsensusRequestPB contains a sequence of 0 or more operations to apply
  // on the replica. If there are 0 operations the request is considered
  // 'status-only' i.e. the leader is communicating with the follower only
  // in order to pass back and forth information on watermarks (eg committed
  // operation ID, replicated op id, etc).
  //
  // If the sequence contains 1 or more operations they will be replicated
  // in the same order as the leader, and submitted for asynchronous Prepare
  // in the same order.
  //
  // The leader also provides information on the index of the latest
  // operation considered committed by consensus. The replica uses this
  // information to update the state of any pending (previously
  // replicated/prepared) transactions.
  //
  // Returns Status::OK if the response has been filled (regardless of accepting
  // or rejecting the specific request). Returns non-OK Status if a specific
  // error response could not be formed, which will result in the service
  // returning an UNKNOWN_ERROR RPC error code to the caller and including the
  // stringified Status message.
  Status Update(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response);

  // Messages sent from CANDIDATEs to voting peers to request their vote
  // in leader election.
  //
  // If 'tombstone_last_logged_opid' is set, this replica will attempt to vote
  // in kInitialized and kStopped states, instead of just in the kRunning
  // state.
  Status RequestVote(
      const VoteRequestPB* request,
      TabletVotingState tablet_voting_state,
      VoteResponsePB* response);

  // Utility Function:
  // From a simple ChangeConfigRequest, create a BulkChangeConfigRequest
  static void GetBulkConfigChangeRequest(
      const ChangeConfigRequestPB& req,
      BulkChangeConfigRequestPB* bulk_req);

  // Utility function:
  // Takes a bulk change config request and returns a new config by
  // building it from the current committed_config + the changes
  // This helper has been excised out of kudu
  // BulkChangeConfig function. It does several sanity checks
  // to adhere to one at a time change
  Status CheckBulkConfigChangeAndGetNewConfigUnlocked(
      const BulkChangeConfigRequestPB& req,
      std::optional<ServerErrorPB::Code>* error_code,
      RaftConfigPB* new_config);

  // This returns a ReplicateMsg to the caller, without actually running
  // consensus. The term and index can shift after the return and therefore
  // the caller has to hold on to some mutex to serialize the calls.
  // The message should be resent via ReplicateMsg() API to do actual
  // consensus
  Status CheckAndPopulateChangeConfigMessage(
      const ChangeConfigRequestPB& req,
      std::optional<ServerErrorPB::Code>* error_code,
      ReplicateMsg* replicate_msg);

  // Implement a ChangeConfig() request.
  Status ChangeConfig(
      const ChangeConfigRequestPB& req,
      StdStatusCallback client_cb,
      std::optional<ServerErrorPB::Code>* error_code);

  // Implement a BulkChangeConfig() request.
  Status BulkChangeConfig(
      const BulkChangeConfigRequestPB& req,
      StdStatusCallback client_cb,
      std::optional<ServerErrorPB::Code>* error_code);

  // Implement an UnsafeChangeConfig() request.
  Status UnsafeChangeConfig(
      const UnsafeChangeConfigRequestPB& req,
      std::optional<ServerErrorPB::Code>* error_code);

  // Change the proxy topology.
  Status ChangeProxyTopology(const ProxyTopologyPB& proxy_topology);

  // Update region group for RegionGroupRoutingTable
  Status UpdateProxyRegionGroup(
      const std::vector<std::unordered_set<std::string>>& region_groups);

  std::vector<std::unordered_set<std::string>> GetProxyRegionGroup();

  // On a live Raft Instance allow for changes to voter_distribution map
  Status ChangeVoterDistribution(
      const TopologyConfigPB& topology_config,
      bool force = false);

  // Get the voter distribution from the committed config
  Status GetVoterDistribution(std::map<std::string, int32>* vd) const;

  // Return the proxy topology.
  ProxyTopologyPB GetProxyTopology() const;

  // On a live Raft Instance to use quorum_id instead of region for flexiraft
  // dynamic mode
  Status ChangeQuorumType(QuorumType type);

  // Get QuorumType from committed config
  QuorumType GetQuorumType() const;

  // Update peer's quorum_id given uuid to quorum_id map. This is an atomic
  // write, meaning that either all peers in map are updated or none gets
  // updated.
  //
  // When force = false, reject update when use_quorum_id is true or one or more
  // peers in active config does not exist in the map. Set force = true to
  // bypass the check.
  Status SetPeerQuorumIds(
      std::map<std::string, std::string> uuid2quorum_ids,
      bool force = false);

  // Only relevant for abstracted logs.
  // Callback the log abstraction's TruncateOpsAfter function
  // while holding Raft Consensus lock. This is to serialize
  // the operation with Raft Consensus lock, which is the same locking
  // pattern that is used by UpdateReplica while invoking TruncateOpsAfter
  // @param index_if_truncated - the log specialization will return the
  // truncated index if truncation happened.
  Status TruncateCallbackWithRaftLock(int64_t* index_if_truncated);

  // Returns the last OpId (either received or committed, depending on the
  // 'type' argument) that the Consensus implementation knows about.
  // Returns {} if RaftConsensus was not properly initialized.
  std::optional<OpId> GetLastOpId(OpIdType type);

  std::optional<OpId> GetNextOpId() const;

  // Returns the current Raft role of this instance.
  RaftPeerPB::Role role(bool lock = true) const;

  // Returns the current term.
  int64_t CurrentTerm() const;

  // Returns uuid of the current leader
  std::string GetLeaderUuid() const;

  // Returns hostport of the current leader
  std::pair<std::string, unsigned int> GetLeaderHostPort() const;

  // Returns the uuid of this peer.
  // Thread-safe.
  const std::string& peer_uuid() const;

  // Returns the hostport of this peer.
  std::pair<std::string, unsigned int> peer_hostport() const;

  // relevant for Flexi-Raft
  std::string peer_region() const;

  // It is own peer region or quorum_id
  std::string peer_quorum_id(bool need_lock = true) const;

  // Returns the id of the tablet whose updates this consensus instance helps
  // coordinate. Thread-safe.
  const std::string& tablet_id() const;

  scoped_refptr<ITimeManager> time_manager() const {
    return time_manager_;
  }

  // Returns the time we should snooze in UpdateReplica while we process the
  // update. Snooze is reset to normal election timeout after Update is complete
  MonoDelta UpdateReplicaSnoozeTimeout() const;

  // Return the minimum election timeout. Due to backoff and random
  // jitter, election timeouts may be longer than this.
  MonoDelta MinimumElectionTimeout() const;

  // Return the minimum election timeout considering ban-factor
  MonoDelta MinimumElectionTimeoutWithBan();

  // Returns a copy of the state of the consensus system.
  // If 'report_health' is set to 'INCLUDE_HEALTH_REPORT', and if the
  // local replica believes it is the leader of the config, it will include a
  // health report about each active peer in the committed config.
  // If RaftConsensus has been shut down, returns Status::IllegalState.
  // Does not modify the out-param 'cstate' unless an OK status is returned.
  Status ConsensusState(
      ConsensusStatePB* cstate,
      IncludeHealthReport report_health = EXCLUDE_HEALTH_REPORT,
      bool lock = true) const;

  // Returns a copy of the current committed Raft configuration.
  RaftConfigPB CommittedConfig() const;

  // Returns a copy of the current pending Raft configuration.
  Status PendingConfig(RaftConfigPB* pendingConfig) const;

  // Transition to kStopped state. See State enum definition for details.
  // This is a no-op if the tablet is already in kStopped or kShutdown state;
  // otherwise, Raft will pass through the kStopping state on the way to
  // kStopped.
  void Stop();

  // Transition to kShutdown state. See State enum definition for details.
  // It is legal to call this method while in any lifecycle state.
  void Shutdown();

  // Makes this peer advance it's term (and step down if leader), for tests.
  Status AdvanceTermForTests(int64_t new_term);

  int update_calls_for_tests() const {
    return update_calls_for_tests_.Load();
  }

  //------------------------------------------------------------
  // PeerMessageQueueObserver implementation
  //------------------------------------------------------------

  // Updates the committed_index and triggers the Apply()s for whatever
  // transactions were pending.
  // This is idempotent.
  void NotifyCommitIndex(int64_t commit_index, bool need_lock) override;

  void NotifyTermChange(int64_t term) override;

  void NotifyFailedFollower(
      const std::string& uuid,
      int64_t term,
      const std::string& reason) override;

  void NotifyPeerToPromote(const std::string& peer_uuid) override;

  void NotifyPeerToStartElection(
      const std::string& peer_uuid,
      std::optional<PeerMessageQueue::TransferContext> transfer_context,
      std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise,
      std::optional<OpId> mock_election_snapshot_op_id) override;

  void NotifyPeerHealthChange() override;

  // Return the log indexes which the consensus implementation would like to
  // retain.
  //
  // The returned 'for_durability' index ensures that no logs are GCed before
  // the operation is fully committed. The returned 'for_peers' index indicates
  // the index of the farthest-behind peer so that the log will try to avoid
  // GCing these before the peer has caught up.
  log::RetentionIndexes GetRetentionIndexes();

  // Return the on-disk size of the consensus metadata, in bytes.
  int64_t MetadataOnDiskSize() const;

  int64_t GetMillisSinceLastLeaderHeartbeat() const;

  // Returns true if the request is intended to be proxied.
  bool IsProxyRequest(const ConsensusRequestPB* request) const;

  // Handle proxy RPC request.
  // This method is intended to be executed on an RPC worker thread.
  void HandleProxyRequest(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcContext* context);

  // Trigger that a non-Transaction ConsensusRound has finished replication.
  // If the replication was successful, an status will be OK. Otherwise, it
  // may be Aborted or some other error status.
  // If 'status' is OK, write a Commit message to the local WAL based on the
  // type of message it is.
  // The 'client_cb' will be invoked at the end of this execution.
  //
  // NOTE: Must be called while holding 'lock_'.
  void NonTxRoundReplicationFinished(
      ConsensusRound* round,
      const StdStatusCallback& client_cb,
      const Status& status);

  // Set the compression codec to be used to compress ReplicateMsg payload
  Status SetCompressionCodec(const std::string& codec);

  // Set the compression level to be used to compress ReplicateMsg payload
  Status SetCompressionLevel(int level);

  enum LeaderLeaseState {
    RENEW = 0,
    REVOKE = 1,
  };

  // Set whether Leader Lease is set to Renewal/Revoke state
  Status SetLeaseRenewStateUnlocked(LeaderLeaseState state);

  bool IsLeaderLeaseSetForRevoke() const;

  // Gets the Leader Lease timestamp
  MonoTime GetLeaderLeaseUntil();

  // Get the bounded data loss window expiry timestamp
  MonoTime GetBoundedDataLossWindowUntil();

  // Enables (or disables) compression of messages read from log
  Status EnableCompressionOnCacheMiss(bool enable);

  // Load and set compression dictionary from file
  Status LoadCompressionDict(const std::string& filename);

  std::string GetCompressionStats() const;

  // Clear the 'removed_peers_' list managed by consensus_meta
  void ClearRemovedPeersList();

  // Delete uuids in 'peer_uuids_ from 'removed_peers_' list
  void DeleteFromRemovedPeersList(const std::vector<std::string>& peer_uuids);

  // Returns the 'removed_peers_' list managed by consensus-meta
  std::vector<std::string> RemovedPeersList();

  // Sets the callback that is called when we detect a failure in the
  // CheckQuorum detector.
  void SetCheckQuorumFailureCallback(
      CheckQuorumFailureCallback failure_callback);

  // Changes the interval at which to call the Check Quorum detector.
  // This method exists because changing the interval requires re-initializing
  // the check quorum timer.
  void SetCheckQuorumFailureIntervalHeartbeats(int heartbeats);

  // See kudu::consensus::PeerMessageQueue::GetAvailableCommitPeers().
  int32_t GetAvailableCommitPeers();

  Status GetQuorumHealth(PeerMessageQueue::QuorumHealth* health);

  /**
   * Pauses the failure detector by saving the time left on it and replacing the
   * timout with resumeTimeout.
   *
   * Calling resume will reset the detector to the same timeout unless we
   * snoozed the detector in the interim.
   *
   * @param maxPause The maximum amount of time to pause. If this time is
   *                 exceeded, the failure detector will detect a failure even
   *                 when paused. Defaults to UpdateReplicaSnoozeTimeout
   */
  void PauseFailureDetector(std::optional<MonoDelta> maxPause = {});

  /**
   * Resumes the failure detector at the point where it was paused by
   * PauseFailureDectector, assuming that we did not snooze the timer in the
   * interim.
   */
  void ResumeFailureDetector();

 protected:
  RaftConsensus(
      ConsensusOptions options,
      RaftPeerPB local_peer_pb,
      scoped_refptr<ConsensusMetadataManager> cmeta_manager,
      scoped_refptr<PersistentVarsManager> persistent_vars_manager,
      ThreadPool* raft_pool);

 private:
  friend class RaftConsensusQuorumTest;
  friend class tserver::TSTabletManager;
  friend class tserver::RaftConsensusManager;
  friend class tserver::RaftConsensusInstance;
  friend class facebook::datashuttle::KuduRingManager;
  FRIEND_TEST(
      RaftConsensusQuorumTest,
      TestConsensusContinuesIfAMinorityFallsBehind);
  FRIEND_TEST(
      RaftConsensusQuorumTest,
      TestConsensusStopsIfAMajorityFallsBehind);
  FRIEND_TEST(RaftConsensusQuorumTest, TestLeaderElectionWithQuiescedQuorum);
  FRIEND_TEST(
      RaftConsensusQuorumTest,
      TestReplicasEnforceTheLogMatchingProperty);
  FRIEND_TEST(RaftConsensusQuorumTest, TestRequestVote);

  // RaftConsensus lifecycle states.
  //
  // Legal state transitions:
  //
  //   kNew -> kInitialized -+-> kRunning -> kStopping -> kStopped -> kShutdown
  //                          `----------------^
  //
  // NOTE: When adding / changing values in this enum, add the corresponding
  // values to State_Name() as well.
  //
  enum State {
    // The RaftConsensus object has been freshly constructed and is not yet
    // initialized. A RaftConsensus object will never be made externally
    // visible in this state.
    kNew,

    // Raft has been initialized. It cannot accept writes, but it may be able
    // to vote. See RequestVote() for details.
    kInitialized,

    // Raft is running normally and will accept write requests and vote
    // requests.
    kRunning,

    // Raft is in the process of stopping and will not accept writes. Voting
    // may still be allowed. See RequestVote() for details.
    kStopping,

    // Raft is stopped and no longer accepting writes. However, voting may
    // still be allowed; See RequestVote() for details.
    kStopped,

    // Raft is fully shut down and cannot accept writes or vote requests.
    kShutdown,
  };

  // Enum for the 'flush' argument to SetCurrentTermUnlocked() below.
  enum FlushToDisk {
    SKIP_FLUSH_TO_DISK,
    FLUSH_TO_DISK,
  };

  // Helper struct that contains the messages from the leader that we need to
  // append to our log, after they've been deduplicated.
  struct LeaderRequest {
    std::string leader_uuid;
    const OpId* preceding_opid;
    std::vector<ReplicateRefPtr> messages;
    // The positional index of the first message selected to be appended, in the
    // original leader's request message sequence.
    int64_t first_message_idx;

    std::string OpsRangeString() const;
  };

  using LockGuard = std::lock_guard<simple_mutexlock>;
  using UniqueLock = std::unique_lock<simple_mutexlock>;

  // Initializes the RaftConsensus object, including loading the consensus
  // metadata.
  Status Init();

  // Change the lifecycle state of RaftConsensus. The definition of the State
  // enum documents legal state transitions.
  void SetStateUnlocked(State new_state);

  // To be only called during bootstrap, by simple_tablet_manager to
  // make sure that the term of the instance is atleast as high as the
  // term of its last logged entry.
  //
  // Just like SetCurrentTermUnlocked, this function does not let the
  // term to reduce, and will return IlegalState for that case. So should
  // be called only if LogTerm is higher.
  Status SetCurrentTermBootstrap(int64_t new_term);

  // Returns string description for State enum value.
  static const char* State_Name(State state);

  // Set the leader UUID of the configuration and mark the tablet config dirty
  // for reporting to the master.
  Status SetLeaderUuidUnlocked(const std::string& uuid);

  // Utility function to get a replicated message
  // from a old_config -> new_config config change proposal
  Status CreateReplicateMsgFromConfigsUnlocked(
      RaftConfigPB old_config,
      RaftConfigPB new_config,
      ReplicateMsg* cc_replicate);

  // Replicate (as leader) a config change. This includes validating the new
  // config and updating the peers and setting the new_configuration as pending.
  // The old_configuration must be the currently-committed configuration.
  Status ReplicateConfigChangeUnlocked(
      RaftConfigPB old_config,
      RaftConfigPB new_config,
      StdStatusCallback client_cb);

  // Update the peers and queue to be consistent with a new active
  // configuration. Should only be called by the leader.
  Status RefreshConsensusQueueAndPeersUnlocked();

  // Makes the peer become leader.
  // Returns OK once the change config transaction that has this peer as leader
  // has been enqueued, the transaction will complete asynchronously.
  //
  // 'lock_' must be held for configuration change before calling.
  Status BecomeLeaderUnlocked();

  // Makes the peer become a replica, i.e. a FOLLOWER or a LEARNER.
  // See EnableFailureDetector() for description of the 'fd_delta' parameter.
  //
  // 'lock_' must be held for configuration change before calling.
  Status BecomeReplicaUnlocked(std::optional<MonoDelta> fd_delta = {});

  // Updates the state in a replica by storing the received operations in the
  // log and triggering the required transactions. This method won't return
  // until all operations have been stored in the log and all Prepares() have
  // been completed, and a replica cannot accept any more Update() requests
  // until this is done.
  Status UpdateReplica(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response);

  // Deduplicates an RPC request making sure that we get only messages that we
  // haven't appended to our log yet.
  // On return 'deduplicated_req' is instantiated with only the new messages
  // and the correct preceding id.
  void DeduplicateLeaderRequestUnlocked(
      ConsensusRequestPB* rpc_req,
      LeaderRequest* deduplicated_req);

  // Handles a request from a leader, refusing the request if the term is lower
  // than ours or stepping down if it's higher.
  Status HandleLeaderRequestTermUnlocked(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response);

  // Checks that the preceding op in 'req' is locally committed or pending and
  // sets an appropriate error message in 'response' if not. If there is term
  // mismatch between the preceding op id in 'req' and the local log's pending
  // operations, we proactively abort those pending operations after and
  // including the preceding op in 'req' to avoid a pointless cache miss in the
  // leader's log cache.
  Status EnforceLogMatchingPropertyMatchesUnlocked(
      const LeaderRequest& req,
      ConsensusResponsePB* response) WARN_UNUSED_RESULT;

  // Check a request received from a leader, making sure:
  // - The request is in the right term
  // - The log matching property holds
  // - Messages are de-duplicated so that we only process previously unprocessed
  // requests.
  // - We abort transactions if the leader sends transactions that have the same
  // index as
  //   transactions currently on the pendings set, but different terms.
  // If this returns ok and the response has no errors, 'deduped_req' is set
  // with only the messages to add to our state machine.
  Status CheckLeaderRequestUnlocked(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      LeaderRequest* deduped_req) WARN_UNUSED_RESULT;

  // Abort any pending operations after the given op index,
  // and also truncate the LogCache accordingly.
  void TruncateAndAbortOpsAfterUnlocked(int64_t truncate_after_index);

  // Begin a replica transaction. If the type of message in 'msg' is not a type
  // that uses transactions, delegates to StartConsensusOnlyRoundUnlocked().
  Status StartFollowerTransactionUnlocked(const ReplicateRefPtr& msg);

  // Just like StartFollowerTransactionUnlocked() above but with msg wrapper as
  // input
  Status StartFollowerTransactionUnlocked(
      const ReplicateMsgWrapper& msg_wrapper);

  // Returns true if this node is the only voter in the Raft configuration.
  bool IsSingleVoterConfig() const;

  // Return header string for RequestVote log messages. 'lock_' must be held.
  std::string GetRequestVoteLogPrefixUnlocked(
      const VoteRequestPB& request) const;

  // Helper function to fill in the previous vote history and last pruned term
  // from the vote history.
  void FillVoteResponsePreviousVoteHistory(VoteResponsePB* response);

  // Helper function to populate last known leader information in the
  // vote response.
  void FillVoteResponseLastKnownLeader(VoteResponsePB* response);

  // Fills the response with the current status, if an update was successful.
  void FillConsensusResponseOKUnlocked(ConsensusResponsePB* response);

  // Fills the response with an error code and error message.
  void FillConsensusResponseError(
      ConsensusResponsePB* response,
      ConsensusErrorPB::Code error_code,
      const Status& status);

  // Fill VoteResponsePB with the following information:
  // - Update responder_term to current local term.
  // - Set vote_granted to true.
  void FillVoteResponseVoteGranted(VoteResponsePB* response);

  // Fill VoteResponsePB with the following information:
  // - Update responder_term to current local term.
  // - Set vote_granted to false.
  // - Set consensus_error.code to the given code.
  void FillVoteResponseVoteDenied(
      ConsensusErrorPB::Code error_code,
      VoteResponsePB* response);

  // Respond to VoteRequest that the candidate has an old term.
  Status RequestVoteRespondInvalidTerm(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to the candidate.
  Status RequestVoteRespondVoteAlreadyGranted(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to someone else.
  Status RequestVoteRespondAlreadyVotedForOther(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest that the candidate's last-logged OpId is too old.
  Status RequestVoteRespondLastOpIdTooOld(
      const OpId& local_last_logged_opid,
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest with a denial because votes are being witheld
  // for testing.
  Status RequestVoteRespondVoteWitheld(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      const std::string& withhold_reason,
      VoteResponsePB* response);

  // Respond to VoteRequest that the vote was not granted because we believe
  // the leader to be alive.
  Status RequestVoteRespondLeaderIsAlive(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest that the replica is already in the middle of
  // servicing another vote request or an update from a valid leader.
  Status RequestVoteRespondIsBusy(
      const VoteRequestPB* request,
      VoteResponsePB* response);

  // Respond to VoteRequest that the vote is granted for candidate.
  Status RequestVoteRespondVoteGranted(
      const VoteRequestPB* request,
      const std::string& hostname_port,
      VoteResponsePB* response);

  // Respond to VoteRequest that the request is invalid.
  Status RequestVoteRespondInvalidClientRequest(
      VoteResponsePB* response,
      const std::string& error_message);

  // Get the context sent by the candidate as a string. Used for logging
  std::string GetCandidateContextString(const VoteRequestPB* request);

  // Callback for leader election driver. The "callback" arg is run in the same
  // caller thread but the rest of the ElectionCallback is run on the
  // reactor thread, so it simply defers its work to DoElectionCallback.
  void ElectionCallback(
      ElectionContext context,
      const ElectionResult& result,
      std::function<void(const ElectionResult&)> callback);
  void DoElectionCallback(
      const ElectionContext& context,
      const ElectionResult& result);
  void NestedElectionDecisionCallback(
      const ElectionContext& context,
      const ElectionResult& result);

  // Enables or disables the failure detector based on the role of the local
  // peer in the active config. If the local peer a VOTER, but not the leader,
  // then failure detection will be enabled. If the local peer is the leader,
  // or a NON_VOTER, then failure detection will be disabled.
  //
  // See EnableFailureDetector() for an explanation of the 'delta' parameter,
  // which is used if it is determined that the failure detector should be
  // enabled.
  void UpdateFailureDetectorState(std::optional<MonoDelta> delta = {});

  // "Reset" the failure detector to indicate leader activity.
  //
  // When this is called a failure is guaranteed not to be detected before
  // 'FLAGS_leader_failure_max_missed_heartbeat_periods' *
  // 'FLAGS_raft_heartbeat_interval_ms' has elapsed, unless 'delta' is set, in
  // which case its value is used as the next failure period.
  //
  // If 'reason_for_log' is set, then this method will print a log message when
  // called.
  //
  // If the failure detector is unregistered, has no effect.
  void SnoozeFailureDetector(
      std::optional<std::string> reason_for_log = {},
      std::optional<MonoDelta> delta = {});

  // Calculates a snooze delta for leader election.
  //
  // The delta increases exponentially with the difference between the current
  // term and the term of the last committed operation.
  //
  // The maximum delta is capped by
  // 'FLAGS_leader_failure_exp_backoff_max_delta_ms'.
  MonoDelta LeaderElectionExpBackoffDeltaUnlocked();
  MonoDelta LeaderElectionExpBackoffNotInConfig();

  MonoDelta TimeoutBackoffHelper(double backoff_factor);

  // Handle when the term has advanced beyond the current term.
  //
  // 'flush' may be used to control whether the term change is flushed to disk.
  Status HandleTermAdvanceUnlocked(
      ConsensusTerm new_term,
      FlushToDisk flush = FLUSH_TO_DISK);

  // Handle when a op id with a new term has been appended
  void HandleNewTermAppendedUnlocked(int64_t new_term);

  // Asynchronously (on thread_pool_) notify the TabletReplica that the
  // consensus configuration has changed, thus reporting it back to the master.
  void MarkDirty(const std::string& reason);

  // Calls MarkDirty() if 'status' == OK. Then, always calls 'client_cb' with
  // 'status' as its argument.
  void MarkDirtyOnSuccess(
      const std::string& reason,
      const StdStatusCallback& client_cb,
      const Status& status);

  // Attempt to remove the follower with the specified 'uuid' from the config,
  // if the 'committed_config' is still the committed config and if the current
  // node is the leader.
  //
  // Since this is inherently an asynchronous operation run on a thread pool,
  // it may fail due to the configuration changing, the local node losing
  // leadership, or the tablet shutting down.
  // Logs a warning on failure.
  void TryRemoveFollowerTask(
      const std::string& uuid,
      const RaftConfigPB& committed_config,
      const std::string& reason);

  // Attempt to promote the given non-voter to a voter.
  void TryPromoteNonVoterTask(const std::string& peer_uuid);

  void TryStartElectionOnPeerTask(
      const std::string& peer_uuid,
      const std::optional<PeerMessageQueue::TransferContext>& transfer_context,
      std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise = nullptr,
      std::optional<OpId> mock_election_snapshot_op_id = {});

  // Called when the failure detector expires.
  // Submits ReportFailureDetectedTask() to a thread pool.
  void ReportFailureDetected();

  // Call StartElection(), log a warning if the call fails (usually due to
  // being shut down).
  void ReportFailureDetectedTask();

  // Handle the completion of replication of a config change operation.
  // If 'status' is OK, this takes care of persisting the new configuration
  // to disk as the committed configuration. A non-OK status indicates that
  // the replication failed, in which case the pending configuration needs
  // to be cleared such that we revert back to the old configuration.
  void CompleteConfigChangeRoundUnlocked(
      ConsensusRound* round,
      const Status& status);

  // As a leader, append a new ConsensusRound to the queue.
  Status AppendNewRoundToQueueUnlocked(
      const scoped_refptr<ConsensusRound>& round);

  // As a follower, start a consensus round not associated with a Transaction.
  Status StartConsensusOnlyRoundUnlocked(const ReplicateRefPtr& msg);

  // Add a new pending operation to PendingRounds, including the special
  // handling necessary if this round contains a configuration change. These
  // rounds must take effect as soon as they are received, rather than waiting
  // for commitment (see Diego Ongaro's thesis section 4.1).
  Status AddPendingOperationUnlocked(
      const scoped_refptr<ConsensusRound>& round);

  // Checks that the replica is in the appropriate state and role to replicate
  // the provided operation and that the replicate message does not yet have an
  // OpId assigned.
  Status CheckSafeToReplicateUnlocked(const ReplicateMsg& msg) const
      WARN_UNUSED_RESULT;

  // Return Status::IllegalState if 'state_' != kRunning, OK otherwise.
  Status CheckRunningUnlocked() const WARN_UNUSED_RESULT;

  // Ensure the local peer is the active leader.
  // Returns OK if leader, IllegalState otherwise.
  Status CheckActiveLeaderUnlocked() const WARN_UNUSED_RESULT;

  // Returns OK if there is currently *no* configuration change pending, and
  // IllegalState is there *is* a configuration change pending.
  Status CheckNoConfigChangePendingUnlocked() const WARN_UNUSED_RESULT;

  // Sets the given configuration as pending commit. Does not persist into the
  // peers metadata. In order to be persisted, SetCommittedConfigUnlocked() must
  // be called.
  Status SetPendingConfigUnlocked(const RaftConfigPB& new_config)
      WARN_UNUSED_RESULT;

  // Changes the committed config for this replica. Checks that there is a
  // pending configuration and that it is equal to this one. Persists changes to
  // disk. Resets the pending configuration to null.
  Status SetCommittedConfigUnlocked(const RaftConfigPB& config_to_commit);

  void ScheduleTermAdvancementCallback(int64_t term);
  void DoTermAdvancmentCallback(int64_t term);

  void ScheduleNoOpReceivedCallback(const ReplicateRefPtr& msg);
  void DoNoOpReceivedCallback(
      const OpId& opid,
      const RaftPeerPB& leader_details);

  void ScheduleLeaderDetectedCallback(int64_t term);
  void DoLeaderDetectedCallback(int64_t term, const RaftPeerPB& leader_details);

  // Checks if the term change is legal. If so, sets 'current_term'
  // to 'new_term' and sets 'has voted' to no for the current term.
  //
  // If the caller knows that it will call another method soon after
  // to flush the change to disk, it may set 'flush' to 'SKIP_FLUSH_TO_DISK'.
  Status SetCurrentTermUnlocked(int64_t new_term, FlushToDisk flush)
      WARN_UNUSED_RESULT;

  // Returns the term set in the last config change round.
  const int64_t CurrentTermUnlocked() const;

  // Accessors for the leader of the current term.
  std::string GetLeaderUuidUnlocked() const;
  bool HasLeaderUnlocked() const;
  void ClearLeaderUnlocked();

  // Return whether this peer has voted in the current term.
  const bool HasVotedCurrentTermUnlocked() const;

  // Record replica's vote for the current term, then flush the consensus
  // metadata to disk.
  Status SetVotedForCurrentTermUnlocked(const std::string& uuid)
      WARN_UNUSED_RESULT;

  // Return replica's vote for the current term.
  // The vote must be set; use HasVotedCurrentTermUnlocked() to check.
  const std::string& GetVotedForCurrentTermUnlocked() const;

  const ConsensusOptions& GetOptions() const;

  // See GetLastOpId().
  std::optional<OpId> GetLastOpIdUnlocked(OpIdType type);

  std::string LogPrefix() const;
  std::string LogPrefixUnlocked() const;

  // A variant of LogPrefix which does not take the lock. This is a slightly
  // less thorough prefix which only includes immutable (and thus thread-safe)
  // information, but does not require the lock.
  std::string LogPrefixThreadSafe() const;

  std::string ToString() const;
  std::string ToStringUnlocked() const;

  ConsensusMetadata* consensus_metadata_for_tests() const;

  void SetElectionDecisionCallback(ElectionDecisionCallback edcb);
  void SetTermAdvancementCallback(TermAdvancementCallback tacb);
  void SetNoOpReceivedCallback(NoOpReceivedCallback norcb);
  void SetLeaderDetectedCallback(LeaderDetectedCallback ldcb);
  void SetVoteLogger(std::shared_ptr<VoteLoggerInterface> vote_logger);

  Status ValidateTransferLeadership(
      const std::optional<std::string>& new_leader_uuid,
      LeaderStepDownResponsePB* resp);

  void UpdateLocalPeerUnlocked(RaftConfigPB& active_config);

  // Initializes timer that periodically checks whether leader can satisfy
  // commit quorum.
  void InitCheckQuorumDetectorUnlocked();

  void SnoozeCheckQuorumDetector(MonoDelta snooze_time);

  void StopCheckQuorumDetectorUnlocked();

  const ConsensusOptions options_;

  // Information about the local peer, including the local UUID.
  RaftPeerPB local_peer_pb_;

  // Consensus metadata service.
  const scoped_refptr<ConsensusMetadataManager> cmeta_manager_;

  // Persistent Vars service
  const scoped_refptr<PersistentVarsManager> persistent_vars_manager_;

  ThreadPool* const raft_pool_;

  // TODO(dralves) hack to serialize updates due to repeated/out-of-order
  // messages should probably be refactored out.
  //
  // Lock ordering note: If both 'update_lock_' and 'lock_' are to be taken,
  // 'update_lock_' lock must be taken first.
  mutable simple_mutexlock update_lock_;

  // Coarse-grained lock that protects all mutable data members.
  mutable simple_mutexlock lock_;

  State state_;

  // Consensus metadata persistence object.
  scoped_refptr<ConsensusMetadata> cmeta_;

  // Persistent vars object
  scoped_refptr<PersistentVars> persistent_vars_;

  // The policy used to route requests from leader through intermediate proxy
  // peers
  ProxyPolicy proxy_policy_ = ProxyPolicy::DURABLE_ROUTING_POLICY;

  std::vector<std::unordered_set<std::string>> proxy_region_groups_ = {};

  // Proxy routing table object. The right table is built based on proxy_policy
  std::shared_ptr<RoutingTableContainer> routing_table_container_;

  // Threadpool token for constructing requests to peers, handling RPC
  // callbacks, etc.
  std::unique_ptr<ThreadPoolToken> raft_pool_token_;

  scoped_refptr<log::Log> log_;
  scoped_refptr<ITimeManager> time_manager_;
  std::unique_ptr<PeerProxyFactory> peer_proxy_factory_;

  // When we receive a message from a remote peer telling us to start a
  // transaction, or finish a round, we use this handler to handle it.
  // This may update replica state (e.g. the tablet replica).
  ConsensusRoundHandler* round_handler_;

  std::unique_ptr<PeerManager> peer_manager_;

  // The queue of messages that must be sent to peers.
  std::unique_ptr<PeerMessageQueue> queue_;

  // The currently pending rounds that have not yet been committed by
  // consensus. Protected by 'lock_'.
  // TODO(todd) these locks will become more fine-grained.
  std::unique_ptr<PendingRounds> pending_;

  Random rng_;

  std::shared_ptr<rpc::PeriodicTimer> failure_detector_;
  std::chrono::system_clock::time_point failure_detector_last_snoozed_;
  folly::Synchronized<std::optional<MonoDelta>> failure_detector_time_left_ =
      {};

  AtomicBool leader_transfer_in_progress_;
  std::optional<std::string> designated_successor_uuid_;
  std::shared_ptr<rpc::PeriodicTimer> transfer_period_timer_;

  // Lock held while starting a failure-triggered election.
  //
  // After reporting a failure and asynchronously starting an election, the
  // failure detector immediately rearms. If the election starts slowly (i.e.
  // there's a lot of contention on the consensus lock, or persisting votes is
  // really slow due to other I/O), more elections may start and "stack" on
  // top of the first. Forcing the starting of elections to serialize on this
  // lock prevents that from happening. See KUDU-2149 for more details.
  //
  // Note: the lock is only ever acquired via try_lock(); if it cannot be
  // acquired, a StartElection() is in progress so the next one is skipped.
  //
  // TODO(KUDU-2155): should be replaced with explicit disabling/enabling of
  // the failure detector during elections.
  simple_mutexlock failure_detector_election_lock_;

  // If any RequestVote() RPC arrives before this timestamp,
  // the request will be ignored. This prevents abandoned or partitioned
  // nodes from disturbing the healthy leader.
  MonoTime withhold_votes_until_;

  // Identifies the term for which the Leader Lease is active.
  int64_t leader_lease_term_;

  // This is used in tests to reject AppendEntries RPC requests.
  bool reject_append_entries_;

  // Should we adjust voter distribution based on current config?
  bool adjust_voter_distribution_;

  // This is used in tests to reject RequestVote RPC requests.
  bool withhold_votes_;

  // The last OpId received from the current leader. This is updated whenever
  // the follower accepts operations from a leader, and passed back so that the
  // leader knows from what point to continue sending operations.
  OpId last_received_cur_leader_;

  // The number of times this node has called and lost a leader election since
  // the last time it saw a stable leader (either itself or another node).
  // This is used to calculate back-off of the election timeout.
  int64_t failed_elections_since_stable_leader_;

  // Number of times this node has started and lost a leader (pre) election and
  // the voters responded with 'candidate-removed' response i.e this candidate
  // was not present in the active config of the voters.
  // The counter is reset when this node hears from a valid leader
  int64_t failed_elections_candidate_not_in_config_;

  std::atomic<bool> leader_lease_state_;

  Callback<void(const std::string& reason)> mark_dirty_clbk_;

  // Explicitly registered callbacks.
  ElectionDecisionCallback edcb_;
  TermAdvancementCallback tacb_;
  NoOpReceivedCallback norcb_;
  LeaderDetectedCallback ldcb_;

  // this is not expected to change after a create of Raft.
  bool disable_noop_;

  // Vote logger for voting events
  std::shared_ptr<VoteLoggerInterface> vote_logger_;

  // A flag to help us avoid taking a lock on the reactor thread if the object
  // is already in kShutdown state.
  // TODO(mpercy): Try to get rid of this extra flag.
  AtomicBool shutdown_;

  // The number of times Update() has been called, used for some test
  // assertions.
  AtomicInt<int32_t> update_calls_for_tests_;

  FunctionGaugeDetacher metric_detacher_;

  std::atomic<int64_t> last_leader_communication_time_micros_;

  scoped_refptr<Counter> follower_memory_pressure_rejections_;
  scoped_refptr<AtomicGauge<int64_t>> term_metric_;
  scoped_refptr<AtomicGauge<int64_t>> num_failed_elections_metric_;

  // Have we queued LDCB or NORCB in raft thread pool at least once?
  bool have_queued_ldcb_or_norcb_ = false;

  // Number of times ops in raft log were truncated as a result of new leader
  // overwriting the log
  scoped_refptr<Counter> raft_log_truncation_counter_;

  // Proxy metrics.
  scoped_refptr<Counter> raft_proxy_num_requests_received_;
  scoped_refptr<Counter> raft_proxy_num_requests_success_;
  scoped_refptr<Counter> raft_proxy_num_requests_unknown_dest_;
  scoped_refptr<Counter> raft_proxy_num_requests_log_read_timeout_;
  scoped_refptr<Counter> raft_proxy_num_requests_hops_remaining_exhausted_;

  faststring compression_buffer_;

  CheckQuorumFailureCallback check_quorum_failure_callback_;
  int32_t check_quorum_interval_heartbeats_;
  std::mutex check_quorum_running_;
  std::shared_ptr<kudu::rpc::PeriodicTimer> check_quorum_timer_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensus);
};

// Handler for consensus rounds.
// An implementation of this handler must be registered prior to consensus
// start, and is used to:
// - Create transactions when the consensus implementation receives messages
//   from the leader.
// - Handle when the consensus implementation finishes a non-transaction round
//
// Follower transactions execute the following way:
//
// - When a ReplicateMsg is first received from the leader, the RaftConsensus
//   instance creates the ConsensusRound and calls StartFollowerTransaction().
//   This will trigger the Prepare(). At the same time, the follower's consensus
//   instance immediately stores the ReplicateMsg in the Log. Once the
//   message is stored in stable storage an ACK is sent to the leader (i.e. the
//   replica RaftConsensus instance does not wait for Prepare() to finish).
//
// - When the CommitMsg for a replicate is first received from the leader, the
//   follower waits for the corresponding Prepare() to finish (if it has not
//   completed yet) and then proceeds to trigger the Apply().
//
// - Once Apply() completes the ConsensusRoundHandler is responsible for logging
//   a CommitMsg to the log to ensure that the operation can be properly
//   restored on a restart.
class ConsensusRoundHandler {
 public:
  virtual ~ConsensusRoundHandler() = default;

  virtual Status StartFollowerTransaction(
      const scoped_refptr<ConsensusRound>& context) = 0;

  virtual Status StartConsensusOnlyRound(
      const scoped_refptr<ConsensusRound>& context) = 0;

  // Consensus-only rounds complete when non-transaction ops finish
  // replication. This can be used to trigger callbacks, akin to an Apply() for
  // transaction ops.
  virtual void FinishConsensusOnlyRound(ConsensusRound* round) = 0;
};

// Context for a consensus round on the LEADER side, typically created as an
// out-parameter of RaftConsensus::Append().
// This class is ref-counted because we want to ensure it stays alive for the
// duration of the Transaction when it is associated with a Transaction, while
// we also want to ensure it has a proper lifecycle when a ConsensusRound is
// pushed that is not associated with a Tablet transaction.
class ConsensusRound : public RefCountedThreadSafe<ConsensusRound> {
 public:
  // Ctor used for leader transactions. Leader transactions can and must specify
  // the callbacks prior to initiating the consensus round.
  ConsensusRound(
      RaftConsensus* consensus,
      std::unique_ptr<ReplicateMsg> replicate_msg,
      ConsensusReplicatedCallback replicated_cb);

  // Ctor used when the ConsensusReplicatedCallback will be set after the round
  // is created.
  ConsensusRound(RaftConsensus* consensus, ReplicateRefPtr replicate_msg);

  ReplicateMsg* replicate_msg() {
    return replicate_msg_->get();
  }

  const ReplicateRefPtr& replicate_scoped_refptr() {
    return replicate_msg_;
  }

  // Returns the id of the (replicate) operation this context
  // refers to. This is only set _after_ RaftConsensus::Replicate(context).
  OpId id() const {
    return replicate_msg_->get()->id();
  }

  // Register a callback that is called by RaftConsensus to notify that the
  // round is considered either replicated, if 'status' is OK(), or that it has
  // permanently failed to replicate if 'status' is anything else. If 'status'
  // is OK() then the operation can be applied to the state machine, otherwise
  // the operation should be aborted.
  void SetConsensusReplicatedCallback(
      ConsensusReplicatedCallback replicated_cb) {
    replicated_cb_ = std::move(replicated_cb);
  }

  // If a continuation was set, notifies it that the round has been replicated.
  void NotifyReplicationFinished(const Status& status);

  // Binds this round such that it may not be eventually executed in any term
  // other than 'term'.
  // See CheckBoundTerm().
  void BindToTerm(int64_t term) {
    DCHECK_EQ(bound_term_, -1);
    bound_term_ = term;
  }

  // Check for a rare race in which an operation is submitted to the LEADER in
  // some term, then before the operation is prepared, the replica loses its
  // leadership, receives more operations as a FOLLOWER, and then regains its
  // leadership. We detect this case by setting the ConsensusRound's "bound
  // term" when it is first submitted to the PREPARE queue, and validate that
  // the term is still the same when we have finished preparing it. See KUDU-597
  // for details.
  //
  // If this round has not been bound to any term, this is a no-op.
  Status CheckBoundTerm(int64_t current_term) const;

  const std::string& tablet_id() const {
    return consensus_->tablet_id();
  }

 private:
  friend class RefCountedThreadSafe<ConsensusRound>;
  friend class RaftConsensusQuorumTest;

  ~ConsensusRound() = default;

  RaftConsensus* consensus_;
  // This round's replicate message.
  ReplicateRefPtr replicate_msg_;

  // The continuation that will be called once the transaction is
  // deemed committed/aborted by consensus.
  ConsensusReplicatedCallback replicated_cb_;

  // The leader term that this round was submitted in. CheckBoundTerm()
  // ensures that, when it is eventually replicated, the term has not
  // changed in the meantime.
  //
  // Set to -1 if no term has been bound.
  int64_t bound_term_;
};

} // namespace consensus
} // namespace kudu
