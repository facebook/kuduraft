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

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <optional>

#include "kudu/consensus/flags_layering.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/peer_message_buffer.h"
#include "kudu/consensus/persistent_vars.h"
#include "kudu/consensus/persistent_vars_manager.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

DECLARE_int32(bounded_dataloss_window_interval_ms);
DECLARE_bool(buffer_messages_between_rpcs);
DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_bool(enable_bounded_dataloss_window);
DECLARE_bool(enable_flexi_raft);
DECLARE_bool(enable_raft_leader_lease);
DECLARE_int32(raft_leader_lease_interval_ms);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_int32(min_corruption_count);
DECLARE_int32(min_single_corruption_count);

namespace kudu {
class ThreadPoolToken;

namespace log {
class Log;
}

namespace logging {
class LogThrottler;
}

namespace consensus {
class ConsensusRequestPB;
class ConsensusResponsePB;
class ConsensusStatusPB;
class PeerMessageQueueObserver;
class ReplicateMsgWrapper;

// The id for the server-wide consensus queue MemTracker.
extern const char kConsensusQueueParentTrackerId[];

// State enum for the last known status of a peer tracked by the
// ConsensusQueue.
enum class PeerStatus {
  // The peer has not yet had a round of communication.
  NEW,

  // The last exchange with the peer was successful. We transmitted
  // an update to the peer and it accepted it.
  OK,

  // Some tserver-level or consensus-level error occurred that didn't
  // fall into any of the below buckets.
  REMOTE_ERROR,

  // Some RPC-layer level error occurred. For example, a network error or
  // timeout
  // occurred while attempting to send the RPC.
  RPC_LAYER_ERROR,

  // The remote tablet server indicated that the tablet was in a FAILED state.
  TABLET_FAILED,

  // The remote tablet server indicated that the tablet was in a NOT_FOUND
  // state.
  TABLET_NOT_FOUND,

  // The remote tablet server indicated that the term of this leader was older
  // than its latest seen term.
  INVALID_TERM,

  // The remote tablet server was unable to prepare any operations in the most
  // recent
  // batch.
  CANNOT_PREPARE,

  // The remote tablet server's log was divergent from the leader's log.
  LMP_MISMATCH,
};

const char* PeerStatusToString(PeerStatus p);

// Tracks the state of the peers and which transactions they have replicated.
// Owns the LogCache which actually holds the replicate messages which are
// en route to the various peers.
//
// This also takes care of pushing requests to peers as new operations are
// added, and notifying RaftConsensus when the commit index advances.
//
// TODO(todd): Right now this class is able to track one outstanding operation
// per peer. If we want to have more than one outstanding RPC we need to
// modify it.
class PeerMessageQueue {
 public:
  struct TrackedPeer {
    explicit TrackedPeer(RaftPeerPB peer_pb, const PeerMessageQueue* queue);

    TrackedPeer() = default;

    // Copy a given TrackedPeer.
    TrackedPeer& operator=(const TrackedPeer& tracked_peer) = default;

    // Check that the terms seen from a given peer only increase
    // monotonically.
    void CheckMonotonicTerms(int64_t term) {
      DCHECK_GE(term, last_seen_term_);
      last_seen_term_ = term;
    }

    const std::string& uuid() const {
      return peer_pb.permanent_uuid();
    }

    std::string ToString() const;

    RaftPeerPB peer_pb;

    // Next index to send to the peer.
    // This corresponds to "nextIndex" as specified in Raft.
    int64_t next_index;

    // The last operation that we've sent to this peer and that
    // it acked. Used for watermark movement.
    OpId last_received;

    // The last committed index this peer knows about.
    int64_t last_known_committed_index;

    // The status after our last attempt to communicate with the peer.
    // See the comments within the PeerStatus enum above for details.
    PeerStatus last_exchange_status;

    // Last OpId when Peer granted Lease duration
    OpId lease_granted;

    // Last OpId when Peer ACKed the Leader for Bounded DataLoss window.
    OpId bounded_dataloss_window_acked;

    // The time of the last successful Raft consensus exchange with the peer
    // Defaults to the time of construction, so does not necessarily mean that
    // successful communication ever took place.
    MonoTime last_successful_exchange;

    // The time of the last communication with the peer.
    //
    // NOTE: this does not indicate that the peer successfully made progress at
    // the given time -- this only indicates that we got some indication that
    // the tablet server process was alive. It could be that the tablet was not
    // found, etc. Consult last_exchange_status and last_successful_exchange for
    // details.
    //
    // Defaults to the time of construction, so does not necessarily mean that
    // successful communication ever took place.
    MonoTime last_communication_time;

    // The number of times we've reported corruption since last successful
    // exchange
    int32_t corruption_count = 0;

    // Leader Leases: captures UpdateConsensus rpc start time for each peer
    MonoTime rpc_start_;

    // Set to false if it is determined that the remote peer has fallen behind
    // the local peer's WAL.
    bool wal_catchup_possible;

    // Should we send compression dictionary in the next request to this peer?
    bool should_send_compression_dict = true;

    // The peer's latest overall health status.
    HealthReportPB::HealthStatus last_overall_health_status;

    // Throttler for how often we will log status messages pertaining to this
    // peer (eg when it is lagging, etc).
    std::shared_ptr<logging::LogThrottler> status_log_throttler;

    std::optional<bool> is_peer_in_local_quorum;
    std::optional<bool> is_peer_in_local_region;

    std::shared_ptr<PeerMessageBuffer> peer_msg_buffer;

    void PopulateIsPeerInLocalRegion();
    void PopulateIsPeerInLocalQuorum();

    // Determines health based on number of consecutive rpc failures exceeding
    // a configured limit.
    bool is_healthy() const;

    int32_t consecutive_failures() const;
    void incr_consecutive_failures();
    void reset_consecutive_failures();
    // Used for tests.
    void set_consecutive_failures(int32_t value);

    // Whether proxying to this instance is enabled.
    bool ProxyTargetEnabled() const;

    // Disable proxying to this instance for some time delta.
    void SnoozeProxying(MonoDelta delta);

    // Report that peer thinks the message for next_index is corrupted
    void ReportCorruption();

   private:
    // The last term we saw from a given peer.
    // This is only used for sanity checking that a peer doesn't
    // go backwards in time.
    int64_t last_seen_term_;

    // Number of consecutive errors received by a leader from a peer. This
    // counter is not meaningful for a non-leader.
    int32_t consecutive_failures_;

    MonoTime proxying_disabled_until_;

    std::shared_ptr<TimeProvider> time_provider_;

    const PeerMessageQueue* queue = nullptr;
  };

  struct TransferContext {
    std::chrono::system_clock::time_point original_start_time;
    std::string original_uuid;
    bool is_origin_dead_promotion;
  };

  enum QuorumIdHealthStatus {
    UNKNOWN = 0,
    UNHEALTHY = 1,
    AT_RISK = 2,
    DEGRADED = 3,
    HEALTHY = 4
  };

  struct QuorumIdHealth {
    // Whether the leader exists in this quorum id.
    bool primary = false;

    // Number of active voting peers.
    int total_voters = 0;

    // Number of voters defined in voter distribution.
    int num_vd_voters = 0;

    int quorum_size = 0;

    QuorumIdHealthStatus health_status = UNKNOWN;

    std::vector<RaftPeerPB> healthy_peers;

    std::vector<RaftPeerPB> unhealthy_peers;
  };

  struct QuorumHealth {
    std::unordered_map<std::string, QuorumIdHealth> by_quorum_id;
  };

  static const std::string kVanillaRaftQuorumId;

  PeerMessageQueue(
      const scoped_refptr<MetricEntity>& metric_entity,
      scoped_refptr<log::Log> log,
      scoped_refptr<ITimeManager> time_manager,
      const scoped_refptr<PersistentVarsManager>& persistent_vars_manager,
      RaftPeerPB local_peer_pb,
      std::shared_ptr<RoutingTableContainer> routing_table_container,
      std::string tablet_id,
      std::unique_ptr<ThreadPoolToken> raft_pool_observers_token,
      OpId last_locally_replicated,
      const OpId& last_locally_committed);

  // Changes the queue to leader mode, meaning it tracks majority replicated
  // operations and notifies observers when those change.
  // 'committed_index' corresponds to the id of the last committed operation,
  // i.e. operations with ids <= 'committed_index' should be considered
  // committed.
  //
  // 'current_term' corresponds to the leader's current term, this is different
  // from 'committed_index.term()' if the leader has not yet committed an
  // operation in the current term.
  // 'active_config' is the currently-active Raft config. This must always be
  // a superset of the tracked peers, and that is enforced with runtime CHECKs.
  void SetLeaderMode(
      int64_t committed_index,
      int64_t current_term,
      const RaftConfigPB& active_config);

  // Changes the queue to non-leader mode. Currently tracked peers will still
  // be tracked so that the cache is only evicted when the peers no longer need
  // the operations but the queue will no longer advance the majority replicated
  // index or notify observers of its advancement.
  void SetNonLeaderMode(const RaftConfigPB& active_config);

  // Makes the queue track this peer.
  void TrackPeer(const RaftPeerPB& peer_pb);

  // Makes the queue untrack this peer.
  void UntrackPeer(const std::string& uuid);

  // Returns a health report for all active peers.
  // Returns IllegalState if the local peer is not the leader of the config.
  std::unordered_map<std::string, HealthReportPB> ReportHealthOfPeers() const;

  // Appends a single message to be replicated to the peers.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size).
  // If it returns OK the queue takes ownership of 'msg'.
  //
  // This is thread-safe against all of the read methods, but not thread-safe
  // with concurrent Append calls.
  Status AppendOperation(const ReplicateRefPtr& msg);

  // Just like AppendOperation() above but with msg wrapper as input
  Status AppendOperation(const ReplicateMsgWrapper& msg_wrappers);

  // Appends a vector of messages to be replicated to the peers.
  // Returns OK unless the message could not be added to the queue for some
  // reason (e.g. the queue reached max size), calls 'log_append_callback' when
  // the messages are durable in the local Log.
  // If it returns OK the queue takes ownership of 'msgs'.
  //
  // This is thread-safe against all of the read methods, but not thread-safe
  // with concurrent Append calls.
  Status AppendOperations(
      const std::vector<ReplicateRefPtr>& msgs,
      const StatusCallback& log_append_callback);

  // Just like AppendOperations() above but with msg wrappers as input
  Status AppendOperations(
      const std::vector<ReplicateMsgWrapper>& msgs,
      const StatusCallback& log_append_callback);

  // Truncate all operations coming after 'index'. Following this, the
  // 'last_appended' operation is reset to the OpId with this index, and the log
  // cache will be truncated accordingly.
  void TruncateOpsAfter(int64_t index);

  // Return the last OpId in the log.
  // Note that this can move backwards after a truncation (TruncateOpsAfter).
  OpId GetLastOpIdInLog() const;

  // Return the next OpId to be appended to the queue in the current term.
  OpId GetNextOpId() const;

  // Get the TrackedPeer corresponding to uuid. The tracked-peer is returned in
  // 'peer'
  //
  // Returns Status::OK() if peer exists.
  // Returns Status::NotFound() if peer does not exist (and peer is set to
  // nullptr)
  Status FindPeer(const std::string& uuid, TrackedPeer* peer);

  // Assembles a request for a peer, adding entries past 'op_id' up to
  // 'consensus_max_batch_size_bytes'.
  // Returns OK if the request was assembled, or Status::NotFound() if the
  // peer with 'uuid' was not tracked, of if the queue is not in leader mode.
  // Returns Status::Incomplete if we try to read an operation index from the
  // log that has not been written.
  //
  // WARNING: In order to avoid copying the same messages to every peer,
  // entries are added to 'request' via AddAllocated() methods.
  // The owner of 'request' is expected not to delete the request prior
  // to removing the entries through ExtractSubRange() or any other method
  // that does not delete the entries. The simplest way is to pass the same
  // instance of ConsensusRequestPB to RequestForPeer(): the buffer will
  // replace the old entries with new ones without de-allocating the old
  // ones if they are still required.
  Status RequestForPeer(
      const std::string& uuid,
      bool read_ops,
      ConsensusRequestPB* request,
      std::vector<ReplicateRefPtr>* msg_refs,
      bool* needs_tablet_copy,
      std::string* next_hop_uuid);

  /**
   * Fills up the buffer for a peer.
   *
   * Each TrackedPeer has a buffer that's filled up while we wait for the RPC to
   * the peer to return. This method looks into the log cache and fills up the
   * buffer optimistically with new ops that are not buffered. The fill is
   * optimistic in the sense that we assume the RPC in progress will fully
   * succeed, and we can simply extract the buffer to send as the next RPC as
   * it'll be continuous from the RPC in progress.
   *
   * Fill is proxy aware. So it will entire leave the buffer with op formats
   * that matches the proxy setting (if we're proxying or not), or not leave it
   * with anything at all.
   *
   * A latest_appended_replicate can also be passed to this method. If the new
   * replicate happens to be the very next op we should be appending into the
   * buffer, this method directly appends the RPC and exits. If not, it'll
   * append the next op by reading the log cache.
   *
   * @param uuid The uuid of the TrackedPeer to fill buffers for
   * @param latest_appended_replicate If not a nullptr, the next replicate to
   * fill into the buffer
   */
  void FillBufferForPeer(
      const std::string& uuid,
      ReplicateRefPtr latest_appended_replicate = nullptr);

  // Inform the queue of a new status known for one of its peers.
  // 'ps' indicates an interpretation of the status, while 'status'
  // may contain a more specific error message in the case of one of
  // the error statuses.
  void UpdatePeerStatus(
      const std::string& peer_uuid,
      PeerStatus ps,
      const Status& status);

  // Updates the request queue with the latest response from a request to a
  // consensus peer.
  // Returns true iff there are more requests pending in the queue for this
  // peer and another request should be sent immediately, with no intervening
  // delay.
  bool ResponseFromPeer(
      const std::string& peer_uuid,
      const ConsensusResponsePB& response);

  // The method that does most of the heavy lifting of ResponseFromPeer
  bool DoResponseFromPeer(
      const std::string& peer_uuid,
      const ConsensusResponsePB& response,
      std::optional<int64_t>& updated_commit_index);

  // Called by the consensus implementation to update the queue's watermarks
  // based on information provided by the leader. This is used for metrics and
  // log retention.
  void UpdateFollowerWatermarks(
      int64_t committed_index,
      int64_t all_replicated_index,
      int64_t region_durable_index);

  // Updates the last op appended to the leader and the corresponding lag
  // metric. This should not be called by a leader.
  void UpdateLastIndexAppendedToLeader(int64_t last_idx_appended_to_leader);

  // If leader, checks whether it can successfully commit to majority of peers.
  bool CheckQuorum();

  // Closes the queue. Once the queue is closed, peers are still allowed to
  // call UntrackPeer() and ResponseFromPeer(), however no additional peers may
  // be tracked and no additional messages may be enqueued.
  void Close();

  int64_t GetQueuedOperationsSizeBytesForTests() const;

  // Returns the last message replicated by all peers.
  int64_t GetAllReplicatedIndex() const;

  // Returns the committed index. All operations with index less than or equal
  // to this index have been committed.
  int64_t GetCommittedIndex() const;

  // Returns the index that is deemed to be 'region-durable'
  // Check region_durable_index
  int64_t GetRegionDurableIndex() const;

  // Return true if the committed index falls within the current term.
  bool IsCommittedIndexInCurrentTerm() const;

  // Whether the queue run in the leader mode.
  bool IsInLeaderMode() const;

  // Returns the current majority replicated index, for tests.
  int64_t GetMajorityReplicatedIndexForTests() const;

  // Updates peer. Only used for tests.
  void UpdatePeerForTests(
      const std::string& peer_uuid,
      const std::function<void(TrackedPeer*)>& fn);

  // Returns a copy of the TrackedPeer with 'uuid' or crashes if the peer is
  // not being tracked.
  TrackedPeer GetTrackedPeerForTests(const std::string& uuid);

  // Returns TrackedPeer reference with 'uuid' or crashes if the peer is not
  // being tracked.
  TrackedPeer* GetTrackedPeerRefForTests(const std::string& uuid);

  std::string ToString() const;

  // Dumps the contents of the queue to the provided string vector.
  void DumpToStrings(std::vector<std::string>* lines) const;

  void RegisterObserver(PeerMessageQueueObserver* observer);

  Status UnRegisterObserver(PeerMessageQueueObserver* observer);

  struct Metrics {
    // Keeps track of the number of ops. that are completed by a majority but
    // still need to be replicated to a minority (IsDone() is true, IsAllDone()
    // is false).
    scoped_refptr<AtomicGauge<int64_t>> num_majority_done_ops;
    // Keeps track of the number of ops. that are still in progress (IsDone()
    // returns false).
    scoped_refptr<AtomicGauge<int64_t>> num_in_progress_ops;
    // Keeps track of the number of ops. behind the leader the peer is, measured
    // as the difference between the latest appended op index on this peer
    // versus on the leader (0 if leader).
    scoped_refptr<AtomicGauge<int64_t>> num_ops_behind_leader;
    // Number of check quorum runs.
    scoped_refptr<Counter> check_quorum_runs;
    // Number of check quorum failures.
    scoped_refptr<Counter> check_quorum_failures;
    // Keeps track of number of remote peers that are Leader lease grantors.
    scoped_refptr<AtomicGauge<int64_t>> available_leader_lease_grantors;
    // Keeps track of number of remote peers that are Bounded DataLoss window
    // ACKers.
    scoped_refptr<AtomicGauge<int64_t>>
        available_bounded_dataloss_window_ackers;
    // Number of peers, including leader, that are healthy in commit quorum.
    scoped_refptr<AtomicGauge<int64_t>> available_commit_peers;
    // Number of cache drops due to corruption
    scoped_refptr<Counter> corruption_cache_drops;
    // Number of cache drops from errors of a single peer
    scoped_refptr<Counter> single_corruption_cache_drops;

    explicit Metrics(const scoped_refptr<MetricEntity>& metric_entity);
  };

  ~PeerMessageQueue();

  // Begin or end the watch for an eligible successor. If 'successor_uuid' is
  // not {}, the queue will notify its observers when 'successor_uuid'
  // is caught up to the leader. Otherwise, it will notify its observers with
  // the UUID of the first voter that is caught up.
  void BeginWatchForSuccessor(
      const std::optional<std::string>& successor_uuid,
      const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
      TransferContext transfer_context);
  void EndWatchForSuccessor();

  Status GetSnapshotForMockElection(
      const std::string& new_leader_uuid,
      OpId* snapshot_op_id);

  // If the previous call to BeginWatchForSuccessor had resulted in a
  // notification to a peer to start an election
  bool WatchForSuccessorPeerNotified();

  // Get the UUID of the next routing hop from the local node.
  // Results not guaranteed to be valid if the current node is not the leader.
  Status GetNextRoutingHopFromLeader(
      const std::string& dest_uuid,
      std::string* next_hop) const;

  // TODO(mpercy): It's probably not safe in general to access a queue's log
  // cache via bare pointer, since (IIRC) a queue will be reconstructed
  // transitioning to/from leader. Check this.
  std::shared_ptr<LogCache> log_cache() {
    return log_cache_;
  }

  Status SetCompressionDictionary(const std::string& dict);

  // Set the threshold (in milliseconds) that is used to determine the health of
  // the 'proxy peer'
  void SetProxyFailureThreshold(int32_t proxy_failure_threshold_ms);

  // Set the lag threshold (as compared to destination peer) that is used to
  // determine the health of the 'proxy peer'
  void SetProxyFailureThresholdLag(int32_t proxy_failure_threshold_lag);

  // Check if the 'proxy_peer' is healthy enough to act as a proxy to ship
  // messages to 'dest_peer'.
  // Returns 'true' if the 'proxy_peer' has failed proxy health checks and
  // cannot act as a proxy peer, 'false' otherwise.
  // TODO: The method used to check for the proxy peer's health only works on
  // the leader. Hence it does not support multi hop proxying yet
  bool HasProxyPeerFailedUnlocked(
      const TrackedPeer* proxy_peer,
      const TrackedPeer* dest_peer);

  void SetAdjustVoterDistribution(bool val) {
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    adjust_voter_distribution_ = val;
  }

  // Update quorum id in peers_map
  void UpdatePeerQuorumIdUnlocked(
      const std::map<std::string, std::string>& quorum_id_map);

  // Whether peer's region/quorum id has a majority of committers being tracked.
  bool RegionHasQuorumCommitUnlocked(const RaftPeerPB& target_peer);

  // Returns number of healthy peers that can most likely accept replicated
  // writes based on recent RPC failure rates.
  //
  // If local peer is a leader, this method return at least '1' since it always
  // deems itself as a healthy peer.
  // If local peer is not a leader, this method will return -1.
  //
  // This method only works for SINGLE_REGION_DYNAMIC mode. If we're not in this
  // mode, this method will return -1.
  int32_t GetAvailableCommitPeers();

  // If leader, returns quorum health for all regions/quorum ids.
  Status GetQuorumHealth(QuorumHealth* health);

  // Gets the Leader Lease timestamp
  MonoTime GetLeaderLeaseUntil();

  // Get the bounded data loss window expiry timestamp
  MonoTime GetBoundedDataLossWindowUntil();

  // Sets the Leader lease until timestamp
  void SetLeaderLeaseUntil(MonoTime update) {
    leader_lease_until_ = update;
  }

  // Return the Leader Lease timeout.
  static MonoDelta LeaderLeaseTimeout();

  // Return the default window size for the bounded data loss tracker.
  static MonoDelta BoundedDataLossDefaultWindowInMsec();

  // Sets the UpdateConsensus rpc start time for peer
  void SetPeerRpcStartTime(const std::string& peer_uuid, MonoTime rpcStart);

 private:
  FRIEND_TEST(ConsensusQueueTest, TestQueueAdvancesCommittedIndex);
  FRIEND_TEST(ConsensusQueueTest, TestQueueMovesWatermarksBackward);
  FRIEND_TEST(ConsensusQueueTest, TestFollowerCommittedIndexAndMetrics);
  FRIEND_TEST(ConsensusQueueUnitTest, PeerHealthStatus);
  FRIEND_TEST(
      RaftConsensusQuorumTest,
      TestReplicasEnforceTheLogMatchingProperty);

  // Mode specifies how the queue currently behaves:
  // LEADER - Means the queue tracks remote peers and replicates whatever
  // messages
  //          are appended. Observers are notified of changes.
  // NON_LEADER - Means the queue only tracks the local peer (remote peers are
  // ignored).
  //              Observers are not notified of changes.
  enum Mode { LEADER, NON_LEADER };

  enum State { kQueueOpen, kQueueClosed };

  // Types of replicas to count when advancing a queue watermark.
  enum ReplicaTypes {
    ALL_REPLICAS,
    VOTER_REPLICAS,
  };

  struct QueueState {
    // The first operation that has been replicated to all currently
    // tracked peers.
    int64_t all_replicated_index;

    // The index of the last operation replicated to a majority.
    // This is usually the same as 'committed_index' but might not
    // be if the terms changed.
    int64_t majority_replicated_index;

    // The index of the last operation to be considered committed.
    int64_t committed_index;

    // The index that is deemed to have been 'region-durable'.
    // This index is updated when the OpId is replicated to atleast one
    // additional region (other than the current leader region). This is useful
    // only when the raft ring is configured to have nodes in multiple region as
    // defined in RaftPeerAttrsPB
    int64_t region_durable_index;

    // The index of the last operation appended to the leader. A follower will
    // use this to determine how many ops behind the leader it is, as a soft
    // metric for follower lag.
    int64_t last_idx_appended_to_leader;

    // The opid of the last operation appended to the queue.
    OpId last_appended;

    // The queue's owner current_term.
    // Set by the last appended operation.
    // If the queue owner's term is less than the term observed
    // from another peer the queue owner must step down.
    int64_t current_term;

    // The first index that we saw that was part of this current term.
    // When the term advances, this is set to {}, and then set
    // when the first operation is appended in the new term.
    std::optional<int64_t> first_index_in_current_term;

    // The size of the majority for the queue.
    int majority_size_;

    State state;

    // The current mode of the queue.
    Mode mode;

    // The currently-active raft config. Only set if in LEADER mode.
    std::unique_ptr<RaftConfigPB> active_config;

    std::string ToString() const;
  };

  struct QuorumResults {
    bool quorum_satisfied;
    int32_t num_satisfied;
    int32_t quorum_size;
    std::string quorum_id;
    std::vector<TrackedPeer*> quorum_peers;
  };

  // Returns true iff given 'desired_op' is found in the local WAL.
  // If the op is not found, returns false.
  // If the log cache returns some error other than NotFound, crashes with a
  // fatal error.
  bool IsOpInLog(const OpId& desired_op) const;

  // Return true if it would be safe to evict the peer 'evict_uuid' at this
  // point in time.
  bool SafeToEvictUnlocked(const std::string& evict_uuid) const;

  // Update a peer's last_health_status field and trigger the appropriate
  // notifications.
  void UpdatePeerHealthUnlocked(TrackedPeer* peer);

  // Update the peer's last exchange status, and other fields, based on the
  // response. Sets 'lmp_mismatch' to true if the given response indicates
  // there was a log-matching property mismatch on the remote, otherwise sets
  // it to false.
  void UpdateExchangeStatus(
      TrackedPeer* peer,
      const TrackedPeer& prev_peer_state,
      const ConsensusResponsePB& response,
      bool* lmp_mismatch);

  /**
   * Process a total append failure (couldn't append anything) from the peer
   * based on the reason the append failed.
   *
   * Currently, we have two cases:
   * 1. CompressionDictMismatch
   *  - Send the dictionary on the next RPC
   * 2. Corruption
   *  - Evicts the log cache up till where we suspect corruption had occured
   *  - See CorruptionLikely for the corruption detection policy
   *
   * @param peer The peer that failed to append
   * @param status The status of the append from the peer. See
   * ConsensusErrorPB::error
   */
  void UpdatePeerAppendFailure(TrackedPeer* peer, const Status& status);

  /***
   * Returns true if it's likely that there's a corruption for the next index on
   * the peer.
   *
   * @param peer The peer complaining about corruption
   * @return true if it's likely the next op the peer needs is corrupted on
   * leader
   */
  bool CorruptionLikely(TrackedPeer* peer) const;

  // Check if the peer is a NON_VOTER candidate ready for promotion. If so,
  // trigger promotion.
  void PromoteIfNeeded(
      TrackedPeer* peer,
      const TrackedPeer& prev_peer_state,
      const ConsensusStatusPB& status);

  // If there is a graceful leadership change underway, notify queue observers
  // to initiate leadership transfer to the specified peer under the following
  // conditions:
  // * 'peer' has fully caught up to the leader
  // * 'peer' is the designated successor, or no successor was designated
  void TransferLeadershipIfNeeded(
      const TrackedPeer& peer,
      const ConsensusStatusPB& status);

  // returns true if basic checks to transfer leadership to this peer are OK
  // current queue should be in leader state and peer must be a VOTER
  // Also returns a pointer to the RaftPeerPB if successful
  bool BasicChecksOKToTransferAndGetPeerUnlocked(
      const TrackedPeer& peer,
      RaftPeerPB** peer_pb_ptr);

  // If the peer is caught up, notify the peer to start an election
  // immediately
  bool PeerTransferLeadershipImmediatelyUnlocked(const std::string& peer_uuid);

  // Calculate a peer's up-to-date health status based on internal fields.
  static HealthReportPB::HealthStatus PeerHealthStatus(const TrackedPeer& peer);

  // Asynchronously trigger various types of observer notifications on a
  // separate thread.
  void NotifyObserversOfCommitIndexChange(
      int64_t new_commit_index,
      bool need_lock = true);
  void NotifyObserversOfTermChange(int64_t term);
  void NotifyObserversOfFailedFollower(
      const std::string& uuid,
      int64_t term,
      const std::string& reason);
  void NotifyObserversOfPeerToPromote(const std::string& peer_uuid);
  void NotifyObserversOfSuccessor(const std::string& peer_uuid);
  void NotifyObserversOfPeerHealthChange();

  // Notify all PeerMessageQueueObservers using the given callback function.
  void NotifyObserversTask(
      const std::function<void(PeerMessageQueueObserver*)>& func);

  using PeersMap = std::unordered_map<std::string, TrackedPeer*>;

  std::string ToStringUnlocked() const;

  std::string LogPrefixUnlocked() const;

  void DumpToStringsUnlocked(std::vector<std::string>* lines) const;

  // Updates the metrics based on index math.
  void UpdateMetricsUnlocked();

  // Update the metric that measures how many ops behind the leader the local
  // replica believes it is (0 if leader).
  void UpdateLagMetricsUnlocked();

  void ClearUnlocked();

  // Returns the last operation in the message queue, or
  // 'preceding_first_op_in_queue_' if the queue is empty.
  const OpId& GetLastOp() const;

  // Tracks a peer.
  // If a peer is the local peer, set is_local_peer to true so that it has the
  // correct defaults. ie. consecutive_failures for local peer is always 0.
  void TrackPeerUnlocked(const RaftPeerPB& peer_pb, bool is_local_peer = false);

  void UntrackPeerUnlocked(const std::string& uuid);

  // We need the local peer in the config because it contains the current
  // 'member_type' of the local node while 'local_peer_pb_' does not.
  void TrackLocalPeerUnlocked();

  // Checks that if the queue is in LEADER mode then all registered peers are
  // in the active config. Crashes with a FATAL log message if this invariant
  // does not hold. If the queue is in NON_LEADER mode, does nothing.
  void CheckPeersInActiveConfigIfLeaderUnlocked() const;

  // Generates a fake response to count the local peer's (leader's) vote after
  // appending to the log
  void DoLocalPeerAppendFinished(const OpId& id, bool need_lock);

  // Callback when a REPLICATE message has finished appending to the local log.
  void LocalPeerAppendFinished(
      const OpId& id,
      const StatusCallback& callback,
      const Status& status);

  // Advances the 'region_durable_index' maintained by the queue
  void AdvanceQueueRegionDurableIndex();

  // Advances 'watermark' to the smallest op that 'num_peers_required' have.
  // If 'replica_types' is set to VOTER_REPLICAS, the 'num_peers_required' is
  // interpreted as "number of voters required". If 'replica_types' is set to
  // ALL_REPLICAS, 'num_peers_required' counts any peer, regardless of its
  // voting status.
  void AdvanceQueueWatermark(
      const char* type,
      int64_t* watermark,
      const OpId& replicated_before,
      const OpId& replicated_after,
      int num_peers_required,
      ReplicaTypes replica_types,
      const TrackedPeer* who_caused);

  // Function to compute the new `watermark` in one of the static modes
  // given a pointer to it, the voter distribution and the watermarks
  // classified by region.
  // This function returns the old watermark.
  int64_t DoComputeNewWatermarkStaticMode(
      const std::map<std::string, int>& voter_distribution,
      const std::map<std::string, std::vector<int64_t>>& watermarks_by_region,
      int64_t* watermark);
  int64_t ComputeNewWatermarkStaticMode(int64_t* watermark);

  // Function to compute the new `watermark` in the single region dynamic
  // mode given a pointer to it, the voter distribution and the watermarks
  // classified by region.
  // This function returns the old watermark.
  int64_t ComputeNewWatermarkDynamicMode(int64_t* watermark);

  // Function to compute the commit index in FlexiRaft. Same as
  // `AdvanceQueueWatermark` except that its only used for commit index
  // advancement.
  // Please note: `queue_lock_` is held as well as `lock_` from the associated
  // RaftConsensus instance while this function gets called.
  void AdvanceMajorityReplicatedWatermarkFlexiRaft(
      int64_t* watermark,
      const OpId& replicated_before,
      const OpId& replicated_after,
      const TrackedPeer* who_caused);

  // return the region of peer or quorum_id of peer based on use_quorum_id
  // in commit rule
  const std::string& getQuorumIdUsingCommitRule(const RaftPeerPB& peer) const;

  // Canonical method to determine whether a commit quorum is satisfied.
  QuorumResults IsQuorumSatisfiedUnlocked(
      const RaftPeerPB& peer,
      const std::function<bool(const TrackedPeer*)>& predicate);

  QuorumResults IsSecondRegionDurabilitySatisfiedUnlocked(
      const std::function<bool(const TrackedPeer*)>& predicate);

  // Checks and renews Leader lease if the UpdateConsensus response has
  // lease_granted by followers
  bool CanLeaderLeaseRenewUnlocked(QuorumResults& qresults);

  // Checks and renews Bounded Data Loss window lease
  bool CanBoundedDataLossWindowRenewUnlocked(QuorumResults& qresults);

  MonoTime GetQuorumMajorityOfPeerRpcStarts(QuorumResults& qresults);

  MonoTime GetMaximumOfPeerRpcStarts(QuorumResults& qresults);

  Status GetQuorumHealthForFlexiRaftUnlocked(QuorumHealth* health);

  Status GetQuorumHealthForVanillaRaftUnlocked(QuorumHealth* health);

  Status ReadMessagesForRequest(
      const TrackedPeer& peer_copy,
      bool route_via_proxy,
      std::vector<ReplicateRefPtr>* messages,
      OpId* preceding_id);

  Status ExtractBuffer(
      const TrackedPeer& peer_copy,
      bool route_via_proxy,
      std::vector<ReplicateRefPtr>* messages,
      OpId* preceding_id);

  /**
   * Takes the debouncing lock on the buffer and then fills it. See FillBuffer
   * below.
   *
   * @param read_context The context that provides append watermarks for the
   * peer we're buffering for
   * @param peer_message_buffer The buffer we're filling
   * @param latest_appended_replicate A new op to append to the buffer
   * @return Status::OK() if we filled the buffer or we were deduped by the
   * debouncer, or an error status if we encounter and error filling
   */
  Status FillBuffer(
      const ReadContext& read_context,
      std::shared_ptr<PeerMessageBuffer>& peer_message_buffer,
      ReplicateRefPtr latest_appended_replicate = nullptr);

  /**
   * Fills up a PeerMessageBuffer based on the what's already in the buffers and
   * the watermarks in read_context from the log cache.
   *
   * If latest_appended_replicate is not a nullptr, this method will first try
   * to append this new op to the buffer and skip reading from the log cache if
   * successful.
   *
   * If not, this method will buffer up from the last buffered index recorded
   * in the buffer. If no last buffered index is recorded, this function will
   * not fill anything. See HandOffBufferIfNeeded.
   *
   * This function also checks if the proxying settings provided by read_context
   * matches the proxy settings of the buffered ops.
   *
   * If this function fails to fill the buffer, it'll reset the buffer and
   * clear all buffered ops. Buffer will need to be reinited in
   * HandOffBufferIfNeeded.
   *
   * @param read_context The context that provides append watermarks for the
   * peer we're buffering for
   * @param peer_message_buffer A locked pointer to the buffer we're filling
   * @param latest_appended_replicate A new op to append to the buffer
   * @return Status::OK() if we successfully filled the buffer, or an error
   * status if we encounter an error filling
   */
  Status FillBuffer(
      const ReadContext& read_context,
      PeerMessageBuffer::LockedBufferHandle peer_message_buffer,
      ReplicateRefPtr latest_appended_replicate = nullptr);

  /**
   * Checks to see if we need to handoff the buffer to a waiting promise.
   *
   * This function will also check that the buffer is consistent with the
   * requirements for the handoff (index matches, proxy requirements are
   * correct, etc.)
   *
   * If the buffer is inconsistent, this function resolves it by discarding the
   * buffer and re-reading once from the log cache.
   *
   * This function is also what inits the buffer (moves last_buffered out of
   * -1). That will allow FillBuffer to continue filling from last_buffered
   * onwards.
   *
   * @param peer_message_buffer A locked pointer to the buffer
   * @param read_context The context for filling the buffer
   */
  void HandOffBufferIfNeeded(
      PeerMessageBuffer::LockedBufferHandle& peer_message_buffer,
      const ReadContext& read_context);

  std::vector<PeerMessageQueueObserver*> observers_;

  // The pool token which executes observer notifications.
  std::unique_ptr<ThreadPoolToken> raft_pool_observers_token_;

  // PB containing identifying information about the local peer.
  RaftPeerPB local_peer_pb_;

  std::shared_ptr<RoutingTableContainer> routing_table_container_;

  // The id of the tablet.
  const std::string tablet_id_;

  QueueState queue_state_;

  // Should we adjust voter distribution based on current config?
  bool adjust_voter_distribution_;

  // The currently tracked peers.
  PeersMap peers_map_;
  mutable simple_mutexlock queue_lock_; // TODO(todd): rename

  bool successor_watch_in_progress_;
  std::optional<std::string> designated_successor_uuid_;
  std::optional<TransferContext> transfer_context_;
  bool successor_watch_peer_notified_ = false;

  std::function<bool(const kudu::consensus::RaftPeerPB&)> tl_filter_fn_;
  // We assume that we never have multiple threads racing to append to the
  // queue. This fake mutex adds some extra assurance that this implementation
  // property doesn't change.
  DFAKE_MUTEX(append_fake_lock_);

  std::shared_ptr<LogCache> log_cache_;

  Metrics metrics_;

  scoped_refptr<ITimeManager> time_manager_;

  // Duration in milliseconds before a peer is marked as 'failed' to being a
  // proxy-peer.
  // If the leader has not communicated with a peer within this threshold, then
  // such a peer is deemed to have failed proxy health check and cannot act as a
  // proxy peer
  int32_t proxy_failure_threshold_ms_ = INT_MAX;

  // Maximum lag (in terms of #ops) as compared to the destination peer after
  // which proxy peer is marked unhealthy
  int64_t proxy_failure_threshold_lag_ = 1000;

  // An instance of PersistentVars with access to some persistent global vars
  scoped_refptr<PersistentVars> persistent_vars_;

  // Leader Leases to support strong reads on primary
  std::atomic<MonoTime> leader_lease_until_;

  // Bounded Data loss to support halting/start-throttling commits
  // using a time bound window
  std::atomic<MonoTime> bounded_dataloss_window_until_;

  std::shared_ptr<TimeProvider> time_provider_;
};

// The interface between RaftConsensus and the PeerMessageQueue.
class PeerMessageQueueObserver {
 public:
  // Notify the observer that the commit index has advanced to
  // 'committed_index'.
  virtual void NotifyCommitIndex(int64_t committed_index, bool need_lock) = 0;

  // Notify the observer that a follower replied with a term
  // higher than that established in the queue.
  virtual void NotifyTermChange(int64_t term) = 0;

  // Notify the observer that a peer is unable to catch up due to falling behind
  // the leader's log GC threshold.
  virtual void NotifyFailedFollower(
      const std::string& peer_uuid,
      int64_t term,
      const std::string& reason) = 0;

  // Notify the observer that the specified peer is ready to be promoted from
  // NON_VOTER to VOTER.
  virtual void NotifyPeerToPromote(const std::string& peer_uuid) = 0;

  // Notify the observer that the specified peer is ready to become leader, and
  // and it should be told to run an election.
  virtual void NotifyPeerToStartElection(
      const std::string& peer_uuid,
      std::optional<PeerMessageQueue::TransferContext> transfer_context,
      std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise,
      std::optional<OpId> mock_election_snapshot_op_id) = 0;

  // Notify the observer that the health of one of the peers has changed.
  virtual void NotifyPeerHealthChange() = 0;

  virtual ~PeerMessageQueueObserver() = default;
};

} // namespace consensus
} // namespace kudu
