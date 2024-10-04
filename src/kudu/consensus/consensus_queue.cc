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

#include "kudu/consensus/consensus_queue.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/url-coding.h"

DEFINE_bool(
    buffer_messages_between_rpcs,
    false,
    "Read and buffer messages to send in the time while waiting for a RPC to "
    "return. Turning this on changes the behavior of "
    "consensus_max_batch_size_bytes to refer to the max size of each read "
    "instead of each RPC message (previously 1 read == 1 RPC message so "
    "they're equivalent)");
TAG_FLAG(buffer_messages_between_rpcs, advanced);

DEFINE_int32(
    follower_unavailable_considered_failed_sec,
    300,
    "Seconds that a leader is unable to successfully heartbeat to a "
    "follower after which the follower is considered to be failed and "
    "evicted from the config.");
TAG_FLAG(follower_unavailable_considered_failed_sec, advanced);
TAG_FLAG(follower_unavailable_considered_failed_sec, runtime);

DEFINE_int32(
    consensus_inject_latency_ms_in_notifications,
    0,
    "Injects a random sleep between 0 and this many milliseconds into "
    "asynchronous notifications from the consensus queue back to the "
    "consensus implementation.");
TAG_FLAG(consensus_inject_latency_ms_in_notifications, hidden);
TAG_FLAG(consensus_inject_latency_ms_in_notifications, unsafe);

DEFINE_int32(
    consensus_rpc_timeout_ms,
    30000,
    "Timeout used for all consensus internal RPC communications.");
TAG_FLAG(consensus_rpc_timeout_ms, advanced);

DECLARE_bool(safe_time_advancement_without_writes);

// Enable improved re-replication (KUDU-1097).
DEFINE_bool(
    raft_prepare_replacement_before_eviction,
    true,
    "When enabled, failed replicas will only be evicted after a "
    "replacement has been prepared for them.");
TAG_FLAG(raft_prepare_replacement_before_eviction, advanced);
TAG_FLAG(raft_prepare_replacement_before_eviction, experimental);

DEFINE_bool(
    raft_attempt_to_replace_replica_without_majority,
    false,
    "When enabled, the replica replacement logic attempts to perform "
    "desired Raft configuration changes even if the majority "
    "of voter replicas is reported failed or offline. "
    "Warning! This is only intended for testing.");
TAG_FLAG(raft_attempt_to_replace_replica_without_majority, unsafe);

DEFINE_bool(
    enable_raft_leader_lease,
    false,
    "Whether to enable leader leases support in raft. If enabled, before Lease times out "
    "Leader attempts to renew. And Followers either accept or reject.");
TAG_FLAG(enable_raft_leader_lease, experimental);

DEFINE_bool(
    synchronous_transfer_leadership,
    false,
    "When a transfer leadership call is made, it checks if peer is already "
    "caught up and initiates transfer leadership, short circuiting async "
    "wait for next response");

// FB - warning - this is disabled in upstream Mysql raft, because automatic
// health management of peers is risky. It also reduces contention on consensus
// queue lock, as it does not have to be reacquired.
DEFINE_HANDLER(
    bool,
    update_peer_health_status,
    true,
    "After every request for peer, maintain the health status of the peer "
    " This can be used to evict an irrecovarable peer");

DEFINE_HANDLER(
    bool,
    async_local_vote_count,
    true,
    "Should the local voter counting be done async? (not sync in the append path)");

DEFINE_bool(
    async_notify_commit_index,
    true,
    "Should the commit index notification be done async?");

TAG_FLAG(synchronous_transfer_leadership, advanced);

DEFINE_bool(
    enable_flexi_raft,
    false,
    "Enables flexi raft mode. All the configurations need to be already"
    " present and setup before the flag can be enabled.");

DECLARE_int32(default_quorum_size);

DEFINE_int32(
    raft_leader_lease_interval_ms,
    2000,
    "The lease interval for Leader leases. The Leader creates a Lease and waits for "
    "Followers to accept the lease before becoming active-lease. The Followers expect "
    "the Lease to be renewed for all updates until the Leader is active.");
TAG_FLAG(raft_leader_lease_interval_ms, experimental);

DEFINE_int32(
    unhealthy_threshold,
    10,
    "Number of consecutive failed requests before we consider a peer "
    "unhealthy");

DEFINE_int32(proxy_disable_secs, 600, "Number of seconds to disable proxying.");

DEFINE_bool(
    filter_out_bad_quorums_in_lmp,
    true,
    "Whether to filter out candidates that don't have a quorum of voters being "
    "tracked during untargeted LMPs.");

DEFINE_bool(
    enable_bounded_dataloss_window,
    false,
    "Whether to enable Bounded DataLoss window support in raft. If enabled, Leader keeps "
    "renewing the window using Vote quorum on every commit requtest."
    "And Followers will ACK on each of the commits sent by Leader.");
TAG_FLAG(enable_bounded_dataloss_window, experimental);

DEFINE_int32(
    bounded_dataloss_window_interval_ms,
    2 * 60 * 60 * 1000,
    "The Bounded DataLoss Window interval after which commits on Leader are "
    "stopped/write-throttled. The Leader creates a sliding window and waits for "
    "Vote quorum of nodes to ACK the window. The Followers expect "
    "the Window to be renewed for all updates until the Leader is active.");
TAG_FLAG(bounded_dataloss_window_interval_ms, experimental);

DEFINE_int32(
    min_corruption_count,
    5,
    "The minimum times a peer reports corruption since the start of exchanges "
    "failing before we start to suspect a real corruption. When the count "
    "reaches this minimum, we will check if there is another peer that is also "
    "reporting the same symptom.");

DEFINE_int32(
    min_single_corruption_count,
    60,
    "The minimum times a peer reports corruption since the start of exchanges "
    "failing before we consider a corruption on the leader. This is the "
    "threshold whereby a single peer can trigger the corruption mitigation "
    "(dropping log cache)");

DECLARE_bool(warm_storage_catchup);

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu::consensus {

METRIC_DEFINE_gauge_int64(
    server,
    majority_done_ops,
    "Leader Operations Acked by Majority",
    MetricUnit::kOperations,
    "Number of operations in the leader queue ack'd by a majority but "
    "not all peers. This metric is always zero for followers.");
METRIC_DEFINE_gauge_int64(
    server,
    in_progress_ops,
    "Operations in Progress",
    MetricUnit::kOperations,
    "Number of operations in the peer's queue ack'd by a minority of "
    "peers.");
METRIC_DEFINE_gauge_int64(
    server,
    ops_behind_leader,
    "Operations Behind Leader",
    MetricUnit::kOperations,
    "Number of operations this server believes it is behind the leader.");
METRIC_DEFINE_counter(
    server,
    check_quorum_runs,
    "Check Quorum Runs",
    kudu::MetricUnit::kRequests,
    "Number of times Check Quorum was run.");
METRIC_DEFINE_counter(
    server,
    check_quorum_failures,
    "Check Quorum Failures",
    kudu::MetricUnit::kRequests,
    "Number of times Check Quorum failed.");
METRIC_DEFINE_counter(
    server,
    corruption_cache_drops,
    "Corruption cache drops",
    MetricUnit::kOperations,
    "Number of times we evicted log cache ops due to suspected corruption");
METRIC_DEFINE_counter(
    server,
    single_corruption_cache_drops,
    "Single corruption cache drops",
    MetricUnit::kOperations,
    "Number of times we determined corruption from repeated append failures of "
    "a single peer.");
METRIC_DEFINE_gauge_int64(
    server,
    available_commit_peers,
    "Available Commit Peers",
    MetricUnit::kUnits,
    "Number of peers, including leader, that are healthy in commit quorum. If "
    "local peer is not a leader, -1 is returned. If quorum mode is not "
    "SINGLE_REGION_DYNAMIC, -1 is returned.");
METRIC_DEFINE_gauge_int64(
    server,
    available_leader_lease_grantors,
    "Available Leader lease grantors",
    MetricUnit::kUnits,
    "Number of remote peers who are Leader lease grantors.");
METRIC_DEFINE_gauge_int64(
    server,
    available_bounded_dataloss_window_ackers,
    "Available Bounded DataLoss Window ACKers",
    MetricUnit::kUnits,
    "Number of remote peers who are Bounded DataLoss window ACKers.");

const char* PeerStatusToString(PeerStatus p) {
  switch (p) {
    case PeerStatus::OK:
      return "OK";
    case PeerStatus::REMOTE_ERROR:
      return "REMOTE_ERROR";
    case PeerStatus::RPC_LAYER_ERROR:
      return "RPC_LAYER_ERROR";
    case PeerStatus::TABLET_FAILED:
      return "TABLET_FAILED";
    case PeerStatus::TABLET_NOT_FOUND:
      return "TABLET_NOT_FOUND";
    case PeerStatus::INVALID_TERM:
      return "INVALID_TERM";
    case PeerStatus::LMP_MISMATCH:
      return "LMP_MISMATCH";
    case PeerStatus::CANNOT_PREPARE:
      return "CANNOT_PREPARE";
    case PeerStatus::NEW:
      return "NEW";
  }
  DCHECK(false);
  return "<unknown>";
}

PeerMessageQueue::TrackedPeer::TrackedPeer(
    RaftPeerPB peer_pb,
    const PeerMessageQueue* queue)
    : peer_pb(std::move(peer_pb)),
      next_index(kInvalidOpIdIndex),
      last_received(MinimumOpId()),
      last_known_committed_index(MinimumOpId().index()),
      last_exchange_status(PeerStatus::NEW),
      lease_granted(MinimumOpId()),
      bounded_dataloss_window_acked(MinimumOpId()),
      rpc_start_(MonoTime::Min()),
      wal_catchup_possible(true),
      last_overall_health_status(HealthReportPB::UNKNOWN),
      status_log_throttler(std::make_shared<logging::LogThrottler>()),
      peer_msg_buffer(std::make_shared<PeerMessageBuffer>()),
      last_seen_term_(0),
      // We initialize to max to ensure that a peer, that was never
      // successfully contacted, is considered unhealthy.
      consecutive_failures_(INT_MAX),
      proxying_disabled_until_(MonoTime::Min()),
      time_provider_(TimeProvider::getInstance()),
      queue(queue) {
  last_successful_exchange = time_provider_->Now();
  last_communication_time = time_provider_->Now();
  PopulateIsPeerInLocalQuorum();
  PopulateIsPeerInLocalRegion();
}

void PeerMessageQueue::TrackedPeer::PopulateIsPeerInLocalQuorum() {
  is_peer_in_local_quorum.reset();

  const RaftPeerPB& local_peer_pb = queue->local_peer_pb_;

  if (peer_pb.permanent_uuid() == local_peer_pb.permanent_uuid()) {
    is_peer_in_local_quorum = true;
    return;
  }

  if (!peer_pb.has_attrs() || !local_peer_pb.has_attrs()) {
    return;
  }

  const std::string& local_peer_quorum_id =
      queue->getQuorumIdUsingCommitRule(local_peer_pb);
  const std::string& peer_quorum_id =
      queue->getQuorumIdUsingCommitRule(peer_pb);
  if (!local_peer_quorum_id.empty() && !peer_quorum_id.empty()) {
    is_peer_in_local_quorum = (local_peer_quorum_id == peer_quorum_id);
  }
}

void PeerMessageQueue::TrackedPeer::PopulateIsPeerInLocalRegion() {
  is_peer_in_local_region.reset();

  const RaftPeerPB& local_peer_pb = queue->local_peer_pb_;

  if (peer_pb.permanent_uuid() == local_peer_pb.permanent_uuid()) {
    is_peer_in_local_region = true;
    return;
  }

  if (!peer_pb.attrs().has_region() || !local_peer_pb.attrs().has_region()) {
    return;
  }

  const std::string& peer_region = peer_pb.attrs().region();
  const std::string& local_peer_region = local_peer_pb.attrs().region();
  if (!local_peer_region.empty() && !peer_region.empty()) {
    is_peer_in_local_region = (local_peer_region == peer_region);
  }
}

bool PeerMessageQueue::TrackedPeer::is_healthy() const {
  return consecutive_failures_ < FLAGS_unhealthy_threshold;
}

int32_t PeerMessageQueue::TrackedPeer::consecutive_failures() const {
  return consecutive_failures_;
}

void PeerMessageQueue::TrackedPeer::incr_consecutive_failures() {
  // avoid overflow
  if (consecutive_failures_ != INT_MAX) {
    consecutive_failures_++;
  }
}

void PeerMessageQueue::TrackedPeer::reset_consecutive_failures() {
  consecutive_failures_ = 0;
}

void PeerMessageQueue::TrackedPeer::set_consecutive_failures(int32_t value) {
  consecutive_failures_ = value;
}

bool PeerMessageQueue::TrackedPeer::ProxyTargetEnabled() const {
  return time_provider_->Now() >= proxying_disabled_until_;
}

void PeerMessageQueue::TrackedPeer::SnoozeProxying(MonoDelta delta) {
  proxying_disabled_until_ = time_provider_->Now() + delta;
}

std::string PeerMessageQueue::TrackedPeer::ToString() const {
  return Substitute(
      "Peer: $0, Status: $1, Last received: $2, Next index: $3, "
      "Last known committed idx: $4, Time since last communication: $5",
      SecureShortDebugString(peer_pb),
      PeerStatusToString(last_exchange_status),
      OpIdToString(last_received),
      next_index,
      last_known_committed_index,
      (MonoTime::Now() - last_communication_time).ToString());
}

#define INSTANTIATE_METRIC(x) x.Instantiate(metric_entity, 0)
PeerMessageQueue::Metrics::Metrics(
    const scoped_refptr<MetricEntity>& metric_entity)
    : num_majority_done_ops(INSTANTIATE_METRIC(METRIC_majority_done_ops)),
      num_in_progress_ops(INSTANTIATE_METRIC(METRIC_in_progress_ops)),
      num_ops_behind_leader(INSTANTIATE_METRIC(METRIC_ops_behind_leader)),
      available_leader_lease_grantors(
          INSTANTIATE_METRIC(METRIC_available_leader_lease_grantors)),
      available_bounded_dataloss_window_ackers(
          INSTANTIATE_METRIC(METRIC_available_bounded_dataloss_window_ackers)),
      available_commit_peers(
          INSTANTIATE_METRIC(METRIC_available_commit_peers)) {
  check_quorum_runs =
      metric_entity->FindOrCreateCounter(&METRIC_check_quorum_runs);
  check_quorum_failures =
      metric_entity->FindOrCreateCounter(&METRIC_check_quorum_failures);
  corruption_cache_drops =
      metric_entity->FindOrCreateCounter(&METRIC_corruption_cache_drops);
  single_corruption_cache_drops =
      metric_entity->FindOrCreateCounter(&METRIC_single_corruption_cache_drops);
}
#undef INSTANTIATE_METRIC

const std::string PeerMessageQueue::kVanillaRaftQuorumId = "__default__";

PeerMessageQueue::PeerMessageQueue(
    const scoped_refptr<MetricEntity>& metric_entity,
    scoped_refptr<log::Log> log,
    scoped_refptr<ITimeManager> time_manager,
    const scoped_refptr<PersistentVarsManager>& persistent_vars_manager,
    RaftPeerPB local_peer_pb,
    std::shared_ptr<RoutingTableContainer> routing_table_container,
    string tablet_id,
    unique_ptr<ThreadPoolToken> raft_pool_observers_token,
    OpId last_locally_replicated,
    const OpId& last_locally_committed)
    : raft_pool_observers_token_(std::move(raft_pool_observers_token)),
      local_peer_pb_(std::move(local_peer_pb)),
      routing_table_container_(std::move(routing_table_container)),
      tablet_id_(std::move(tablet_id)),
      adjust_voter_distribution_(true),
      successor_watch_in_progress_(false),
      log_cache_(std::make_shared<LogCache>(
          metric_entity,
          std::move(log),
          local_peer_pb_.permanent_uuid(),
          tablet_id_)),
      metrics_(metric_entity),
      time_manager_(std::move(time_manager)),
      leader_lease_until_(MonoTime::Min()),
      bounded_dataloss_window_until_(MonoTime::Min()),
      time_provider_(TimeProvider::getInstance()) {
  DCHECK(local_peer_pb_.has_permanent_uuid());
  DCHECK(local_peer_pb_.has_last_known_addr());
  DCHECK(last_locally_replicated.IsInitialized());
  DCHECK(last_locally_committed.IsInitialized());
  queue_state_.current_term = 0;
  queue_state_.first_index_in_current_term = {};
  queue_state_.committed_index = 0;
  queue_state_.all_replicated_index = 0;
  queue_state_.majority_replicated_index = 0;
  queue_state_.region_durable_index = 0;
  queue_state_.last_idx_appended_to_leader = 0;
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = -1;
  queue_state_.last_appended = std::move(last_locally_replicated);
  queue_state_.committed_index = last_locally_committed.index();
  queue_state_.state = kQueueOpen;
  // TODO(mpercy): Merge LogCache::Init() with its constructor.
  log_cache_->Init(queue_state_.last_appended);

  CHECK_OK(persistent_vars_manager->LoadPersistentVars(
      tablet_id_, &persistent_vars_));
}

void PeerMessageQueue::SetProxyFailureThreshold(
    int32_t proxy_failure_threshold_ms) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  proxy_failure_threshold_ms_ = proxy_failure_threshold_ms;
}

void PeerMessageQueue::SetProxyFailureThresholdLag(
    int32_t proxy_failure_threshold_lag) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  proxy_failure_threshold_lag_ = proxy_failure_threshold_lag;
}

bool PeerMessageQueue::HasProxyPeerFailedUnlocked(
    const TrackedPeer* proxy_peer,
    const TrackedPeer* dest_peer) {
  auto max_proxy_failure_threshold =
      MonoDelta::FromMilliseconds(proxy_failure_threshold_ms_);

  if (time_provider_->Now() - proxy_peer->last_successful_exchange >
      max_proxy_failure_threshold) {
    KLOG_EVERY_N_SECS(INFO, 180)
        << "Peer " << proxy_peer->uuid()
        << " did not complete a successful exchange after "
        << proxy_failure_threshold_ms_ << "ms. Will not use as a proxy.";

    // The leader has not communicated with proxy_peer within the
    // proxy_failure_threshold_ms. Hence this peer cannot act as a 'proxy peer'
    // and is considered failed
    return true;
  }

  bool is_proxy_lagging = (dest_peer->next_index > proxy_peer->next_index) &&
      ((dest_peer->next_index - proxy_peer->next_index) >
       proxy_failure_threshold_lag_);

  if (is_proxy_lagging) {
    // The proxy peer is lagging farther than the destination peer. Hence it
    // cannot act as a proxy for the destination peer
    return true;
  }

  return false;
}

Status PeerMessageQueue::SetCompressionDictionary(const std::string& dict) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  RETURN_NOT_OK(log_cache()->Clear());
  RETURN_NOT_OK(CompressionCodecManager::SetDictionary(dict));
  for (const PeersMap::value_type& entry : peers_map_) {
    entry.second->should_send_compression_dict = true;
  }
  return Status::OK();
}

void PeerMessageQueue::SetLeaderMode(
    int64_t committed_index,
    int64_t current_term,
    const RaftConfigPB& active_config) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  if (current_term != queue_state_.current_term) {
    CHECK_GT(current_term, queue_state_.current_term)
        << "Terms should only increase";
    queue_state_.first_index_in_current_term = {};
    queue_state_.current_term = current_term;
  }

  queue_state_.committed_index = committed_index;
  queue_state_.majority_replicated_index = committed_index;
  queue_state_.active_config.reset(new RaftConfigPB(active_config));
  queue_state_.majority_size_ =
      MajoritySize(CountVoters(*queue_state_.active_config));
  queue_state_.mode = LEADER;

  TrackLocalPeerUnlocked();
  CheckPeersInActiveConfigIfLeaderUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Queue going to LEADER mode. State: " << queue_state_.ToString();

  time_manager_->SetLeaderMode();
}

void PeerMessageQueue::SetNonLeaderMode(const RaftConfigPB& active_config) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  queue_state_.active_config.reset(new RaftConfigPB(active_config));
  queue_state_.mode = NON_LEADER;
  queue_state_.majority_size_ = -1;

  // Update this when stepping down, since it doesn't get tracked as LEADER.
  queue_state_.last_idx_appended_to_leader = queue_state_.last_appended.index();

  TrackLocalPeerUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Queue going to NON_LEADER mode. State: " << queue_state_.ToString();

  time_manager_->SetNonLeaderMode();
}

void PeerMessageQueue::TrackPeer(const RaftPeerPB& peer_pb) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  TrackPeerUnlocked(peer_pb);
}

void PeerMessageQueue::TrackPeerUnlocked(
    const RaftPeerPB& peer_pb,
    bool is_local_peer) {
  CHECK(!peer_pb.permanent_uuid().empty()) << SecureShortDebugString(peer_pb);
  CHECK(peer_pb.has_member_type()) << SecureShortDebugString(peer_pb);
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(queue_state_.state, kQueueOpen);

  TrackedPeer* tracked_peer = new TrackedPeer(peer_pb, this);
  // We don't know the last operation received by the peer so, following the
  // Raft protocol, we set next_index to one past the end of our own log. This
  // way, if calling this method is the result of a successful leader election
  // and the logs between the new leader and remote peer match, the
  // peer->next_index will point to the index of the soon-to-be-written NO_OP
  // entry that is used to assert leadership. If we guessed wrong, and the peer
  // does not have a log that matches ours, the normal queue negotiation
  // process will eventually find the right point to resume from.
  tracked_peer->next_index = queue_state_.last_appended.index() + 1;

  if (is_local_peer) {
    tracked_peer->reset_consecutive_failures();
  }

  InsertOrDie(&peers_map_, tracked_peer->uuid(), tracked_peer);

  CheckPeersInActiveConfigIfLeaderUnlocked();

  // We don't know how far back this peer is, so set the all replicated
  // watermark to 0. We'll advance it when we know how far along the peer is.
  queue_state_.all_replicated_index = 0;
}

void PeerMessageQueue::UntrackPeer(const string& uuid) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  UntrackPeerUnlocked(uuid);
}

void PeerMessageQueue::UntrackPeerUnlocked(const string& uuid) {
  DCHECK(queue_lock_.is_locked());
  TrackedPeer* peer = EraseKeyReturnValuePtr(&peers_map_, uuid);
  delete peer; // Deleting a nullptr is safe.
}

void PeerMessageQueue::TrackLocalPeerUnlocked() {
  DCHECK(queue_lock_.is_locked());
  RaftPeerPB* local_peer_in_config;
  Status s = GetRaftConfigMember(
      queue_state_.active_config.get(),
      local_peer_pb_.permanent_uuid(),
      &local_peer_in_config);
  auto local_copy = local_peer_pb_;
  if (!s.ok()) {
    // The local peer is not a member of the config. The queue requires the
    // 'member_type' field to be set for any tracked peer, so we explicitly
    // mark the local peer as a NON_VOTER. This case is only possible when the
    // local peer is not the leader, so the choice is not particularly
    // important, but NON_VOTER is the most reasonable option.
    local_copy.set_member_type(RaftPeerPB::NON_VOTER);
    local_peer_in_config = &local_copy;
  }
  // TODO (T172552337) Unify local_peer_pb_ in raft_consensus, consensus_peer,
  // and consensus_queue. Right now there are multiple copies of local_peer_pb,
  // which can easily diverge and cause problems.
  local_peer_pb_ = *local_peer_in_config;
  CHECK(
      local_peer_in_config->member_type() == RaftPeerPB::VOTER ||
      queue_state_.mode != LEADER)
      << "local peer " << local_peer_pb_.permanent_uuid()
      << " is not a voter in config: " << queue_state_.ToString();
  if (ContainsKey(peers_map_, local_peer_pb_.permanent_uuid())) {
    UntrackPeerUnlocked(local_peer_pb_.permanent_uuid());
  }
  TrackPeerUnlocked(*local_peer_in_config, /*is_local_peer=*/true);
}

unordered_map<string, HealthReportPB> PeerMessageQueue::ReportHealthOfPeers()
    const {
  unordered_map<string, HealthReportPB> reports;
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  for (const auto& entry : peers_map_) {
    const string& peer_uuid = entry.first;
    const TrackedPeer* peer = entry.second;
    HealthReportPB report;
    auto overall_health = peer->last_overall_health_status;
    // We always consider the local peer (ourselves) to be healthy.
    // TODO(mpercy): Is this always a safe assumption?
    if (peer_uuid == local_peer_pb_.permanent_uuid()) {
      overall_health = HealthReportPB::HEALTHY;
    }
    report.set_overall_health(overall_health);
    reports.emplace(peer_uuid, std::move(report));
  }
  return reports;
}

void PeerMessageQueue::CheckPeersInActiveConfigIfLeaderUnlocked() const {
  DCHECK(queue_lock_.is_locked());
  if (queue_state_.mode != LEADER) {
    return;
  }
  std::unordered_set<string> config_peer_uuids;
  for (const RaftPeerPB& peer_pb : queue_state_.active_config->peers()) {
    InsertOrDie(&config_peer_uuids, peer_pb.permanent_uuid());
  }
  for (const PeersMap::value_type& entry : peers_map_) {
    if (!ContainsKey(config_peer_uuids, entry.first)) {
      LOG_WITH_PREFIX_UNLOCKED(FATAL) << Substitute(
          "Peer $0 is not in the active config. "
          "Queue state: $1",
          entry.first,
          queue_state_.ToString());
    }
  }
}

void PeerMessageQueue::DoLocalPeerAppendFinished(
    const OpId& id,
    bool need_lock) {
  // Fake an RPC response from the local peer.
  // TODO: we should probably refactor the ResponseFromPeer function
  // so that we don't need to construct this fake response, but this
  // seems to work for now.
  ConsensusResponsePB fake_response;
  fake_response.set_responder_uuid(local_peer_pb_.permanent_uuid());
  *fake_response.mutable_status()->mutable_last_received() = id;
  *fake_response.mutable_status()->mutable_last_received_current_leader() = id;
  {
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    fake_response.mutable_status()->set_last_committed_idx(
        queue_state_.committed_index);
  }

  std::optional<int64_t> updated_commit_index;
  DoResponseFromPeer(
      local_peer_pb_.permanent_uuid(), fake_response, updated_commit_index);

  if (updated_commit_index) {
    NotifyObserversOfCommitIndexChange(*updated_commit_index, need_lock);
  }
}

void PeerMessageQueue::LocalPeerAppendFinished(
    const OpId& id,
    const StatusCallback& callback,
    const Status& status) {
  CHECK_OK(status);

  // Schedule the function to gather local response and count local vote to run
  // asynchronously (so as not to block the thread writing to local log from
  // blocking on queue_lock_)
  OpId local_id = id;
  if (FLAGS_HANDLER(FLAGS_async_local_vote_count)) {
    CHECK_OK(raft_pool_observers_token_->SubmitClosure(Bind(
        &PeerMessageQueue::DoLocalPeerAppendFinished,
        Unretained(this),
        local_id,
        true)));
  } else {
    // NOTE: no need to lock RaftConsensus::lock_ because we're executing in
    // sync mode
    DoLocalPeerAppendFinished(local_id, /* need_lock */ false);
  }

  callback.Run(status);
}

Status PeerMessageQueue::AppendOperation(const ReplicateRefPtr& msg) {
  return AppendOperations(
      {msg},
      Bind(
          CrashIfNotOkStatusCB,
          "Enqueued replicate operation failed to write to WAL"));
}

Status PeerMessageQueue::AppendOperations(
    const vector<ReplicateRefPtr>& msgs,
    const StatusCallback& log_append_callback) {
  DFAKE_SCOPED_LOCK(append_fake_lock_);
  std::unique_lock<simple_mutexlock> lock(queue_lock_);

  OpId last_id = msgs.back()->get()->id();

  // "Snoop" on the appended operations to watch for term changes (as follower)
  // and to determine the first index in our term (as leader).
  //
  // TODO: it would be a cleaner design to explicitly set the first index in the
  // leader term as part of SetLeaderMode(). However, we are currently also
  // using that method to handle refreshing the peer list during configuration
  // changes, so the refactor isn't trivial.
  for (const auto& msg : msgs) {
    const auto& id = msg->get()->id();
    if (id.term() > queue_state_.current_term) {
      queue_state_.current_term = id.term();
      queue_state_.first_index_in_current_term = id.index();
    } else if (
        id.term() == queue_state_.current_term &&
        !queue_state_.first_index_in_current_term) {
      queue_state_.first_index_in_current_term = id.index();
    }
  }

  // Update safe time in the TimeManager if we're leader.
  // This will 'unpin' safe time advancement, which had stopped since we
  // assigned a timestamp to the message. Until we have leader leases, replicas
  // only call this when the message is committed.
  if (queue_state_.mode == LEADER) {
    time_manager_->AdvanceSafeTimeWithMessage(*msgs.back()->get());
  }

  // Unlock ourselves during Append to prevent a deadlock: it's possible that
  // the log buffer is full, in which case AppendOperations would block.
  // However, for the log buffer to empty, it may need to call
  // LocalPeerAppendFinished() which also needs queue_lock_.
  lock.unlock();
  RETURN_NOT_OK(log_cache_->AppendOperations(
      msgs,
      Bind(
          &PeerMessageQueue::LocalPeerAppendFinished,
          Unretained(this),
          last_id,
          log_append_callback)));
  lock.lock();
  DCHECK(last_id.IsInitialized());
  queue_state_.last_appended = last_id;
  UpdateMetricsUnlocked();

  return Status::OK();
}

Status PeerMessageQueue::AppendOperation(
    const ReplicateMsgWrapper& msg_wrapper) {
  return AppendOperations(
      {msg_wrapper},
      Bind(
          CrashIfNotOkStatusCB,
          "Enqueued replicate operation failed to write to WAL"));
}

Status PeerMessageQueue::AppendOperations(
    const vector<ReplicateMsgWrapper>& msg_wrappers,
    const StatusCallback& log_append_callback) {
  DFAKE_SCOPED_LOCK(append_fake_lock_);
  std::unique_lock<simple_mutexlock> lock(queue_lock_);

  OpId last_id = msg_wrappers.back().GetOrigMsg()->get()->id();

  // "Snoop" on the appended operations to watch for term changes (as follower)
  // and to determine the first index in our term (as leader).
  //
  // TODO: it would be a cleaner design to explicitly set the first index in the
  // leader term as part of SetLeaderMode(). However, we are currently also
  // using that method to handle refreshing the peer list during configuration
  // changes, so the refactor isn't trivial.
  for (const auto& msg_wrapper : msg_wrappers) {
    const auto& id = msg_wrapper.GetOrigMsg()->get()->id();
    if (id.term() > queue_state_.current_term) {
      queue_state_.current_term = id.term();
      queue_state_.first_index_in_current_term = id.index();
    } else if (
        id.term() == queue_state_.current_term &&
        !queue_state_.first_index_in_current_term) {
      queue_state_.first_index_in_current_term = id.index();
    }
  }

  // Update safe time in the TimeManager if we're leader.
  // This will 'unpin' safe time advancement, which had stopped since we
  // assigned a timestamp to the message. Until we have leader leases, replicas
  // only call this when the message is committed.
  if (queue_state_.mode == LEADER) {
    time_manager_->AdvanceSafeTimeWithMessage(
        *msg_wrappers.back().GetOrigMsg()->get());
  }

  // Unlock ourselves during Append to prevent a deadlock: it's possible that
  // the log buffer is full, in which case AppendOperations would block.
  // However, for the log buffer to empty, it may need to call
  // LocalPeerAppendFinished() which also needs queue_lock_.
  lock.unlock();
  RETURN_NOT_OK(log_cache_->AppendOperations(
      msg_wrappers,
      Bind(
          &PeerMessageQueue::LocalPeerAppendFinished,
          Unretained(this),
          last_id,
          log_append_callback)));
  lock.lock();
  DCHECK(last_id.IsInitialized());
  queue_state_.last_appended = last_id;
  UpdateMetricsUnlocked();

  return Status::OK();
}

void PeerMessageQueue::TruncateOpsAfter(int64_t index) {
  DFAKE_SCOPED_LOCK(append_fake_lock_); // should not race with append.
  OpId op;
  CHECK_OK_PREPEND(
      log_cache_->LookupOpId(index, &op),
      Substitute(
          "$0: cannot truncate ops after bad index $1",
          LogPrefixUnlocked(),
          index));
  {
    std::unique_lock<simple_mutexlock> lock(queue_lock_);
    DCHECK(op.IsInitialized());
    queue_state_.last_appended = op;
  }
  log_cache_->TruncateOpsAfter(op.index());
}

OpId PeerMessageQueue::GetLastOpIdInLog() const {
  std::unique_lock<simple_mutexlock> lock(queue_lock_);
  DCHECK(queue_state_.last_appended.IsInitialized());
  return queue_state_.last_appended;
}

OpId PeerMessageQueue::GetNextOpId() const {
  std::unique_lock<simple_mutexlock> lock(queue_lock_);
  DCHECK(queue_state_.last_appended.IsInitialized());
  return MakeOpId(
      queue_state_.current_term, queue_state_.last_appended.index() + 1);
}

MonoTime PeerMessageQueue::GetLeaderLeaseUntil() {
  if (queue_state_.mode != LEADER) {
    return MonoTime().Min();
  }
  return leader_lease_until_;
}

MonoTime PeerMessageQueue::GetBoundedDataLossWindowUntil() {
  if (queue_state_.mode != LEADER) {
    return MonoTime().Min();
  }
  return bounded_dataloss_window_until_;
}

bool PeerMessageQueue::SafeToEvictUnlocked(const string& evict_uuid) const {
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(LEADER, queue_state_.mode);
  auto now = time_provider_->Now();

  int remaining_voters = 0;
  int remaining_viable_voters = 0;

  for (const auto& e : peers_map_) {
    const auto& uuid = e.first;
    const auto& peer = e.second;
    if (uuid == evict_uuid) {
      continue;
    }
    if (!IsRaftConfigVoter(uuid, *queue_state_.active_config)) {
      continue;
    }
    remaining_voters++;

    bool viable = true;
    // Being alive, the local peer itself (the leader) is always a viable
    // voter: the criteria below apply only to non-local peers.
    if (uuid != local_peer_pb_.permanent_uuid()) {
      // Only consider a peer to be a viable voter if...
      // ...its last exchange was successful
      viable &= peer->last_exchange_status == PeerStatus::OK;

      // ...the peer is up to date with the latest majority.
      //
      //    This indicates that it's actively participating in majorities and
      //    likely to replicate a config change immediately when we propose it.
      viable &=
          peer->last_received.index() >= queue_state_.majority_replicated_index;

      // ...we have communicated successfully with it recently.
      //
      //    This handles the case where the tablet has had no recent writes and
      //    therefore even a replica that is down would have participated in the
      //    latest majority.
      auto unreachable_time = now - peer->last_communication_time;
      viable &=
          unreachable_time.ToMilliseconds() < FLAGS_consensus_rpc_timeout_ms;
    }
    if (viable) {
      remaining_viable_voters++;
    }
  }

  // We never drop from 2 to 1 automatically, at least for now. We may want
  // to revisit this later, we're just being cautious with this.
  if (remaining_voters <= 1) {
    VLOG(2) << LogPrefixUnlocked()
            << "Not evicting P $0 (only one voter would remain)";
    return false;
  }
  // Unless the --raft_attempt_to_replace_replica_without_majority flag is set,
  // don't evict anything if the remaining number of viable voters is not enough
  // to form a majority of the remaining voters.
  if (PREDICT_TRUE(!FLAGS_raft_attempt_to_replace_replica_without_majority) &&
      remaining_viable_voters < MajoritySize(remaining_voters)) {
    VLOG(2)
        << LogPrefixUnlocked()
        << Substitute(
               "Not evicting P $0 (only $1/$2 remaining voters appear viable)",
               evict_uuid,
               remaining_viable_voters,
               remaining_voters);
    return false;
  }

  return true;
}

void PeerMessageQueue::UpdatePeerHealthUnlocked(TrackedPeer* peer) {
  DCHECK(queue_lock_.is_locked());
  DCHECK_EQ(LEADER, queue_state_.mode);

  auto overall_health_status = PeerHealthStatus(*peer);

  // Prepare error messages for different conditions.
  string error_msg;
  if (overall_health_status == HealthReportPB::FAILED ||
      overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) {
    if (peer->last_exchange_status == PeerStatus::TABLET_FAILED) {
      error_msg = Substitute(
          "The tablet replica hosted on peer $0 has failed", peer->uuid());
    } else if (!peer->wal_catchup_possible) {
      error_msg = Substitute(
          "The logs necessary to catch up peer $0 have been "
          "garbage collected. The replica will never be able "
          "to catch up",
          peer->uuid());
    } else {
      error_msg = Substitute(
          "Leader has been unable to successfully communicate "
          "with peer $0 for more than $1 seconds ($2)",
          peer->uuid(),
          FLAGS_follower_unavailable_considered_failed_sec,
          (time_provider_->Now() - peer->last_communication_time).ToString());
    }
  }

  bool changed = overall_health_status != peer->last_overall_health_status;
  peer->last_overall_health_status = overall_health_status;

  if (FLAGS_raft_prepare_replacement_before_eviction) {
    // Only take action when there is a change.
    if (changed) {
      // Only log a message when the status changes to some flavor of failure.
      if (overall_health_status == HealthReportPB::FAILED ||
          overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << error_msg;
      }
      NotifyObserversOfPeerHealthChange();
    }
  } else {
    if ((overall_health_status == HealthReportPB::FAILED ||
         overall_health_status == HealthReportPB::FAILED_UNRECOVERABLE) &&
        SafeToEvictUnlocked(peer->uuid())) {
      NotifyObserversOfFailedFollower(
          peer->uuid(), queue_state_.current_term, error_msg);
    }
  }
}

// While reporting on the replica health status, it's important to report on
// the 'definitive' health statuses once they surface. That allows the system
// to expedite decisions on replica replacement because the more 'definitive'
// statuses have less uncertainty and provide more information (compared
// with less 'definitive' statuses). Informally, the level of 'definitiveness'
// could be measured by the number of possible state transitions on the replica
// health status state diagram.
//
// The health status chain below has increasing level of 'definitiveness'
// left to right:
//
//   UNKNOWN --> HEALTHY --> FAILED --> FAILED_UNRECOVERABLE
//
// For example, in the case when a replica has been unreachable longer than the
// time interval specified by the --follower_unavailable_considered_failed_sec
// flag, the system should start reporting its health status as FAILED.
// However, once the replica falls behind the WAL log GC threshold, the system
// should start reporting its healths status as FAILED_UNRECOVERABLE. The code
// below is written to adhere to that informal policy.
HealthReportPB::HealthStatus PeerMessageQueue::PeerHealthStatus(
    const TrackedPeer& peer) {
  // Replicas that have fallen behind the leader's retained WAL segments are
  // failed irrecoverably and will not come back because they cannot ever catch
  // up with the leader replica.
  if (!peer.wal_catchup_possible) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas returning TABLET_FAILED status are considered irrecoverably
  // failed because the TABLED_FAILED status manifests about IO failures
  // caused by disk corruption, etc.
  if (peer.last_exchange_status == PeerStatus::TABLET_FAILED) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas which have been unreachable for too long are considered failed,
  // unless it's known that they have failed irrecoverably (see above). They
  // might come back at some point and successfully catch up with the leader.
  auto max_unreachable =
      MonoDelta::FromSeconds(FLAGS_follower_unavailable_considered_failed_sec);
  if (MonoTime::Now() - peer.last_communication_time > max_unreachable) {
    return HealthReportPB::FAILED;
  }

  // The happy case: replicas returned OK during the recent exchange are
  // considered healthy.
  if (peer.last_exchange_status == PeerStatus::OK) {
    return HealthReportPB::HEALTHY;
  }

  // Other cases are for various situations when there hasn't been a contact
  // with the replica yet or it's impossible to definitely tell the health
  // status of the replica based on the last exchange status (transient error,
  // etc.). For such cases, the replica health status is reported as UNKNOWN.
  return HealthReportPB::UNKNOWN;
}

Status PeerMessageQueue::FindPeer(const std::string& uuid, TrackedPeer* peer) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  TrackedPeer* peer_copy = FindPtrOrNull(peers_map_, uuid);

  if (peer_copy == nullptr) {
    return Status::NotFound(Substitute("peer $0 is no longer tracked", uuid));
  }

  *peer = *peer_copy;
  return Status::OK();
}

Status PeerMessageQueue::RequestForPeer(
    const string& uuid,
    bool read_ops,
    ConsensusRequestPB* request,
    vector<ReplicateRefPtr>* msg_refs,
    bool* needs_tablet_copy,
    std::string* next_hop_uuid) {
  // Maintain a thread-safe copy of necessary members.
  OpId preceding_id;
  int64_t current_term;
  TrackedPeer peer_copy;
  MonoDelta unreachable_time;
  {
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    DCHECK_EQ(queue_state_.state, kQueueOpen);
    DCHECK_NE(uuid, local_peer_pb_.permanent_uuid());

    TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
    if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
      return Status::NotFound(Substitute(
          "peer $0 is no longer tracked or "
          "queue is not in leader mode",
          uuid));
    }
    peer_copy = *peer;

    // Clear the requests without deleting the entries, as they may be in use by
    // other peers.
    request->mutable_ops()->UnsafeArenaExtractSubrange(
        0, request->ops_size(), nullptr);

    // This is initialized to the queue's last appended op but gets set to the
    // id of the log entry preceding the first one in 'messages' if messages are
    // found for the peer.
    preceding_id = queue_state_.last_appended;
    current_term = queue_state_.current_term;

    request->set_committed_index(queue_state_.committed_index);
    request->set_all_replicated_index(queue_state_.all_replicated_index);
    request->set_last_idx_appended_to_leader(
        queue_state_.last_appended.index());
    request->set_caller_term(current_term);
    request->set_region_durable_index(queue_state_.region_durable_index);
    if (auto rpc_token = persistent_vars_->raft_rpc_token()) {
      request->set_raft_rpc_token(*rpc_token);
    }
    request->clear_compression_dictionary();
    if (peer->should_send_compression_dict) {
      KLOG_EVERY_N_SECS(INFO, 180)
          << "Setting compression dictionary in request to: " << peer->uuid()
          << " as " << CompressionCodecManager::GetCurrentDictionaryID();
      request->set_compression_dictionary(
          CompressionCodecManager::GetDictionary());
    }
    unreachable_time =
        time_provider_->Now() - peer_copy.last_communication_time;

    RETURN_NOT_OK(routing_table_container_->NextHop(
        local_peer_pb_.permanent_uuid(), uuid, next_hop_uuid));

    if (*next_hop_uuid != uuid) {
      // If proxy_peer is not healthy, then route directly to the destination
      // TODO: Multi hop proxy support needs better failure and health checks
      // for proxy peer. The current method of detecting unhealthy proxy peer
      // works only on the leader. One solution could be for the leader to
      // periodically exchange the health report of all peers as part of
      // UpdateReplica() call

      if (peer->ProxyTargetEnabled()) {
        bool should_proxy = false;
        if (peer->is_healthy()) {
          TrackedPeer* proxy_peer = FindPtrOrNull(peers_map_, *next_hop_uuid);
          if (proxy_peer != nullptr &&
              !HasProxyPeerFailedUnlocked(proxy_peer, peer)) {
            should_proxy = true;
          }
        }

        if (!should_proxy) {
          *next_hop_uuid = uuid;
          peer->SnoozeProxying(
              MonoDelta::FromSeconds(FLAGS_proxy_disable_secs));
          LOG(WARNING) << "Proxy target " << uuid
                       << " is unhealthy. Snooze proxying to this instance for "
                       << FLAGS_proxy_disable_secs << " seconds";
        }
      } else {
        *next_hop_uuid = uuid;
      }
    }
  }

  // Always trigger a health status update check at the end of this function.
  bool wal_catchup_progress = false;
  bool wal_catchup_failure = false;
  // Preventing the overhead of this as we need to take consensus queue lock
  // again
  SCOPED_CLEANUP({
    if (!FLAGS_HANDLER(FLAGS_update_peer_health_status)) {
      return;
    }
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
    if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
      VLOG(1) << LogPrefixUnlocked() << "peer " << uuid
              << " is no longer tracked or queue is not in leader mode";
      return;
    }
    if (wal_catchup_progress)
      peer->wal_catchup_possible = true;
    if (wal_catchup_failure)
      peer->wal_catchup_possible = false;
    UpdatePeerHealthUnlocked(peer);
  });

  if (peer_copy.last_exchange_status == PeerStatus::TABLET_NOT_FOUND) {
    VLOG(3) << LogPrefixUnlocked() << "Peer " << uuid << " needs tablet copy"
            << THROTTLE_MSG;
    *needs_tablet_copy = true;
    return Status::OK();
  }
  *needs_tablet_copy = false;

  // If the next hop != the destination, we are sending these messages via a
  // proxy.
  bool route_via_proxy = *next_hop_uuid != uuid;
  if (route_via_proxy) {
    // Set proxy uuid
    request->set_proxy_dest_uuid(*next_hop_uuid);
  } else {
    // Clear proxy uuid to ensure that this message is not rejected by the
    // destination
    request->clear_proxy_dest_uuid();
  }

  // If we've never communicated with the peer, we don't know what messages to
  // send, so we'll send a status-only request. Otherwise, we grab requests
  // from the log starting at the last_received point.
  // If the caller has explicitly indicated to not read the ops (as indicated by
  // 'read_ops'), then we skip reading ops from log-cache/log. The caller
  // ususally does this when the leader detects that a peer is unhealthy and
  // hence needs to be degraded to a 'status-only' request
  if (peer_copy.last_exchange_status != PeerStatus::NEW && read_ops) {
    // The batch of messages to send to the peer.
    vector<ReplicateRefPtr> messages;
    Status s = FLAGS_buffer_messages_between_rpcs
        ? ExtractBuffer(peer_copy, route_via_proxy, &messages, &preceding_id)
        : ReadMessagesForRequest(
              peer_copy, route_via_proxy, &messages, &preceding_id);

    if (PREDICT_FALSE(!s.ok())) {
      // It's normal to have a NotFound() here if a follower falls behind where
      // the leader has GCed its logs. The follower replica will hang around
      // for a while until it's evicted.
      if (PREDICT_TRUE(s.IsNotFound())) {
        KLOG_EVERY_N_SECS_THROTTLER(
            INFO, 60, *peer_copy.status_log_throttler, "logs_gced")
            << LogPrefixUnlocked()
            << Substitute(
                   "The logs necessary to catch up peer $0 have been "
                   "garbage collected. The follower will never be able "
                   "to catch up ($1)",
                   uuid,
                   s.ToString());
        wal_catchup_failure = true;
        return s;
      }
      if (s.IsUninitialized()) {
        LOG_WITH_PREFIX_UNLOCKED_EVERY_N(ERROR, 10)
            << "Log is not ready to be read yet while preparing peer request: "
            << s.ToString() << ". Destination peer: " << peer_copy.ToString();
        return s;
      }
      if (s.IsIncomplete()) {
        // IsIncomplete() means that we tried to read beyond the head of the log
        // (in the future). See KUDU-1078.
        LOG_WITH_PREFIX_UNLOCKED(ERROR)
            << "Error trying to read ahead of the log "
            << "while preparing peer request: " << s.ToString()
            << ". Destination peer: " << peer_copy.ToString();
        return s;
      }
      LOG_WITH_PREFIX_UNLOCKED(FATAL)
          << "Error reading the log while preparing peer request: "
          << s.ToString() << ". Destination peer: " << peer_copy.ToString();
    }

    // Since we were able to read ops through the log cache, we know that
    // catchup is possible.
    wal_catchup_progress = true;

    // We use AddAllocated rather than copy, because we pin the log cache at the
    // "all replicated" point. At some point we may want to allow partially
    // loading (and not pinning) earlier messages. At that point we'll need to
    // do something smarter here, like copy or ref-count.
    if (!route_via_proxy) {
      for (const ReplicateRefPtr& msg : messages) {
        request->mutable_ops()->AddAllocated(msg->get());
      }
      msg_refs->swap(messages);
    } else {
      vector<ReplicateRefPtr> proxy_ops;
      for (const ReplicateRefPtr& msg : messages) {
        ReplicateRefPtr proxy_op =
            make_scoped_refptr_replicate(new ReplicateMsg);
        *proxy_op->get()->mutable_id() = msg->get()->id();
        proxy_op->get()->set_timestamp(msg->get()->timestamp());
        proxy_op->get()->set_op_type(PROXY_OP);
        request->mutable_ops()->AddAllocated(proxy_op->get());
        proxy_ops.emplace_back(std::move(proxy_op));
      }
      msg_refs->swap(proxy_ops);
    }
  }

  DCHECK(preceding_id.IsInitialized());
  request->mutable_preceding_id()->CopyFrom(preceding_id);

  // If we are sending ops to the follower, but the batch doesn't reach the
  // current committed index, we can consider the follower lagging, and it's
  // worth logging this fact periodically.
  if (request->ops_size() > 0) {
    int64_t last_op_sent = request->ops(request->ops_size() - 1).id().index();
    if (last_op_sent < request->committed_index()) {
      // Will use metrics to cover this and alarm on it, otherwise it can
      // overwhelm logs
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Peer " << uuid << " is lagging by at least "
          << (request->committed_index() - last_op_sent)
          << " ops behind the committed index " << THROTTLE_MSG;
    }
    // If we're not sending ops to the follower, set the safe time on the
    // request.
    // TODO(dralves) When we have leader leases, send this all the time.
  } else {
    if (PREDICT_TRUE(FLAGS_safe_time_advancement_without_writes)) {
      request->set_safe_timestamp(time_manager_->GetSafeTime().value());
    } else {
      KLOG_EVERY_N_SECS(WARNING, 300)
          << "Safe time advancement without writes is disabled. "
             "Snapshot reads on non-leader replicas may stall if there are no writes in progress.";
    }
  }

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    if (request->ops_size() > 0) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Sending request with operations to Peer: " << uuid
          << ". Size: " << request->ops_size()
          << ". From: " << SecureShortDebugString(request->ops(0).id())
          << ". To: "
          << SecureShortDebugString(request->ops(request->ops_size() - 1).id())
          << ". Preceding Opid: "
          << SecureShortDebugString(request->preceding_id());
    } else {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Sending status only request to Peer: " << uuid << ": "
          << SecureDebugString(*request);
    }
  }

  return Status::OK();
}

Status PeerMessageQueue::ReadMessagesForRequest(
    const TrackedPeer& peer_copy,
    bool route_via_proxy,
    std::vector<ReplicateRefPtr>* messages,
    OpId* preceding_id) {
  ReadContext read_context;
  read_context.for_peer_uuid = &peer_copy.uuid();
  read_context.for_peer_host = &peer_copy.peer_pb.last_known_addr().host();
  read_context.for_peer_port = peer_copy.peer_pb.last_known_addr().port();
  read_context.route_via_proxy = route_via_proxy;
  // When warm storage catchups are enabled, we avoid reporting errors to
  // error manager to avoid unnecessary replacements since we can catch up
  // from warm storage.
  read_context.report_errors = !FLAGS_warm_storage_catchup;

  // We try to get the follower's next_index from our log.
  LogCache::ReadOpsStatus s = log_cache_->ReadOps(
      peer_copy.next_index - 1,
      FLAGS_consensus_max_batch_size_bytes,
      read_context,
      messages);
  if (s.status.ok()) {
    *preceding_id = std::move(s.preceding_op);
  }
  return std::move(s.status);
}

Status PeerMessageQueue::ExtractBuffer(
    const TrackedPeer& peer_copy,
    bool route_via_proxy,
    std::vector<ReplicateRefPtr>* messages,
    OpId* preceding_id) {
  VLOG_WITH_PREFIX_UNLOCKED(3)
      << "Extracting buffer for peer: " << peer_copy.uuid() << "["
      << peer_copy.peer_pb.last_known_addr().host() << ":"
      << peer_copy.peer_pb.last_known_addr().port()
      << "] starting at index: " << peer_copy.next_index
      << ", route_via_proxy: " << route_via_proxy;

  std::future<HandedOffBufferData> future =
      peer_copy.peer_msg_buffer->RequestHandoff(
          peer_copy.next_index, route_via_proxy);

  {
    PeerMessageBuffer::LockedBufferHandle handle =
        peer_copy.peer_msg_buffer->TryLock();
    // If we did not get the lock here, we know we have a FillBuffer queued
    // after we set the handoff index, so we can just wait for that to populate
    // the future. If we got the lock, either the current filler or  this block
    // (but not both) will populate the future.
    if (handle) {
      ReadContext read_context;
      read_context.for_peer_uuid = &peer_copy.uuid();
      read_context.for_peer_host = &peer_copy.peer_pb.last_known_addr().host();
      read_context.for_peer_port = peer_copy.peer_pb.last_known_addr().port();
      read_context.route_via_proxy = route_via_proxy;
      auto peer_msg_buffer_ptr = peer_copy.peer_msg_buffer;
      FillBuffer(read_context, std::move(handle));
    }
  }

  HandedOffBufferData buffer_data = std::move(future).get();
  Status s = buffer_data.status.IsContinue() ? Status::OK()
                                             : std::move(buffer_data.status);
  std::move(buffer_data).GetData(messages, preceding_id);
  return s;
}

void PeerMessageQueue::FillBufferForPeer(
    const std::string& uuid,
    ReplicateRefPtr latest_appended_replicate) {
  TrackedPeer peer_copy;
  bool route_via_proxy = false;
  {
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    DCHECK_EQ(queue_state_.state, kQueueOpen);
    DCHECK_NE(uuid, local_peer_pb_.permanent_uuid());

    TrackedPeer* peer = FindPtrOrNull(peers_map_, uuid);
    if (PREDICT_FALSE(peer == nullptr)) {
      return;
    } else if (PREDICT_FALSE(!peer->is_healthy())) {
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "Not buffering for unhealthy peer: " << uuid << "["
          << peer->peer_pb.last_known_addr().host() << ":"
          << peer->peer_pb.last_known_addr().port() << "]";
      return;
    }
    std::string next_hop_uuid;
    routing_table_container_->NextHop(
        local_peer_pb_.permanent_uuid(), uuid, &next_hop_uuid);

    if (next_hop_uuid != uuid) {
      TrackedPeer* proxy_peer = FindPtrOrNull(peers_map_, next_hop_uuid);
      if (proxy_peer != nullptr &&
          !HasProxyPeerFailedUnlocked(proxy_peer, peer)) {
        route_via_proxy = true;
      }
    }

    peer_copy = *peer;
  }
  ReadContext read_context;
  read_context.for_peer_uuid = &uuid;
  read_context.for_peer_host = &peer_copy.peer_pb.last_known_addr().host();
  read_context.for_peer_port = peer_copy.peer_pb.last_known_addr().port();
  read_context.route_via_proxy = route_via_proxy;
  FillBuffer(
      read_context,
      peer_copy.peer_msg_buffer,
      std::move(latest_appended_replicate));
}

Status PeerMessageQueue::FillBuffer(
    const ReadContext& read_context,
    std::shared_ptr<PeerMessageBuffer>& peer_message_buffer,
    ReplicateRefPtr latest_appended_replicate) {
  return FillBuffer(
      read_context,
      peer_message_buffer->TryLock(),
      std::move(latest_appended_replicate));
}

Status PeerMessageQueue::FillBuffer(
    const ReadContext& read_context,
    PeerMessageBuffer::LockedBufferHandle peer_message_buffer,
    ReplicateRefPtr latest_appended_replicate) {
  if (!peer_message_buffer) {
    return Status::OK();
  }
  if (peer_message_buffer->LastIndex() == -1) {
    // There's no buffer watermark. If this was a fresh state, we use the logic
    // in the handoff for the first rpc to bootstrap the last_buffered
    // watermark.
    HandOffBufferIfNeeded(peer_message_buffer, read_context);
    return Status::OK();
  }

  if (!peer_message_buffer->Empty() &&
      peer_message_buffer->ForProxying() != read_context.route_via_proxy) {
    VLOG_WITH_PREFIX_UNLOCKED(1)
        << "Abandoning buffer for peer: " << *read_context.for_peer_uuid << "["
        << *read_context.for_peer_host << ":" << read_context.for_peer_port
        << "] as proxy settings have changed. Buffer: "
        << peer_message_buffer->ForProxying()
        << ", request: " << read_context.route_via_proxy;

    peer_message_buffer->ResetBuffer();
    return Status::OK();
  }

  Status s =
      peer_message_buffer->AppendMessage(std::move(latest_appended_replicate));
  if (!s.ok()) {
    s = peer_message_buffer->ReadFromCache(read_context, log_cache_.get());
  }
  if (s.ok() || s.IsIncomplete() || s.IsContinue()) {
    HandOffBufferIfNeeded(peer_message_buffer, read_context);
    if (s.IsContinue() && !peer_message_buffer->BufferFull()) {
      // Checking for max batch after handoff is intended since if we did
      // handoff we should start filling for the next rpc
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Continue buffering for peer: " << *read_context.for_peer_uuid
          << "[" << *read_context.for_peer_host << ":"
          << read_context.for_peer_port << "] since there are more ops to read";

      WARN_NOT_OK(
          raft_pool_observers_token_->SubmitClosure(Bind(
              &PeerMessageQueue::FillBufferForPeer,
              Unretained(this),
              *read_context.for_peer_uuid,
              nullptr)),
          LogPrefixUnlocked() + "Unable to continue filling buffer for " +
              *read_context.for_peer_uuid);
    }
  } else {
    VLOG_WITH_PREFIX_UNLOCKED(1)
        << "Error filling buffer for peer: " << *read_context.for_peer_uuid
        << "[" << *read_context.for_peer_host << ":"
        << read_context.for_peer_port << "]: " << s.ToString();
  }

  return s;
}

void PeerMessageQueue::HandOffBufferIfNeeded(
    PeerMessageBuffer::LockedBufferHandle& peer_message_buffer,
    const ReadContext& read_context) {
  int64_t initial_index;
  if (auto opt_handoff_index = peer_message_buffer.GetIndexForHandoff()) {
    initial_index = *opt_handoff_index;
  } else {
    return;
  }

  VLOG_WITH_PREFIX_UNLOCKED(3)
      << "Responding to handoff request for peer: "
      << *read_context.for_peer_uuid << "[" << *read_context.for_peer_host
      << ":" << read_context.for_peer_port << "] for index: " << initial_index;

  bool buffer_empty = peer_message_buffer->Empty();
  bool proxy_requirement_different =
      !peer_message_buffer.ProxyRequirementSatisfied();
  bool index_mismatch =
      !buffer_empty && peer_message_buffer->FirstIndex() != initial_index;

  Status s = Status::OK();
  if (buffer_empty || proxy_requirement_different || index_mismatch) {
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Handoff buffer mismatch for peer: " << *read_context.for_peer_uuid
        << "[" << *read_context.for_peer_host << ":"
        << read_context.for_peer_port << "] "
        << (buffer_empty ? "(Buffer empty) " : "")
        << (proxy_requirement_different ? "(Proxy req different) " : "")
        << (index_mismatch ? "(Index mismatch) " : "")
        << ", first bufferred: " << peer_message_buffer->FirstIndex()
        << ", next index: " << peer_message_buffer->LastIndex()
        << ", requested_index: " << initial_index
        << ", bufferred for proxy: " << peer_message_buffer->ForProxying();

    // Buffer not suitable for handoff, dump the buffer and reread
    // TODO: this can be more graceful, like we can try to fix the buffer
    peer_message_buffer->ResetBuffer(
        read_context.route_via_proxy, initial_index - 1);
    s = peer_message_buffer->ReadFromCache(read_context, log_cache_.get());
    if (!s.ok() && !s.IsIncomplete() && !s.IsContinue()) {
      VLOG_WITH_PREFIX_UNLOCKED(1)
          << "Error filling buffer for peer during handoff: "
          << *read_context.for_peer_uuid << "[" << *read_context.for_peer_host
          << ":" << read_context.for_peer_port << "]: " << s.ToString();
    }
  }

  peer_message_buffer.FulfillPromiseWithBuffer(std::move(s));
}

void PeerMessageQueue::AdvanceQueueRegionDurableIndex() {
  int64_t max_region_durable_index = -1;

  if (!local_peer_pb_.attrs().has_region()) {
    return;
  }

  // region_durable_index is updated only if following constraints are satisfied
  // 1. region_durable_index <= committed_index
  // 2. Atleast one non-leader region has received this index
  for (const PeersMap::value_type& peer : peers_map_) {
    if (!peer.second->is_peer_in_local_region.has_value()) {
      continue;
    }
    if (!peer.second->is_peer_in_local_region.value() &&
        peer.second->last_received.index() <= queue_state_.committed_index) {
      // This peer is outside our region and the last received index is
      // lower than the current committed_index. Include this in the
      // calculation of region_durable_index
      max_region_durable_index = std::max(
          max_region_durable_index, peer.second->last_received.index());
    }
  }

  queue_state_.region_durable_index =
      std::max(queue_state_.region_durable_index, max_region_durable_index);
}

void PeerMessageQueue::AdvanceQueueWatermark(
    const char* type,
    int64_t* watermark,
    const OpId& replicated_before,
    const OpId& replicated_after,
    int num_peers_required,
    ReplicaTypes replica_types,
    const TrackedPeer* who_caused) {
  if (VLOG_IS_ON(2)) {
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Updating " << type << " watermark: " << "Peer ("
        << who_caused->ToString() << ") changed from " << replicated_before
        << " to " << replicated_after << ". "
        << "Current value: " << *watermark;
  }

  // Go through the peer's watermarks, we want the highest watermark that
  // 'num_peers_required' of peers has replicated. To find this we do the
  // following:
  // - Store all the peer's 'last_received' in a vector
  // - Sort the vector
  // - Find the vector.size() - 'num_peers_required' position, this
  //   will be the new 'watermark'.
  std::vector<int64_t> watermarks;
  watermarks.reserve(peers_map_.size());
  for (const PeersMap::value_type& peer : peers_map_) {
    if (replica_types == VOTER_REPLICAS &&
        peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    // TODO(todd): The fact that we only consider peers whose last exchange was
    // successful can cause the "all_replicated" watermark to lag behind
    // farther than necessary. For example:
    // - local peer has replicated opid 100
    // - remote peer A has replicated opid 100
    // - remote peer B has replication opid 10 and is catching up
    // - remote peer A goes down
    // Here we'd start getting a non-OK last_exchange_status for peer A.
    // In that case, the 'all_replicated_watermark', which requires 3 peers,
    // would not be updateable, even once we've replicated peer 'B' up to opid
    // 100. It would get "stuck" at 10. In fact, in this case, the
    // 'majority_replicated_watermark' would also move *backwards* when peer A
    // started getting errors.
    //
    // The issue with simply removing this condition is that 'last_received'
    // does not perfectly correspond to the 'match_index' in Raft Figure 2. It
    // is simply the highest operation in a peer's log, regardless of whether
    // that peer currently holds a prefix of the leader's log. So, in the case
    // that the last exchange was an error (LMP mismatch, for example), the
    // 'last_received' is _not_ usable for watermark calculation. This could be
    // fixed by separately storing the 'match_index' on a per-peer basis and
    // using that for watermark calculation.
    if (peer.second->last_exchange_status == PeerStatus::OK) {
      watermarks.push_back(peer.second->last_received.index());
    }
  }

  // If we haven't enough peers to calculate the watermark return.
  if (watermarks.size() < num_peers_required) {
    VLOG_WITH_PREFIX_UNLOCKED(3)
        << "Watermarks size: " << watermarks.size() << ", "
        << "Num peers required: " << num_peers_required;
    return;
  }

  std::sort(watermarks.begin(), watermarks.end());

  int64_t new_watermark = watermarks[watermarks.size() - num_peers_required];
  int64_t old_watermark = *watermark;
  *watermark = new_watermark;

  VLOG_WITH_PREFIX_UNLOCKED(1) << "Updated " << type << " watermark " << "from "
                               << old_watermark << " to " << new_watermark;
  if (VLOG_IS_ON(3)) {
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Peers: ";
    for (const PeersMap::value_type& peer : peers_map_) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Peer: " << peer.second->ToString();
    }
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Sorted watermarks:";
    for (int64_t watermark : watermarks) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << watermark;
    }
  }
}

int64_t PeerMessageQueue::DoComputeNewWatermarkStaticMode(
    const std::map<std::string, int>& voter_distribution,
    const std::map<std::string, std::vector<int64_t>>& watermarks_by_region,
    int64_t* watermark) {
  CHECK(watermark);
  CHECK(queue_state_.active_config->has_commit_rule());
  CHECK(queue_state_.active_config->commit_rule().rule_predicates_size() > 0);

  const QuorumMode& mode = queue_state_.active_config->commit_rule().mode();
  CHECK(
      mode == QuorumMode::STATIC_DISJUNCTION ||
      mode == QuorumMode::STATIC_CONJUNCTION);
  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Computing new commit index in static "
      << ((mode == QuorumMode::STATIC_DISJUNCTION) ? "disjunction"
                                                   : "conjunction")
      << " mode";
  const auto& rule_predicates =
      queue_state_.active_config->commit_rule().rule_predicates();

  // For each individual predicate, the commit index corresponding to that
  // predicate is appeneded to the following vector. For eg. if the commit
  // rule is defined as:
  // p1: majority in 1 out of 3 regions in {R1, R2, R3}  AND / OR
  // p2: majority in 3 out of 5 regions in {R4, R5, R6, R7, R8},
  // then the following vector would have at most two entries denoting
  // the commit index allowed by each predicate p1 & p2.
  std::vector<int64_t> predicate_commit_indexes;

  for (const CommitRulePredicatePB& rule_predicate : rule_predicates) {
    int regions_subset_size = rule_predicate.regions_subset_size();

    if (VLOG_IS_ON(3)) {
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "Computing commit index for a predicate with "
          << rule_predicate.regions_size()
          << ", Number of majority regions required : " << regions_subset_size;
    }

    // For each of the regions featuring in a predicate, the following vector
    // stores commit indexes corresponding to those regions.
    // Lets take p2: majority in 3 out of 5 regions in {R4, R5, R6, R7, R8}
    // from the example above. The vector will contain commit indexes
    // corresponding to each of the regions R4, R5, R6, R7 and R8.
    std::vector<int64_t> regional_commit_indexes;

    for (const std::string& region : rule_predicate.regions()) {
      int total_voters = FindOrDie(voter_distribution, region);
      DCHECK(total_voters >= 1 || !adjust_voter_distribution_);
      int commit_req = MajoritySize(total_voters);
      std::map<std::string, std::vector<int64_t>>::const_iterator it =
          watermarks_by_region.find(region);

      // If we haven't got responses from enough number of servers in region,
      // we simply move on.
      if (it == watermarks_by_region.end() || it->second.size() < commit_req) {
        if (VLOG_IS_ON(3)) {
          VLOG_WITH_PREFIX_UNLOCKED(3)
              << "Skipping region: " << region
              << ", Majority size: " << commit_req << ", Servers responded: "
              << ((it == watermarks_by_region.end()) ? 0 : it->second.size());
        }
        continue;
      }

      const std::vector<int64_t>& watermarks_in_region = it->second;

      // Computing the commit index in each region.
      int64_t regional_commit_index =
          watermarks_in_region[watermarks_in_region.size() - commit_req];

      if (VLOG_IS_ON(3)) {
        VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermarks in region: " << region;
        for (int64_t watermark_it : watermarks_in_region) {
          VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << watermark_it;
        }
        VLOG_WITH_PREFIX_UNLOCKED(3)
            << "Regional commit index: " << regional_commit_index;
      }

      regional_commit_indexes.push_back(regional_commit_index);
    }

    // If we haven't got enough majorities in regions listed in the predicate,
    // we simply move on.
    if (regional_commit_indexes.size() < regions_subset_size) {
      if (VLOG_IS_ON(3)) {
        VLOG_WITH_PREFIX_UNLOCKED(3)
            << "Skipping predicate."
            << " Number of regions required: " << regions_subset_size
            << ", Number of regions responded: "
            << regional_commit_indexes.size();
      }
      continue;
    }

    // Computing the commit index as per the predicate.
    int64_t predicate_commit_index = regional_commit_indexes
        [regional_commit_indexes.size() - regions_subset_size];
    if (VLOG_IS_ON(3)) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermarks in regions: ";
      for (int64_t watermark_it : regional_commit_indexes) {
        VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << watermark_it;
      }
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "Predicate commit index: " << predicate_commit_index;
    }
    predicate_commit_indexes.push_back(predicate_commit_index);
  }

  int64_t old_watermark = *watermark;
  if (mode == QuorumMode::STATIC_DISJUNCTION) {
    // Maximum commit index is chosen from the predicate commit indexes
    // because of disjunction.
    const std::vector<int64_t>::const_iterator it = std::max_element(
        predicate_commit_indexes.begin(), predicate_commit_indexes.end());
    // Checking the possibility that predicate_commit_indexes
    // can be empty in case we didn't get majorities from enough
    // regions for any of the predicates.
    if (it != predicate_commit_indexes.end()) {
      *watermark = *it;
    } else if (VLOG_IS_ON(3)) {
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "None of the predicates have got enough majorities.";
    }
  }

  if (mode == QuorumMode::STATIC_CONJUNCTION) {
    // We only compute the new commit index if the all of the predicates have
    // contributed to the predicate_commit_indexes vector with their individual
    // commit_index. We cannot afford to overlook any single predicate in
    // the conjunctive mode.
    if (predicate_commit_indexes.size() == rule_predicates.size()) {
      // Minimum commit index is chosen from the predicate commit indexes
      // because of conjunction.
      const std::vector<int64_t>::const_iterator it = std::min_element(
          predicate_commit_indexes.begin(), predicate_commit_indexes.end());
      CHECK(it != predicate_commit_indexes.end());
      *watermark = *it;
    } else if (VLOG_IS_ON(3)) {
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "At least one of the predicates hasn't got enough majorities."
          << " Number of predicates: " << rule_predicates.size()
          << " Number of predicates with enough majorities: "
          << predicate_commit_indexes.size();
    }
  }

  return old_watermark;
}

PeerMessageQueue::QuorumResults PeerMessageQueue::IsQuorumSatisfiedUnlocked(
    const RaftPeerPB& peer,
    const std::function<bool(const TrackedPeer*)>& predicate) {
  if (!FLAGS_enable_flexi_raft) {
    // For Vanilla raft mode, peer (local_peer_pb_) might not have fields
    // populated other than uuid
    int num_satisfied = 0;
    std::vector<TrackedPeer*> quorum_peers;
    for (const PeersMap::value_type& peer : peers_map_) {
      if (!peer.second->peer_pb.has_member_type() ||
          peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
        continue;
      }
      if (predicate(peer.second)) {
        num_satisfied++;
        quorum_peers.push_back(peer.second);
      }
    }
    return {
        num_satisfied >= queue_state_.majority_size_,
        num_satisfied,
        queue_state_.majority_size_,
        kVanillaRaftQuorumId,
        quorum_peers};
  }

  const std::string& peer_quorum_id = getQuorumIdUsingCommitRule(peer);

  // Compute total number of voters in each region.
  std::optional<int> total_from_vd = GetTotalVotersFromVoterDistribution(
      *(queue_state_.active_config), peer_quorum_id);

  int total_voters_from_voter_distribution = total_from_vd.value_or(0);

  // Compute number of voters in each region in the active config.
  // As voter distribution provided in topology config can lag,
  // we need to take into account the active voters as well due to
  // membership changes.
  // Check for more comments in AdjustVoterDistributionWithCurrentVoters() which
  // does the same for static mode watermark calculation
  int total_voters_from_active_config = 0;
  for (const RaftPeerPB& peer_pb : queue_state_.active_config->peers()) {
    if (!peer_pb.has_member_type() ||
        peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }

    CHECK(peer_pb.has_permanent_uuid());
    const std::string& peer_pb_quorum_id = getQuorumIdUsingCommitRule(peer_pb);
    if (peer_pb_quorum_id != peer_quorum_id) {
      // In dynamic mode, only the leader region matters
      continue;
    }

    total_voters_from_active_config++;
  }

  int total_voters = std::max(
      total_voters_from_voter_distribution, total_voters_from_active_config);

  // adjust_voter_distribution_ is set to false on in cases where we want to
  // perform an election forcefully i.e. unsafe config change
  if (PREDICT_FALSE(!adjust_voter_distribution_)) {
    total_voters = total_voters_from_voter_distribution;
  }

  DCHECK(total_voters >= 1 || !adjust_voter_distribution_);
  int majority_size = MajoritySize(total_voters);

  bool is_local_peer = peer.permanent_uuid() == local_peer_pb_.permanent_uuid();
  int num_satisfied = 0;
  std::vector<TrackedPeer*> quorum_peers;
  for (const PeersMap::value_type& tracked_peer : peers_map_) {
    if (!tracked_peer.second->peer_pb.has_member_type() ||
        tracked_peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }

    // We are either computing quorum on the local peer or on a remote peer.
    // For local peer, we can resort to the optimization of looking at
    // is_peer_in_local_quorum which is previously set. This is a worthy
    // optimization because this code path is called on every write.
    if (PREDICT_TRUE(is_local_peer)) {
      if (!tracked_peer.second->is_peer_in_local_quorum.has_value() ||
          !tracked_peer.second->is_peer_in_local_quorum.value()) {
        continue;
      }
    } else {
      string quorum_id =
          getQuorumIdUsingCommitRule(tracked_peer.second->peer_pb);
      if (quorum_id != peer_quorum_id) {
        continue;
      }
    }

    if (predicate(tracked_peer.second)) {
      num_satisfied++;
      quorum_peers.push_back(tracked_peer.second);
    }
  }

  QuorumResults results = {
      num_satisfied >= majority_size,
      num_satisfied,
      majority_size,
      peer_quorum_id,
      quorum_peers};
  return results;
}

PeerMessageQueue::QuorumResults
PeerMessageQueue::IsSecondRegionDurabilitySatisfiedUnlocked(
    const std::function<bool(const TrackedPeer*)>& predicate) {
  int acks_outoflocalregion = 0;
  std::vector<TrackedPeer*> outoflocalregion_peers;
  for (const PeersMap::value_type& peer : peers_map_) {
    if (!peer.second->peer_pb.has_member_type() ||
        peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    if (predicate(peer.second)) {
      if (peer.second->is_peer_in_local_region.has_value() &&
          !peer.second->is_peer_in_local_region.value()) {
        acks_outoflocalregion++;
        outoflocalregion_peers.push_back(peer.second);
      }
    }
  }
  return {// Check if atleast one of the acks is out of local region
          acks_outoflocalregion > 0,
          acks_outoflocalregion,
          queue_state_.majority_size_,
          kVanillaRaftQuorumId,
          outoflocalregion_peers};
}

int64_t PeerMessageQueue::ComputeNewWatermarkDynamicMode(int64_t* watermark) {
  CHECK(watermark);
  CHECK(queue_state_.active_config->has_commit_rule());
  CHECK(
      queue_state_.active_config->commit_rule().mode() ==
      QuorumMode::SINGLE_REGION_DYNAMIC);

  // Compute the watermarks in leader quorum. As an example, at the end of this
  // loop, watermarks_in_leader_quorum might have entries (3, 7, 5) which
  // indicates that the leader quorum has 3 peers that have responded to OpId
  // indexes 3, 7 and 5 respectively
  std::vector<int64_t> watermarks_in_leader_quorum;
  watermarks_in_leader_quorum.reserve(FLAGS_default_quorum_size * 2);

  auto results = IsQuorumSatisfiedUnlocked(
      local_peer_pb_, [&watermarks_in_leader_quorum](auto peer) {
        // Refer to the comment in AdvanceQueueWatermark method for why only
        // successful last exchanges are considered.
        if (peer->last_exchange_status == PeerStatus::OK) {
          watermarks_in_leader_quorum.push_back(peer->last_received.index());
          return true;
        }
        return false;
      });

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Computing new commit index in single " << "region dynamic mode.";

  // Return without advancing the commit watermark, if majority in leader
  // region is not satisfied, ie. not enough number of replicas have responded
  // from that region.
  if (!results.quorum_satisfied) {
    if (VLOG_IS_ON(3)) {
      VLOG_WITH_PREFIX_UNLOCKED(3)
          << "Watermarks size: " << watermarks_in_leader_quorum.size()
          << ", Num peers required: " << results.quorum_size
          << ", Quorum: " << results.quorum_id;
    }
    return *watermark;
  }

  // Sort the watermarks
  std::sort(
      watermarks_in_leader_quorum.begin(), watermarks_in_leader_quorum.end());

  int64_t old_watermark = *watermark;
  *watermark = watermarks_in_leader_quorum
      [watermarks_in_leader_quorum.size() - std::max(results.quorum_size, 1)];
  return old_watermark;
}

int64_t PeerMessageQueue::ComputeNewWatermarkStaticMode(int64_t* watermark) {
  CHECK(watermark);
  CHECK(queue_state_.active_config->has_commit_rule());

  if (!IsStaticQuorumMode(queue_state_.active_config->commit_rule().mode())) {
    return *watermark;
  }

  // clang-format off
  // For each region, we compute a vector of indexes that were replicated.
  // It might look like the following example:
  // prn: <4,4,5,7>
  // frc: <2,3,4>
  // lla: <5,5>
  // This example suggests that we received non-erroneous responses from 4
  // replicas in prn, 3 in frc and 2 in lla. Two replicas in prn have received
  // entries until index 4, one has received until 5 and one until 7. Similarly,
  // 2 replicas in lla have received entries until index 5.
  // clang-format on
  std::map<std::string, std::vector<int64_t>> watermarks_by_region;
  for (const PeersMap::value_type& peer : peers_map_) {
    if (peer.second->peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    // Refer to the comment in AdvanceQueueWatermark method for why only
    // successful last exchanges are considered.
    if (peer.second->last_exchange_status == PeerStatus::OK) {
      const string& peer_region = peer.second->peer_pb.attrs().region();
      std::vector<int64_t>& regional_watermarks = LookupOrInsert(
          &watermarks_by_region, peer_region, std::vector<int64_t>());
      regional_watermarks.push_back(peer.second->last_received.index());
    }
  }

  // Sort all the watermarks.
  for (std::map<std::string, std::vector<int64_t>>::iterator it =
           watermarks_by_region.begin();
       it != watermarks_by_region.end();
       it++) {
    std::sort(it->second.begin(), it->second.end());
  }

  // Map to store the number of voters in each region from the active config.
  std::map<std::string, int> voter_distribution;

  // Compute total number of voters in each region.
  voter_distribution.insert(
      queue_state_.active_config->voter_distribution().begin(),
      queue_state_.active_config->voter_distribution().end());

  // adjust_voter_distribution_ is set to false on in cases where we want to
  // perform an election forcefully i.e. unsafe config change
  if (PREDICT_TRUE(adjust_voter_distribution_)) {
    // Compute number of voters in each region in the active config.
    // As voter distribution provided in topology config can lag,
    // we need to take into account the active voters as well due to
    // membership changes.
    AdjustVoterDistributionWithCurrentVoters(
        *(queue_state_.active_config), &voter_distribution);
  }

  return DoComputeNewWatermarkStaticMode(
      voter_distribution, watermarks_by_region, watermark);
}

void PeerMessageQueue::AdvanceMajorityReplicatedWatermarkFlexiRaft(
    int64_t* watermark,
    const OpId& replicated_before,
    const OpId& replicated_after,
    const TrackedPeer* who_caused) {
  CHECK(watermark);
  CHECK(who_caused);

  if (VLOG_IS_ON(2)) {
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Updating majority_replicated watermark: " << "Peer ("
        << who_caused->ToString() << ") changed from " << replicated_before
        << " to " << replicated_after << ". "
        << "Current value: " << *watermark;
  }

  // Update the watermark based on the acknowledgements so far.
  int64_t old_watermark = -1;
  if (queue_state_.active_config->commit_rule().mode() ==
      QuorumMode::SINGLE_REGION_DYNAMIC) {
    const std::string& leader_quorum =
        getQuorumIdUsingCommitRule(local_peer_pb_);
    const std::string& peer_quorum =
        getQuorumIdUsingCommitRule(who_caused->peer_pb);

    // In SINGLE_REGION_DYNAMIC mode, only an ack from the leader region can
    // advance the watermark. Skip this expensive operation otherwise
    if (leader_quorum == peer_quorum) {
      old_watermark = ComputeNewWatermarkDynamicMode(watermark);
    }
  } else {
    old_watermark = ComputeNewWatermarkStaticMode(watermark);
  }

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Updated majority_replicated watermark " << "from " << old_watermark
      << " to " << (*watermark);
}

void PeerMessageQueue::BeginWatchForSuccessor(
    const std::optional<string>& successor_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
    PeerMessageQueue::TransferContext transfer_context) {
  std::lock_guard<simple_mutexlock> l(queue_lock_);

  transfer_context_ = std::move(transfer_context);
  successor_watch_peer_notified_ = false;

  if (successor_uuid && FLAGS_synchronous_transfer_leadership &&
      PeerTransferLeadershipImmediatelyUnlocked(*successor_uuid)) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leadership transfer to " << *successor_uuid
        << " started synchronously";
    return;
  }

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Leadership transfer: Watching for successor asynchronously";

  successor_watch_in_progress_ = true;
  designated_successor_uuid_ = successor_uuid;
  tl_filter_fn_ = filter_fn;
}

void PeerMessageQueue::EndWatchForSuccessor() {
  std::lock_guard<simple_mutexlock> l(queue_lock_);
  successor_watch_in_progress_ = false;
  transfer_context_ = {};
  tl_filter_fn_ = nullptr;
}

bool PeerMessageQueue::WatchForSuccessorPeerNotified() {
  std::lock_guard<simple_mutexlock> l(queue_lock_);
  return successor_watch_peer_notified_;
}

Status PeerMessageQueue::GetNextRoutingHopFromLeader(
    const string& dest_uuid,
    string* next_hop) const {
  return routing_table_container_->NextHop(
      local_peer_pb_.permanent_uuid(), dest_uuid, next_hop);
}

void PeerMessageQueue::UpdateFollowerWatermarks(
    int64_t committed_index,
    int64_t all_replicated_index,
    int64_t region_durable_index) {
  std::lock_guard<simple_mutexlock> l(queue_lock_);
  DCHECK_EQ(queue_state_.mode, NON_LEADER);
  queue_state_.committed_index = committed_index;
  queue_state_.all_replicated_index = all_replicated_index;

  if (region_durable_index > queue_state_.region_durable_index) {
    queue_state_.region_durable_index = region_durable_index;
  }

  UpdateMetricsUnlocked();
}

void PeerMessageQueue::UpdateLastIndexAppendedToLeader(
    int64_t last_idx_appended_to_leader) {
  std::lock_guard<simple_mutexlock> l(queue_lock_);
  DCHECK_EQ(queue_state_.mode, NON_LEADER);
  queue_state_.last_idx_appended_to_leader = last_idx_appended_to_leader;
  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdatePeerStatus(
    const string& peer_uuid,
    PeerStatus ps,
    const Status& status) {
  std::unique_lock<simple_mutexlock> l(queue_lock_);
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  if (PREDICT_FALSE(peer == nullptr || queue_state_.mode == NON_LEADER)) {
    VLOG(1) << LogPrefixUnlocked() << "peer " << peer_uuid
            << " is no longer tracked or queue is not in leader mode";
    return;
  }
  peer->last_exchange_status = ps;

  if (ps != PeerStatus::RPC_LAYER_ERROR) {
    // So long as we got _any_ response from the follower, we consider it a
    // 'communication'. RPC_LAYER_ERROR indicates something like a connection
    // failure, indicating that the host itself is likely down.
    //
    // This indicates that the node is at least online.
    peer->last_communication_time = MonoTime::Now();
  }

  switch (ps) {
    case PeerStatus::NEW:
      LOG_WITH_PREFIX_UNLOCKED(DFATAL)
          << "Should not update an existing peer to 'NEW' state";
      break;

    case PeerStatus::RPC_LAYER_ERROR:
      peer->incr_consecutive_failures();
      // Most controller errors are caused by network issues or corner cases
      // like shutdown and failure to deserialize a protobuf. Therefore, we
      // generally consider these errors to indicate an unreachable peer.
      DCHECK(!status.ok());
      break;

    case PeerStatus::TABLET_NOT_FOUND:
      peer->incr_consecutive_failures();
      VLOG_WITH_PREFIX_UNLOCKED(1)
          << "Peer needs tablet copy: " << peer->ToString();
      break;

    case PeerStatus::TABLET_FAILED: {
      peer->incr_consecutive_failures();
      UpdatePeerHealthUnlocked(peer);
      return;
    }

    case PeerStatus::REMOTE_ERROR:
    case PeerStatus::INVALID_TERM:
    case PeerStatus::LMP_MISMATCH:
    case PeerStatus::CANNOT_PREPARE:
      peer->incr_consecutive_failures();
      UpdatePeerAppendFailure(peer, status);
      break;

    case PeerStatus::OK:
      peer->reset_consecutive_failures();
      DCHECK(status.ok());
      break;
  }
}

void PeerMessageQueue::UpdateExchangeStatus(
    TrackedPeer* peer,
    const TrackedPeer& prev_peer_state,
    const ConsensusResponsePB& response,
    bool* lmp_mismatch) {
  DCHECK(queue_lock_.is_locked());
  const ConsensusStatusPB& status = response.status();

  MonoTime now = time_provider_->Now();
  peer->last_communication_time = now;
  peer->last_known_committed_index = status.last_committed_idx();

  if (PREDICT_TRUE(!status.has_error())) {
    peer->last_exchange_status = PeerStatus::OK;
    peer->last_successful_exchange = now;
    peer->corruption_count = 0;
    peer->reset_consecutive_failures();
    *lmp_mismatch = false;
    if (peer->should_send_compression_dict) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Resetting compression dict flag for peer: " << peer->ToString();
      peer->should_send_compression_dict = false;
    }
    return;
  }

  peer->incr_consecutive_failures();

  switch (status.error().code()) {
    case ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH:
      peer->last_exchange_status = PeerStatus::LMP_MISMATCH;
      DCHECK(status.has_last_received());
      if (prev_peer_state.last_exchange_status == PeerStatus::NEW) {
        LOG_WITH_PREFIX_UNLOCKED(INFO)
            << "Connected to new peer: " << peer->ToString();
      } else {
        LOG_WITH_PREFIX_UNLOCKED(INFO)
            << "Got LMP mismatch error from peer: " << peer->ToString();
      }
      *lmp_mismatch = true;
      return;

    case ConsensusErrorPB::INVALID_TERM:
      peer->last_exchange_status = PeerStatus::INVALID_TERM;
      CHECK(response.has_responder_term());
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Peer responded invalid term: " << peer->ToString();
      NotifyObserversOfTermChange(response.responder_term());
      *lmp_mismatch = false;
      return;

    default:
      // Other ConsensusStatusPB error codes (such as remote errors) are
      // supposed to be handled higher up in the stack.
      LOG_WITH_PREFIX_UNLOCKED(FATAL)
          << "Unexpected consensus error. Code: "
          << ConsensusErrorPB::Code_Name(status.error().code())
          << ". Response: " << SecureShortDebugString(response);
  }
}

void PeerMessageQueue::UpdatePeerAppendFailure(
    TrackedPeer* peer,
    const Status& status) {
  if (status.IsCompressionDictMismatch()) {
    peer->should_send_compression_dict = true;
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Got compression dict error from peer: " << peer->ToString();
  } else if (status.IsCorruption()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Corruption reported by peer. " << peer->ToString()
        << " [ERROR]: " << status.ToString();
    peer->corruption_count++;
    if (CorruptionLikely(peer)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Corruption likely at " << peer->next_index
          << ", evicting log cache";
      metrics_.corruption_cache_drops->Increment();
      log_cache_->EvictThroughOp(peer->next_index, true);
    }
  }
}

bool PeerMessageQueue::CorruptionLikely(TrackedPeer* peer) const {
  DCHECK(queue_lock_.is_locked());

  if (FLAGS_min_corruption_count <= 0 ||
      peer->corruption_count < FLAGS_min_corruption_count) {
    return false;
  }

  if (FLAGS_min_single_corruption_count > 0 &&
      peer->corruption_count >= FLAGS_min_single_corruption_count) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer " << peer->uuid()
        << " corruption count: " << peer->corruption_count << " > "
        << FLAGS_min_single_corruption_count;
    metrics_.single_corruption_cache_drops->Increment();
    return true;
  }

  size_t total_corrupted_peers = 0;
  for (const auto& [_, other_peer] : peers_map_) {
    if (other_peer->corruption_count >= FLAGS_min_corruption_count &&
        peer->next_index == other_peer->next_index) {
      total_corrupted_peers += 1;
    }
  }

  if (total_corrupted_peers > 1) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer " << peer->uuid()
        << " corruption count: " << peer->corruption_count << " > "
        << FLAGS_min_corruption_count << " and " << total_corrupted_peers
        << " are reporting corruption";

    return true;
  }

  return false;
}

void PeerMessageQueue::PromoteIfNeeded(
    TrackedPeer* peer,
    const TrackedPeer& prev_peer_state,
    const ConsensusStatusPB& status) {
  DCHECK(queue_lock_.is_locked());
  if (queue_state_.mode != PeerMessageQueue::LEADER ||
      peer->last_exchange_status != PeerStatus::OK) {
    return;
  }

  // TODO(mpercy): It would be more efficient to cache the member type in the
  // TrackedPeer data structure.
  RaftPeerPB* peer_pb;
  Status s = GetRaftConfigMember(
      DCHECK_NOTNULL(queue_state_.active_config.get()), peer->uuid(), &peer_pb);
  if (s.ok() && peer_pb->member_type() == RaftPeerPB::NON_VOTER &&
      peer_pb->attrs().promote()) {
    // Only promote the peer if it is within one round-trip of being fully
    // caught-up with the current commit index, as measured by recent
    // UpdateConsensus() operation batch sizes.

    // If we had never previously contacted this peer, wait until the second
    // time we contact them to try to promote them.
    if (prev_peer_state.last_received.index() == 0) {
      return;
    }

    int64_t last_batch_size = std::max<int64_t>(
        0, peer->last_received.index() - prev_peer_state.last_received.index());
    bool peer_caught_up =
        !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
        status.last_received_current_leader().index() + last_batch_size >=
            queue_state_.committed_index;
    if (!peer_caught_up) {
      return;
    }

    // TODO(mpercy): Implement a SafeToPromote() check to ensure that we only
    // try to promote a NON_VOTER to VOTER if we will be able to commit the
    // resulting config change operation.
    NotifyObserversOfPeerToPromote(peer->uuid());
  }
}

bool PeerMessageQueue::BasicChecksOKToTransferAndGetPeerUnlocked(
    const TrackedPeer& peer,
    RaftPeerPB** peer_pb_ptr) {
  DCHECK(queue_lock_.is_locked());

  // This check is redundant for ResponseFromPeer common path
  if (PREDICT_FALSE(queue_state_.state != kQueueOpen)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Queue is not open";
    return false;
  }

  // Only in LEADER mode can you transfer leadership
  if (queue_state_.mode != PeerMessageQueue::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer is not a leader, cannot transfer leadership";
    return false;
  }

  // Peer has to be healthily communicating to LEADER
  if (peer.last_exchange_status != PeerStatus::OK) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer does not have healthy communications with leader";
    return false;
  }

  Status s = GetRaftConfigMember(
      DCHECK_NOTNULL(queue_state_.active_config.get()),
      peer.uuid(),
      peer_pb_ptr);
  if (!s.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to get target peer " << peer.uuid() << ":" << s.ToString();
    return false;
  }

  if ((*peer_pb_ptr)->member_type() != RaftPeerPB::VOTER) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Target peer " << peer.uuid() << " is not a voter";
    return false;
  }

  return true;
}

bool PeerMessageQueue::PeerTransferLeadershipImmediatelyUnlocked(
    const std::string& peer_uuid) {
  DCHECK(queue_lock_.is_locked());
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  if (PREDICT_FALSE(peer == nullptr)) {
    return false;
  }

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(*peer, &peer_pb)) {
    return false;
  }

  // peer needs to be caught up so that if it runs an election,
  // it has the longest log and is ready to become LEADER
  // TODO - Verify if first check is redundant
  bool peer_caught_up = !OpIdEquals(peer->last_received, MinimumOpId()) &&
      OpIdEquals(peer->last_received, queue_state_.last_appended);
  if (peer_caught_up) {
    NotifyObserversOfSuccessor(peer_uuid);
  }
  return peer_caught_up;
}

MonoDelta PeerMessageQueue::LeaderLeaseTimeout() {
  int32_t const lease_timeout = FLAGS_raft_leader_lease_interval_ms;
  return MonoDelta::FromMilliseconds(lease_timeout);
}

MonoDelta PeerMessageQueue::BoundedDataLossDefaultWindowInMsec() {
  int32_t const bounded_data_loss_window_ms =
      FLAGS_bounded_dataloss_window_interval_ms;
  return MonoDelta::FromMilliseconds(bounded_data_loss_window_ms);
}

void PeerMessageQueue::SetPeerRpcStartTime(
    const std::string& peer_uuid,
    MonoTime rpc_start) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  if (PREDICT_FALSE(peer == nullptr)) {
    LOG(WARNING) << "Candidate peer " << peer_uuid
                 << " is not foung in Message Queue's Peers map";
    return;
  }

  if (peer != nullptr) {
    peer->rpc_start_ = rpc_start;
  }
}

void PeerMessageQueue::UpdatePeerRtt(
    const std::string& peer_uuid,
    MonoDelta rtt) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  if (PREDICT_FALSE(peer == nullptr)) {
    LOG(WARNING) << "Candidate peer " << peer_uuid
                 << " is not foung in Message Queue's Peers map";
    return;
  } else {
    routing_table_container_->UpdateRtt(
        peer_uuid, std::chrono::microseconds(rtt.ToMicroseconds()));
  }
}

void PeerMessageQueue::TransferLeadershipIfNeeded(
    const TrackedPeer& peer,
    const ConsensusStatusPB& status) {
  DCHECK(queue_lock_.is_locked());
  if (!successor_watch_in_progress_) {
    return;
  }

  if (designated_successor_uuid_ &&
      peer.uuid() != *designated_successor_uuid_) {
    return;
  }

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(peer, &peer_pb)) {
    return;
  }

  // check if this instance is filtered, if filter_fn has been provided
  if (!designated_successor_uuid_ && tl_filter_fn_ && tl_filter_fn_(*peer_pb)) {
    return;
  }

  // We want to make sure that we are not promoting to a region that doesn't
  // have a majority of nodes running otherwise, it won't be able to accept
  // writes. We do a quick local check to see if there are a quorum number of
  // nodes being tracked. It is not bulletproof since it doesn't actually
  // verify that the nodes are up and running but the common case is that
  // tracked nodes are up and running.
  if (FLAGS_filter_out_bad_quorums_in_lmp &&
      !RegionHasQuorumCommitUnlocked(*peer_pb)) {
    LOG(WARNING) << "Candidate peer " << peer_pb->permanent_uuid()
                 << " does not have majority voters running";
    return;
  }

  bool peer_caught_up =
      !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
      OpIdEquals(
          status.last_received_current_leader(), queue_state_.last_appended);
  if (!peer_caught_up) {
    return;
  }

  VLOG(1) << "Successor watch: peer " << peer.uuid() << " is caught up to "
          << "the leader at OpId "
          << OpIdToString(status.last_received_current_leader());
  successor_watch_in_progress_ = false;
  NotifyObserversOfSuccessor(peer.uuid());
}

bool PeerMessageQueue::ResponseFromPeer(
    const std::string& peer_uuid,
    const ConsensusResponsePB& response) {
  std::optional<int64_t> updated_commit_index;
  const bool ret =
      DoResponseFromPeer(peer_uuid, response, updated_commit_index);

  if (updated_commit_index) {
    NotifyObserversOfCommitIndexChange(*updated_commit_index);
  }

  return ret;
}

bool PeerMessageQueue::DoResponseFromPeer(
    const std::string& peer_uuid,
    const ConsensusResponsePB& response,
    std::optional<int64_t>& updated_commit_index) {
  DCHECK(response.IsInitialized())
      << "Error: Uninitialized: " << response.InitializationErrorString()
      << ". Response: " << SecureShortDebugString(response);

  bool send_more_immediately = false;
  Mode mode_copy;
  {
    std::lock_guard<simple_mutexlock> scoped_lock(queue_lock_);

    // TODO(mpercy): Handle response from proxy on behalf of another peer.
    // For now, we'll try to ignore proxying here, but we may need to
    // eventually handle that here for better health status and error logging.

    TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
    if (PREDICT_FALSE(queue_state_.state != kQueueOpen || peer == nullptr)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Queue is closed or peer was untracked, disregarding "
             "peer response. Response: "
          << SecureShortDebugString(response);
      return send_more_immediately;
    }

    // Sanity checks.
    // Some of these can be eventually removed, but they are handy for now.
    DCHECK(response.status().IsInitialized())
        << "Error: Uninitialized: " << response.InitializationErrorString()
        << ". Response: " << SecureShortDebugString(response);
    // TODO(mpercy): Include uuid in error messages as well.
    DCHECK(response.has_responder_uuid() && !response.responder_uuid().empty())
        << "Got response from peer with empty UUID";

    DCHECK(response.has_status()); // Responses should always have a status.
    // The status must always have a last received op id and a last committed
    // index.
    const ConsensusStatusPB& status = response.status();
    DCHECK(status.has_last_received());
    DCHECK(status.has_last_received_current_leader());
    DCHECK(status.has_last_committed_idx());

    // Take a snapshot of the previously-recorded peer state.
    const TrackedPeer prev_peer_state = *peer;

    // Update the peer's last exchange status based on the response.
    // In this case, if there is a log matching property (LMP) mismatch, we
    // want to immediately send another request as we attempt to sync the log
    // offset between the local leader and the remote peer.
    UpdateExchangeStatus(
        peer, prev_peer_state, response, &send_more_immediately);

    // If the reported last-received op for the replica is in our local log,
    // then resume sending entries from that point onward. Otherwise, resume
    // after the last op they received from us. If we've never successfully
    // sent them anything, start after the last-committed op in their log, which
    // is guaranteed by the Raft protocol to be a valid op.

    bool peer_has_prefix_of_log = IsOpInLog(status.last_received());
    if (peer_has_prefix_of_log) {
      // If the latest thing in their log is in our log, we are in sync.
      peer->last_received = status.last_received();
      peer->next_index = peer->last_received.index() + 1;

      // Check if the peer is a NON_VOTER candidate ready for promotion.
      PromoteIfNeeded(peer, prev_peer_state, status);

      TransferLeadershipIfNeeded(*peer, status);
    } else if (!OpIdEquals(
                   status.last_received_current_leader(), MinimumOpId())) {
      // Their log may have diverged from ours, however we are in the process
      // of replicating our ops to them, so continue doing so. Eventually, we
      // will cause the divergent entry in their log to be overwritten.
      peer->last_received = status.last_received_current_leader();
      peer->next_index = peer->last_received.index() + 1;

    } else {
      // The peer is divergent and they have not (successfully) received
      // anything from us yet. Start sending from their last committed index.
      // This logic differs from the Raft spec slightly because instead of
      // stepping back one-by-one from the end until we no longer have an LMP
      // error, we jump back to the last committed op indicated by the peer with
      // the hope that doing so will result in a faster catch-up process.
      DCHECK_GE(peer->last_known_committed_index, 0);
      peer->next_index = peer->last_known_committed_index + 1;
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Peer " << peer_uuid
          << " log is divergent from this leader: " << "its last log entry "
          << OpIdToString(status.last_received()) << " is not in "
          << "this leader's log and it has not received anything from this leader yet. "
          << "Falling back to committed index "
          << peer->last_known_committed_index;
    }

    if (peer->last_exchange_status != PeerStatus::OK) {
      // In this case, 'send_more_immediately' has already been set by
      // UpdateExchangeStatus() to true in the case of an LMP mismatch, false
      // otherwise.
      return send_more_immediately;
    }

    if (response.has_responder_term()) {
      // The peer must have responded with a term that is greater than or equal
      // to the last known term for that peer.
      peer->CheckMonotonicTerms(response.responder_term());

      // If the responder didn't send an error back that must mean that it has
      // a term that is the same or lower than ours.
      CHECK_LE(response.responder_term(), queue_state_.current_term);
    }

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Received Response from Peer (" << peer->ToString() << "). "
          << "Response: " << SecureShortDebugString(response);
    }

    if (peer->last_exchange_status == PeerStatus::OK) {
      if (FLAGS_enable_raft_leader_lease && response.has_lease_granted() &&
          response.lease_granted()) {
        peer->lease_granted = peer->last_received;
      }

      if (FLAGS_enable_bounded_dataloss_window) {
        peer->bounded_dataloss_window_acked = peer->last_received;
      }
    }

    mode_copy = queue_state_.mode;

    // If we're the leader, we can compute the new watermarks based on the
    // progress of our followers. NOTE: it's possible this node might have lost
    // its leadership (and the notification is just pending behind the lock
    // we're holding), but any future leader will observe the same watermarks
    // and make the same advancement, so this is safe.
    int64_t old_all_replicated_index = 0;
    int64_t new_all_replicated_index = 0;

    if (mode_copy == LEADER) {
      // Advance the majority replicated index.
      if (!FLAGS_enable_flexi_raft) {
        AdvanceQueueWatermark(
            "majority_replicated",
            &queue_state_.majority_replicated_index,
            /*replicated_before=*/prev_peer_state.last_received,
            /*replicated_after=*/peer->last_received,
            /*num_peers_required=*/queue_state_.majority_size_,
            VOTER_REPLICAS,
            peer);
      } else if (
          peer->last_received.index() >
              queue_state_.majority_replicated_index ||
          peer->last_exchange_status != PeerStatus::OK) {
        // This method is expensive. The 'watermark' can change only if this
        // peer's last received index is higer than the current
        // majority_replicated_index. We also call this method when the
        // last_exhange_status of the peer indicates an error. This is because
        // 'majority_replicated_index' can go down. It sould be safe to
        // completely skip calling this method when 'last_exhange_status' is an
        // error, but we do not want to introduce a behavior change at this
        // point. Check AdvanceQueueWatermark() for more comments
        AdvanceMajorityReplicatedWatermarkFlexiRaft(
            &queue_state_.majority_replicated_index,
            /*replicated_before=*/prev_peer_state.last_received,
            /*replicated_after=*/peer->last_received,
            peer);
      }

      old_all_replicated_index = queue_state_.all_replicated_index;

      // Advance the all replicated index.
      AdvanceQueueWatermark(
          "all_replicated",
          &queue_state_.all_replicated_index,
          /*replicated_before=*/prev_peer_state.last_received,
          /*replicated_after=*/peer->last_received,
          /*num_peers_required=*/peers_map_.size(),
          ALL_REPLICAS,
          peer);

      new_all_replicated_index = queue_state_.all_replicated_index;

      // If the majority-replicated index is in our current term,
      // and it is above our current committed index, then
      // we can advance the committed index.
      //
      // It would seem that the "it is above our current committed index"
      // check is redundant (and could be a CHECK), but in fact the
      // majority-replicated index can currently go down, since we don't
      // consider peers whose last contact was an error in the watermark
      // calculation. See the TODO in AdvanceQueueWatermark() for more details.
      int64_t commit_index_before = queue_state_.committed_index;
      if (queue_state_.first_index_in_current_term &&
          queue_state_.majority_replicated_index >=
              queue_state_.first_index_in_current_term &&
          queue_state_.majority_replicated_index >
              queue_state_.committed_index) {
        queue_state_.committed_index = queue_state_.majority_replicated_index;

        if (FLAGS_enable_raft_leader_lease && response.has_lease_granted()) {
          // Check for Quorum of lease renewal approvals from followers
          QuorumResults qresults;
          if (CanLeaderLeaseRenewUnlocked(qresults)) {
            leader_lease_until_.store(std::max(
                leader_lease_until_.load(),
                GetQuorumMajorityOfPeerRpcStarts(qresults) +
                    LeaderLeaseTimeout()));
          }
        }

        if (FLAGS_enable_bounded_dataloss_window) {
          // Check for Vote Quorum of Bounded DataLoss ACKs from followers
          QuorumResults qresults;
          if (CanBoundedDataLossWindowRenewUnlocked(qresults)) {
            bounded_dataloss_window_until_.store(std::max(
                bounded_dataloss_window_until_.load(),
                GetMaximumOfPeerRpcStarts(qresults) +
                    BoundedDataLossDefaultWindowInMsec()));
          }
        }
      } else {
        VLOG_WITH_PREFIX_UNLOCKED(2)
            << "Cannot advance commit index, waiting for > "
            << "first index in current leader term: "
            << queue_state_.first_index_in_current_term.value_or(-1) << ". "
            << "current majority_replicated_index: "
            << queue_state_.majority_replicated_index << ", "
            << "current committed_index: " << queue_state_.committed_index;
      }

      // Once the commit index has been updated, go ahead and update the
      // region_durable_index
      AdvanceQueueRegionDurableIndex();

      // Only notify observers if the commit index actually changed.
      if (mode_copy == LEADER &&
          queue_state_.committed_index != commit_index_before) {
        DCHECK_GT(queue_state_.committed_index, commit_index_before);
        updated_commit_index = queue_state_.committed_index;
        VLOG_WITH_PREFIX_UNLOCKED(2)
            << "Commit index advanced from " << commit_index_before << " to "
            << *updated_commit_index;
      }
    }

    // If the peer's committed index is lower than our own, or if our log has
    // the next request for the peer, set 'send_more_immediately' to true.
    send_more_immediately =
        peer->last_known_committed_index < queue_state_.committed_index ||
        log_cache_->HasOpBeenWritten(peer->next_index);

    // Evict ops from log_cache only if:
    // 1. This is not a leader node OR
    // 2. 'all_replicated_index' has changed after processing this response
    if (mode_copy != LEADER ||
        (old_all_replicated_index != new_all_replicated_index)) {
      log_cache_->EvictThroughOp(queue_state_.all_replicated_index);
    }

    UpdateMetricsUnlocked();
  }

  return send_more_immediately;
}

MonoTime PeerMessageQueue::GetQuorumMajorityOfPeerRpcStarts(
    QuorumResults& qresults) {
  MonoTime result = MonoTime::Min();
  std::vector<MonoTime> rpc_starts;
  rpc_starts.reserve(qresults.quorum_peers.size());
  for (const TrackedPeer* peer : qresults.quorum_peers) {
    rpc_starts.emplace_back(peer->rpc_start_);
  }

  // sort rpc_start times in descending order
  if (rpc_starts.size() > 0 && qresults.quorum_size > 1 &&
      rpc_starts.size() >= qresults.quorum_size - 1) {
    std::sort(rpc_starts.begin(), rpc_starts.end(), std::greater<>());
    result = rpc_starts
        [qresults.quorum_size - 1 - 1 /* Leader rpc_start does not exist */];
  } else {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to run GetQuorumMajorityOfPeerRpcStarts, "
        << "Quorum size: " << qresults.quorum_size << ". "
        << "Number of remote peers: " << rpc_starts.size() << ".";
  }
  return result;
}

MonoTime PeerMessageQueue::GetMaximumOfPeerRpcStarts(QuorumResults& qresults) {
  MonoTime result = MonoTime::Min();
  std::vector<MonoTime> rpc_starts;
  rpc_starts.reserve(qresults.quorum_peers.size());
  for (const TrackedPeer* peer : qresults.quorum_peers) {
    rpc_starts.emplace_back(peer->rpc_start_);
  }

  if (rpc_starts.size() > 0) {
    result = *std::max_element(rpc_starts.begin(), rpc_starts.end());
  } else {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to run GetMaximumOfPeerRpcStarts, "
        << "Number of remote peers: " << rpc_starts.size() << ".";
  }
  return result;
}

PeerMessageQueue::TrackedPeer PeerMessageQueue::GetTrackedPeerForTests(
    const string& uuid) {
  std::lock_guard<simple_mutexlock> scoped_lock(queue_lock_);
  TrackedPeer* tracked = FindOrDie(peers_map_, uuid);
  return *tracked;
}

PeerMessageQueue::TrackedPeer* PeerMessageQueue::GetTrackedPeerRefForTests(
    const std::string& uuid) {
  std::lock_guard<simple_mutexlock> scoped_lock(queue_lock_);
  return FindOrDie(peers_map_, uuid);
}

bool PeerMessageQueue::CanLeaderLeaseRenewUnlocked(QuorumResults& qresults) {
  DCHECK(queue_lock_.is_locked());
  string local_uuid = local_peer_pb_.permanent_uuid();
  auto results =
      IsQuorumSatisfiedUnlocked(local_peer_pb_, [this, &local_uuid](auto peer) {
        // Check for Leader
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid) {
          return true;
        }
        return peer->lease_granted.index() >= queue_state_.committed_index;
      });

  metrics_.available_leader_lease_grantors->set_value(results.num_satisfied);

  if (!results.quorum_satisfied) {
    LOG(WARNING) << "Lease granted quorum failed. " << results.quorum_size
                 << " is required lease grant quorum. " << results.num_satisfied
                 << " peers grants are healthy.";
    return false;
  }
  qresults = std::move(results);
  return true;
}

bool PeerMessageQueue::CanBoundedDataLossWindowRenewUnlocked(
    QuorumResults& qresults) {
  DCHECK(queue_lock_.is_locked());
  string local_uuid = local_peer_pb_.permanent_uuid();
  auto results =
      IsSecondRegionDurabilitySatisfiedUnlocked([this, &local_uuid](auto peer) {
        // Check for the Leader
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid) {
          return true;
        }
        return peer->bounded_dataloss_window_acked.index() >=
            queue_state_.committed_index;
      });

  metrics_.available_bounded_dataloss_window_ackers->set_value(
      results.num_satisfied);

  if (!results.quorum_satisfied) {
    LOG(WARNING) << "Bounded Data Loss window lease granted, quorum failed. "
                 << results.quorum_size << " is required lease grant quorum. "
                 << results.num_satisfied << " peers grants are healthy.";
    return false;
  }
  qresults = std::move(results);
  return true;
}

int64_t PeerMessageQueue::GetAllReplicatedIndex() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.all_replicated_index;
}

int64_t PeerMessageQueue::GetCommittedIndex() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.committed_index;
}

int64_t PeerMessageQueue::GetRegionDurableIndex() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.region_durable_index;
}

bool PeerMessageQueue::IsCommittedIndexInCurrentTerm() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.first_index_in_current_term.has_value() &&
      queue_state_.committed_index >= *queue_state_.first_index_in_current_term;
}

bool PeerMessageQueue::IsInLeaderMode() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.mode == Mode::LEADER;
}

int64_t PeerMessageQueue::GetMajorityReplicatedIndexForTests() const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return queue_state_.majority_replicated_index;
}

void PeerMessageQueue::UpdateMetricsUnlocked() {
  DCHECK(queue_lock_.is_locked());
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  // For non-leaders, majority_done_ops isn't meaningful because followers don't
  // track when an op is replicated to all peers.
  metrics_.num_majority_done_ops->set_value(
      queue_state_.mode == LEADER
          ? queue_state_.committed_index - queue_state_.all_replicated_index
          : 0);
  metrics_.num_in_progress_ops->set_value(
      queue_state_.last_appended.index() - queue_state_.committed_index);

  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdateLagMetricsUnlocked() {
  DCHECK(queue_lock_.is_locked());
  metrics_.num_ops_behind_leader->set_value(
      queue_state_.mode == LEADER ? 0
                                  : queue_state_.last_idx_appended_to_leader -
              queue_state_.last_appended.index());
}

void PeerMessageQueue::DumpToStrings(vector<string>* lines) const {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  DumpToStringsUnlocked(lines);
}

void PeerMessageQueue::DumpToStringsUnlocked(vector<string>* lines) const {
  DCHECK(queue_lock_.is_locked());
  lines->push_back("Watermarks:");
  for (const PeersMap::value_type& entry : peers_map_) {
    lines->push_back(Substitute(
        "Peer: $0 Watermark: $1", entry.first, entry.second->ToString()));
  }

  log_cache_->DumpToStrings(lines);
}

void PeerMessageQueue::ClearUnlocked() {
  DCHECK(queue_lock_.is_locked());
  STLDeleteValues(&peers_map_);
  queue_state_.state = kQueueClosed;
}

void PeerMessageQueue::Close() {
  raft_pool_observers_token_->Shutdown();

  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  ClearUnlocked();
  // Reset here to appease folly::Singleton's check for leaky references
  time_provider_.reset();
}

int64_t PeerMessageQueue::GetQueuedOperationsSizeBytesForTests() const {
  return log_cache_->BytesUsed();
}

string PeerMessageQueue::ToString() const {
  // Even though metrics are thread-safe obtain the lock so that we get
  // a "consistent" snapshot of the metrics.
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  return ToStringUnlocked();
}

string PeerMessageQueue::ToStringUnlocked() const {
  DCHECK(queue_lock_.is_locked());
  return Substitute(
      "Consensus queue metrics: "
      "Only Majority Done Ops: $0, In Progress Ops: $1, Cache: $2",
      metrics_.num_majority_done_ops->value(),
      metrics_.num_in_progress_ops->value(),
      log_cache_->StatsString());
}

void PeerMessageQueue::RegisterObserver(PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    observers_.push_back(observer);
  }
}

Status PeerMessageQueue::UnRegisterObserver(
    PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    return Status::NotFound("Can't find observer.");
  }
  observers_.erase(iter);
  return Status::OK();
}

bool PeerMessageQueue::IsOpInLog(const OpId& desired_op) const {
  OpId log_op;
  Status s = log_cache_->LookupOpId(desired_op.index(), &log_op);
  if (PREDICT_TRUE(s.ok())) {
    return OpIdEquals(desired_op, log_op);
  }
  if (PREDICT_TRUE(s.IsNotFound() || s.IsIncomplete())) {
    return false;
  }
  LOG_WITH_PREFIX_UNLOCKED(FATAL)
      << "Error while reading the log: " << s.ToString();
  return false; // Unreachable; here to squelch GCC warning.
}

void PeerMessageQueue::NotifyObserversOfCommitIndexChange(
    int64_t new_commit_index,
    bool need_lock) {
  if (!FLAGS_async_notify_commit_index) {
    NotifyObserversTask([=](PeerMessageQueueObserver* observer) {
      observer->NotifyCommitIndex(new_commit_index, need_lock);
    });
    return;
  }
  // NOTE: if we're scheduling this to run async we always need to lock, so we
  // ignore the needs_lock param
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->NotifyCommitIndex(new_commit_index, true);
          })),
      LogPrefixUnlocked() +
          "Unable to notify RaftConsensus of commit index change.");
}

void PeerMessageQueue::NotifyObserversOfTermChange(int64_t term) {
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->NotifyTermChange(term);
          })),
      LogPrefixUnlocked() + "Unable to notify RaftConsensus of term change.");
}

void PeerMessageQueue::NotifyObserversOfFailedFollower(
    const string& uuid,
    int64_t term,
    const string& reason) {
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->NotifyFailedFollower(uuid, term, reason);
          })),
      LogPrefixUnlocked() +
          "Unable to notify RaftConsensus of abandoned follower.");
}

void PeerMessageQueue::NotifyObserversOfPeerToPromote(const string& peer_uuid) {
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->NotifyPeerToPromote(peer_uuid);
          })),
      LogPrefixUnlocked() +
          "Unable to notify RaftConsensus of peer to promote.");
}

void PeerMessageQueue::NotifyObserversOfSuccessor(const string& peer_uuid) {
  DCHECK(queue_lock_.is_locked());
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [=, transfer_context = std::move(transfer_context_)](
              PeerMessageQueueObserver* observer) mutable {
            observer->NotifyPeerToStartElection(
                peer_uuid,
                std::move(transfer_context),
                /*promise=*/nullptr,
                /*mock_election_snapshot_op_id=*/std::nullopt);
          })),
      LogPrefixUnlocked() +
          "Unable to notify RaftConsensus of available successor.");
  successor_watch_peer_notified_ = true;
  transfer_context_ = {};
}

Status PeerMessageQueue::GetSnapshotForMockElection(
    const std::string& new_leader_uuid,
    OpId* snapshot_op_id) {
  std::unique_lock<simple_mutexlock> l(queue_lock_);

  TrackedPeer* peer = FindPtrOrNull(peers_map_, new_leader_uuid);
  if (PREDICT_FALSE(peer == nullptr)) {
    return Status::IllegalState("Target peer is not tracked.");
  }

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(*peer, &peer_pb)) {
    return Status::IllegalState("Failed basic leadership transfer checks.");
  }

  *snapshot_op_id = queue_state_.last_appended;

  return Status::OK();
}

void PeerMessageQueue::NotifyObserversOfPeerHealthChange() {
  WARN_NOT_OK(
      raft_pool_observers_token_->SubmitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          Unretained(this),
          [](PeerMessageQueueObserver* observer) {
            observer->NotifyPeerHealthChange();
          })),
      LogPrefixUnlocked() +
          "Unable to notify RaftConsensus peer health change.");
}

void PeerMessageQueue::NotifyObserversTask(
    const std::function<void(PeerMessageQueueObserver*)>& func) {
  MAYBE_INJECT_RANDOM_LATENCY(
      FLAGS_consensus_inject_latency_ms_in_notifications);
  std::vector<PeerMessageQueueObserver*> observers_copy;
  {
    std::lock_guard<simple_mutexlock> lock(queue_lock_);
    observers_copy = observers_;
  }
  for (PeerMessageQueueObserver* observer : observers_copy) {
    func(observer);
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

string PeerMessageQueue::LogPrefixUnlocked() const {
  // TODO: we should probably use an atomic here. We'll just annotate
  // away the TSAN error for now, since the worst case is a slightly out-of-date
  // log message, and not very likely.
  Mode mode = KUDU_ANNONTATE_UNPROTECTED_READ(queue_state_.mode);
  return Substitute(
      "T $0 P $1 [$2]: ",
      tablet_id_,
      local_peer_pb_.permanent_uuid(),
      mode == LEADER ? "LEADER" : "NON_LEADER");
}

string PeerMessageQueue::QueueState::ToString() const {
  return Substitute(
      "All replicated index: $0, Majority replicated index: $1, "
      "Committed index: $2, Last appended: $3, Last appended by leader: $4, Current term: $5, "
      "Majority size: $6, State: $7, Mode: $8$9",
      all_replicated_index,
      majority_replicated_index,
      committed_index,
      OpIdToString(last_appended),
      last_idx_appended_to_leader,
      current_term,
      majority_size_,
      state,
      (mode == LEADER ? "LEADER" : "NON_LEADER"),
      active_config
          ? ", active raft config: " + SecureShortDebugString(*active_config)
          : "");
}

const std::string& PeerMessageQueue::getQuorumIdUsingCommitRule(
    const RaftPeerPB& peer) const {
  return GetQuorumId(peer, queue_state_.active_config->commit_rule());
}

void PeerMessageQueue::UpdatePeerQuorumIdUnlocked(
    const std::map<std::string, std::string>& quorum_id_map) {
  // Update local peer's quorum id
  auto it = quorum_id_map.find(local_peer_pb_.permanent_uuid());
  if (it != quorum_id_map.end()) {
    local_peer_pb_.mutable_attrs()->set_quorum_id(it->second);
  }
  // Update quorum_ids in peers_map
  for (const PeersMap::value_type& entry : peers_map_) {
    auto it = quorum_id_map.find(entry.first);
    if (it != quorum_id_map.end()) {
      entry.second->peer_pb.mutable_attrs()->set_quorum_id(it->second);
    }
    entry.second->PopulateIsPeerInLocalQuorum();
  }
}

bool PeerMessageQueue::CheckQuorum() {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);

  // We only check quorum if we're a leader.
  if (queue_state_.mode != LEADER) {
    return true;
  }

  // We only support check quorum for Single Region Dynamic for now.
  if (queue_state_.active_config->commit_rule().mode() !=
      QuorumMode::SINGLE_REGION_DYNAMIC) {
    return true;
  }

  metrics_.check_quorum_runs->Increment();

  vector<string> unhealthy_peers;
  string local_uuid = local_peer_pb_.permanent_uuid();
  QuorumResults results = IsQuorumSatisfiedUnlocked(
      local_peer_pb_, [&local_uuid, &unhealthy_peers](auto peer) {
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid || peer->is_healthy()) {
          return true;
        }
        unhealthy_peers.push_back(peer_uuid);
        return false;
      });

  metrics_.available_commit_peers->set_value(results.num_satisfied);

  if (!results.quorum_satisfied) {
    metrics_.check_quorum_failures->Increment();
    LOG(WARNING) << "Check quorum failed. " << results.quorum_size
                 << " is required commit quorum. " << results.num_satisfied
                 << " peers are healthy. " << unhealthy_peers.size()
                 << " peers have failed: "
                 << JoinStrings(unhealthy_peers, ", ");
  }
  return results.quorum_satisfied;
}

void PeerMessageQueue::UpdatePeerForTests(
    const std::string& peer_uuid,
    const std::function<void(TrackedPeer*)>& fn) {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);
  TrackedPeer* peer = FindPtrOrNull(peers_map_, peer_uuid);
  CHECK(peer);
  fn(peer);
}

bool PeerMessageQueue::RegionHasQuorumCommitUnlocked(
    const RaftPeerPB& target_peer) {
  DCHECK(queue_lock_.is_locked());

  // If we don't have VD, we assume this is vanilla raft and we don't need to
  // check if peer region has majority.
  if (!FLAGS_enable_flexi_raft) {
    return true;
  }

  QuorumResults results = IsQuorumSatisfiedUnlocked(
      target_peer, [](auto peer) { return peer->is_healthy(); });
  return results.quorum_satisfied;
}

int32_t PeerMessageQueue::GetAvailableCommitPeers() {
  std::lock_guard<simple_mutexlock> lock(queue_lock_);

  // If we are not leader, we are not concerned about commit peers, hence, we
  // simply return 0.
  if (queue_state_.mode != LEADER) {
    return -1;
  }

  // We only support getting available commit peers for Single Region Dynamic
  // for now.
  if (queue_state_.active_config->commit_rule().mode() !=
      QuorumMode::SINGLE_REGION_DYNAMIC) {
    return -1;
  }

  QuorumResults results = IsQuorumSatisfiedUnlocked(
      local_peer_pb_, [](auto peer) { return peer->is_healthy(); });
  return results.num_satisfied;
}

Status PeerMessageQueue::GetQuorumHealthForFlexiRaftUnlocked(
    QuorumHealth* health) {
  CHECK(health);
  DCHECK(queue_lock_.is_locked());
  std::unordered_multimap<std::string, TrackedPeer*> by_quorum_id;
  std::unordered_set<std::string> quorum_ids;

  for (const PeersMap::value_type& entry : peers_map_) {
    auto* peer = entry.second;
    // We only include voters.
    if (peer->peer_pb.has_member_type() &&
        peer->peer_pb.member_type() == RaftPeerPB::VOTER) {
      const std::string quorum_id = getQuorumIdUsingCommitRule(peer->peer_pb);
      by_quorum_id.insert(std::make_pair(quorum_id, peer));
      quorum_ids.insert(quorum_id);
    }
  }

  const std::string& leader_quorum_id =
      getQuorumIdUsingCommitRule(local_peer_pb_);

  for (const auto& quorum_id : quorum_ids) {
    QuorumIdHealth quorum_id_health;

    quorum_id_health.primary = leader_quorum_id == quorum_id;

    quorum_id_health.num_vd_voters =
        GetTotalVotersFromVoterDistribution(
            *(queue_state_.active_config), quorum_id)
            .value_or(0);
    quorum_id_health.quorum_size = MajoritySize(quorum_id_health.num_vd_voters);

    auto range = by_quorum_id.equal_range(quorum_id);
    for (auto it = range.first; it != range.second; it++) {
      auto* peer = it->second;
      if (peer->is_healthy()) {
        quorum_id_health.healthy_peers.push_back(peer->peer_pb);
      } else {
        quorum_id_health.unhealthy_peers.push_back(peer->peer_pb);
      }
    }

    const int num_healthy = quorum_id_health.num_vd_voters -
        static_cast<int>(quorum_id_health.unhealthy_peers.size());
    if (num_healthy < quorum_id_health.quorum_size) {
      quorum_id_health.health_status = UNHEALTHY;
    } else if (num_healthy == quorum_id_health.quorum_size) {
      quorum_id_health.health_status = AT_RISK;
    } else if (num_healthy >= quorum_id_health.num_vd_voters) {
      quorum_id_health.health_status = HEALTHY;
    } else {
      quorum_id_health.health_status = DEGRADED;
    }

    quorum_id_health.total_voters = static_cast<int>(
        quorum_id_health.healthy_peers.size() +
        quorum_id_health.unhealthy_peers.size());
    health->by_quorum_id.emplace(quorum_id, std::move(quorum_id_health));
  }
  return Status::OK();
}

Status PeerMessageQueue::GetQuorumHealthForVanillaRaftUnlocked(
    QuorumHealth* health) {
  CHECK(health);
  DCHECK(queue_lock_.is_locked());
  QuorumIdHealth quorum_id_health;

  // There's only one region, so it is deemed to be the primary.
  quorum_id_health.primary = true;

  for (const PeersMap::value_type& entry : peers_map_) {
    auto* peer = entry.second;
    // We only include voters.
    if (peer->peer_pb.has_member_type() &&
        peer->peer_pb.member_type() == RaftPeerPB::VOTER) {
      if (peer->is_healthy()) {
        quorum_id_health.healthy_peers.push_back(peer->peer_pb);
      } else {
        quorum_id_health.unhealthy_peers.push_back(peer->peer_pb);
      }
    }
  }

  quorum_id_health.total_voters = static_cast<int>(
      quorum_id_health.healthy_peers.size() +
      quorum_id_health.unhealthy_peers.size());
  // We don't use VD in vanilla raft. If we did, it would be equal to
  // total_voters.
  quorum_id_health.num_vd_voters = quorum_id_health.total_voters;
  quorum_id_health.quorum_size = MajoritySize(quorum_id_health.total_voters);

  const size_t num_healthy = quorum_id_health.healthy_peers.size();
  if (num_healthy < quorum_id_health.quorum_size) {
    quorum_id_health.health_status = UNHEALTHY;
  } else if (num_healthy == quorum_id_health.quorum_size) {
    quorum_id_health.health_status = AT_RISK;
  } else if (num_healthy == quorum_id_health.total_voters) {
    quorum_id_health.health_status = HEALTHY;
  } else {
    quorum_id_health.health_status = DEGRADED;
  }

  health->by_quorum_id.emplace(
      kVanillaRaftQuorumId, std::move(quorum_id_health));
  return Status::OK();
}

Status PeerMessageQueue::GetQuorumHealth(QuorumHealth* health) {
  CHECK(health);
  std::lock_guard<simple_mutexlock> lock(queue_lock_);

  // Only leaders can provide quorum health.
  if (queue_state_.mode != LEADER) {
    return Status::OK();
  }

  if (FLAGS_enable_flexi_raft) {
    return GetQuorumHealthForFlexiRaftUnlocked(health);
  }
  return GetQuorumHealthForVanillaRaftUnlocked(health);
}

} // namespace kudu::consensus
