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
#include <range/v3/view/concat.hpp>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
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
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/DCHECKProd.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/threadpool.h"

constexpr int kLmpMismatchLogFrequency = 360;

DEFINE_int32(
    consecutive_failure_backoff_threshold,
    5,
    "Number of consecutive failures from a peer before backing off from "
    "sending more requests immediately. Once the threshold is reached, "
    "the leader will only send empty heartbeats until the situation recovers.");
TAG_FLAG(consecutive_failure_backoff_threshold, advanced);
TAG_FLAG(consecutive_failure_backoff_threshold, runtime);

DEFINE_int32(
    consensus_max_batch_size_bytes,
    1024 * 1024,
    "The maximum per-tablet RPC batch size when updating peers.");
TAG_FLAG(consensus_max_batch_size_bytes, advanced);

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

DEFINE_uint32(
    candidate_max_seconds_behind_master_threshold,
    0,
    "Do not transfer leader to the candidate if SBM is larger than the threshold. "
    "Setting to 0 to skip the check.");

DEFINE_bool(
    warm_storage_reads_for_replication,
    false,
    "Whether to enable reading from Warm Storage when logs are not found "
    "locally)");

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

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

const char* peerStatusToString(PeerStatus p) {
  switch (p) {
    case PeerStatus::Ok:
      return "OK";
    case PeerStatus::RemoteError:
      return "REMOTE_ERROR";
    case PeerStatus::RpcLayerError:
      return "RPC_LAYER_ERROR";
    case PeerStatus::TabletFailed:
      return "TABLET_FAILED";
    case PeerStatus::TabletNotFound:
      return "TABLET_NOT_FOUND";
    case PeerStatus::InvalidTerm:
      return "INVALID_TERM";
    case PeerStatus::LmpMismatch:
      return "LMP_MISMATCH";
    case PeerStatus::CannotPrepare:
      return "CANNOT_PREPARE";
    case PeerStatus::New:
      return "NEW";
  }
  DCHECK(false);
  return "<unknown>";
}

PeerMessageQueue::TrackedPeer::TrackedPeer(
    RaftPeerPB peer_pb,
    const PeerMessageQueue* queue)
    : peerPb(std::move(peer_pb)),
      nextIndex(kInvalidOpIdIndex),
      lastReceived(MinimumOpId()),
      lastKnownCommittedIndex(MinimumOpId().index()),
      lastExchangeStatus(PeerStatus::New),
      leaseGranted(MinimumOpId()),
      boundedDatalossWindowAcked(MinimumOpId()),
      rpcStart(MonoTime::Min()),
      walCatchupPossible(true),
      lastOverallHealthStatus(HealthReportPB::UNKNOWN),
      statusLogThrottler(std::make_shared<logging::LogThrottler>()),
      lastSeenTerm_(0),
      // We initialize to max to ensure that a peer, that was never
      // successfully contacted, is considered unhealthy.
      consecutiveFailures_(INT_MAX),
      proxyingDisabledUntil_(MonoTime::Min()),
      timeProvider_(TimeProvider::getInstance()),
      queue(queue) {
  lastSuccessfulExchange = timeProvider_->Now();
  lastCommunicationTime = timeProvider_->Now();
  populateIsPeerInLocalQuorum();
  populateIsPeerInLocalRegion();
}

void PeerMessageQueue::TrackedPeer::populateIsPeerInLocalQuorum() {
  isPeerInLocalQuorum.reset();

  const RaftPeerPB& localPeerPb = queue->localPeerPb_;

  if (peerPb.permanent_uuid() == localPeerPb.permanent_uuid()) {
    isPeerInLocalQuorum = true;
    return;
  }

  if (!peerPb.has_attrs() || !localPeerPb.has_attrs()) {
    return;
  }

  const std::string& localPeerQuorumId =
      queue->getQuorumIdUsingCommitRule(localPeerPb);
  const std::string& peerQuorumId = queue->getQuorumIdUsingCommitRule(peerPb);
  if (!localPeerQuorumId.empty() && !peerQuorumId.empty()) {
    isPeerInLocalQuorum = (localPeerQuorumId == peerQuorumId);
  }
}

void PeerMessageQueue::TrackedPeer::populateIsPeerInLocalRegion() {
  isPeerInLocalRegion.reset();

  const RaftPeerPB& localPeerPb = queue->localPeerPb_;

  if (peerPb.permanent_uuid() == localPeerPb.permanent_uuid()) {
    isPeerInLocalRegion = true;
    return;
  }

  if (!peerPb.attrs().has_region() || !localPeerPb.attrs().has_region()) {
    return;
  }

  const std::string& peerRegion = peerPb.attrs().region();
  const std::string& localPeerRegion = localPeerPb.attrs().region();
  if (!localPeerRegion.empty() && !peerRegion.empty()) {
    isPeerInLocalRegion = (localPeerRegion == peerRegion);
  }
}

bool PeerMessageQueue::TrackedPeer::isHealthy() const {
  return consecutiveFailures_ < FLAGS_unhealthy_threshold;
}

int32_t PeerMessageQueue::TrackedPeer::consecutiveFailures() const {
  return consecutiveFailures_;
}

void PeerMessageQueue::TrackedPeer::incrConsecutiveFailures() {
  // avoid overflow
  if (consecutiveFailures_ != INT_MAX) {
    consecutiveFailures_++;
  }
}

void PeerMessageQueue::TrackedPeer::resetConsecutiveFailures() {
  consecutiveFailures_ = 0;
}

void PeerMessageQueue::TrackedPeer::setConsecutiveFailures(int32_t value) {
  consecutiveFailures_ = value;
}

bool PeerMessageQueue::TrackedPeer::proxyTargetEnabled() const {
  return timeProvider_->Now() >= proxyingDisabledUntil_;
}

void PeerMessageQueue::TrackedPeer::snoozeProxying(MonoDelta delta) {
  proxyingDisabledUntil_ = timeProvider_->Now() + delta;
}

std::string PeerMessageQueue::TrackedPeer::ToString() const {
  return fmt::format(
      "Peer: {}, Status: {}, Last received: {}, Next index: {}, "
      "Last known committed idx: {}, Time since last communication: {}",
      SecureShortDebugString(peerPb),
      peerStatusToString(lastExchangeStatus),
      OpIdToString(lastReceived),
      nextIndex,
      lastKnownCommittedIndex,
      (MonoTime::Now() - lastCommunicationTime).ToString());
}

#define INSTANTIATE_METRIC(x) x.instantiate(metric_entity, 0)
PeerMessageQueue::Metrics::Metrics(
    const std::shared_ptr<MetricEntity>& metric_entity)
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
      metric_entity->findOrCreateCounter(&METRIC_check_quorum_runs);
  check_quorum_failures =
      metric_entity->findOrCreateCounter(&METRIC_check_quorum_failures);
  corruption_cache_drops =
      metric_entity->findOrCreateCounter(&METRIC_corruption_cache_drops);
  single_corruption_cache_drops =
      metric_entity->findOrCreateCounter(&METRIC_single_corruption_cache_drops);
}
#undef INSTANTIATE_METRIC

const std::string PeerMessageQueue::kVanillaRaftQuorumId = "__default__";

PeerMessageQueue::PeerMessageQueue(
    const std::shared_ptr<MetricEntity>& metric_entity,
    std::shared_ptr<log::Log> log,
    std::shared_ptr<ITimeManager> time_manager,
    const std::shared_ptr<PersistentVarsManager>& persistentVarsManager,
    RaftPeerPB local_peer_pb,
    std::shared_ptr<RoutingTableContainer> routing_table_container,
    string tablet_id,
    unique_ptr<ThreadPoolToken> raft_pool_observers_token,
    OpId last_locally_replicated,
    const OpId& last_locally_committed)
    : raftPoolObserversToken_(std::move(raft_pool_observers_token)),
      localPeerPb_(std::move(local_peer_pb)),
      routingTableContainer_(std::move(routing_table_container)),
      tabletId_(std::move(tablet_id)),
      adjustVoterDistribution_(true),
      successorWatchInProgress_(false),
      log_cache_(
          std::make_shared<LogCache>(
              metric_entity,
              std::move(log),
              localPeerPb_.permanent_uuid(),
              tabletId_)),
      metrics_(metric_entity),
      timeManager_(std::move(time_manager)),
      leaderLeaseUntil_(MonoTime::Min()),
      boundedDatalossWindowUntil_(MonoTime::Min()),
      timeProvider_(TimeProvider::getInstance()) {
  DCHECK(localPeerPb_.has_permanent_uuid());
  DCHECK(localPeerPb_.has_last_known_addr());
  DCHECK(last_locally_replicated.IsInitialized());
  DCHECK(last_locally_committed.IsInitialized());
  queueState_.current_term = 0;
  queueState_.first_index_in_current_term = {};
  queueState_.committed_index = 0;
  queueState_.all_replicated_index = 0;
  queueState_.majority_replicated_index = 0;
  queueState_.region_durable_index = 0;
  queueState_.last_idx_appended_to_leader = 0;
  queueState_.mode = NON_LEADER;
  queueState_.majority_size_ = -1;
  queueState_.last_appended = std::move(last_locally_replicated);
  queueState_.committed_index = last_locally_committed.index();
  queueState_.state = kQueueOpen;
  // TODO(mpercy): Merge LogCache::init() with its constructor.
  log_cache_->init(queueState_.last_appended);

  CHECK_OK(
      persistentVarsManager->loadPersistentVars(tabletId_, &persistentVars_));
}

void PeerMessageQueue::SetProxyFailureThreshold(
    int32_t proxyFailureThresholdMs) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  proxyFailureThresholdMs_ = proxyFailureThresholdMs;
}

void PeerMessageQueue::SetProxyFailureThresholdLag(
    int32_t proxyFailureThresholdLag) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  proxyFailureThresholdLag_ = proxyFailureThresholdLag;
}

bool PeerMessageQueue::HasProxyPeerFailedUnlocked(
    const TrackedPeer* proxyPeer,
    const TrackedPeer* destPeer) {
  auto maxProxyFailureThreshold =
      MonoDelta::FromMilliseconds(proxyFailureThresholdMs_);

  if (timeProvider_->Now() - proxyPeer->lastSuccessfulExchange >
      maxProxyFailureThreshold) {
    KLOG_EVERY_N_SECS(INFO, 180)
        << "Peer " << proxyPeer->uuid()
        << " did not complete a successful exchange after "
        << proxyFailureThresholdMs_ << "ms. Will not use as a proxy.";

    // The leader has not communicated with proxyPeer within the
    // proxy_failure_threshold_ms. Hence this peer cannot act as a 'proxy peer'
    // and is considered failed
    return true;
  }

  bool isProxyLagging = (destPeer->nextIndex > proxyPeer->nextIndex) &&
      ((destPeer->nextIndex - proxyPeer->nextIndex) >
       proxyFailureThresholdLag_);

  if (isProxyLagging) {
    // The proxy peer is lagging farther than the destination peer. Hence it
    // cannot act as a proxy for the destination peer
    return true;
  }

  return false;
}

Status PeerMessageQueue::SetCompressionDictionary(const std::string& dict) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  RETURN_NOT_OK(log_cache()->clear());
  RETURN_NOT_OK(CompressionCodecManager::setDictionary(dict));
  for (const PeersMap::value_type& entry : peersMap_) {
    entry.second->shouldSendCompressionDict = true;
  }
  return Status::OK();
}

void PeerMessageQueue::setLeaderMode(
    int64_t committed_index,
    int64_t current_term,
    const RaftConfigPB& active_config) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  if (current_term != queueState_.current_term) {
    CHECK_GT(current_term, queueState_.current_term)
        << "Terms should only increase";
    queueState_.first_index_in_current_term = {};
    queueState_.current_term = current_term;
  }

  queueState_.committed_index = committed_index;
  queueState_.majority_replicated_index = committed_index;
  queueState_.active_config.reset(new RaftConfigPB(active_config));
  queueState_.majority_size_ =
      majoritySize(countVoters(*queueState_.active_config));
  queueState_.mode = LEADER;

  trackLocalPeerUnlocked();
  CheckPeersInActiveConfigIfLeaderUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Queue going to LEADER mode. State: " << queueState_.ToString();

  timeManager_->setLeaderMode();
}

void PeerMessageQueue::setNonLeaderMode(const RaftConfigPB& active_config) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  queueState_.active_config.reset(new RaftConfigPB(active_config));
  queueState_.mode = NON_LEADER;
  queueState_.majority_size_ = -1;

  // Update this when stepping down, since it doesn't get tracked as LEADER.
  queueState_.last_idx_appended_to_leader = queueState_.last_appended.index();

  trackLocalPeerUnlocked();

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Queue going to NON_LEADER mode. State: " << queueState_.ToString();

  timeManager_->setNonLeaderMode();
}

void PeerMessageQueue::trackPeer(const RaftPeerPB& peer_pb) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  trackPeerUnlocked(peer_pb);
}

void PeerMessageQueue::trackPeerUnlocked(
    const RaftPeerPB& peer_pb,
    bool is_local_peer) {
  CHECK(!peer_pb.permanent_uuid().empty()) << SecureShortDebugString(peer_pb);
  CHECK(peer_pb.has_member_type()) << SecureShortDebugString(peer_pb);
  DCHECK(queueLock_.is_locked());
  DCHECK_EQ(queueState_.state, kQueueOpen);

  TrackedPeer* tracked_peer = new TrackedPeer(peer_pb, this);
  // Ensure we never insert nullptr into peersMap_ to prevent crashes when
  // migrating from FindPtrOrNull (which treats missing keys and nullptr values
  // identically) to standard map.find() + nullptr checks.
  DCHECK(tracked_peer != nullptr)
      << "Attempting to insert nullptr into peersMap_";
  // We don't know the last operation received by the peer so, following the
  // Raft protocol, we set nextIndex to one past the end of our own log. This
  // way, if calling this method is the result of a successful leader election
  // and the logs between the new leader and remote peer match, the
  // peer->nextIndex will point to the index of the soon-to-be-written NO_OP
  // entry that is used to assert leadership. If we guessed wrong, and the peer
  // does not have a log that matches ours, the normal queue negotiation
  // process will eventually find the right point to resume from.
  tracked_peer->nextIndex = queueState_.last_appended.index() + 1;

  if (is_local_peer) {
    tracked_peer->resetConsecutiveFailures();
  }

  auto [it, inserted] = peersMap_.insert({tracked_peer->uuid(), tracked_peer});
  DCHECK(inserted) << "Peer already exists: " << tracked_peer->uuid();

  CheckPeersInActiveConfigIfLeaderUnlocked();

  // We don't know how far back this peer is, so set the all replicated
  // watermark to 0. We'll advance it when we know how far along the peer is.
  queueState_.all_replicated_index = 0;
}

void PeerMessageQueue::untrackPeer(const string& uuid) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  untrackPeerUnlocked(uuid);
}

void PeerMessageQueue::untrackPeerUnlocked(const string& uuid) {
  DCHECK(queueLock_.is_locked());
  auto it = peersMap_.find(uuid);
  TrackedPeer* peer = nullptr;
  if (it != peersMap_.end()) {
    peer = it->second;
    peersMap_.erase(it);
  }
  delete peer; // Deleting a nullptr is safe.
}

void PeerMessageQueue::trackLocalPeerUnlocked() {
  DCHECK(queueLock_.is_locked());
  RaftPeerPB* localPeerInConfig;
  Status s = getRaftConfigMember(
      queueState_.active_config.get(),
      localPeerPb_.permanent_uuid(),
      &localPeerInConfig);
  auto localCopy = localPeerPb_;
  if (!s.ok()) {
    // The local peer is not a member of the config. The queue requires the
    // 'member_type' field to be set for any tracked peer, so we explicitly
    // mark the local peer as a NON_VOTER. This case is only possible when the
    // local peer is not the leader, so the choice is not particularly
    // important, but NON_VOTER is the most reasonable option.
    localCopy.set_member_type(RaftPeerPB::NON_VOTER);
    localPeerInConfig = &localCopy;
  }
  // TODO (T172552337) Unify localPeerPb_ in raft_consensus, consensus_peer,
  // and consensus_queue. Right now there are multiple copies of localPeerPb,
  // which can easily diverge and cause problems.
  localPeerPb_ = *localPeerInConfig;
  K_CHECK(
      localPeerInConfig->member_type() == RaftPeerPB::VOTER ||
          queueState_.mode != LEADER,
      non_voter_in_consensus_queue,
      "Local peer {} is not a voter in config: {}",
      localPeerPb_.permanent_uuid(),
      queueState_.ToString());
  if (peersMap_.contains(localPeerPb_.permanent_uuid())) {
    untrackPeerUnlocked(localPeerPb_.permanent_uuid());
  }
  trackPeerUnlocked(*localPeerInConfig, /*is_local_peer=*/true);
}

unordered_map<string, HealthReportPB> PeerMessageQueue::reportHealthOfPeers()
    const {
  unordered_map<string, HealthReportPB> reports;
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  for (const auto& entry : peersMap_) {
    const string& peerUuid = entry.first;
    const TrackedPeer* peer = entry.second;
    HealthReportPB report;
    auto overallHealth = peer->lastOverallHealthStatus;
    // We always consider the local peer (ourselves) to be healthy.
    // TODO(mpercy): Is this always a safe assumption?
    if (peerUuid == localPeerPb_.permanent_uuid()) {
      overallHealth = HealthReportPB::HEALTHY;
    }
    report.set_overall_health(overallHealth);
    reports.emplace(peerUuid, std::move(report));
  }
  return reports;
}

void PeerMessageQueue::CheckPeersInActiveConfigIfLeaderUnlocked() const {
  DCHECK(queueLock_.is_locked());
  if (queueState_.mode != LEADER) {
    return;
  }
  // Gather uuid of all peers from the active config
  std::unordered_set<string> configPeerUuids;
  for (const RaftPeerPB& peerPb : queueState_.active_config->peers()) {
    auto [it, inserted] = configPeerUuids.insert(peerPb.permanent_uuid());
    DCHECK(inserted) << "Duplicate peer uuid in config: "
                     << peerPb.permanent_uuid();
  }

  // Handle active transitional config (used in joint-consensus phase)
  // that may have to-be-added peers for the next config.
  for (const RaftPeerPB& peerPb :
       queueState_.active_config->next_config_peers()) {
    configPeerUuids.insert(peerPb.permanent_uuid());
  }

  // Ensure that all instances of Peer exist on the active config
  for (const PeersMap::value_type& entry : peersMap_) {
    if (!configPeerUuids.contains(entry.first)) {
      LOG_WITH_PREFIX_UNLOCKED(FATAL) << fmt::format(
          "Peer {} is not in the active config. "
          "Queue state: {}",
          entry.first,
          queueState_.ToString());
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
  fake_response.set_responder_uuid(localPeerPb_.permanent_uuid());
  *fake_response.mutable_status()->mutable_last_received() = id;
  *fake_response.mutable_status()->mutable_last_received_current_leader() = id;
  {
    std::lock_guard<simple_mutexlock> lock(queueLock_);
    fake_response.mutable_status()->set_last_committed_idx(
        queueState_.committed_index);
  }

  std::optional<int64_t> updated_commit_index;
  DoResponseFromPeer(
      localPeerPb_.permanent_uuid(), fake_response, updated_commit_index);

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
  // blocking on queueLock_)
  OpId local_id = id;
  if (FLAGS_HANDLER(FLAGS_async_local_vote_count)) {
    CHECK_OK(raftPoolObserversToken_->submitClosure(Bind(
        &PeerMessageQueue::DoLocalPeerAppendFinished,
        unretained(this),
        local_id,
        true)));
  } else {
    // NOTE: no need to lock RaftConsensus::lock_ because we're executing in
    // sync mode
    DoLocalPeerAppendFinished(local_id, /* need_lock */ false);
  }

  callback.Run(status);
}

Status PeerMessageQueue::appendOperation(const ReplicateRefPtr& msg) {
  return appendOperations(
      {msg},
      Bind(
          crashIfNotOkStatusCb,
          "Enqueued replicate operation failed to write to WAL"));
}

Status PeerMessageQueue::appendOperations(
    const vector<ReplicateRefPtr>& msgs,
    const StatusCallback& log_append_callback) {
  DFAKE_SCOPED_LOCK(appendFakeLock_);
  std::unique_lock<simple_mutexlock> lock(queueLock_);

  OpId last_id = msgs.back()->get()->id();

  // "Snoop" on the appended operations to watch for term changes (as follower)
  // and to determine the first index in our term (as leader).
  //
  // TODO: it would be a cleaner design to explicitly set the first index in the
  // leader term as part of setLeaderMode(). However, we are currently also
  // using that method to handle refreshing the peer list during configuration
  // changes, so the refactor isn't trivial.
  for (const auto& msg : msgs) {
    const auto& id = msg->get()->id();
    if (id.term() > queueState_.current_term) {
      queueState_.current_term = id.term();
      queueState_.first_index_in_current_term = id.index();
    } else if (
        id.term() == queueState_.current_term &&
        !queueState_.first_index_in_current_term) {
      queueState_.first_index_in_current_term = id.index();
    }
  }

  // Update safe time in the TimeManager if we're leader.
  // This will 'unpin' safe time advancement, which had stopped since we
  // assigned a timestamp to the message. Until we have leader leases, replicas
  // only call this when the message is committed.
  if (queueState_.mode == LEADER) {
    timeManager_->advanceSafeTimeWithMessage(*msgs.back()->get());
  }

  // Unlock ourselves during Append to prevent a deadlock: it's possible that
  // the log buffer is full, in which case AppendOperations would block.
  // However, for the log buffer to empty, it may need to call
  // LocalPeerAppendFinished() which also needs queueLock_.
  lock.unlock();
  RETURN_NOT_OK(log_cache_->appendOperations(
      msgs,
      Bind(
          &PeerMessageQueue::LocalPeerAppendFinished,
          unretained(this),
          last_id,
          log_append_callback)));
  lock.lock();
  DCHECK(last_id.IsInitialized());
  queueState_.last_appended = last_id;
  UpdateMetricsUnlocked();

  return Status::OK();
}

Status PeerMessageQueue::appendOperation(
    const ReplicateMsgWrapper& msg_wrapper) {
  return appendOperations(
      {msg_wrapper},
      Bind(
          crashIfNotOkStatusCb,
          "Enqueued replicate operation failed to write to WAL"));
}

Status PeerMessageQueue::appendOperations(
    const vector<ReplicateMsgWrapper>& msg_wrappers,
    const StatusCallback& log_append_callback) {
  DFAKE_SCOPED_LOCK(appendFakeLock_);
  std::unique_lock<simple_mutexlock> lock(queueLock_);

  OpId last_id = msg_wrappers.back().getOrigMsg()->get()->id();

  // "Snoop" on the appended operations to watch for term changes (as follower)
  // and to determine the first index in our term (as leader).
  //
  // TODO: it would be a cleaner design to explicitly set the first index in the
  // leader term as part of setLeaderMode(). However, we are currently also
  // using that method to handle refreshing the peer list during configuration
  // changes, so the refactor isn't trivial.
  for (const auto& msg_wrapper : msg_wrappers) {
    const auto& id = msg_wrapper.getOrigMsg()->get()->id();
    if (id.term() > queueState_.current_term) {
      queueState_.current_term = id.term();
      queueState_.first_index_in_current_term = id.index();
    } else if (
        id.term() == queueState_.current_term &&
        !queueState_.first_index_in_current_term) {
      queueState_.first_index_in_current_term = id.index();
    }
  }

  // Update safe time in the TimeManager if we're leader.
  // This will 'unpin' safe time advancement, which had stopped since we
  // assigned a timestamp to the message. Until we have leader leases, replicas
  // only call this when the message is committed.
  if (queueState_.mode == LEADER) {
    timeManager_->advanceSafeTimeWithMessage(
        *msg_wrappers.back().getOrigMsg()->get());
  }

  // Unlock ourselves during Append to prevent a deadlock: it's possible that
  // the log buffer is full, in which case appendOperations would block.
  // However, for the log buffer to empty, it may need to call
  // LocalPeerAppendFinished() which also needs queueLock_.
  lock.unlock();
  RETURN_NOT_OK(log_cache_->appendOperations(
      msg_wrappers,
      Bind(
          &PeerMessageQueue::LocalPeerAppendFinished,
          unretained(this),
          last_id,
          log_append_callback)));
  lock.lock();
  DCHECK(last_id.IsInitialized());
  queueState_.last_appended = last_id;
  UpdateMetricsUnlocked();

  return Status::OK();
}

void PeerMessageQueue::truncateOpsAfter(int64_t index) {
  DFAKE_SCOPED_LOCK(appendFakeLock_); // should not race with append.
  OpId op;
  CHECK_OK_PREPEND(
      log_cache_->lookupOpId(index, &op),
      fmt::format(
          "{}: cannot truncate ops after bad index {}",
          logPrefixUnlocked(),
          index));
  {
    std::unique_lock<simple_mutexlock> lock(queueLock_);
    DCHECK(op.IsInitialized());
    queueState_.last_appended = op;
  }
  log_cache_->truncateOpsAfter(op.index());
}

OpId PeerMessageQueue::getLastOpIdInLog() const {
  std::unique_lock<simple_mutexlock> lock(queueLock_);
  DCHECK(queueState_.last_appended.IsInitialized());
  return queueState_.last_appended;
}

OpId PeerMessageQueue::getNextOpId() const {
  std::unique_lock<simple_mutexlock> lock(queueLock_);
  DCHECK(queueState_.last_appended.IsInitialized());
  return MakeOpId(
      queueState_.current_term, queueState_.last_appended.index() + 1);
}

MonoTime PeerMessageQueue::getLeaderLeaseUntil() {
  if (queueState_.mode != LEADER) {
    return MonoTime().Min();
  }
  return leaderLeaseUntil_;
}

MonoTime PeerMessageQueue::getBoundedDataLossWindowUntil() {
  if (queueState_.mode != LEADER) {
    return MonoTime().Min();
  }
  return boundedDatalossWindowUntil_;
}

bool PeerMessageQueue::SafeToEvictUnlocked(const string& evictUuid) const {
  DCHECK(queueLock_.is_locked());
  DCHECK_EQ(LEADER, queueState_.mode);
  auto now = timeProvider_->Now();

  int remainingVoters = 0;
  int remainingViableVoters = 0;

  for (const auto& e : peersMap_) {
    const auto& uuid = e.first;
    const auto& peer = e.second;
    if (uuid == evictUuid) {
      continue;
    }
    if (!isRaftConfigVoter(uuid, *queueState_.active_config)) {
      continue;
    }
    remainingVoters++;

    bool viable = true;
    // Being alive, the local peer itself (the leader) is always a viable
    // voter: the criteria below apply only to non-local peers.
    if (uuid != localPeerPb_.permanent_uuid()) {
      // Only consider a peer to be a viable voter if...
      // ...its last exchange was successful
      viable &= peer->lastExchangeStatus == PeerStatus::Ok;

      // ...the peer is up to date with the latest majority.
      //
      //    This indicates that it's actively participating in majorities and
      //    likely to replicate a config change immediately when we propose it.
      viable &=
          peer->lastReceived.index() >= queueState_.majority_replicated_index;

      // ...we have communicated successfully with it recently.
      //
      //    This handles the case where the tablet has had no recent writes and
      //    therefore even a replica that is down would have participated in the
      //    latest majority.
      auto unreachableTime = now - peer->lastCommunicationTime;
      viable &=
          unreachableTime.ToMilliseconds() < FLAGS_consensus_rpc_timeout_ms;
    }
    if (viable) {
      remainingViableVoters++;
    }
  }

  // We never drop from 2 to 1 automatically, at least for now. We may want
  // to revisit this later, we're just being cautious with this.
  if (remainingVoters <= 1) {
    VLOG(2) << logPrefixUnlocked()
            << "Not evicting P $0 (only one voter would remain)";
    return false;
  }
  // Unless the --raft_attempt_to_replace_replica_without_majority flag is set,
  // don't evict anything if the remaining number of viable voters is not enough
  // to form a majority of the remaining voters.
  if (PREDICT_TRUE(!FLAGS_raft_attempt_to_replace_replica_without_majority) &&
      remainingViableVoters < majoritySize(remainingVoters)) {
    VLOG(2)
        << logPrefixUnlocked()
        << fmt::format(
               "Not evicting P {} (only {}/{} remaining voters appear viable)",
               evictUuid,
               remainingViableVoters,
               remainingVoters);
    return false;
  }

  return true;
}

void PeerMessageQueue::UpdatePeerHealthUnlocked(TrackedPeer* peer) {
  DCHECK(queueLock_.is_locked());
  DCHECK_EQ(LEADER, queueState_.mode);

  auto overallHealthStatus = PeerHealthStatus(*peer);

  // Prepare error messages for different conditions.
  string errorMsg;
  if (overallHealthStatus == HealthReportPB::FAILED ||
      overallHealthStatus == HealthReportPB::FAILED_UNRECOVERABLE) {
    if (peer->lastExchangeStatus == PeerStatus::TabletFailed) {
      errorMsg = fmt::format(
          "The tablet replica hosted on peer {} has failed", peer->uuid());
    } else if (!peer->walCatchupPossible) {
      errorMsg = fmt::format(
          "The logs necessary to catch up peer {} have been "
          "garbage collected. The replica will never be able "
          "to catch up",
          peer->uuid());
    } else {
      errorMsg = fmt::format(
          "Leader has been unable to successfully communicate "
          "with peer {} for more than {} seconds ({})",
          peer->uuid(),
          FLAGS_follower_unavailable_considered_failed_sec,
          (timeProvider_->Now() - peer->lastCommunicationTime).ToString());
    }
  }

  bool changed = overallHealthStatus != peer->lastOverallHealthStatus;
  peer->lastOverallHealthStatus = overallHealthStatus;

  if (FLAGS_raft_prepare_replacement_before_eviction) {
    // Only take action when there is a change.
    if (changed) {
      // Only log a message when the status changes to some flavor of failure.
      if (overallHealthStatus == HealthReportPB::FAILED ||
          overallHealthStatus == HealthReportPB::FAILED_UNRECOVERABLE) {
        LOG_WITH_PREFIX_UNLOCKED(INFO) << errorMsg;
      }
      NotifyObserversOfPeerHealthChange();
    }
  } else {
    if ((overallHealthStatus == HealthReportPB::FAILED ||
         overallHealthStatus == HealthReportPB::FAILED_UNRECOVERABLE) &&
        SafeToEvictUnlocked(peer->uuid())) {
      NotifyObserversOfFailedFollower(
          peer->uuid(), queueState_.current_term, errorMsg);
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
  if (!peer.walCatchupPossible) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas returning TABLET_FAILED status are considered irrecoverably
  // failed because the TABLED_FAILED status manifests about IO failures
  // caused by disk corruption, etc.
  if (peer.lastExchangeStatus == PeerStatus::TabletFailed) {
    return HealthReportPB::FAILED_UNRECOVERABLE;
  }

  // Replicas which have been unreachable for too long are considered failed,
  // unless it's known that they have failed irrecoverably (see above). They
  // might come back at some point and successfully catch up with the leader.
  auto maxUnreachable =
      MonoDelta::FromSeconds(FLAGS_follower_unavailable_considered_failed_sec);
  if (MonoTime::Now() - peer.lastCommunicationTime > maxUnreachable) {
    return HealthReportPB::FAILED;
  }

  // The happy case: replicas returned OK during the recent exchange are
  // considered healthy.
  if (peer.lastExchangeStatus == PeerStatus::Ok) {
    return HealthReportPB::HEALTHY;
  }

  // Other cases are for various situations when there hasn't been a contact
  // with the replica yet or it's impossible to definitely tell the health
  // status of the replica based on the last exchange status (transient error,
  // etc.). For such cases, the replica health status is reported as UNKNOWN.
  return HealthReportPB::UNKNOWN;
}

Status PeerMessageQueue::RequestForPeer(
    const string& uuid,
    bool read_ops,
    ConsensusRequestPB* request,
    vector<ReplicateRefPtr>* msg_refs,
    bool* needs_tablet_copy,
    std::string* next_hop_uuid) {
  // Maintain a thread-safe copy of necessary members.
  OpId precedingId;
  int64_t currentTerm;
  TrackedPeer peerCopy;
  MonoDelta unreachableTime;
  {
    std::lock_guard<simple_mutexlock> lock(queueLock_);
    DCHECK_EQ(queueState_.state, kQueueOpen);
    DCHECK_NE(uuid, localPeerPb_.permanent_uuid());

    auto it = peersMap_.find(uuid);
    // Validate peer exists and has non-null value.
    if (PREDICT_FALSE(
            it == peersMap_.end() || it->second == nullptr ||
            queueState_.mode == NON_LEADER)) {
      return Status::NotFound(
          fmt::format(
              "peer {} is no longer tracked or "
              "queue is not in leader mode",
              uuid));
    }
    TrackedPeer* peer = it->second;
    peerCopy = *peer;

    // Clear the requests without deleting the entries, as they may be in use by
    // other peers.
    request->mutable_ops()->UnsafeArenaExtractSubrange(
        0, request->ops_size(), nullptr);

    // Initialized to head for new peers but to last appended for peers
    // otherwise
    precedingId = (peerCopy.lastExchangeStatus == PeerStatus::New ||
                   !peerCopy.lastReceived.IsInitialized())
        ? queueState_.last_appended
        : peerCopy.lastReceived;
    currentTerm = queueState_.current_term;

    request->set_committed_index(queueState_.committed_index);
    request->set_all_replicated_index(queueState_.all_replicated_index);
    request->set_last_idx_appended_to_leader(queueState_.last_appended.index());
    request->set_caller_term(currentTerm);
    request->set_region_durable_index(queueState_.region_durable_index);
    if (auto rpc_token = persistentVars_->raftRpcToken()) {
      request->set_raft_rpc_token(*rpc_token);
    }
    request->clear_compression_dictionary();
    if (peer->shouldSendCompressionDict) {
      KLOG_EVERY_N_SECS(INFO, 180)
          << "Setting compression dictionary in request to: " << peer->uuid()
          << " as " << CompressionCodecManager::getCurrentDictionaryId();
      request->set_compression_dictionary(
          CompressionCodecManager::getDictionary());
    }
    unreachableTime = timeProvider_->Now() - peerCopy.lastCommunicationTime;

    RETURN_NOT_OK(routingTableContainer_->nextHop(
        localPeerPb_.permanent_uuid(), uuid, next_hop_uuid));

    if (*next_hop_uuid != uuid) {
      // If proxy_peer is not healthy, then route directly to the destination
      // TODO: Multi hop proxy support needs better failure and health checks
      // for proxy peer. The current method of detecting unhealthy proxy peer
      // works only on the leader. One solution could be for the leader to
      // periodically exchange the health report of all peers as part of
      // UpdateReplica() call

      if (peer->proxyTargetEnabled()) {
        bool should_proxy = false;
        if (peer->isHealthy()) {
          auto proxy_it = peersMap_.find(*next_hop_uuid);
          // Validate proxy peer exists and has non-null value.
          if (proxy_it != peersMap_.end() && proxy_it->second != nullptr &&
              !HasProxyPeerFailedUnlocked(proxy_it->second, peer)) {
            should_proxy = true;
          }
        }

        if (!should_proxy) {
          *next_hop_uuid = uuid;
          peer->snoozeProxying(
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
  bool walCatchupProgress = false;
  bool walCatchupFailure = false;
  // Preventing the overhead of this as we need to take consensus queue lock
  // again
  SCOPE_EXIT {
    if (!FLAGS_HANDLER(FLAGS_update_peer_health_status)) {
      return;
    }
    std::lock_guard<simple_mutexlock> lock(queueLock_);
    auto it = peersMap_.find(uuid);
    // Validate peer exists and has non-null value.
    if (PREDICT_FALSE(
            it == peersMap_.end() || it->second == nullptr ||
            queueState_.mode == NON_LEADER)) {
      VLOG(1) << logPrefixUnlocked() << "peer " << uuid
              << " is no longer tracked or queue is not in leader mode";
      return;
    }
    TrackedPeer* peer = it->second;
    if (walCatchupProgress) {
      peer->walCatchupPossible = true;
    }
    if (walCatchupFailure) {
      peer->walCatchupPossible = false;
    }
    UpdatePeerHealthUnlocked(peer);
  };

  if (peerCopy.lastExchangeStatus == PeerStatus::TabletNotFound) {
    VLOG(3) << logPrefixUnlocked() << "Peer " << uuid << " needs tablet copy"
            << kThrottleMsg;
    *needs_tablet_copy = true;
    return Status::OK();
  }
  *needs_tablet_copy = false;

  // If the next hop != the destination, we are sending these messages via a
  // proxy.
  bool routeViaProxy = *next_hop_uuid != uuid;
  if (routeViaProxy) {
    // Set proxy uuid
    request->set_proxy_dest_uuid(*next_hop_uuid);
  } else {
    // Clear proxy uuid to ensure that this message is not rejected by the
    // destination
    request->clear_proxy_dest_uuid();
  }

  if (peerCopy.lastExchangeStatus != PeerStatus::New && read_ops) {
    // The batch of messages to send to the peer.
    vector<ReplicateRefPtr> messages;
    Status s = ReadMessagesForRequest(
        peerCopy, routeViaProxy, &messages, &precedingId);

    if (PREDICT_FALSE(!s.ok())) {
      // It's normal to have a NotFound() here if a follower falls behind where
      // the leader has GCed its logs. The follower replica will hang around
      // for a while until it's evicted.
      if (PREDICT_TRUE(s.IsNotFound())) {
        KLOG_EVERY_N_SECS_THROTTLER(
            INFO, 600, *peerCopy.statusLogThrottler, "logs_gced")
            << logPrefixUnlocked()
            << fmt::format(
                   "The logs necessary to catch up peer {} have been "
                   "garbage collected. The follower will never be able "
                   "to catch up ({})",
                   uuid,
                   s.ToString());
        walCatchupFailure = true;
        return s;
      }
      if (s.isUninitialized()) {
        LOG_WITH_PREFIX_UNLOCKED_EVERY_N(ERROR, 10)
            << "Log is not ready to be read yet while preparing peer request: "
            << s.ToString() << ". Destination peer: " << peerCopy.ToString();
        return s;
      }
      if (s.isIncomplete()) {
        // isIncomplete() means that we tried to read beyond the head of the log
        // (in the future). See KUDU-1078.
        LOG_WITH_PREFIX_UNLOCKED(ERROR)
            << "Error trying to read ahead of the log "
            << "while preparing peer request: " << s.ToString()
            << ". Destination peer: " << peerCopy.ToString();
        return s;
      }
      LOG_WITH_PREFIX_UNLOCKED(FATAL)
          << "Error reading the log while preparing peer request: "
          << s.ToString() << ". Destination peer: " << peerCopy.ToString();
    }

    // Since we were able to read ops through the log cache, we know that
    // catchup is possible.
    walCatchupProgress = true;

    // We use AddAllocated rather than copy, because we pin the log cache at the
    // "all replicated" point. At some point we may want to allow partially
    // loading (and not pinning) earlier messages. At that point we'll need to
    // do something smarter here, like copy or ref-count.
    if (!routeViaProxy) {
      for (const ReplicateRefPtr& msg : messages) {
        request->mutable_ops()->AddAllocated(msg->get());
      }
      msg_refs->swap(messages);
    } else {
      vector<ReplicateRefPtr> proxy_ops;
      for (const ReplicateRefPtr& msg : messages) {
        ReplicateRefPtr proxy_op = makeScopedRefptrReplicate(
            std::make_unique<ReplicateMsg>(), msg->source());
        *proxy_op->get()->mutable_id() = msg->get()->id();
        proxy_op->get()->set_timestamp(msg->get()->timestamp());
        proxy_op->get()->set_op_type(PROXY_OP);
        request->mutable_ops()->AddAllocated(proxy_op->get());
        proxy_ops.emplace_back(std::move(proxy_op));
      }
      msg_refs->swap(proxy_ops);
    }
  }

  DCHECK(precedingId.IsInitialized());
  request->mutable_preceding_id()->CopyFrom(precedingId);

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
          << " ops behind the committed index " << kThrottleMsg;
    }
    // If we're not sending ops to the follower, set the safe time on the
    // request.
    // TODO(dralves) When we have leader leases, send this all the time.
  } else {
    if (PREDICT_TRUE(FLAGS_safe_time_advancement_without_writes)) {
      request->set_safe_timestamp(timeManager_->getSafeTime().value());
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
    const TrackedPeer& peerCopy,
    bool routeViaProxy,
    std::vector<ReplicateRefPtr>* messages,
    OpId* precedingId) {
  ReadContext readContext;
  readContext.forPeerUuid = &peerCopy.uuid();
  readContext.forPeerHost = &peerCopy.peerPb.last_known_addr().host();
  readContext.forPeerPort = peerCopy.peerPb.last_known_addr().port();
  readContext.routeViaProxy = routeViaProxy;
  // Note, we will report errors when warm storage catchup cannot find logs
  readContext.reportErrors = true;
  readContext.enableWarmStorageReads = FLAGS_warm_storage_reads_for_replication;

  // We try to get the follower's nextIndex from our log.
  LogCache::ReadOpsStatus s = log_cache_->readOps(
      peerCopy.nextIndex - 1,
      FLAGS_consensus_max_batch_size_bytes,
      readContext,
      messages);
  if (s.status.ok()) {
    *precedingId = std::move(s.precedingOp);
  }
  return std::move(s.status);
}

void PeerMessageQueue::AdvanceQueueRegionDurableIndex() {
  int64_t maxRegionDurableIndex = -1;

  if (!localPeerPb_.attrs().has_region()) {
    return;
  }

  // region_durable_index is updated only if following constraints are satisfied
  // 1. region_durable_index <= committed_index
  // 2. Atleast one non-leader region has received this index
  for (const PeersMap::value_type& peer : peersMap_) {
    if (!peer.second->isPeerInLocalRegion.has_value()) {
      continue;
    }
    if (!peer.second->isPeerInLocalRegion.value() &&
        peer.second->lastReceived.index() <= queueState_.committed_index) {
      // This peer is outside our region and the last received index is
      // lower than the current committed_index. Include this in the
      // calculation of region_durable_index
      maxRegionDurableIndex =
          std::max(maxRegionDurableIndex, peer.second->lastReceived.index());
    }
  }

  queueState_.region_durable_index =
      std::max(queueState_.region_durable_index, maxRegionDurableIndex);
}

void PeerMessageQueue::AdvanceQueueWatermark(
    const char* type,
    int64_t* watermark,
    const OpId& replicated_before,
    const OpId& replicated_after,
    int num_peers_required,
    ReplicaTypes replica_types,
    const TrackedPeer* who_caused,
    RaftPeerRange auto&& considered_peers) {
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
  // - Store all the considered peer's 'last_received' in a vector
  // - Sort the vector
  // - Find the vector.size() - 'num_peers_required' position, this
  //   will be the new 'watermark'.
  std::vector<int64_t> watermarks;
  watermarks.reserve(considered_peers.size());
  for (const RaftPeerPB& peer_pb : considered_peers) {
    DCHECK(peer_pb.has_permanent_uuid() && peer_pb.has_member_type())
        << "Expecting a non-null peer with uuid and member type.";
    if (replica_types == kVoterReplicas &&
        peer_pb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }

    auto it = peersMap_.find(peer_pb.permanent_uuid());
    if (it == peersMap_.end()) {
      // NOTE: We assume `peersMap_` always has all peers from the
      // considered_peers, which commonly is populated from peers in config.
      LOG(WARNING)
          << "A considered Peer " << peer_pb.permanent_uuid() << " "
          << "is not yet tracked or registered for watermark calculation.";
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
    const auto& peer = it->second;
    if (peer->lastExchangeStatus == PeerStatus::Ok) {
      watermarks.push_back(peer->lastReceived.index());
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
    for (const PeersMap::value_type& peer : peersMap_) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Peer: " << peer.second->ToString();
    }
    VLOG_WITH_PREFIX_UNLOCKED(3) << "Sorted watermarks:";
    for (int64_t wm : watermarks) {
      VLOG_WITH_PREFIX_UNLOCKED(3) << "Watermark: " << wm;
    }
  }
}

PeerMessageQueue::QuorumResults PeerMessageQueue::IsQuorumSatisfiedUnlocked(
    const RaftPeerPB& peer,
    const std::function<bool(const TrackedPeer*)>& predicate) {
  if (!FLAGS_enable_flexi_raft) {
    // For Vanilla raft mode, peer (localPeerPb_) might not have fields
    // populated other than uuid
    int num_satisfied = 0;
    std::vector<TrackedPeer*> quorum_peers;
    for (const PeersMap::value_type& tracked_peer : peersMap_) {
      if (!tracked_peer.second->peerPb.has_member_type() ||
          tracked_peer.second->peerPb.member_type() != RaftPeerPB::VOTER) {
        continue;
      }
      if (predicate(tracked_peer.second)) {
        num_satisfied++;
        quorum_peers.push_back(tracked_peer.second);
      }
    }
    return {
        num_satisfied >= queueState_.majority_size_,
        num_satisfied,
        queueState_.majority_size_,
        kVanillaRaftQuorumId,
        quorum_peers};
  }

  const std::string& peer_quorum_id = getQuorumIdUsingCommitRule(peer);

  // Compute total number of voters in each region.
  std::optional<int> total_from_vd = getTotalVotersFromVoterDistribution(
      *(queueState_.active_config), peer_quorum_id);

  int total_voters_from_voter_distribution = total_from_vd.value_or(0);

  // Compute number of voters in each region in the active config.
  // As voter distribution provided in topology config can lag,
  // we need to take into account the active voters as well due to
  // membership changes.
  // Check for more comments in adjustVoterDistributionWithCurrentVoters() which
  // does the same for static mode watermark calculation
  int total_voters_from_active_config = 0;
  for (const RaftPeerPB& peer_pb : queueState_.active_config->peers()) {
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

  // adjustVoterDistribution_ is set to false on in cases where we want to
  // perform an election forcefully i.e. unsafe config change
  if (PREDICT_FALSE(!adjustVoterDistribution_)) {
    total_voters = total_voters_from_voter_distribution;
  }

  DCHECK(total_voters >= 1 || !adjustVoterDistribution_);
  int majority_size = majoritySize(total_voters);

  bool is_local_peer = peer.permanent_uuid() == localPeerPb_.permanent_uuid();
  int num_satisfied = 0;
  std::vector<TrackedPeer*> quorum_peers;
  for (const PeersMap::value_type& tracked_peer : peersMap_) {
    if (!tracked_peer.second->peerPb.has_member_type() ||
        tracked_peer.second->peerPb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }

    // We are either computing quorum on the local peer or on a remote peer.
    // For local peer, we can resort to the optimization of looking at
    // is_peer_in_local_quorum which is previously set. This is a worthy
    // optimization because this code path is called on every write.
    if (PREDICT_TRUE(is_local_peer)) {
      if (!tracked_peer.second->isPeerInLocalQuorum.has_value() ||
          !tracked_peer.second->isPeerInLocalQuorum.value()) {
        continue;
      }
    } else {
      string quorum_id =
          getQuorumIdUsingCommitRule(tracked_peer.second->peerPb);
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
  for (const PeersMap::value_type& peer : peersMap_) {
    if (!peer.second->peerPb.has_member_type() ||
        peer.second->peerPb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    if (predicate(peer.second)) {
      if (peer.second->isPeerInLocalRegion.has_value() &&
          !peer.second->isPeerInLocalRegion.value()) {
        acks_outoflocalregion++;
        outoflocalregion_peers.push_back(peer.second);
      }
    }
  }
  return {
      // Check if atleast one of the acks is out of local region
      acks_outoflocalregion > 0,
      acks_outoflocalregion,
      queueState_.majority_size_,
      kVanillaRaftQuorumId,
      outoflocalregion_peers};
}

int64_t PeerMessageQueue::ComputeNewWatermarkDynamicMode(int64_t* watermark) {
  CHECK(watermark);
  CHECK(queueState_.active_config->has_commit_rule());
  CHECK(
      queueState_.active_config->commit_rule().mode() ==
      QuorumMode::SINGLE_REGION_DYNAMIC);

  // Compute the watermarks in leader quorum. As an example, at the end of this
  // loop, watermarks_in_leader_quorum might have entries (3, 7, 5) which
  // indicates that the leader quorum has 3 peers that have responded to OpId
  // indexes 3, 7 and 5 respectively
  std::vector<int64_t> watermarks_in_leader_quorum;
  watermarks_in_leader_quorum.reserve(FLAGS_default_quorum_size * 2);

  auto results = IsQuorumSatisfiedUnlocked(
      localPeerPb_, [&watermarks_in_leader_quorum](auto peer) {
        // Refer to the comment in AdvanceQueueWatermark method for why only
        // successful last exchanges are considered.
        if (peer->lastExchangeStatus == PeerStatus::Ok) {
          watermarks_in_leader_quorum.push_back(peer->lastReceived.index());
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
  int old_watermark = -1;
  const std::string& leader_quorum = getQuorumIdUsingCommitRule(localPeerPb_);
  const std::string& peer_quorum =
      getQuorumIdUsingCommitRule(who_caused->peerPb);

  // Only an ack from the leader region can advance the watermark. Skip this
  // expensive operation otherwise
  if (leader_quorum == peer_quorum) {
    old_watermark = ComputeNewWatermarkDynamicMode(watermark);
  }

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Updated majority_replicated watermark " << "from " << old_watermark
      << " to " << (*watermark);
}

void PeerMessageQueue::BeginWatchForSuccessor(
    const std::optional<string>& successor_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
    PeerMessageQueue::TransferContext transfer_context) {
  std::lock_guard<simple_mutexlock> l(queueLock_);

  transferContext_ = std::move(transfer_context);
  successorWatchPeerNotified_ = false;

  if (successor_uuid &&
      PeerTransferLeadershipImmediatelyUnlocked(*successor_uuid)) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leadership transfer to " << *successor_uuid
        << " started synchronously";
    return;
  }

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Leadership transfer: Watching for successor asynchronously";

  successorWatchInProgress_ = true;
  designatedSuccessorUuid_ = successor_uuid;
  tlFilterFn_ = filter_fn;
}

void PeerMessageQueue::EndWatchForSuccessor() {
  std::lock_guard<simple_mutexlock> l(queueLock_);
  successorWatchInProgress_ = false;
  transferContext_ = {};
  tlFilterFn_ = nullptr;
}

bool PeerMessageQueue::WatchForSuccessorPeerNotified() {
  std::lock_guard<simple_mutexlock> l(queueLock_);
  return successorWatchPeerNotified_;
}

Status PeerMessageQueue::GetNextRoutingHopFromLeader(
    const string& dest_uuid,
    string* next_hop) const {
  return routingTableContainer_->nextHop(
      localPeerPb_.permanent_uuid(), dest_uuid, next_hop);
}

void PeerMessageQueue::updateFollowerWatermarks(
    int64_t committed_index,
    int64_t all_replicated_index,
    int64_t region_durable_index) {
  std::lock_guard<simple_mutexlock> l(queueLock_);
  DCHECK_EQ(queueState_.mode, NON_LEADER);
  queueState_.committed_index = committed_index;
  queueState_.all_replicated_index = all_replicated_index;

  if (region_durable_index > queueState_.region_durable_index) {
    queueState_.region_durable_index = region_durable_index;
  }

  UpdateMetricsUnlocked();
}

void PeerMessageQueue::updateLastIndexAppendedToLeader(
    int64_t last_idx_appended_to_leader) {
  std::lock_guard<simple_mutexlock> l(queueLock_);
  DCHECK_EQ(queueState_.mode, NON_LEADER);
  queueState_.last_idx_appended_to_leader = last_idx_appended_to_leader;
  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdatePeerStatus(
    const string& peer_uuid,
    PeerStatus ps,
    const Status& status) {
  std::unique_lock<simple_mutexlock> l(queueLock_);
  auto it = peersMap_.find(peer_uuid);
  // Validate peer exists and has non-null value.
  if (PREDICT_FALSE(
          it == peersMap_.end() || it->second == nullptr ||
          queueState_.mode == NON_LEADER)) {
    VLOG(1) << logPrefixUnlocked() << "peer " << peer_uuid
            << " is no longer tracked or queue is not in leader mode";
    return;
  }
  TrackedPeer* peer = it->second;
  peer->lastExchangeStatus = ps;

  if (ps != PeerStatus::RpcLayerError) {
    // So long as we got _any_ response from the follower, we consider it a
    // 'communication'. RPC_LAYER_ERROR indicates something like a connection
    // failure, indicating that the host itself is likely down.
    //
    // This indicates that the node is at least online.
    peer->lastCommunicationTime = MonoTime::Now();
  }

  switch (ps) {
    case PeerStatus::New:
      LOG_WITH_PREFIX_UNLOCKED(DFATAL)
          << "Should not update an existing peer to 'NEW' state";
      break;

    case PeerStatus::RpcLayerError:
      peer->incrConsecutiveFailures();
      // Most controller errors are caused by network issues or corner cases
      // like shutdown and failure to deserialize a protobuf. Therefore, we
      // generally consider these errors to indicate an unreachable peer.
      DCHECK(!status.ok());
      break;

    case PeerStatus::TabletNotFound:
      peer->incrConsecutiveFailures();
      VLOG_WITH_PREFIX_UNLOCKED(1)
          << "Peer needs tablet copy: " << peer->ToString();
      break;

    case PeerStatus::TabletFailed: {
      peer->incrConsecutiveFailures();
      UpdatePeerHealthUnlocked(peer);
      return;
    }

    case PeerStatus::RemoteError:
    case PeerStatus::InvalidTerm:
    case PeerStatus::LmpMismatch:
    case PeerStatus::CannotPrepare:
      peer->incrConsecutiveFailures();
      UpdatePeerAppendFailure(peer, status);
      break;

    case PeerStatus::Ok:
      peer->resetConsecutiveFailures();
      DCHECK(status.ok());
      break;
  }
}

void PeerMessageQueue::UpdateExchangeStatus(
    TrackedPeer* peer,
    PeerStatus last_exchange_status,
    const ConsensusResponsePB& response,
    bool* sendMoreImmediately) {
  DCHECK(queueLock_.is_locked());
  const ConsensusStatusPB& status = response.status();

  MonoTime now = timeProvider_->Now();
  peer->lastCommunicationTime = now;
  peer->lastKnownCommittedIndex = status.last_committed_idx();

  if (PREDICT_TRUE(!status.has_error())) {
    peer->lastExchangeStatus = PeerStatus::Ok;
    peer->lastSuccessfulExchange = now;
    peer->corruptionCount = 0;
    peer->resetConsecutiveFailures();
    *sendMoreImmediately = false;
    if (peer->shouldSendCompressionDict) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Resetting compression dict flag for peer: " << peer->ToString();
      peer->shouldSendCompressionDict = false;
    }
    return;
  }

  peer->incrConsecutiveFailures();

  switch (status.error().code()) {
    case ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH:
      peer->lastExchangeStatus = PeerStatus::LmpMismatch;
      DCHECK(status.has_last_received());
      if (last_exchange_status == PeerStatus::New) {
        LOG_WITH_PREFIX_UNLOCKED(INFO)
            << "Connected to new peer: " << peer->ToString();
        peer->resetConsecutiveFailures();
      } else {
        if (peer->consecutiveFailures() <
            FLAGS_consecutive_failure_backoff_threshold) {
          LOG_WITH_PREFIX_UNLOCKED(INFO)
              << "Got LMP mismatch error from peer: " << peer->ToString();
        } else if (
            peer->consecutiveFailures() % kLmpMismatchLogFrequency == 0) {
          LOG_WITH_PREFIX_UNLOCKED(INFO)
              << "(THROTTLED EVERY " << kLmpMismatchLogFrequency << ") "
              << "Got LMP mismatch error from peer: " << peer->ToString();
        }
      }
      *sendMoreImmediately = last_exchange_status == PeerStatus::New ||
          peer->consecutiveFailures() <
              FLAGS_consecutive_failure_backoff_threshold;
      return;

    case ConsensusErrorPB::INVALID_TERM:
      peer->lastExchangeStatus = PeerStatus::InvalidTerm;
      CHECK(response.has_responder_term());
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Peer responded invalid term: " << peer->ToString();
      NotifyObserversOfTermChange(response.responder_term());
      *sendMoreImmediately = false;
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
  if (status.isCompressionDictMismatch()) {
    peer->shouldSendCompressionDict = true;
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Got compression dict error from peer: " << peer->ToString();
  } else if (status.IsCorruption()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Corruption reported by peer. " << peer->ToString()
        << " [ERROR]: " << status.ToString();
    peer->corruptionCount++;
    if (CorruptionLikely(peer)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Corruption likely at " << peer->nextIndex
          << ", evicting log cache";
      STATS_corruptionCacheDrops.add(1, KUDU_STATS_TAG);
      log_cache_->evictThroughOp(peer->nextIndex, true);
    }
  }
}

bool PeerMessageQueue::CorruptionLikely(TrackedPeer* peer) const {
  DCHECK(queueLock_.is_locked());

  if (FLAGS_min_corruption_count <= 0 ||
      peer->corruptionCount < FLAGS_min_corruption_count) {
    return false;
  }

  if (FLAGS_min_single_corruption_count > 0 &&
      peer->corruptionCount >= FLAGS_min_single_corruption_count) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer " << peer->uuid()
        << " corruption count: " << peer->corruptionCount << " > "
        << FLAGS_min_single_corruption_count;
    STATS_singleCorruptionCacheDrops.add(1, KUDU_STATS_TAG);
    return true;
  }

  size_t total_corrupted_peers = 0;
  for (const auto& [_, other_peer] : peersMap_) {
    if (other_peer->corruptionCount >= FLAGS_min_corruption_count &&
        peer->nextIndex == other_peer->nextIndex) {
      total_corrupted_peers += 1;
    }
  }

  if (total_corrupted_peers > 1) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer " << peer->uuid()
        << " corruption count: " << peer->corruptionCount << " > "
        << FLAGS_min_corruption_count << " and " << total_corrupted_peers
        << " are reporting corruption";

    return true;
  }

  return false;
}

void PeerMessageQueue::PromoteIfNeeded(
    TrackedPeer* peer,
    const OpId& prev_last_received,
    const ConsensusStatusPB& status) {
  DCHECK(queueLock_.is_locked());
  if (queueState_.mode != PeerMessageQueue::LEADER ||
      peer->lastExchangeStatus != PeerStatus::Ok) {
    return;
  }

  // TODO(mpercy): It would be more efficient to cache the member type in the
  // TrackedPeer data structure.
  RaftPeerPB* peer_pb;
  Status s = getRaftConfigMember(
      DCHECK_NOTNULL(queueState_.active_config.get()), peer->uuid(), &peer_pb);
  if (s.ok() && peer_pb->member_type() == RaftPeerPB::NON_VOTER &&
      peer_pb->attrs().promote()) {
    // Only promote the peer if it is within one round-trip of being fully
    // caught-up with the current commit index, as measured by recent
    // UpdateConsensus() operation batch sizes.

    // If we had never previously contacted this peer, wait until the second
    // time we contact them to try to promote them.
    if (prev_last_received.index() == 0) {
      return;
    }

    int64_t last_batch_size = std::max<int64_t>(
        0, peer->lastReceived.index() - prev_last_received.index());
    bool peer_caught_up =
        !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
        status.last_received_current_leader().index() + last_batch_size >=
            queueState_.committed_index;
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
  DCHECK(queueLock_.is_locked());

  // This check is redundant for ResponseFromPeer common path
  if (PREDICT_FALSE(queueState_.state != kQueueOpen)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Queue is not open";
    return false;
  }

  // Only in LEADER mode can you transfer leadership
  if (queueState_.mode != PeerMessageQueue::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer is not a leader, cannot transfer leadership";
    return false;
  }

  // Peer has to be healthily communicating to LEADER
  if (peer.lastExchangeStatus != PeerStatus::Ok) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Peer does not have healthy communications with leader";
    return false;
  }

  Status s = getRaftConfigMember(
      DCHECK_NOTNULL(queueState_.active_config.get()),
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
  DCHECK(queueLock_.is_locked());
  auto it = peersMap_.find(peer_uuid);
  // Validate peer exists and has non-null value.
  if (PREDICT_FALSE(it == peersMap_.end() || it->second == nullptr)) {
    return false;
  }
  TrackedPeer* peer = it->second;

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(*peer, &peer_pb)) {
    return false;
  }

  // peer needs to be caught up so that if it runs an election,
  // it has the longest log and is ready to become LEADER
  // TODO - Verify if first check is redundant
  bool peer_caught_up = !OpIdEquals(peer->lastReceived, MinimumOpId()) &&
      OpIdEquals(peer->lastReceived, queueState_.last_appended);
  if (peer_caught_up) {
    NotifyObserversOfSuccessor(peer_uuid);
  }
  return peer_caught_up;
}

MonoDelta PeerMessageQueue::leaderLeaseTimeout() {
  int32_t const lease_timeout = FLAGS_raft_leader_lease_interval_ms;
  return MonoDelta::FromMilliseconds(lease_timeout);
}

MonoDelta PeerMessageQueue::boundedDataLossDefaultWindowInMsec() {
  int32_t const bounded_data_loss_window_ms =
      FLAGS_bounded_dataloss_window_interval_ms;
  return MonoDelta::FromMilliseconds(bounded_data_loss_window_ms);
}

void PeerMessageQueue::setPeerRpcStartTime(
    const std::string& peer_uuid,
    MonoTime rpc_start) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  auto it = peersMap_.find(peer_uuid);
  // Validate peer exists and has non-null value.
  if (PREDICT_FALSE(it == peersMap_.end() || it->second == nullptr)) {
    LOG(WARNING) << "Candidate peer " << peer_uuid
                 << " is not foung in Message Queue's Peers map";
    return;
  }
  TrackedPeer* peer = it->second;
  peer->rpcStart = rpc_start;
}

void PeerMessageQueue::updatePeerRtt(
    const std::string& peer_uuid,
    MonoDelta rtt) {
  routingTableContainer_->updateRtt(
      peer_uuid, std::chrono::microseconds(rtt.ToMicroseconds()));
}

void PeerMessageQueue::TransferLeadershipIfNeeded(
    const TrackedPeer& peer,
    const ConsensusStatusPB& status) {
  DCHECK(queueLock_.is_locked());
  if (!successorWatchInProgress_) {
    return;
  }

  if (designatedSuccessorUuid_ && peer.uuid() != *designatedSuccessorUuid_) {
    return;
  }

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(peer, &peer_pb)) {
    return;
  }

  if (isBackingDbPresent(*peer_pb)) {
    if (!IsStateMachineHealthyForElectionUnlock(peer.stateMachineMetrics)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Peer " << peer.uuid() << "is lagging "
          << " and not suitable for election.";
      return;
    }
  }

  // check if this instance is filtered, if filter_fn has been provided
  if (!designatedSuccessorUuid_ && tlFilterFn_ && tlFilterFn_(*peer_pb)) {
    return;
  }

  // We want to make sure that we are not promoting to a region that doesn't
  // have a majority of nodes running otherwise, it won't be able to accept
  // writes. We do a quick local check to see if there are a quorum number of
  // nodes being tracked. It is not bulletproof since it doesn't actually
  // verify that the nodes are up and running but the common case is that
  // tracked nodes are up and running.
  if (!RegionHasQuorumCommitUnlocked(*peer_pb)) {
    LOG(WARNING) << "Candidate peer " << peer_pb->permanent_uuid()
                 << " does not have majority voters running";
    return;
  }

  bool peer_caught_up =
      !OpIdEquals(status.last_received_current_leader(), MinimumOpId()) &&
      OpIdEquals(
          status.last_received_current_leader(), queueState_.last_appended);
  if (!peer_caught_up) {
    return;
  }

  VLOG(1) << "Successor watch: peer " << peer.uuid() << " is caught up to "
          << "the leader at OpId "
          << OpIdToString(status.last_received_current_leader());
  successorWatchInProgress_ = false;
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

  bool sendMoreImmediately = false;
  Mode mode_copy;
  {
    std::lock_guard<simple_mutexlock> scoped_lock(queueLock_);

    // TODO(mpercy): Handle response from proxy on behalf of another peer.
    // For now, we'll try to ignore proxying here, but we may need to
    // eventually handle that here for better health status and error logging.

    auto it = peersMap_.find(peer_uuid);
    // Validate peer exists and has non-null value.
    if (PREDICT_FALSE(
            queueState_.state != kQueueOpen || it == peersMap_.end() ||
            it->second == nullptr)) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Queue is closed or peer was untracked, disregarding "
             "peer response. Response: "
          << SecureShortDebugString(response);
      return sendMoreImmediately;
    }
    TrackedPeer* peer = it->second;

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

    // populate server metrics
    peer->stateMachineMetrics = response.state_machine_metrics();

    // Take a snapshot of the previously-recorded peer state.
    const PeerStatus prev_last_exchange_status = peer->lastExchangeStatus;
    const OpId prev_last_received = peer->lastReceived;

    // Update the peer's last exchange status based on the response.
    // In this case, if there is a log matching property (LMP) mismatch, we
    // want to immediately send another request as we attempt to sync the log
    // offset between the local leader and the remote peer.
    UpdateExchangeStatus(
        peer, prev_last_exchange_status, response, &sendMoreImmediately);

    // If the reported last-received op for the replica is in our local log,
    // then resume sending entries from that point onward. Otherwise, resume
    // after the last op they received from us. If we've never successfully
    // sent them anything, start after the last-committed op in their log, which
    // is guaranteed by the Raft protocol to be a valid op.

    bool peer_has_prefix_of_log = IsOpInLog(status.last_received());
    if (peer_has_prefix_of_log) {
      // If the latest thing in their log is in our log, we are in sync.
      peer->lastReceived = status.last_received();
      peer->nextIndex = peer->lastReceived.index() + 1;

      // Check if the peer is a NON_VOTER candidate ready for promotion.
      PromoteIfNeeded(peer, prev_last_received, status);

      TransferLeadershipIfNeeded(*peer, status);
    } else if (!OpIdEquals(
                   status.last_received_current_leader(), MinimumOpId())) {
      // Their log may have diverged from ours, however we are in the process
      // of replicating our ops to them, so continue doing so. Eventually, we
      // will cause the divergent entry in their log to be overwritten.
      peer->lastReceived = status.last_received_current_leader();
      peer->nextIndex = peer->lastReceived.index() + 1;

    } else {
      // The peer is divergent and they have not (successfully) received
      // anything from us yet. Start sending from their last committed index.
      // This logic differs from the Raft spec slightly because instead of
      // stepping back one-by-one from the end until we no longer have an LMP
      // error, we jump back to the last committed op indicated by the peer with
      // the hope that doing so will result in a faster catch-up process.
      DCHECK_GE(peer->lastKnownCommittedIndex, 0);
      peer->nextIndex = peer->lastKnownCommittedIndex + 1;
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Peer " << peer_uuid
          << " log is divergent from this leader: " << "its last log entry "
          << OpIdToString(status.last_received()) << " is not in "
          << "this leader's log and it has not received anything from this leader yet. "
          << "Falling back to committed index "
          << peer->lastKnownCommittedIndex;
    }

    if (peer->lastExchangeStatus != PeerStatus::Ok) {
      // In this case, 'sendMoreImmediately' has already been set by
      // UpdateExchangeStatus() to true in the case of an LMP mismatch, false
      // otherwise.
      return sendMoreImmediately;
    }

    if (response.has_responder_term()) {
      // The peer must have responded with a term that is greater than or equal
      // to the last known term for that peer.
      peer->checkMonotonicTerms(response.responder_term());

      // If the responder didn't send an error back that must mean that it has
      // a term that is the same or lower than ours.
      CHECK_LE(response.responder_term(), queueState_.current_term);
    }

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Received Response from Peer (" << peer->ToString() << "). "
          << "Response: " << SecureShortDebugString(response);
    }

    if (peer->lastExchangeStatus == PeerStatus::Ok) {
      if (FLAGS_enable_raft_leader_lease && response.has_lease_granted() &&
          response.lease_granted()) {
        peer->leaseGranted = peer->lastReceived;
      }

      if (FLAGS_enable_bounded_dataloss_window) {
        peer->boundedDatalossWindowAcked = peer->lastReceived;
      }
    }

    mode_copy = queueState_.mode;

    // If we're the leader, we can compute the new watermarks based on the
    // progress of our followers. NOTE: it's possible this node might have lost
    // its leadership (and the notification is just pending behind the lock
    // we're holding), but any future leader will observe the same watermarks
    // and make the same advancement, so this is safe.
    int64_t old_all_replicated_index = 0;
    int64_t new_all_replicated_index = 0;

    if (mode_copy == LEADER) {
      // Gather all peers from config to be considered for watermark calculation
      auto considered_peers = std::ranges::subrange(
          queueState_.active_config->peers().begin(),
          queueState_.active_config->peers().end());

      // Gather peers in the next config for joint-consensus phase
      const bool is_joint_consensus_phase =
          queueState_.active_config->next_config_peers_size() > 0;
      auto considered_next_peers = std::ranges::subrange(
          queueState_.active_config->next_config_peers().begin(),
          queueState_.active_config->next_config_peers().end());

      // Advance the majority replicated index.
      if (!FLAGS_enable_flexi_raft) {
        int64_t curr_majority_rpl_idx = queueState_.majority_replicated_index;
        AdvanceQueueWatermark(
            /*type=*/"majority_replicated",
            /*watermark=*/&curr_majority_rpl_idx,
            /*replicated_before=*/prev_last_received,
            /*replicated_after=*/peer->lastReceived,
            /*num_peers_required=*/queueState_.majority_size_,
            /*replica_types=*/kVoterReplicas,
            /*who_caused=*/peer,
            /*considered_peers=*/considered_peers);

        if (is_joint_consensus_phase) {
          // Get the size of a simple majority for voters in next config's peers
          int32_t num_new_voter_peers = 0;
          for (const RaftPeerPB& peer_pb : considered_next_peers) {
            if (peer_pb.member_type() == RaftPeerPB::VOTER) {
              num_new_voter_peers++;
            }
          }

          int64_t next_peers_curr_majority_rpl_idx =
              queueState_.majority_replicated_index;
          AdvanceQueueWatermark(
              /*type=*/"majority_replicated",
              /*watermark=*/&next_peers_curr_majority_rpl_idx,
              /*replicated_before=*/prev_last_received,
              /*replicated_after=*/peer->lastReceived,
              /*num_peers_required=*/majoritySize(num_new_voter_peers),
              /*replica_types=*/kVoterReplicas,
              /*who_caused=*/peer,
              /*considered_peers=*/considered_next_peers);

          VLOG_WITH_PREFIX_UNLOCKED(2)
              << "Joint-consensus watermark calculation is about to update "
              << "majority watermark from "
              << queueState_.majority_replicated_index
              << "into min(C_old=" << curr_majority_rpl_idx << ", "
              << "C_new=" << next_peers_curr_majority_rpl_idx << ")";
          queueState_.majority_replicated_index =
              std::min(curr_majority_rpl_idx, next_peers_curr_majority_rpl_idx);
        } else {
          queueState_.majority_replicated_index = curr_majority_rpl_idx;
        }

      } else if (
          peer->lastReceived.index() > queueState_.majority_replicated_index ||
          peer->lastExchangeStatus != PeerStatus::Ok) {
        // Here, Flexiraft is enabled.
        //
        // This method is expensive. The 'watermark' can change only if this
        // peer's last received index is higer than the current
        // majority_replicated_index. We also call this method when the
        // last_exhange_status of the peer indicates an error. This is because
        // 'majority_replicated_index' can go down. It sould be safe to
        // completely skip calling this method when 'last_exhange_status' is an
        // error, but we do not want to introduce a behavior change at this
        // point. Check AdvanceQueueWatermark() for more comments
        AdvanceMajorityReplicatedWatermarkFlexiRaft(
            &queueState_.majority_replicated_index,
            /*replicated_before=*/prev_last_received,
            /*replicated_after=*/peer->lastReceived,
            peer);

        if (is_joint_consensus_phase) {
          // TODO(fadhil): Handle joint-consensus watermark calculation when
          // flexiraft is enabled.
          LOG(FATAL) << "Joint-consensus reconfiguration is not yet "
                     << "supported when Flexiraft is enabled";
        }
      }

      old_all_replicated_index = queueState_.all_replicated_index;

      // Advance the all replicated index.
      if (is_joint_consensus_phase) {
        auto considered_old_new_peers =
            ranges::views::concat(considered_peers, considered_next_peers);
        int32_t num_all_peers =
            (int32_t)std::ranges::distance(considered_old_new_peers);
        AdvanceQueueWatermark(
            /*type=*/"all_replicated",
            /*watermark=*/&queueState_.all_replicated_index,
            /*replicated_before=*/prev_last_received,
            /*replicated_after=*/peer->lastReceived,
            /*num_peers_required=*/num_all_peers,
            /*replica_types=*/kAllReplicas,
            /*who_caused=*/peer,
            /*considered_peers=*/considered_old_new_peers);
      } else {
        int32_t num_all_peers = (int32_t)peersMap_.size();
        AdvanceQueueWatermark(
            /*type=*/"all_replicated",
            /*watermark=*/&queueState_.all_replicated_index,
            /*replicated_before=*/prev_last_received,
            /*replicated_after=*/peer->lastReceived,
            /*num_peers_required=*/num_all_peers,
            /*replica_types=*/kAllReplicas,
            /*who_caused=*/peer,
            /*considered_peers=*/considered_peers);
      }

      new_all_replicated_index = queueState_.all_replicated_index;

      // If the majority-replicated index is in our current term,
      // and it is above our current committed index, then
      // we can advance the committed index.
      //
      // It would seem that the "it is above our current committed index"
      // check is redundant (and could be a CHECK), but in fact the
      // majority-replicated index can currently go down, since we don't
      // consider peers whose last contact was an error in the watermark
      // calculation. See the TODO in AdvanceQueueWatermark() for more details.
      int64_t commit_index_before = queueState_.committed_index;
      if (queueState_.first_index_in_current_term &&
          queueState_.majority_replicated_index >=
              queueState_.first_index_in_current_term &&
          queueState_.majority_replicated_index > queueState_.committed_index) {
        queueState_.committed_index = queueState_.majority_replicated_index;

        if (FLAGS_enable_raft_leader_lease && response.has_lease_granted()) {
          // Check for Quorum of lease renewal approvals from followers
          QuorumResults qresults;
          if (CanLeaderLeaseRenewUnlocked(qresults)) {
            leaderLeaseUntil_.store(
                std::max(
                    leaderLeaseUntil_.load(),
                    GetQuorumMajorityOfPeerRpcStarts(qresults) +
                        leaderLeaseTimeout()));
          }
        }

        if (FLAGS_enable_bounded_dataloss_window) {
          // Check for Vote Quorum of Bounded DataLoss ACKs from followers
          QuorumResults qresults;
          if (CanBoundedDataLossWindowRenewUnlocked(qresults)) {
            boundedDatalossWindowUntil_.store(
                std::max(
                    boundedDatalossWindowUntil_.load(),
                    GetMaximumOfPeerRpcStarts(qresults) +
                        boundedDataLossDefaultWindowInMsec()));
          }
        }
      } else {
        VLOG_WITH_PREFIX_UNLOCKED(2)
            << "Cannot advance commit index, waiting for > "
            << "first index in current leader term: "
            << queueState_.first_index_in_current_term.value_or(-1) << ". "
            << "current majority_replicated_index: "
            << queueState_.majority_replicated_index << ", "
            << "current committed_index: " << queueState_.committed_index;
      }

      // Once the commit index has been updated, go ahead and update the
      // region_durable_index
      AdvanceQueueRegionDurableIndex();

      // Only notify observers if the commit index actually changed.
      if (mode_copy == LEADER &&
          queueState_.committed_index != commit_index_before) {
        DCHECK_GT(queueState_.committed_index, commit_index_before);
        updated_commit_index = queueState_.committed_index;
        VLOG_WITH_PREFIX_UNLOCKED(2)
            << "Commit index advanced from " << commit_index_before << " to "
            << *updated_commit_index;
      }
    }

    // If the peer's committed index is lower than our own, or if our log has
    // the next request for the peer, set 'sendMoreImmediately' to true.
    sendMoreImmediately =
        peer->lastKnownCommittedIndex < queueState_.committed_index ||
        log_cache_->hasOpBeenWritten(peer->nextIndex);

    // Evict ops from log_cache only if:
    // 1. This is not a leader node OR
    // 2. 'all_replicated_index' has changed after processing this response
    if (mode_copy != LEADER ||
        (old_all_replicated_index != new_all_replicated_index)) {
      log_cache_->evictThroughOp(queueState_.all_replicated_index);
    }

    UpdateMetricsUnlocked();
  }

  return sendMoreImmediately;
}

MonoTime PeerMessageQueue::GetQuorumMajorityOfPeerRpcStarts(
    QuorumResults& qresults) {
  MonoTime result = MonoTime::Min();
  std::vector<MonoTime> rpc_starts;
  rpc_starts.reserve(qresults.quorum_peers.size());
  for (const TrackedPeer* peer : qresults.quorum_peers) {
    rpc_starts.emplace_back(peer->rpcStart);
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
    rpc_starts.emplace_back(peer->rpcStart);
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

PeerMessageQueue::TrackedPeer PeerMessageQueue::getTrackedPeerForTests(
    const string& uuid) {
  std::lock_guard<simple_mutexlock> scoped_lock(queueLock_);
  auto it = peersMap_.find(uuid);
  CHECK(it != peersMap_.end()) << "Map key not found: " << uuid;
  TrackedPeer* tracked = it->second;
  return *tracked;
}

PeerMessageQueue::TrackedPeer* PeerMessageQueue::getTrackedPeerRefForTests(
    const std::string& uuid) {
  std::lock_guard<simple_mutexlock> scoped_lock(queueLock_);
  auto it = peersMap_.find(uuid);
  CHECK(it != peersMap_.end()) << "Map key not found: " << uuid;
  return it->second;
}

std::optional<bool> PeerMessageQueue::IsPeerInLocalRegion(
    const std::string& uuid) {
  std::lock_guard<simple_mutexlock> scoped_lock(queueLock_);
  auto it = peersMap_.find(uuid);
  CHECK(it != peersMap_.end()) << "Map key not found: " << uuid;
  TrackedPeer* tracked = it->second;
  if (tracked) {
    return tracked->isPeerInLocalRegion;
  }
  return std::nullopt;
}

bool PeerMessageQueue::CanLeaderLeaseRenewUnlocked(QuorumResults& qresults) {
  DCHECK(queueLock_.is_locked());
  string local_uuid = localPeerPb_.permanent_uuid();
  auto results =
      IsQuorumSatisfiedUnlocked(localPeerPb_, [this, &local_uuid](auto peer) {
        // Check for Leader
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid) {
          return true;
        }
        return peer->leaseGranted.index() >= queueState_.committed_index;
      });

  STATS_availableLeaderLeaseGrantors.addValue(
      results.num_satisfied, KUDU_STATS_TAG);

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
  DCHECK(queueLock_.is_locked());
  string local_uuid = localPeerPb_.permanent_uuid();
  auto results =
      IsSecondRegionDurabilitySatisfiedUnlocked([this, &local_uuid](auto peer) {
        // Check for the Leader
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid) {
          return true;
        }
        return peer->boundedDatalossWindowAcked.index() >=
            queueState_.committed_index;
      });

  STATS_availableBoundedDatalossWindowAckers.addValue(
      results.num_satisfied, KUDU_STATS_TAG);

  if (!results.quorum_satisfied) {
    LOG(WARNING) << "Bounded Data Loss window lease granted, quorum failed. "
                 << results.quorum_size << " is required lease grant quorum. "
                 << results.num_satisfied << " peers grants are healthy.";
    return false;
  }
  qresults = std::move(results);
  return true;
}

int64_t PeerMessageQueue::getAllReplicatedIndex() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.all_replicated_index;
}

int64_t PeerMessageQueue::getCommittedIndex() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.committed_index;
}

int64_t PeerMessageQueue::getRegionDurableIndex() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.region_durable_index;
}

bool PeerMessageQueue::isCommittedIndexInCurrentTerm() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.first_index_in_current_term.has_value() &&
      queueState_.committed_index >= *queueState_.first_index_in_current_term;
}

bool PeerMessageQueue::isInLeaderMode() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.mode == Mode::LEADER;
}

int64_t PeerMessageQueue::getMajorityReplicatedIndexForTests() const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return queueState_.majority_replicated_index;
}

void PeerMessageQueue::UpdateMetricsUnlocked() {
  DCHECK(queueLock_.is_locked());
  // Since operations have consecutive indices we can update the metrics based
  // on simple index math.
  // For non-leaders, majority_done_ops isn't meaningful because followers don't
  // track when an op is replicated to all peers.
  auto majorityDoneOpsVal = queueState_.mode == LEADER
      ? queueState_.committed_index - queueState_.all_replicated_index
      : 0;
  metrics_.num_majority_done_ops->setValue(
      majorityDoneOpsVal); // needed for tests
  STATS_majorityDoneOps.addValue(majorityDoneOpsVal, KUDU_STATS_TAG);
  auto inProgressOpsVal =
      queueState_.last_appended.index() - queueState_.committed_index;
  metrics_.num_in_progress_ops->setValue(inProgressOpsVal); // needed for tests
  STATS_inProgressOps.addValue(inProgressOpsVal, KUDU_STATS_TAG);

  UpdateLagMetricsUnlocked();
}

void PeerMessageQueue::UpdateLagMetricsUnlocked() {
  DCHECK(queueLock_.is_locked());
  auto opsBehindVal = queueState_.mode == LEADER
      ? 0
      : queueState_.last_idx_appended_to_leader -
          queueState_.last_appended.index();
  metrics_.num_ops_behind_leader->setValue(opsBehindVal); // needed for tests
  STATS_opsBehindLeader.addValue(opsBehindVal, KUDU_STATS_TAG);
}

void PeerMessageQueue::DumpToStrings(vector<string>* lines) const {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  DumpToStringsUnlocked(lines);
}

void PeerMessageQueue::DumpToStringsUnlocked(vector<string>* lines) const {
  DCHECK(queueLock_.is_locked());
  lines->push_back("Watermarks:");
  for (const PeersMap::value_type& entry : peersMap_) {
    lines->push_back(
        fmt::format(
            "Peer: {} Watermark: {}", entry.first, entry.second->ToString()));
  }

  log_cache_->dumpToStrings(lines);
}

void PeerMessageQueue::ClearUnlocked() {
  DCHECK(queueLock_.is_locked());
  // TODO(modernization): Consider std::unordered_map<std::string,
  // std::unique_ptr<TrackedPeer>>
  for (auto& entry : peersMap_) {
    delete entry.second;
  }
  peersMap_.clear();
  queueState_.state = kQueueClosed;
}

void PeerMessageQueue::Close() {
  raftPoolObserversToken_->Shutdown();

  std::lock_guard<simple_mutexlock> lock(queueLock_);
  ClearUnlocked();
  // Reset here to appease folly::Singleton's check for leaky references
  timeProvider_.reset();
}

int64_t PeerMessageQueue::getQueuedOperationsSizeBytesForTests() const {
  return log_cache_->bytesUsed();
}

string PeerMessageQueue::ToString() const {
  // Even though metrics are thread-safe obtain the lock so that we get
  // a "consistent" snapshot of the metrics.
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  return ToStringUnlocked();
}

string PeerMessageQueue::ToStringUnlocked() const {
  DCHECK(queueLock_.is_locked());
  return fmt::format(
      "Consensus queue metrics: "
      "Only Majority Done Ops: {}, In Progress Ops: {}, Cache: {}",
      metrics_.num_majority_done_ops->value(),
      metrics_.num_in_progress_ops->value(),
      log_cache_->statsString());
}

void PeerMessageQueue::RegisterObserver(PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    observers_.push_back(observer);
  }
}

Status PeerMessageQueue::UnRegisterObserver(
    PeerMessageQueueObserver* observer) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  auto iter = std::find(observers_.begin(), observers_.end(), observer);
  if (iter == observers_.end()) {
    return Status::NotFound("Can't find observer.");
  }
  observers_.erase(iter);
  return Status::OK();
}

bool PeerMessageQueue::IsOpInLog(const OpId& desired_op) const {
  OpId log_op;
  Status s = log_cache_->lookupOpId(desired_op.index(), &log_op);
  if (PREDICT_TRUE(s.ok())) {
    return OpIdEquals(desired_op, log_op);
  }
  if (PREDICT_TRUE(s.IsNotFound() || s.isIncomplete())) {
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
      observer->notifyCommitIndex(new_commit_index, need_lock);
    });
    return;
  }
  // NOTE: if we're scheduling this to run async we always need to lock, so we
  // ignore the needs_lock param
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->notifyCommitIndex(new_commit_index, true);
          })),
      logPrefixUnlocked() +
          "Unable to notify RaftConsensus of commit index change.");
}

void PeerMessageQueue::NotifyObserversOfTermChange(int64_t term) {
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->notifyTermChange(term);
          })),
      logPrefixUnlocked() + "Unable to notify RaftConsensus of term change.");
}

void PeerMessageQueue::NotifyObserversOfFailedFollower(
    const string& uuid,
    int64_t term,
    const string& reason) {
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->notifyFailedFollower(uuid, term, reason);
          })),
      logPrefixUnlocked() +
          "Unable to notify RaftConsensus of abandoned follower.");
}

void PeerMessageQueue::NotifyObserversOfPeerToPromote(const string& peer_uuid) {
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [=](PeerMessageQueueObserver* observer) {
            observer->notifyPeerToPromote(peer_uuid);
          })),
      logPrefixUnlocked() +
          "Unable to notify RaftConsensus of peer to promote.");
}

void PeerMessageQueue::NotifyObserversOfSuccessor(const string& peer_uuid) {
  DCHECK(queueLock_.is_locked());
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [=, transfer_context = std::move(transferContext_)](
              PeerMessageQueueObserver* observer) mutable {
            observer->notifyPeerToStartElection(
                peer_uuid,
                std::move(transfer_context),
                /*promise=*/nullptr,
                /*mockElectionSnapshotOpId=*/std::nullopt);
          })),
      logPrefixUnlocked() +
          "Unable to notify RaftConsensus of available successor.");
  successorWatchPeerNotified_ = true;
  transferContext_ = {};
}

Status PeerMessageQueue::GetSnapshotForMockElection(
    const std::string& new_leader_uuid,
    OpId* snapshot_op_id) {
  std::unique_lock<simple_mutexlock> l(queueLock_);

  auto it = peersMap_.find(new_leader_uuid);
  // Validate peer exists and has non-null value.
  if (PREDICT_FALSE(it == peersMap_.end() || it->second == nullptr)) {
    return Status::IllegalState("Target peer is not tracked.");
  }
  TrackedPeer* peer = it->second;

  RaftPeerPB* peer_pb = nullptr;
  if (!BasicChecksOKToTransferAndGetPeerUnlocked(*peer, &peer_pb)) {
    return Status::IllegalState("Failed basic leadership transfer checks.");
  }

  *snapshot_op_id = queueState_.last_appended;

  return Status::OK();
}

void PeerMessageQueue::NotifyObserversOfPeerHealthChange() {
  WARN_NOT_OK(
      raftPoolObserversToken_->submitClosure(Bind(
          &PeerMessageQueue::NotifyObserversTask,
          unretained(this),
          [](PeerMessageQueueObserver* observer) {
            observer->notifyPeerHealthChange();
          })),
      logPrefixUnlocked() +
          "Unable to notify RaftConsensus peer health change.");
}

void PeerMessageQueue::NotifyObserversTask(
    const std::function<void(PeerMessageQueueObserver*)>& func) {
  MAYBE_INJECT_RANDOM_LATENCY(
      FLAGS_consensus_inject_latency_ms_in_notifications);
  std::vector<PeerMessageQueueObserver*> observers_copy;
  {
    std::lock_guard<simple_mutexlock> lock(queueLock_);
    observers_copy = observers_;
  }
  for (PeerMessageQueueObserver* observer : observers_copy) {
    func(observer);
  }
}

PeerMessageQueue::~PeerMessageQueue() {
  Close();
}

string PeerMessageQueue::logPrefixUnlocked() const {
  // TODO: we should probably use an atomic here. We'll just annotate
  // away the TSAN error for now, since the worst case is a slightly out-of-date
  // log message, and not very likely.
  Mode mode = KUDU_ANNONTATE_UNPROTECTED_READ(queueState_.mode);
  return fmt::format(
      "T {} P {} [{}]: ",
      tabletId_,
      localPeerPb_.permanent_uuid(),
      mode == LEADER ? "LEADER" : "NON_LEADER");
}

string PeerMessageQueue::QueueState::ToString() const {
  return fmt::format(
      "All replicated index: {}, Majority replicated index: {}, "
      "Committed index: {}, Last appended: {}, Last appended by leader: {}, Current term: {}, "
      "Majority size: {}, State: {}, Mode: {}{}",
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
  return getQuorumId(peer, queueState_.active_config->commit_rule());
}

bool PeerMessageQueue::CheckQuorum() {
  std::lock_guard<simple_mutexlock> lock(queueLock_);

  // We only check quorum if we're a leader.
  if (queueState_.mode != LEADER) {
    return true;
  }

  // We only support check quorum for Single Region Dynamic for now.
  if (queueState_.active_config->commit_rule().mode() !=
      QuorumMode::SINGLE_REGION_DYNAMIC) {
    return true;
  }

  STATS_checkQuorumRuns.add(1, KUDU_STATS_TAG);

  vector<string> unhealthy_peers;
  string local_uuid = localPeerPb_.permanent_uuid();
  QuorumResults results = IsQuorumSatisfiedUnlocked(
      localPeerPb_, [&local_uuid, &unhealthy_peers](auto peer) {
        const string& peer_uuid = peer->uuid();
        if (peer_uuid == local_uuid || peer->isHealthy()) {
          return true;
        }
        unhealthy_peers.push_back(peer_uuid);
        return false;
      });

  STATS_availableCommitPeers.addValue(results.num_satisfied, KUDU_STATS_TAG);

  if (!results.quorum_satisfied) {
    STATS_checkQuorumFailures.add(1, KUDU_STATS_TAG);
    LOG(WARNING) << "Check quorum failed. " << results.quorum_size
                 << " is required commit quorum. " << results.num_satisfied
                 << " peers are healthy. " << unhealthy_peers.size()
                 << " peers have failed: "
                 << JoinStrings(unhealthy_peers, ", ");
  }
  return results.quorum_satisfied;
}

void PeerMessageQueue::updatePeerForTests(
    const std::string& peer_uuid,
    const std::function<void(TrackedPeer*)>& fn) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  auto it = peersMap_.find(peer_uuid);
  TrackedPeer* peer = (it != peersMap_.end()) ? it->second : nullptr;
  CHECK(peer);
  fn(peer);
}

bool PeerMessageQueue::RegionHasQuorumCommitUnlocked(
    const RaftPeerPB& target_peer) {
  DCHECK(queueLock_.is_locked());

  // If we don't have VD, we assume this is vanilla raft and we don't need to
  // check if peer region has majority.
  if (!FLAGS_enable_flexi_raft) {
    return true;
  }

  QuorumResults results = IsQuorumSatisfiedUnlocked(
      target_peer, [](auto peer) { return peer->isHealthy(); });
  return results.quorum_satisfied;
}

int32_t PeerMessageQueue::GetAvailableCommitPeers() {
  std::lock_guard<simple_mutexlock> lock(queueLock_);

  // If we are not leader, we are not concerned about commit peers, hence, we
  // simply return 0.
  if (queueState_.mode != LEADER) {
    return -1;
  }

  // We only support getting available commit peers for Single Region Dynamic
  // for now.
  if (queueState_.active_config->commit_rule().mode() !=
      QuorumMode::SINGLE_REGION_DYNAMIC) {
    return -1;
  }

  QuorumResults results = IsQuorumSatisfiedUnlocked(
      localPeerPb_, [](auto peer) { return peer->isHealthy(); });
  return results.num_satisfied;
}

Status PeerMessageQueue::GetQuorumHealthForFlexiRaftUnlocked(
    QuorumHealth* health) const {
  CHECK(health);
  DCHECK(queueLock_.is_locked());
  std::unordered_multimap<std::string, TrackedPeer*> by_quorum_id;
  std::unordered_set<std::string> quorum_ids;

  for (const PeersMap::value_type& entry : peersMap_) {
    auto* peer = entry.second;
    // We only include voters.
    if (peer->peerPb.has_member_type() &&
        peer->peerPb.member_type() == RaftPeerPB::VOTER) {
      const std::string quorum_id = getQuorumIdUsingCommitRule(peer->peerPb);
      by_quorum_id.insert(std::make_pair(quorum_id, peer));
      quorum_ids.insert(quorum_id);
    }
  }

  const std::string& leader_quorum_id =
      getQuorumIdUsingCommitRule(localPeerPb_);

  for (const auto& quorum_id : quorum_ids) {
    QuorumIdHealth quorum_id_health;

    quorum_id_health.primary = leader_quorum_id == quorum_id;

    quorum_id_health.numVdVoters = getTotalVotersFromVoterDistribution(
                                       *(queueState_.active_config), quorum_id)
                                       .value_or(0);
    quorum_id_health.quorumSize = majoritySize(quorum_id_health.numVdVoters);

    auto range = by_quorum_id.equal_range(quorum_id);
    for (auto it = range.first; it != range.second; it++) {
      auto* peer = it->second;
      if (peer->isHealthy()) {
        quorum_id_health.healthyPeers.push_back(peer->peerPb);
      } else {
        quorum_id_health.unhealthyPeers.push_back(peer->peerPb);
      }
    }

    const int num_healthy =
        static_cast<int>(quorum_id_health.healthyPeers.size());
    if (num_healthy < quorum_id_health.quorumSize) {
      quorum_id_health.healthStatus = kUnhealthy;
    } else if (num_healthy == quorum_id_health.quorumSize) {
      quorum_id_health.healthStatus = kAtRisk;
    } else if (num_healthy >= quorum_id_health.numVdVoters) {
      quorum_id_health.healthStatus = kHealthy;
    } else {
      quorum_id_health.healthStatus = kDegraded;
    }

    quorum_id_health.totalVoters = static_cast<int>(
        quorum_id_health.healthyPeers.size() +
        quorum_id_health.unhealthyPeers.size());
    health->byQuorumId.emplace(quorum_id, std::move(quorum_id_health));
  }
  return Status::OK();
}

Status PeerMessageQueue::GetQuorumHealthForVanillaRaftUnlocked(
    QuorumHealth* health) const {
  CHECK(health);
  DCHECK(queueLock_.is_locked());
  const RaftConfigPB* curr_config = queueState_.active_config.get();
  bool is_joint_consensus_mode = isJointConsensusPhase(*curr_config);

  // Gather the considered peers from the active config.
  std::vector<RaftPeerPB> considered_voter_peers;
  for (const RaftPeerPB& peer_pb : curr_config->peers()) {
    if (peer_pb.has_member_type() &&
        peer_pb.member_type() == RaftPeerPB::VOTER) {
      considered_voter_peers.push_back(peer_pb);
    }
  }
  std::vector<RaftPeerPB> considered_next_voter_peers;
  if (is_joint_consensus_mode) {
    for (const RaftPeerPB& peer_pb : curr_config->next_config_peers()) {
      if (peer_pb.has_member_type() &&
          peer_pb.member_type() == RaftPeerPB::VOTER) {
        considered_next_voter_peers.push_back(peer_pb);
      }
    }
  }

  // Populate the quorum health.
  PopulateQuorumIdHealthUnlocked(
      considered_voter_peers, kVanillaRaftQuorumId, &(health->byQuorumId));
  if (is_joint_consensus_mode) {
    PopulateQuorumIdHealthUnlocked(
        considered_next_voter_peers,
        kVanillaRaftQuorumId,
        &(health->nextConfigQuorumHealth));
  }

  return Status::OK();
}

void PeerMessageQueue::PopulateQuorumIdHealthUnlocked(
    const std::vector<RaftPeerPB>& considered_voter_peers,
    const std::string& leader_quorum_id,
    std::unordered_map<std::string, QuorumIdHealth>* quorum_id_health) const {
  CHECK(quorum_id_health);
  DCHECK(queueLock_.is_locked());
  quorum_id_health->clear();

  // Group Peers by their QuorumID, having QuorumID as the key and a
  // list of tracked peers as the value. Also, gather all the QuorumIDs.
  std::unordered_set<std::string> quorum_ids;
  std::unordered_multimap<std::string, const TrackedPeer*> peers_by_qid;
  std::unordered_map<std::string, const RaftPeerPB*> peer_pb_by_uuid;
  for (const RaftPeerPB& peer_pb : considered_voter_peers) {
    const std::string& peer_uuid = peer_pb.permanent_uuid();
    peer_pb_by_uuid.emplace(peer_uuid, &peer_pb);
    // Vanilla Raft does not have QuorumID, and instead use the default
    // kVanillaRaftQuorumID.
    std::string quorum_id = (leader_quorum_id == kVanillaRaftQuorumId)
        ? kVanillaRaftQuorumId
        : getQuorumIdUsingCommitRule(peer_pb);
    quorum_ids.insert(quorum_id);
    auto it = peersMap_.find(peer_uuid);
    const TrackedPeer* peer = (it != peersMap_.end()) ? it->second : nullptr;
    if (!peer) {
      LOG_WITH_PREFIX_UNLOCKED(ERROR)
          << "PopulateQuorumIdHealth: Peer " << peer_pb.permanent_uuid()
          << " is considered but not yet tracked,"
          << " making the health status unknown.";
      continue;
    }
    peers_by_qid.insert(std::make_pair(quorum_id, peer));
  }

  // Infer the health status for each QuorumID. For vanilla Raft, there is only
  // a single QuorumID, the default kVanillaRaftQuorumID.
  for (const auto& quorum_id : quorum_ids) {
    QuorumIdHealth health_detail;

    // Gather all the health and unhealthy peeers in this QuorumID.
    auto range = peers_by_qid.equal_range(quorum_id);
    for (auto it = range.first; it != range.second; it++) {
      const TrackedPeer* peer = it->second;
      const std::string& peer_uuid = peer->uuid();
      auto peer_pb_it = peer_pb_by_uuid.find(peer_uuid);
      const RaftPeerPB* peer_pb =
          (peer_pb_it != peer_pb_by_uuid.end()) ? peer_pb_it->second : nullptr;
      CHECK(peer_pb) << fmt::format(
          "Expecting non-null RaftPeerPB with uuid {}.", peer_uuid);
      if (peer->isHealthy()) {
        health_detail.healthyPeers.push_back(*peer_pb);
      } else {
        health_detail.unhealthyPeers.push_back(*peer_pb);
      }
    }

    // Populate other metadata for this QuorumID.
    health_detail.primary = (leader_quorum_id == quorum_id);
    health_detail.totalVoters = (int)(health_detail.healthyPeers.size() +
                                      health_detail.unhealthyPeers.size());
    health_detail.quorumSize = majoritySize(health_detail.totalVoters);
    if (leader_quorum_id == kVanillaRaftQuorumId) {
      // Voter distribution is not used for VanillaRaft, we use total voters.
      health_detail.numVdVoters = health_detail.totalVoters;
    } else {
      health_detail.numVdVoters = getTotalVotersFromVoterDistribution(
                                      *queueState_.active_config, quorum_id)
                                      .value_or(0);
    }

    // Infer the health status for this QuorumID.
    const int num_healthy = static_cast<int>(health_detail.healthyPeers.size());
    health_detail.healthStatus = InferQuorumIdHealthStatus(
        num_healthy, health_detail.quorumSize, health_detail.numVdVoters);

    quorum_id_health->emplace(quorum_id, std::move(health_detail));
  }
}

PeerMessageQueue::QuorumIdHealthStatus
PeerMessageQueue::InferQuorumIdHealthStatus(
    int num_healthy_voters,
    int majority_size,
    int num_total_voters) {
  if (num_healthy_voters < majority_size) {
    return kUnhealthy;
  } else if (num_healthy_voters == majority_size) {
    return kAtRisk;
  } else if (num_healthy_voters >= num_total_voters) {
    return kHealthy;
  } else {
    // majority_size < num_healthy_voters < num_total_voters
    return kDegraded;
  }
}

Status PeerMessageQueue::GetQuorumHealth(QuorumHealth* health) const {
  CHECK(health);
  std::lock_guard<simple_mutexlock> lock(queueLock_);

  // Only leaders can provide quorum health.
  if (queueState_.mode != LEADER) {
    return Status::OK();
  }

  if (FLAGS_enable_flexi_raft) {
    return GetQuorumHealthForFlexiRaftUnlocked(health);
  }
  return GetQuorumHealthForVanillaRaftUnlocked(health);
}

Status PeerMessageQueue::getAllStateMachineMetrics(
    AllStateMachineMetrics* output) {
  CHECK(output);
  std::lock_guard<simple_mutexlock> lock(queueLock_);

  // Only leaders can provide quorum health.
  if (queueState_.mode != LEADER) {
    return Status::OK();
  }

  for (const PeersMap::value_type& entry : peersMap_) {
    auto* peer = entry.second;
    // Skip server without state machine metrics
    if (!isBackingDbPresent(peer->peerPb)) {
      continue;
    }

    // Skip server is in standby mode
    if (isStandbyMember(peer->peerPb)) {
      continue;
    }

    // Skip server metrics for leader itself
    if (localPeerPb_.permanent_uuid() == peer->uuid()) {
      continue;
    }

    output->push_back(
        RaftStateMachineMetrics(peer->peerPb, peer->stateMachineMetrics));
  }
  return Status::OK();
}

bool PeerMessageQueue::IsStateMachineHealthyForElectionUnlock(
    const StateMachineMetricsPB& metrics,
    std::optional<int> seconds_behind_master_threshold) {
  int threshold = seconds_behind_master_threshold.value_or(
      FLAGS_candidate_max_seconds_behind_master_threshold);

  if (threshold == 0) {
    return true;
  }

  return metrics.has_seconds_behind_master() &&
      metrics.seconds_behind_master() >= 0 &&
      metrics.seconds_behind_master() <= threshold;
}

bool PeerMessageQueue::IsStateMachineHealthyForElection(
    const std::string& candidate_uuid,
    std::optional<int> seconds_behind_master_threshold) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  auto it = peersMap_.find(candidate_uuid);
  // Safe extraction: returns nullptr if peer not found or has null value.
  TrackedPeer* peer = (it != peersMap_.end()) ? it->second : nullptr;
  if (peer == nullptr) {
    LOG(ERROR) << "Could not find peer " << candidate_uuid;
    return false;
  }

  if (!isBackingDbPresent(peer->peerPb)) {
    LOG(INFO) << "Skipping candidate statemachine check for " << candidate_uuid
              << " as it does not have a backing state machine.";
    return true;
  }

  return IsStateMachineHealthyForElectionUnlock(
      peer->stateMachineMetrics, seconds_behind_master_threshold);
}

bool PeerMessageQueue::isHealthyStateMachineForElectionPresent(
    std::optional<int> seconds_behind_master_threshold) {
  std::lock_guard<simple_mutexlock> lock(queueLock_);
  for (const PeersMap::value_type& entry : peersMap_) {
    TrackedPeer* peer = entry.second;

    // Skip server without state machine metrics and skip non_voter
    if (!isBackingDbPresent(peer->peerPb) || isStandbyMember(peer->peerPb) ||
        peer->peerPb.member_type() != RaftPeerPB::VOTER) {
      continue;
    }

    // Skip leader itself
    if (localPeerPb_.permanent_uuid() == peer->uuid()) {
      continue;
    }

    if (IsStateMachineHealthyForElectionUnlock(
            peer->stateMachineMetrics, seconds_behind_master_threshold)) {
      return true;
    }
  }

  return false;
}
} // namespace kudu::consensus
