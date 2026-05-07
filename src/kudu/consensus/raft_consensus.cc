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

#include "kudu/consensus/raft_consensus.h"

#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/ScopeGuard.h>
#include <gflags/gflags.h>
#include <google/protobuf/util/message_differencer.h>
#include <sys/stat.h>
#include <optional>

#include <fb303/ExportType.h>
#include <fb303/QuantileStat.h>
#include <fb303/ThreadCachedServiceData.h>
#include <fb303/Timeseries.h>
#include <fb303/detail/QuantileStatWrappers.h>
#include <fmt/core.h>
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/peer_manager.h"
#include "kudu/consensus/pending_rounds.h"
#include "kudu/consensus/persistent_vars.h"
#include "kudu/consensus/persistent_vars_manager.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/periodic.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/DCHECKProd.h"
#include "kudu/util/async_util.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DEFINE_double(
    leader_failure_max_missed_heartbeat_periods,
    3.0,
    "Maximum heartbeat periods that the leader can fail to heartbeat in before we "
    "consider the leader to be failed. The total failure timeout in milliseconds is "
    "raft_heartbeat_interval_ms times leader_failure_max_missed_heartbeat_periods. "
    "The value passed to this flag may be fractional.");
TAG_FLAG(leader_failure_max_missed_heartbeat_periods, advanced);

DEFINE_double(
    snooze_for_leader_ban_ratio,
    1.0,
    "Failure detector for this instance should be at a higher ratio than other instances"
    ". This will prevent this instance from initiating an election");
TAG_FLAG(snooze_for_leader_ban_ratio, advanced);

DEFINE_int32(
    leader_failure_exp_backoff_max_delta_ms,
    20 * 1000,
    "Maximum time to sleep in between leader election retries, in addition to the "
    "regular timeout. When leader election fails the interval in between retries "
    "increases exponentially, up to this value.");
TAG_FLAG(leader_failure_exp_backoff_max_delta_ms, experimental);

DEFINE_double(
    update_replica_snooze_heartbeat_periods,
    20.0,
    "Heartbeat periods to snooze the failure detector while we process an "
    "append. A large append can take a while, and we shouldn't try to start "
    "elections meanwhile. We don't want to snooze forever though, so this var "
    "controls the time we snooze. Snooze timer is reset when update is done");

DEFINE_bool(
    enable_leader_failure_detection,
    true,
    "Whether to enable failure detection of tablet leaders. If enabled, attempts will be "
    "made to elect a follower as a new leader when the leader is detected to have failed.");
TAG_FLAG(enable_leader_failure_detection, unsafe);

DEFINE_bool(
    evict_failed_followers,
    true,
    "Whether to evict followers from the Raft config that have fallen "
    "too far behind the leader's log to catch up normally or have been "
    "unreachable by the leader for longer than "
    "follower_unavailable_considered_failed_sec");
TAG_FLAG(evict_failed_followers, advanced);

DEFINE_bool(
    follower_reject_update_consensus_requests,
    false,
    "Whether a follower will return an error for all UpdateConsensus() requests. "
    "Warning! This is only intended for testing.");
TAG_FLAG(follower_reject_update_consensus_requests, unsafe);

DEFINE_bool(
    follower_fail_all_prepare,
    false,
    "Whether a follower will fail preparing all transactions. "
    "Warning! This is only intended for testing.");
TAG_FLAG(follower_fail_all_prepare, unsafe);

DEFINE_bool(
    raft_enable_pre_election,
    true,
    "When enabled, candidates will call a pre-election before "
    "running a real leader election.");
TAG_FLAG(raft_enable_pre_election, experimental);
TAG_FLAG(raft_enable_pre_election, runtime);

DEFINE_bool(
    raft_enable_tombstoned_voting,
    true,
    "When enabled, tombstoned tablets may vote in elections.");
TAG_FLAG(raft_enable_tombstoned_voting, experimental);
TAG_FLAG(raft_enable_tombstoned_voting, runtime);

DEFINE_bool(
    raft_enable_multi_hop_proxy_routing,
    false,
    "Enables multi-hop routing. When disabled, causes any tablet "
    "server acting as a proxy to forward any incoming replication "
    "request directly to the destination node, if it is part of the "
    "active config.");
TAG_FLAG(raft_enable_multi_hop_proxy_routing, advanced);
TAG_FLAG(raft_enable_multi_hop_proxy_routing, runtime);

DEFINE_int32(
    raft_log_cache_proxy_wait_time_ms,
    500,
    "Maximum wait time for proxied messages to wait for events to "
    "appear in the local log cache");
TAG_FLAG(raft_log_cache_proxy_wait_time_ms, advanced);
TAG_FLAG(raft_log_cache_proxy_wait_time_ms, runtime);

DECLARE_int32(memory_limit_warn_threshold_percentage);
DECLARE_int32(consensus_max_batch_size_bytes);
DEFINE_bool(
    track_removed_peers,
    true,
    "Should peers removed from the config be tracked for using it in RequestVote()");

DEFINE_bool(
    allow_multiple_backed_by_db_per_quorum,
    false,
    "Can multiple backed_by_db instances be added to the same quorum");

DEFINE_int32(
    lag_threshold_for_request_vote,
    -1,
    "The threshold beyond which a VOTER will not give votes to CANDIDATE. -1 to turn it OFF");

DEFINE_bool(
    notify_commit_index_after_response,
    true,
    "Should we notify peers of commit index after every response?");

DEFINE_int32(
    mock_elections_timeout_ms,
    5000,
    "Max time in milliseconds to wait for mock elections to complete before "
    "timing out");
TAG_FLAG(mock_elections_timeout_ms, advanced);

DEFINE_bool(check_quorum, false, "Enable check quorum");

DEFINE_bool(
    check_quorum_failure_callback,
    false,
    "Enable check quorum failure callback");

DEFINE_int32(
    check_quorum_cooldown_ms,
    300000,
    "Milliseconds to wait before running the check quorum failure callback "
    "again after a failure");

DEFINE_int32(
    check_quorum_interval_heartbeats,
    120,
    "Interval in multiples of heartbeats at which to check whether the leader "
    "can commit to a quorum number of nodes.");

DEFINE_bool(
    report_proxy_errors,
    true,
    "Whether to enable reporting of proxy errors to error manager.");

DEFINE_bool(
    allow_truncate_committed_log,
    false,
    "Raft should never truncate committed log so by default this flag should false. "
    "Make it true to restore availability over data durabitlity and consistency.");
TAG_FLAG(allow_truncate_committed_log, unsafe);

// Metrics
// ---------
METRIC_DEFINE_counter(
    server,
    raft_log_truncation_counter,
    "Log truncation count",
    kudu::MetricUnit::kRequests,
    "Number of times ops written to raft log were truncated "
    "as a result of the new leader overwriting ops");

METRIC_DEFINE_counter(
    server,
    follower_memory_pressure_rejections,
    "Follower Memory Pressure Rejections",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests rejected due to "
    "memory pressure while FOLLOWER.");
METRIC_DEFINE_gauge_int64(
    server,
    raft_term,
    "Current Raft Consensus Term",
    kudu::MetricUnit::kUnits,
    "Current Term of the Raft Consensus algorithm. This number increments "
    "each time a leader election is started.");
METRIC_DEFINE_gauge_int64(
    server,
    failed_elections_since_stable_leader,
    "Failed Elections Since Stable Leader",
    kudu::MetricUnit::kUnits,
    "Number of failed elections on this node since there was a stable "
    "leader. This number increments on each failed election and resets on "
    "each successful one.");

// Proxying metrics.
METRIC_DEFINE_counter(
    server,
    raft_proxy_num_requests_received,
    "Number of RPC requests received for proxying to another node",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests (not events) received for proxying to another node.");
METRIC_DEFINE_counter(
    server,
    raft_proxy_num_requests_success,
    "Number of RPC requests successfully proxied",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests (not events) delivered to the next hop without any "
    "problems. This may include requests where only a subset of events were "
    "delivered.");
METRIC_DEFINE_counter(
    server,
    raft_proxy_num_requests_unknown_dest,
    "Number of RPC requests failed due to unknown destination",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests received that could not be "
    "delivered because the destination node was unroutable.");
METRIC_DEFINE_counter(
    server,
    raft_proxy_num_requests_log_read_timeout,
    "Number of RPC requests degraded to heartbeats due to a log read timeout",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests received that were intended to be "
    "reconstituted and delivered to their ultimate "
    "destination, but due to a log read timeout, were "
    "gracefully degraded to a heartbeat. Use "
    "--raft_log_cache_proxy_wait_time_ms to control the log read timeout.");
METRIC_DEFINE_counter(
    server,
    raft_proxy_num_requests_hops_remaining_exhausted,
    "Number of RPC requests failed due to maximum hops exhausted",
    kudu::MetricUnit::kRequests,
    "Number of RPC requests received that were unable to be delivered due to "
    "exceeding the maximum allowable number of hops. This is usually due to "
    "either a routing loop or a misconfigured value for --raft_proxy_max_hops");

// Metrics ODS - definitions moved to kudu/util/Stats.cpp
// STATS calls use "raft_consensus" tag for ODS namespace prefix.

using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::PeriodicTimer;
// using kudu::ServerErrorPB;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using std::weak_ptr;

namespace kudu::consensus {

PeerMessageQueue::TransferContext ElectionContext::transferContext() const {
  if (isChainedElection) {
    return {chainedStartTime, sourceUuid, isOriginDeadPromotion};
  }
  return {startTime, currentLeaderUuid, isOriginDeadPromotion};
}

RaftConsensus::RaftConsensus(
    ConsensusOptions options,
    RaftPeerPB localPeerPb,
    std::shared_ptr<ConsensusMetadataManager> cmetaManager,
    std::shared_ptr<PersistentVarsManager> persistentVarsManager,
    ThreadPool* raftPool)
    : options_(std::move(options)),
      localPeerPb_(std::move(localPeerPb)),
      cmetaManager_(std::move(cmetaManager)),
      persistentVarsManager_(std::move(persistentVarsManager)),
      raftPool_(raftPool),
      state_(kNew),
      proxyPolicy_(options_.proxy_policy),
      proxyRegionGroups_(options_.proxy_region_groups),
      rng_(getRandomSeed32()),
      leaderTransferInProgress_(false),
      withholdVotesUntil_(MonoTime::Min()),
      leaderLeaseTerm_(-1),
      rejectAppendEntries_(false),
      adjustVoterDistribution_(true),
      withholdVotes_(false),
      lastReceivedCurLeader_(MinimumOpId()),
      failedElectionsSinceStableLeader_(0),
      failedElectionsCandidateNotInConfig_(0),
      leaderLeaseState_(LeaderLeaseState::kRenew),
      disableNoop_(false),
      shutdown_(false),
      updateCallsForTests_(0),
      checkQuorumIntervalHeartbeats_(FLAGS_check_quorum_interval_heartbeats) {
  DCHECK(localPeerPb_.has_permanent_uuid());
  DCHECK(cmetaManager_ != nullptr);
  DCHECK(persistentVarsManager_ != nullptr);
}

Status RaftConsensus::Init() {
  DCHECK_EQ(kNew, state_) << stateName(state_);
  RETURN_NOT_OK(cmetaManager_->loadCMeta(options_.tablet_id, &cmeta_));

  RETURN_NOT_OK(persistentVarsManager_->loadPersistentVars(
      options_.tablet_id, &persistentVars_));

  if (!persistentVars_->raftRpcToken()) {
    persistentVars_->setRaftRpcToken(options_.initial_raft_rpc_token);
    CHECK_OK(persistentVars_->flush());
  }

  // This is the part we reconcile voter_type and quorum_id. Both can be changed
  // after a modify-member. We need to load the source of truth from cmeta to
  // localPeerPb_
  for (const auto& peer : cmeta_->ActiveConfig().peers()) {
    if (peer.has_permanent_uuid() && localPeerPb_.has_permanent_uuid() &&
        peer.permanent_uuid() == localPeerPb_.permanent_uuid()) {
      if (peer.has_attrs()) {
        localPeerPb_.mutable_attrs()->CopyFrom(peer.attrs());
      }

      if (peer.has_member_type()) {
        localPeerPb_.set_member_type(peer.member_type());
      }
    }
  }

  // Durable routing table is persisted - hence better to manage it through
  // consensus_meta_manager.
  std::shared_ptr<DurableRoutingTable> drt;
  RETURN_NOT_OK(
      cmetaManager_->loadDrt(options_.tablet_id, cmeta_->ActiveConfig(), &drt));

  // Build the container which holds all available routing tables
  routingTableContainer_ = std::make_shared<RoutingTableContainer>(
      proxyPolicy_,
      localPeerPb_,
      cmeta_->ActiveConfig(),
      std::move(drt),
      proxyRegionGroups_);

  setStateUnlocked(kInitialized);
  return Status::OK();
}

RaftConsensus::~RaftConsensus() {
  shutdown();
}

Status RaftConsensus::Create(
    ConsensusOptions options,
    RaftPeerPB localPeerPb,
    std::shared_ptr<ConsensusMetadataManager> cmetaManager,
    std::shared_ptr<PersistentVarsManager> persistentVarsManager,
    ThreadPool* raftPool,
    shared_ptr<RaftConsensus>* consensusOut) {
  shared_ptr<RaftConsensus> consensus(
      RaftConsensus::makeShared(
          std::move(options),
          std::move(localPeerPb),
          std::move(cmetaManager),
          std::move(persistentVarsManager),
          raftPool));
  RETURN_NOT_OK_PREPEND(
      consensus->Init(), "Unable to initialize Raft consensus");
  *consensusOut = std::move(consensus);
  return Status::OK();
}

Status RaftConsensus::start(
    const std::shared_ptr<ConsensusBootstrapInfo>& info,
    unique_ptr<PeerProxyFactory> peerProxyFactory,
    std::shared_ptr<log::Log> log,
    std::shared_ptr<ITimeManager> timeManager,
    ConsensusRoundHandler* roundHandler,
    const std::shared_ptr<MetricEntity>& metricEntity,
    Callback<void(const std::string& reason)> markDirtyClbk) {
  DCHECK(metricEntity);
  CHECK(info);

  peerProxyFactory_ = std::move(peerProxyFactory);
  log_ = std::move(log);
  timeManager_ = std::move(timeManager);

  roundHandler_ = DCHECK_NOTNULL(roundHandler);
  markDirtyClbk_ = std::move(markDirtyClbk);

  DCHECK(peerProxyFactory_ != nullptr);
  DCHECK(log_ != nullptr);
  DCHECK(timeManager_ != nullptr);

  raftLogTruncationCounter_ =
      metricEntity->findOrCreateCounter(&METRIC_raft_log_truncation_counter);

  termMetric_ =
      metricEntity->findOrCreateGauge(&METRIC_raft_term, currentTerm());
  followerMemoryPressureRejections_ = metricEntity->findOrCreateCounter(
      &METRIC_follower_memory_pressure_rejections);

  numFailedElectionsMetric_ = metricEntity->findOrCreateGauge(
      &METRIC_failed_elections_since_stable_leader,
      failedElectionsSinceStableLeader_);

  raftProxyNumRequestsReceived_ = metricEntity->findOrCreateCounter(
      &METRIC_raft_proxy_num_requests_received);
  raftProxyNumRequestsSuccess_ = metricEntity->findOrCreateCounter(
      &METRIC_raft_proxy_num_requests_success);
  raftProxyNumRequestsUnknownDest_ = metricEntity->findOrCreateCounter(
      &METRIC_raft_proxy_num_requests_unknown_dest);
  raftProxyNumRequestsLogReadTimeout_ = metricEntity->findOrCreateCounter(
      &METRIC_raft_proxy_num_requests_log_read_timeout);
  raftProxyNumRequestsHopsRemainingExhausted_ =
      metricEntity->findOrCreateCounter(
          &METRIC_raft_proxy_num_requests_hops_remaining_exhausted);

  // A single Raft thread pool token is shared between RaftConsensus and
  // PeerManager. Because PeerManager is owned by RaftConsensus, it receives a
  // raw pointer to the token, to emphasize that RaftConsensus is responsible
  // for destroying the token.
  raftPoolToken_ = raftPool_->NewToken(ThreadPool::ExecutionMode::Concurrent);

  // The message queue that keeps track of which operations need to be
  // replicated where.
  //
  // Note: the message queue receives a dedicated Raft thread pool token so that
  // its submissions don't block other submissions by RaftConsensus (such as
  // heartbeat processing).
  //
  // TODO(adar): the token is SERIAL to match the previous single-thread
  // observer pool behavior, but CONCURRENT may be safe here.
  unique_ptr<PeerMessageQueue> queue(new PeerMessageQueue(
      metricEntity,
      log_,
      timeManager_,
      persistentVarsManager_,
      localPeerPb_,
      routingTableContainer_,
      options_.tablet_id,
      raftPool_->NewToken(ThreadPool::ExecutionMode::Serial),
      info->last_id,
      info->last_committed_id));

  // Proxy failure threshold is set to "2 * leader failure timeout" which
  // is roughly equivalent to 3000 ms
  queue->SetProxyFailureThreshold(
      2 * minimumElectionTimeout().ToMilliseconds());

  // A manager for the set of peers that actually send the operations both
  // remotely and to the local wal.
  unique_ptr<PeerManager> peerManager(new PeerManager(
      options_.tablet_id,
      peer_uuid(),
      peerProxyFactory_.get(),
      queue.get(),
      raftPoolToken_.get()));

  unique_ptr<PendingRounds> pending(
      new PendingRounds(LogPrefixThreadSafe(), timeManager_));

  // Capture a weak_ptr reference into the functor so it can safely handle
  // outliving the consensus instance.
  weak_ptr<RaftConsensus> w = shared_from_this();
  failureDetector_ = PeriodicTimer::Create(
      peerProxyFactory_->messenger(),
      [w]() {
        if (auto consensus = w.lock()) {
          consensus->ReportFailureDetected();
        }
      },
      minimumElectionTimeout());

  PeriodicTimer::Options opts;
  opts.oneShot = true;
  transferPeriodTimer_ = PeriodicTimer::Create(
      peerProxyFactory_->messenger(),
      [w]() {
        if (auto consensus = w.lock()) {
          consensus->endLeaderTransferPeriod();
        }
      },
      minimumElectionTimeout(),
      opts);

  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    CHECK_EQ(kInitialized, state_)
        << logPrefixUnlocked()
        << "Illegal state for Start(): " << stateName(state_);

    queue_ = std::move(queue);
    peerManager_ = std::move(peerManager);
    pending_ = std::move(pending);

    const std::string& compression_dict =
        persistentVars_->compressionDictionary();
    if (!compression_dict.empty()) {
      queue_->SetCompressionDictionary(compression_dict);
    }

    ClearLeaderUnlocked();

    // Our last persisted term can be higher than the last persisted operation
    // (i.e. if we called an election) but reverse should never happen.
    if (info->last_id.term() > currentTermUnlocked()) {
      return Status::Corruption(
          fmt::format(
              "Unable to start RaftConsensus: "
              "The last op in the WAL with id {} has a term ({}) that is greater "
              "than the latest recorded term, which is {}",
              OpIdToString(info->last_id),
              info->last_id.term(),
              currentTermUnlocked()));
    }

    // Append any uncommitted replicate messages found during log replay to the
    // queue.
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Replica starting. Triggering " << info->orphaned_replicates.size()
        << " pending transactions. Active config: "
        << SecureShortDebugString(cmeta_->ActiveConfig());
    for (const auto& replicate_ptr : info->orphaned_replicates) {
      RETURN_NOT_OK(StartFollowerTransactionUnlocked(replicate_ptr));
    }

    // Set the initial committed opid for the PendingRounds only after
    // appending any uncommitted replicate messages to the queue.
    pending_->setInitialCommittedOpId(info->last_committed_id);

    // If this is the first term expire the FD immediately so that we have a
    // fast first election, otherwise we just let the timer expire normally.
    std::optional<MonoDelta> fdInitialDelta;
    if (currentTermUnlocked() == 0) {
      // The failure detector is initialized to a low value to trigger an early
      // election (unless someone else requested a vote from us first, which
      // resets the election timer).
      //
      // We do it this way instead of immediately running an election to get a
      // higher likelihood of enough servers being available when the first one
      // attempts an election to avoid multiple election cycles on startup,
      // while keeping that "waiting period" random.
      if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
        LOG_WITH_PREFIX_UNLOCKED(INFO)
            << "Consensus starting up: Expiring failure detector timer "
               "to make a prompt election more likely";
        fdInitialDelta = MonoDelta::FromMilliseconds(
            rng_.uniform(FLAGS_raft_heartbeat_interval_ms));
      }
    }

    // Now assume non-leader replica duties.
    RETURN_NOT_OK(becomeReplicaUnlocked(fdInitialDelta));

    setStateUnlocked(kRunning);
  }

  if (IsSingleVoterConfig() && FLAGS_enable_leader_failure_detection) {
    LOG_WITH_PREFIX(INFO)
        << "Only one voter in the Raft config. Triggering election immediately";
    RETURN_NOT_OK(startElection(
        NORMAL_ELECTION,
        {kInitialSingleNodeElection, std::chrono::system_clock::now()}));
  }

  // Report become visible to the Master.
  MarkDirty("RaftConsensus started");

  return Status::OK();
}

bool RaftConsensus::isRunning() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return state_ == kRunning;
}

Status RaftConsensus::emulateElection() {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::EmulateElection",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);

  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  RETURN_NOT_OK(CheckRunningUnlocked());

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Emulating election...";

  // Assume leadership of new term.
  RETURN_NOT_OK(HandleTermAdvanceUnlocked(currentTermUnlocked() + 1));
  RETURN_NOT_OK(setLeaderUuidUnlocked(peer_uuid()));
  return becomeLeaderUnlocked();
}

namespace {
const char* ModeString(ElectionMode mode) {
  switch (mode) {
    case ElectionMode::UNKNOWN_ELECTION_MODE:
      return "unknown";
    case ElectionMode::NORMAL_ELECTION:
      return "leader election";
    case ElectionMode::PRE_ELECTION:
      return "pre-election";
    case ElectionMode::ELECT_EVEN_IF_LEADER_IS_ALIVE:
      return "forced leader election";
    case ElectionMode::MOCK_ELECTION:
      return "mock election";
  }
  __builtin_unreachable(); // silence gcc warnings
}
string ReasonString(ElectionReason reason, StringPiece leader_uuid) {
  switch (reason) {
    case ElectionReason::kInitialSingleNodeElection:
      return "initial election of a single-replica configuration";
    case ElectionReason::kExternalRequest:
      return "received explicit request";
    case ElectionReason::kElectionTimeoutExpired:
      if (leader_uuid.empty()) {
        return "no leader contacted us within the election timeout";
      }
      return fmt::format(
          "detected failure of leader {}", leader_uuid.toString());
    case ElectionReason::kFailedCheckQuorum:
      return "failed check quorum";
  }
  __builtin_unreachable(); // silence gcc warnings
}
} // anonymous namespace

Status RaftConsensus::startElection(
    ElectionMode mode,
    ElectionContext context,
    std::function<void(const ElectionResult&)> callback) {
  const char* const modeStr = ModeString(mode);

  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::StartElection",
      "peer",
      LogPrefixThreadSafe(),
      "mode",
      modeStr);
  std::shared_ptr<LeaderElection> election;
  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked());

    if (!persistentVars_->isStartElectionAllowed()) {
      std::string msg = fmt::format(
          "allow_start_election is set to false, not starting {}", modeStr);
      KLOG_EVERY_N_SECS(WARNING, 300)
          << logPrefixUnlocked() << msg << " [EVERY 300 seconds]";
      return Status::Aborted(msg);
    }

    if (!roundHandler_->isLeaderEligible()) {
      constexpr auto msg =
          "Round handler indicates instance is not healthy enough to be leader";
      KLOG_EVERY_N_SECS(WARNING, 300)
          << logPrefixUnlocked() << msg << " [EVERY 300 seconds]";
      return Status::Aborted(msg);
    }

    context.currentLeaderUuid = getLeaderUuidUnlocked();
    if (context.sourceUuid.empty()) {
      context.sourceUuid = context.currentLeaderUuid;
    } else if (context.sourceUuid != context.currentLeaderUuid) {
      // If the origin of the election isn't the same as the leader we're
      // promoting away from, it must mean that this election is part of a chain
      context.isChainedElection = true;
    }

    RaftPeerPB::Role activeRole = cmeta_->activeRole();
    if (activeRole == RaftPeerPB::LEADER) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << fmt::format("Not starting {} -- already a leader", modeStr);
      return Status::OK();
    }
    if (PREDICT_FALSE(!consensus::isVoterRole(activeRole))) {
      // A non-voter should not start leader elections. The leader failure
      // detector should be re-enabled once the non-voter replica is promoted
      // to voter replica.
      return Status::IllegalState(
          "only voting members can start elections",
          SecureShortDebugString(cmeta_->ActiveConfig()));
    }

    // In flexi raft mode, we want to start elections only in Candidate
    // regions which have voter_distribution Information.
    // It can be skipped when using quorum_id
    if (FLAGS_enable_flexi_raft &&
        !isUseQuorumId(cmeta_->ActiveConfig().commit_rule())) {
      const auto& vd_map = cmeta_->ActiveConfig().voter_distribution();
      if (PREDICT_FALSE(!vd_map.contains(peer_region()))) {
        return Status::IllegalState(
            fmt::format(
                "in flexi-raft only regions with valid voter distribution can start election: {}",
                peer_region()));
      }
    }

    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Starting " << modeStr << " ("
        << ReasonString(context.reason, getLeaderUuidUnlocked()) << ")";

    // Snooze to avoid the election timer firing again as much as possible.
    // We do not disable the election timer while running an election, so that
    // if the election times out, we will try again.
    MonoDelta timeout = LeaderElectionExpBackoffDeltaUnlocked();
    SnoozeFailureDetector(string("starting election"), timeout);

    // Increment the term and vote for ourselves, unless it's a pre or mock
    // election.
    if (mode != PRE_ELECTION && mode != MOCK_ELECTION) {
      // TODO(mpercy): Consider using a separate Mutex for voting, which must
      // sync to disk.

      // We skip flushing the term to disk because setting the vote just below
      // also flushes to disk, and the double fsync doesn't buy us anything.
      RETURN_NOT_OK(HandleTermAdvanceUnlocked(
          currentTermUnlocked() + 1, kSkipFlushToDisk));
      RETURN_NOT_OK(SetVotedForCurrentTermUnlocked(peer_uuid()));
    }

    RaftConfigPB activeConfig = cmeta_->ActiveConfig();
    VLOG_WITH_PREFIX_UNLOCKED(1)
        << "Starting " << modeStr
        << " with config: " << SecureShortDebugString(activeConfig);

    int64_t candidateTerm = currentTermUnlocked();
    if (mode == PRE_ELECTION || mode == MOCK_ELECTION) {
      // In a pre or mock election, we haven't bumped our own term yet, so we
      // need to be asking for votes for the next term.
      candidateTerm += 1;
    }

    // Joint consensus election needs two vote counters for C_old and C_new.
    bool is_need_joint_consensus_election = isJointConsensusPhase(activeConfig);

    // Initialize the VoteCounter.
    unique_ptr<VoteCounter> counter;

    VoteInfo voteInfo;
    voteInfo.vote = VOTE_GRANTED;
    if (!FLAGS_enable_flexi_raft) {
      int numVoters = countVoters(activeConfig);
      int majSize = majoritySize(numVoters);
      counter.reset(new VoteCounter(numVoters, majSize));
      if (is_need_joint_consensus_election) {
        counter.reset();
        counter = JointConsensusVoteCounter::Create(activeConfig);
      }
    } else {
      counter.reset(new FlexibleVoteCounter(
          peer_uuid(),
          candidateTerm,
          cmeta_->lastKnownLeader(),
          activeConfig,
          adjustVoterDistribution_));
      if (is_need_joint_consensus_election) {
        LOG(FATAL) << "Leader election during joint-consensus phase "
                      "under FlexiRaft is not yet supported.";
      }

      // Populate vote history for self. Although not really needed, this makes
      // the code simpler.
      const std::map<int64_t, PreviousVotePB>& pvh =
          cmeta_->previousVoteHistory();
      voteInfo.lastPrunedTerm = cmeta_->lastPrunedTerm();
      std::map<int64_t, PreviousVotePB>::const_iterator it = pvh.begin();
      while (it != pvh.end()) {
        voteInfo.previousVoteHistory.push_back(it->second);
        it++;
      }
    }

    // Vote for ourselves.
    bool duplicate;
    RETURN_NOT_OK(counter->RegisterVote(peer_uuid(), voteInfo, &duplicate));
    VLOG_WITH_PREFIX_UNLOCKED(1) << "Self-Voted " << modeStr;
    K_CHECK(
        !duplicate,
        self_voter_duplicate,
        "{} Inexplicable duplicate self-vote for term ",
        logPrefixUnlocked(),
        currentTermUnlocked());

    // The shell VoteRequestPB is used to create the VoteRequestPB
    // for each of the specific peers.
    // NB: below dest_uuid is left unpopulated.
    VoteRequestPB request;
    request.set_candidate_uuid(peer_uuid());
    request.set_candidate_term(candidateTerm);
    *request.mutable_candidate_context()->mutable_candidate_peer_pb() =
        localPeerPb_;
    if (std::shared_ptr<const std::string> rpc_token = getRaftRpcToken()) {
      request.set_raft_rpc_token(*rpc_token);
    }

    request.set_mode(mode);
    request.set_tablet_id(options_.tablet_id);

    if (context.mockElectionSnapshotOpId) {
      *request.mutable_candidate_status()->mutable_last_received() = MinOpId(
          *context.mockElectionSnapshotOpId, queue_->GetLastOpIdInLog());
      *request.mutable_mock_election_snapshot_op_id() =
          *context.mockElectionSnapshotOpId;
    } else {
      *request.mutable_candidate_status()->mutable_last_received() =
          queue_->GetLastOpIdInLog();
    }

    // activeConfig is cached into the LeaderElection, i.e.
    // if it changes during the LeaderElection process that is not
    // reacted to. Since LeaderElection operates on a snapshot of config,
    // it makes LeaderElection simpler, easier to reason with.
    election.reset(new LeaderElection(
        std::move(activeConfig),
        // The RaftConsensus ref passed below ensures that this raw pointer
        // remains safe to use for the entirety of LeaderElection's life.
        peerProxyFactory_.get(),
        std::move(request),
        std::move(counter),
        timeout,
        std::bind(
            &RaftConsensus::ElectionCallback,
            shared_from_this(),
            std::move(context),
            std::placeholders::_1,
            std::move(callback)),
        voteLogger_));
  }

  // Start the election outside the lock.
  election->Run();

  return Status::OK();
}

Status RaftConsensus::waitUntilLeaderForTests(const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  while (role() != consensus::RaftPeerPB::LEADER) {
    if (MonoTime::Now() >= deadline) {
      return Status::TimedOut(
          fmt::format(
              "Peer {} is not leader of tablet {} after {}. Role: {}",
              peer_uuid(),
              options_.tablet_id,
              timeout.ToString(),
              role()));
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::OK();
}

Status RaftConsensus::stepDown(LeaderStepDownResponsePB* resp) {
  TRACE_EVENT0("consensus", "RaftConsensus::stepDown");
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  DCHECK(
      (queue_->IsInLeaderMode() &&
       cmeta_->activeRole() == RaftPeerPB::LEADER) ||
      (!queue_->IsInLeaderMode() &&
       cmeta_->activeRole() != RaftPeerPB::LEADER));
  RETURN_NOT_OK(CheckRunningUnlocked());
  if (cmeta_->activeRole() != RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Rejecting request to step down while not leader";
    resp->mutable_error()->set_code(ServerErrorPB::NOT_THE_LEADER);
    statusToPb(
        Status::IllegalState("Not currently leader"),
        resp->mutable_error()->mutable_status());
    // We return OK so that the tablet service won't overwrite the error code.
    return Status::OK();
  }
  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Received request to step down";
  RETURN_NOT_OK(
      HandleTermAdvanceUnlocked(currentTermUnlocked() + 1, kSkipFlushToDisk));
  // Snooze the failure detector for an extra leader failure timeout.
  // This should ensure that a different replica is elected leader after this
  // one steps down.
  SnoozeFailureDetector(
      string("explicit stepdown request"),
      MonoDelta::FromMilliseconds(
          2 * minimumElectionTimeout().ToMilliseconds()));
  return Status::OK();
}

Status RaftConsensus::ValidateTransferLeadership(
    const std::optional<std::string>& new_leader_uuid,
    LeaderStepDownResponsePB* resp) {
  DCHECK(
      (queue_->IsInLeaderMode() &&
       cmeta_->activeRole() == RaftPeerPB::LEADER) ||
      (!queue_->IsInLeaderMode() &&
       cmeta_->activeRole() != RaftPeerPB::LEADER));
  RETURN_NOT_OK(CheckRunningUnlocked());
  if (cmeta_->activeRole() != RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Rejecting request to tranfser leadership while not leader";
    resp->mutable_error()->set_code(ServerErrorPB::NOT_THE_LEADER);
    statusToPb(
        Status::IllegalState("not currently leader"),
        resp->mutable_error()->mutable_status());
    // We return OK so that the tablet service won't overwrite the error code.
    return Status::OK();
  }
  if (new_leader_uuid) {
    if (*new_leader_uuid == peer_uuid()) {
      // Short-circuit as we are transferring leadership to ourselves and we
      // already checked that we are leader.
      return Status::OK();
    }
    if (!isRaftConfigVoter(*new_leader_uuid, cmeta_->ActiveConfig())) {
      const string msg = fmt::format(
          "tablet server {} is not a voter in the active config",
          *new_leader_uuid);
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Rejecting request to transfer leadership " << "because " << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

Status RaftConsensus::transferLeadership(
    const std::optional<string>& new_leader_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
    const ElectionContext& election_ctx,
    LeaderStepDownResponsePB* resp) {
  TRACE_EVENT0("consensus", "RaftConsensus::transferLeadership");
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Received request to transfer leadership"
      << (new_leader_uuid ? fmt::format(" to TS {}", *new_leader_uuid) : "");
  Status validation_status = ValidateTransferLeadership(new_leader_uuid, resp);
  if (!validation_status.ok()) {
    return validation_status;
  }

  if (FLAGS_enable_raft_leader_lease) {
    // Set lease expire time to now so that we can revoke the lease immediately.
    queue_->SetLeaderLeaseUntil(MonoTime::Now());
    setLeaseRenewStateUnlocked(LeaderLeaseState::kRevoke);
  }

  return beginLeaderTransferPeriodUnlocked(
      new_leader_uuid, filter_fn, election_ctx);
}

Status RaftConsensus::mockTransferLeadership(
    const std::string& new_leader_uuid,
    const ElectionContext& election_ctx,
    const std::chrono::milliseconds& wait_time,
    RunLeaderElectionResponsePB* resp) {
  TRACE_EVENT0("consensus", "RaftConsensus::mockTransferLeadership");
  ThreadRestrictions::assertWaitAllowed();

  {
    LockGuard l(lock_);
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Received request to mock transfer leadership to "
        << new_leader_uuid;

    LeaderStepDownResponsePB validation_resp;
    Status validation_status =
        ValidateTransferLeadership(new_leader_uuid, &validation_resp);
    if (!validation_status.ok()) {
      if (validation_resp.has_error()) {
        resp->set_allocated_error(validation_resp.release_error());
      }
      return validation_status;
    }
  }

  OpId snapshot_op_id;
  RETURN_NOT_OK(
      queue_->GetSnapshotForMockElection(new_leader_uuid, &snapshot_op_id));

  // Wait for candidate to potentially catch up or exceed snapshot op id.
  std::this_thread::sleep_for(wait_time);

  std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise =
      std::make_shared<Promise<RunLeaderElectionResponsePB>>();

  Status status = raftPoolToken_->SubmitClosure(Bind(
      &RaftConsensus::notifyPeerToStartElection,
      Unretained(this),
      new_leader_uuid,
      election_ctx.transferContext(),
      promise,
      snapshot_op_id));

  if (!status.ok()) {
    RunLeaderElectionResponsePB error_resp;
    error_resp.mutable_error()->set_code(ServerErrorPB::SERVICE_UNAVAILABLE);
    statusToPb(status, error_resp.mutable_error()->mutable_status());
    promise->set(error_resp);
  }

  MonoDelta timeout =
      MonoDelta::FromMilliseconds(FLAGS_mock_elections_timeout_ms);

  const RunLeaderElectionResponsePB* election_resp = promise->waitFor(timeout);
  if (election_resp) {
    *resp = *election_resp;
    return Status::OK();
  }

  return Status::Aborted(
      fmt::format(
          "Mock Election timed out for candidate {}.", new_leader_uuid));
}

Status RaftConsensus::cancelTransferLeadership() {
  TRACE_EVENT0("consensus", "RaftConsensus::cancelTransferLeadership");
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  endLeaderTransferPeriod();

  if (queue_->WatchForSuccessorPeerNotified()) {
    return Status::IllegalState(
        "Transfer leadership can't be cancelled, peer already notified to start "
        "election");
  }
  return Status::OK();
}

MonoTime RaftConsensus::getLeaderLeaseUntil() {
  return queue_->GetLeaderLeaseUntil();
}

MonoTime RaftConsensus::getBoundedDataLossWindowUntil() {
  return queue_->GetBoundedDataLossWindowUntil();
}

Status RaftConsensus::beginLeaderTransferPeriodUnlocked(
    const std::optional<string>& successor_uuid,
    const std::function<bool(const kudu::consensus::RaftPeerPB&)>& filter_fn,
    const ElectionContext& election_ctx) {
  DCHECK(lock_.is_locked());
  if (leaderTransferInProgress_.compareAndSwap(false, true)) {
    return Status::ServiceUnavailable(
        fmt::format(
            "leadership transfer for {} already in progress",
            options_.tablet_id));
  }
  leaderTransferInProgress_.store(true, kMemOrderAcquire);

  queue_->BeginWatchForSuccessor(
      successor_uuid, filter_fn, election_ctx.transferContext());

  transferPeriodTimer_->Start();

  if (FLAGS_enable_raft_leader_lease) {
    // Revoke for Leader lease here
    peerManager_->signalRequest(
        /*force_if_queue_empty*/ true, isLeaderLeaseSetForRevoke());
  }

  return Status::OK();
}

void RaftConsensus::endLeaderTransferPeriod() {
  transferPeriodTimer_->Stop();
  queue_->EndWatchForSuccessor();
  leaderTransferInProgress_.store(false, kMemOrderRelease);
}

std::shared_ptr<ConsensusRound> RaftConsensus::newRound(
    unique_ptr<ReplicateMsg> replicate_msg,
    ConsensusReplicatedCallback replicated_cb) {
  return std::shared_ptr<ConsensusRound>(new ConsensusRound(
      this, std::move(replicate_msg), std::move(replicated_cb)));
}

std::shared_ptr<ConsensusRound> RaftConsensus::newRound(
    unique_ptr<ReplicateMsg> replicate_msg) {
  ReplicateRefPtr r(
      std::make_shared<RefCountedReplicate>(
          std::move(replicate_msg), Source::Memory));
  return std::shared_ptr<ConsensusRound>(
      new ConsensusRound(this, std::move(r)));
}

void RaftConsensus::ReportFailureDetectedTask() {
  std::unique_lock<simple_mutexlock> try_lock(
      failureDetectorElectionLock_, std::try_to_lock);
  if (try_lock.owns_lock()) {
    // failureDetectorLastSnoozed_ is the time the failure detector was
    // active from. Adding 1 heartbeat gives a proxy to first heartbeat failure
    std::chrono::system_clock::time_point failureTime =
        failureDetectorLastSnoozed_.load(std::memory_order_relaxed) +
        std::chrono::milliseconds(FLAGS_raft_heartbeat_interval_ms);
    if (failureTime > std::chrono::system_clock::now()) {
      // Sometimes (e.g. first election), failure detector time is lower than
      // heartbeat interval. Reset the failure time if so
      failureTime = std::chrono::system_clock::now();
    }
    WARN_NOT_OK(
        startElection(
            FLAGS_raft_enable_pre_election ? PRE_ELECTION : NORMAL_ELECTION,
            {kElectionTimeoutExpired, failureTime}),
        LogPrefixThreadSafe() + "failed to trigger leader election");
  }
}

void RaftConsensus::ReportFailureDetected() {
  // We're running on a timer thread; start an election on a different thread
  // pool.
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::ReportFailureDetectedTask, shared_from_this())),
      LogPrefixThreadSafe() + "failed to submit failure detected task");
}

Status RaftConsensus::becomeLeaderUnlocked() {
  DCHECK(lock_.is_locked());

  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::becomeLeaderUnlocked",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);
  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Becoming Leader. State: " << ToStringUnlocked();

  // Disable FD while we are leader.
  disableFailureDetector();

  // Don't vote for anyone if we're a leader.
  withholdVotesUntil_ = MonoTime::Max();

  // Leadership never starts in a transfer period.
  endLeaderTransferPeriod();

  queue_->RegisterObserver(this);
  RETURN_NOT_OK(refreshConsensusQueueAndPeersUnlocked());

  InitCheckQuorumDetectorUnlocked();

  if (disableNoop_) {
    return Status::OK();
  }

  if (FLAGS_enable_raft_leader_lease) {
    // Leader Lease initialized
    leaderLeaseTerm_ = currentTermUnlocked();
  }

  // Initiate a NO_OP transaction that is sent at the beginning of every term
  // change in raft.
  auto replicate = std::make_unique<ReplicateMsg>();
  replicate->set_op_type(NO_OP);
  replicate->mutable_noop_request(); // Define the no-op request field.
  replicate->set_timestamp(
      Timestamp::kInitialTimestamp.value()); // some default timestamp
  CHECK_OK(timeManager_->assignTimestamp(replicate.get()));

  std::shared_ptr<ConsensusRound> round(new ConsensusRound(
      this,
      std::make_shared<RefCountedReplicate>(
          std::move(replicate), Source::Memory)));
  round->SetConsensusReplicatedCallback(
      std::bind(
          &RaftConsensus::nonTxRoundReplicationFinished,
          this,
          round.get(),
          &doNothingStatusCb,
          std::placeholders::_1));

  lastLeaderCommunicationTimeMicros_ = 0;

  return AppendNewRoundToQueueUnlocked(round);
}

Status RaftConsensus::becomeReplicaUnlocked(std::optional<MonoDelta> fd_delta) {
  DCHECK(lock_.is_locked());

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Becoming Follower/Learner. State: " << ToStringUnlocked();
  ClearLeaderUnlocked();

  // Enable/disable leader failure detection if becoming VOTER/NON_VOTER replica
  // correspondingly.
  UpdateFailureDetectorState(std::move(fd_delta));

  StopCheckQuorumDetectorUnlocked();

  // Now that we're a replica, we can allow voting for other nodes.
  withholdVotesUntil_ = MonoTime::Min();

  // Deregister ourselves from the queue. We no longer need to track what gets
  // replicated since we're stepping down.
  queue_->UnRegisterObserver(this);
  queue_->SetNonLeaderMode(cmeta_->ActiveConfig());
  peerManager_->close();

  return Status::OK();
}

Status RaftConsensus::replicate(const std::shared_ptr<ConsensusRound>& round) {
  std::lock_guard<simple_mutexlock> lock(updateLock_);
  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckSafeToReplicateUnlocked(*round->replicate_msg()));
    RETURN_NOT_OK(round->CheckBoundTerm(currentTermUnlocked()));
    RETURN_NOT_OK(AppendNewRoundToQueueUnlocked(round));
    round->setFlushCompleteTime(std::chrono::steady_clock::now());
  }

  peerManager_->signalRequest();
  return Status::OK();
}

Status RaftConsensus::truncateCallbackWithRaftLock(
    int64_t* index_if_truncated) {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  RETURN_NOT_OK(CheckRunningUnlocked());

  // We pass -1 to TruncateOpsAfter in the log abstraction
  // It is the responsibility of the derived log to truncate from
  // the cached truncation index and clear it.
  RETURN_NOT_OK(log_->truncateOpsAfter(-1, index_if_truncated));

  if (index_if_truncated && *index_if_truncated != -1) {
    STATS_raft_log_truncation_counter.add(1, KUDU_STATS_TAG);
  }

  return Status::OK();
}

Status RaftConsensus::checkLeadershipAndBindTerm(
    const std::shared_ptr<ConsensusRound>& round) {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  RETURN_NOT_OK(CheckSafeToReplicateUnlocked(*round->replicate_msg()));
  round->BindToTerm(currentTermUnlocked());
  return Status::OK();
}

Status RaftConsensus::AppendNewRoundToQueueUnlocked(
    const std::shared_ptr<ConsensusRound>& round) {
  DCHECK(lock_.is_locked());

  // If index was set in the ReplicateMsgg Round before starting
  // ::Replicate() we need to check that the ground has not shifted
  // under our feet. The term and index has also been serialized
  // into the WRITE_OP which would make it inconsistent to
  // replicate this message
  if (PREDICT_TRUE(round->replicate_msg()->id().index() != 0)) {
    if (PREDICT_FALSE(
            round->replicate_msg()->id().index() !=
            queue_->GetNextOpId().index())) {
      return Status::Aborted(
          fmt::format(
              "Transaction submitted with index {} mismatches with queue index {}",
              round->replicate_msg()->id().index(),
              queue_->GetNextOpId().index()));
    }
  } else {
    *round->replicate_msg()->mutable_id() = queue_->GetNextOpId();
  }
  RETURN_NOT_OK(AddPendingOperationUnlocked(round));

  ReplicateMsgWrapper msg_wrapper(round->replicate_scoped_refptr());
  RETURN_NOT_OK(msg_wrapper.init(&compressionBuffer_));

  // The only reasons for a bad status would be if the log itself were shut
  // down, or if we had an actual IO error, which we currently don't handle.
  CHECK_OK_PREPEND(
      queue_->AppendOperation(msg_wrapper),
      fmt::format("{}: could not append to queue", logPrefixUnlocked()));
  if (round->replicate_msg()->op_type() == NO_OP) {
    HandleNewTermAppendedUnlocked(round->replicate_msg()->id().term());
  }
  return Status::OK();
}

Status RaftConsensus::AddPendingOperationUnlocked(
    const std::shared_ptr<ConsensusRound>& round) {
  DCHECK(lock_.is_locked());
  DCHECK(pending_);

  // If we are adding a pending config change, we need to propagate it to the
  // metadata.
  if (PREDICT_FALSE(round->replicate_msg()->op_type() == CHANGE_CONFIG_OP)) {
    // Fill in the opid for the proposed new configuration. This has to be done
    // here rather than when it's first created because we didn't yet have an
    // OpId assigned at creation time.
    ChangeConfigRecordPB* change_record =
        round->replicate_msg()->mutable_change_config_record();
    change_record->mutable_new_config()->set_opid_index(
        round->replicate_msg()->id().index());

    DCHECK(change_record->IsInitialized())
        << "change_config_record missing required fields: "
        << change_record->InitializationErrorString();

    const RaftConfigPB& new_config = change_record->new_config();

    if (!new_config.unsafe_config_change()) {
      Status s = CheckNoConfigChangePendingUnlocked();
      if (PREDICT_FALSE(!s.ok())) {
        s = s.cloneAndAppend(
            fmt::format(
                "\n  New config: {}", SecureShortDebugString(new_config)));
        LOG_WITH_PREFIX_UNLOCKED(INFO) << s.ToString();
        return s;
      }
    }
    // Check if the pending Raft config has an OpId less than the committed
    // config. If so, this is a replay at startup in which the COMMIT
    // messages were delayed.
    int64_t committed_config_opid_index =
        cmeta_->getConfigOpIdIndex(COMMITTED_CONFIG);
    if (round->replicate_msg()->id().index() > committed_config_opid_index) {
      RETURN_NOT_OK(SetPendingConfigUnlocked(new_config));
      if (cmeta_->activeRole() == RaftPeerPB::LEADER) {
        RETURN_NOT_OK(refreshConsensusQueueAndPeersUnlocked());
      }
    } else {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Ignoring setting pending config change with OpId "
          << round->replicate_msg()->id()
          << " because the committed config has OpId index "
          << committed_config_opid_index
          << ". The config change we are ignoring is: " << "Old config: { "
          << SecureShortDebugString(change_record->old_config()) << " }. "
          << "New config: { " << SecureShortDebugString(new_config) << " }";
    }
  }

  return pending_->addPendingOperation(round);
}

void RaftConsensus::notifyCommitIndex(int64_t commitIndex, bool needLock) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::notifyCommitIndex",
      "tablet",
      options_.tablet_id,
      "commit_index",
      commitIndex);

  ThreadRestrictions::assertWaitAllowed();
  if (needLock) {
    lock_.lock();
  }
  // We will process commit notifications while shutting down because a replica
  // which has initiated a Prepare() / Replicate() may eventually commit even if
  // its state has changed after the initial Append() / Update().
  if (PREDICT_FALSE(state_ != kRunning && state_ != kStopping)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to update committed index: "
        << "Replica not in running state: " << stateName(state_);
  } else {
    pending_->advanceCommittedIndex(commitIndex);

    if (FLAGS_notify_commit_index_after_response &&
        cmeta_->activeRole() == RaftPeerPB::LEADER) {
      peerManager_->signalRequest(false);
    }
  }

  if (needLock) {
    lock_.unlock();
  }
}

void RaftConsensus::notifyTermChange(int64_t term) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::notifyTermChange",
      "tablet",
      options_.tablet_id,
      "term",
      term);

  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  Status s = CheckRunningUnlocked();
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to handle notification of new term " << "(" << term
        << "): " << s.ToString();
    return;
  }
  WARN_NOT_OK(
      HandleTermAdvanceUnlocked(term), "Couldn't advance consensus term.");
}

void RaftConsensus::notifyFailedFollower(
    const string& uuid,
    int64_t term,
    const std::string& reason) {
  // Common info used in all of the log messages within this method.
  string failMsg = fmt::format(
      "Processing failure of peer {} in term {} ({}): ", uuid, term, reason);

  if (!FLAGS_evict_failed_followers) {
    LOG(INFO) << LogPrefixThreadSafe() << failMsg
              << "Eviction of failed followers is disabled. Doing nothing.";
    return;
  }

  RaftConfigPB committedConfig;
  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    int64_t currentTermVal = currentTermUnlocked();
    if (currentTermVal != term) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << failMsg << "Notified about a follower failure in "
          << "previous term " << term << ", but a leader election "
          << "likely occurred since the failure was detected. "
          << "Doing nothing.";
      return;
    }

    if (cmeta_->hasPendingConfig()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << failMsg << "There is already a config change operation "
          << "in progress. Unable to evict follower until it completes. "
          << "Doing nothing.";
      return;
    }
    committedConfig = cmeta_->committedConfig();
  }

  // Run config change on thread pool after dropping lock.
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::TryRemoveFollowerTask,
              shared_from_this(),
              uuid,
              committedConfig,
              reason)),
      LogPrefixThreadSafe() + "Unable to start TryRemoveFollowerTask");
}

void RaftConsensus::notifyPeerToPromote(const std::string& peerUuid) {
  // Run the config change on the raft thread pool.
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::TryPromoteNonVoterTask,
              shared_from_this(),
              peerUuid)),
      LogPrefixThreadSafe() + "Unable to start TryPromoteNonVoterTask");
}

void RaftConsensus::notifyPeerToStartElection(
    const std::string& peerUuid,
    std::optional<PeerMessageQueue::TransferContext> transferContext,
    std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise,
    std::optional<OpId> mockElectionSnapshotOpId) {
  LOG(INFO) << "Instructing follower " << peerUuid << " to start an election";
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::TryStartElectionOnPeerTask,
              shared_from_this(),
              peerUuid,
              std::move(transferContext),
              promise,
              std::move(mockElectionSnapshotOpId))),
      LogPrefixThreadSafe() + "Unable to start TryStartElectionOnPeerTask");
}

void RaftConsensus::notifyPeerHealthChange() {
  MarkDirty("Peer health change");
}

void RaftConsensus::HandleNewTermAppendedUnlocked(int64_t new_term) {
  DCHECK(lock_.is_locked());
  CHECK_OK(cmeta_->syncLastKnownLeader(new_term));
}

void RaftConsensus::TryRemoveFollowerTask(
    const string& uuid,
    const RaftConfigPB& committedConfig,
    const std::string& reason) {
  ChangeConfigRequestPB req;
  req.set_tablet_id(options_.tablet_id);
  req.mutable_server()->set_permanent_uuid(uuid);
  req.set_type(REMOVE_PEER);
  req.set_cas_config_opid_index(committedConfig.opid_index());
  LOG(INFO) << LogPrefixThreadSafe() << "Attempting to remove follower " << uuid
            << " from the Raft config. Reason: " << reason;
  std::optional<ServerErrorPB::Code> errorCode;
  WARN_NOT_OK(
      changeConfig(req, &doNothingStatusCb, &errorCode),
      LogPrefixThreadSafe() + "Unable to remove follower " + uuid);
}

void RaftConsensus::TryPromoteNonVoterTask(const std::string& peerUuid) {
  string msg = fmt::format("attempt to promote peer {}: ", peerUuid);
  int64_t currentCommittedConfigIndex;
  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);

    if (cmeta_->hasPendingConfig()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << msg << "there is already a config change operation "
          << "in progress. Unable to promote follower until it "
          << "completes. Doing nothing.";
      return;
    }

    // Check if the peer is still part of the current committed config.
    RaftConfigPB committedConfig = cmeta_->committedConfig();
    currentCommittedConfigIndex = committedConfig.opid_index();

    RaftPeerPB* peerPb;
    Status s = getRaftConfigMember(&committedConfig, peerUuid, &peerPb);
    if (!s.ok()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << msg << "can't find peer in the "
          << "current committed config: " << committedConfig.ShortDebugString()
          << ". Doing nothing.";
      return;
    }

    // Also check if the peer it still a NON_VOTER waiting for promotion.
    if (peerPb->member_type() != RaftPeerPB::NON_VOTER ||
        !peerPb->attrs().promote()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << msg << "peer is either no longer a NON_VOTER "
          << "or not marked for promotion anymore. Current "
          << "config: " << committedConfig.ShortDebugString()
          << ". Doing nothing.";
      return;
    }
  }

  ChangeConfigRequestPB req;
  req.set_tablet_id(options_.tablet_id);
  req.set_type(MODIFY_PEER);
  req.mutable_server()->set_permanent_uuid(peerUuid);
  req.mutable_server()->set_member_type(RaftPeerPB::VOTER);
  req.mutable_server()->mutable_attrs()->set_promote(false);
  req.set_cas_config_opid_index(currentCommittedConfigIndex);
  LOG(INFO) << LogPrefixThreadSafe() << "attempting to promote NON_VOTER "
            << peerUuid << " to VOTER";
  std::optional<ServerErrorPB::Code> errorCode;
  WARN_NOT_OK(
      changeConfig(req, &doNothingStatusCb, &errorCode),
      LogPrefixThreadSafe() +
          fmt::format("Unable to promote non-voter {}", peerUuid));
}

void RaftConsensus::TryStartElectionOnPeerTask(
    const string& peerUuid,
    const std::optional<PeerMessageQueue::TransferContext>& transferContext,
    std::shared_ptr<Promise<RunLeaderElectionResponsePB>> promise,
    std::optional<OpId> mockElectionSnapshotOpId) {
  ThreadRestrictions::assertWaitAllowed();
  {
    LockGuard l(lock_);
    // Double-check that the peer is a voter in the active config.
    if (!isRaftConfigVoter(peerUuid, cmeta_->ActiveConfig())) {
      std::string msg = fmt::format(
          "Not signalling peer {} to start an election: it's not a voter in "
          "the active config.",
          peerUuid);
      LOG_WITH_PREFIX_UNLOCKED(WARNING) << msg;
      if (promise) {
        RunLeaderElectionResponsePB error_resp;
        error_resp.mutable_error()->set_code(ServerErrorPB::NOT_VOTER);
        statusToPb(
            Status::ConfigurationError(std::move(msg)),
            error_resp.mutable_error()->mutable_status());
        promise->set(error_resp);
      }
      return;
    }
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Signalling peer " << peerUuid << " to start "
        << (mockElectionSnapshotOpId ? "a mock election." : "an election.");
  }

  RunLeaderElectionRequestPB req;
  if (transferContext) {
    LeaderElectionContextPB* ctx = req.mutable_election_context();
    ctx->set_original_start_time(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            transferContext->original_start_time.time_since_epoch())
            .count());
    ctx->set_original_uuid(transferContext->original_uuid);
    ctx->set_is_origin_dead_promotion(
        transferContext->is_origin_dead_promotion);
  }
  if (std::shared_ptr<const std::string> rpc_token = getRaftRpcToken()) {
    req.set_raft_rpc_token(*rpc_token);
  }

  if (mockElectionSnapshotOpId) {
    req.mutable_mock_election_snapshot_op_id()->CopyFrom(
        *mockElectionSnapshotOpId);
    req.set_wait_for_decision(true);
  }

  RunLeaderElectionResponsePB resp;
  Status electionStatus =
      peerManager_->startElection(peerUuid, &resp, std::move(req));
  if (!electionStatus.ok()) {
    LOG_WITH_PREFIX(WARNING)
        << "Unable to start " << (mockElectionSnapshotOpId ? "mock " : "")
        << "election on peer " << peerUuid << ": " << electionStatus.ToString();
  }

  if (promise) {
    promise->set(resp);
  }
}

Status RaftConsensus::update(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response) {
  updateCallsForTests_.increment();

  if (PREDICT_FALSE(
          FLAGS_follower_reject_update_consensus_requests ||
          rejectAppendEntries_)) {
    return Status::IllegalState(
        "Rejected: --follower_reject_update_consensus_requests "
        "is set to true.");
  }

  response->set_responder_uuid(peer_uuid());
  if (stateMachineMetrics_) {
    response->mutable_state_machine_metrics()->CopyFrom(
        stateMachineMetrics_->getStateMachineMetrics());
  }

  VLOG_WITH_PREFIX(2) << "Replica received request: "
                      << SecureShortDebugString(*request);

  // see var declaration
  std::lock_guard<simple_mutexlock> lock(updateLock_);
  Status s = updateReplica(request, response);
  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    if (request->ops().empty()) {
      VLOG_WITH_PREFIX(1) << "Replica replied to status only request. Replica: "
                          << ToString() << ". Response: "
                          << SecureShortDebugString(*response);
    }
  }
  return s;
}

// Helper function to check if the op is a non-Transaction op.
static bool IsConsensusOnlyOperation(OperationType op_type) {
  return op_type == NO_OP || op_type == CHANGE_CONFIG_OP;
}

Status RaftConsensus::StartFollowerTransactionUnlocked(
    const ReplicateMsgWrapper& msg_wrapper) {
  if (!msg_wrapper.getUncompressedMsg()) {
    return Status::IllegalState("Rejected: Msg wrapper is null");
  }
  return StartFollowerTransactionUnlocked(msg_wrapper.getUncompressedMsg());
}

Status RaftConsensus::StartFollowerTransactionUnlocked(
    const ReplicateRefPtr& msg) {
  DCHECK(lock_.is_locked());

  // Validate crc32 checksum
  uint32_t payload_crc32 = msg->get()->write_payload().crc32();
  if (payload_crc32 != 0) {
    const std::string& payload = msg->get()->write_payload().payload();
    uint32_t computed_crc32 = crc::crc32c(payload.c_str(), payload.size());
    if (payload_crc32 != computed_crc32) {
      std::string err_msg = fmt::format(
          "Rejected: Payload corruption for {}",
          OpIdToString(msg->get()->id()));
      return Status::Corruption(err_msg);
    }
  }

  if (IsConsensusOnlyOperation(msg->get()->op_type())) {
    return StartConsensusOnlyRoundUnlocked(msg);
  }

  if (PREDICT_FALSE(FLAGS_follower_fail_all_prepare)) {
    return Status::IllegalState(
        "Rejected: --follower_fail_all_prepare "
        "is set to true.");
  }

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Starting transaction: " << SecureShortDebugString(msg->get()->id());
  std::shared_ptr<ConsensusRound> round(new ConsensusRound(this, msg));
  RETURN_NOT_OK(roundHandler_->startFollowerTransaction(round));
  return AddPendingOperationUnlocked(round);
}

bool RaftConsensus::IsSingleVoterConfig() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->countVotersInConfig(COMMITTED_CONFIG) == 1 &&
      cmeta_->isVoterInConfig(peer_uuid(), COMMITTED_CONFIG);
}

std::string RaftConsensus::LeaderRequest::opsRangeString() const {
  std::string ret;
  ret.reserve(100);
  ret.push_back('[');
  if (!messages.empty()) {
    const OpId& first_op = (*messages.begin())->get()->id();
    const OpId& last_op = (*messages.rbegin())->get()->id();
    ret += fmt::format(
        "{}.{}-{}.{}",
        first_op.term(),
        first_op.index(),
        last_op.term(),
        last_op.index());
  }
  ret.push_back(']');
  return ret;
}

void RaftConsensus::deduplicateLeaderRequestUnlocked(
    ConsensusRequestPB* rpcReq,
    LeaderRequest* deduplicatedReq) {
  DCHECK(lock_.is_locked());

  // TODO(todd): use queue committed index?
  int64_t lastCommittedIndex = pending_->getCommittedIndex();

  // The leader's preceding id.
  deduplicatedReq->precedingOpId = rpcReq->preceding_id();

  int64_t dedupUpToIndex = queue_->GetLastOpIdInLog().index();

  deduplicatedReq->firstMessageIdx = -1;

  // Snapshot the original ops range string before extraction empties the
  // request's ops list (needed for the deduplication log message below).
  std::string originalOpsRange = OpsRangeString(*rpcReq);

  // Extract all ops from the request upfront so that ownership is explicit.
  // UnsafeArenaExtractSubrange releases the protobuf's ownership; we
  // immediately wrap each pointer in unique_ptr to ensure cleanup on all paths.
  int numOps = rpcReq->ops_size();
  std::vector<std::unique_ptr<ReplicateMsg>> extractedOps{(size_t)numOps};
  if (numOps > 0) {
    std::vector<ReplicateMsg*> rawPtrs{(size_t)numOps};
    rpcReq->mutable_ops()->UnsafeArenaExtractSubrange(
        0, numOps, rawPtrs.data());
    for (int i = 0; i < numOps; i++) {
      extractedOps[i].reset(rawPtrs[i]);
    }
  }

  // In this loop we discard duplicates and advance the leader's preceding id
  // accordingly.
  for (size_t i = 0; i < numOps; i++) {
    std::unique_ptr<ReplicateMsg> leaderMsg = std::move(extractedOps[i]);

    if (leaderMsg->id().index() <= lastCommittedIndex) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Skipping op id " << leaderMsg->id() << " (already committed)";
      deduplicatedReq->precedingOpId = leaderMsg->id();
      continue;
    }

    if (leaderMsg->id().index() <= dedupUpToIndex) {
      // If the index is uncommitted and below our match index, then it must be
      // in the pendings set.
      std::shared_ptr<ConsensusRound> round =
          pending_->getPendingOpByIndexOrNull(leaderMsg->id().index());
      DCHECK(round) << "Could not find op with index "
                    << leaderMsg->id().index()
                    << " in pending set. committed= " << lastCommittedIndex
                    << " dedup=" << dedupUpToIndex;

      // If the OpIds match, i.e. if they have the same term and id, then this
      // is just duplicate, we skip...
      if (OpIdEquals(round->replicate_msg()->id(), leaderMsg->id())) {
        VLOG_WITH_PREFIX_UNLOCKED(2)
            << "Skipping op id " << leaderMsg->id() << " (already replicated)";
        deduplicatedReq->precedingOpId = leaderMsg->id();
        continue;
      }

      // ... otherwise we must adjust our match index, i.e. all messages from
      // now on are "new"
      dedupUpToIndex = leaderMsg->id().index();
    }

    if (deduplicatedReq->firstMessageIdx == -1) {
      deduplicatedReq->firstMessageIdx = i;
    }
    deduplicatedReq->messages.push_back(
        makeScopedRefptrReplicate(std::move(leaderMsg), Source::Memory));
  }

  if (deduplicatedReq->messages.size() != numOps) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Deduplicated request from leader. Original: "
        << rpcReq->preceding_id() << "->" << originalOpsRange
        << "   Dedup: " << deduplicatedReq->precedingOpId << "->"
        << deduplicatedReq->opsRangeString();
  }
}

Status RaftConsensus::handleLeaderRequestTermUnlocked(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());
  // Do term checks first:
  if (PREDICT_FALSE(request->caller_term() != currentTermUnlocked())) {
    // If less, reject.
    if (request->caller_term() < currentTermUnlocked()) {
      string msg = fmt::format(
          "Rejecting Update request from peer {} for earlier term {}. "
          "Current term is {}. Ops: {}",
          request->caller_uuid(),
          request->caller_term(),
          currentTermUnlocked(),
          OpsRangeString(*request));
      LOG_WITH_PREFIX_UNLOCKED(INFO) << msg;
      FillConsensusResponseError(
          response, ConsensusErrorPB::INVALID_TERM, Status::IllegalState(msg));
      return Status::OK();
    }
    RETURN_NOT_OK(HandleTermAdvanceUnlocked(request->caller_term()));
  }
  return Status::OK();
}

Status RaftConsensus::enforceLogMatchingPropertyMatchesUnlocked(
    const LeaderRequest& req,
    ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());

  bool termMismatch;
  if (pending_->isOpCommittedOrPending(req.precedingOpId, &termMismatch)) {
    return Status::OK();
  }

  string errorMsg = fmt::format(
      "Log matching property violated."
      " Preceding OpId in replica: {}. Preceding OpId from leader: {}. ({} mismatch)",
      SecureShortDebugString(queue_->GetLastOpIdInLog()),
      SecureShortDebugString(req.precedingOpId),
      termMismatch ? "term" : "index");

  FillConsensusResponseError(
      response,
      ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH,
      Status::IllegalState(errorMsg));

  LOG_EVERY_N(INFO, 360) << logPrefixUnlocked()
                         << "[EVERY 360] Refusing update from remote peer "
                         << req.leaderUuid << ": " << errorMsg;

  // If the terms mismatch we abort down to the index before the leader's
  // preceding, since we know that is the last opid that has a chance of not
  // being overwritten. Aborting preemptively here avoids us reporting a last
  // received index that is possibly higher than the leader's causing an
  // avoidable cache miss on the leader's queue.
  //
  // TODO: this isn't just an optimization! if we comment this out, we get
  // failures on raft_consensus-itest a couple percent of the time! Should
  // investigate why this is actually critical to do here, as opposed to just on
  // requests that append some ops.
  if (termMismatch) {
    auto localCommitIndex = pending_->getCommittedIndex();
    if (localCommitIndex >= req.precedingOpId.index()) {
      std::string errMsg = fmt::format(
          "Raft should not truncate committed log. "
          "Preceding OpId from leader: {}, "
          "local replica commit index: {}, "
          "FLAGS_allow_truncate_committed_log: {}",
          SecureShortDebugString(req.precedingOpId),
          localCommitIndex,
          FLAGS_allow_truncate_committed_log);
      K_DCHECK(false, truncate_committed_log, "{}", errMsg);
      if (PREDICT_TRUE(!FLAGS_allow_truncate_committed_log)) {
        return Status::IllegalState(errMsg);
      }
    }

    truncateAndAbortOpsAfterUnlocked(req.precedingOpId.index() - 1);
  }

  return Status::OK();
}

void RaftConsensus::truncateAndAbortOpsAfterUnlocked(
    int64_t truncateAfterIndex) {
  DCHECK(lock_.is_locked());
  pending_->abortOpsAfter(truncateAfterIndex);
  queue_->TruncateOpsAfter(truncateAfterIndex);
}

Status RaftConsensus::checkLeaderRequestUnlocked(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response,
    LeaderRequest* dedupedReq) {
  DCHECK(lock_.is_locked());

  if (request->has_deprecated_committed_index() ||
      !request->has_all_replicated_index()) {
    return Status::InvalidArgument(
        "Leader appears to be running an earlier version "
        "of Kudu. Please shut down and upgrade all servers "
        "before restarting.");
  }

  ConsensusRequestPB* mutableReq = const_cast<ConsensusRequestPB*>(request);
  deduplicateLeaderRequestUnlocked(mutableReq, dedupedReq);

  // This is an additional check for KUDU-639 that makes sure the message's
  // index and term are in the right sequence in the request, after we've
  // deduplicated them. We do this before we change any of the internal state.
  //
  // TODO move this to raft_consensus-state or whatever we transform that into.
  // We should be able to do this check for each append, but right now the way
  // we initialize raft_consensus-state is preventing us from doing so.
  Status s;
  const OpId* prev = &dedupedReq->precedingOpId;
  for (const ReplicateRefPtr& message : dedupedReq->messages) {
    s = PendingRounds::checkOpInSequence(*prev, message->get()->id());
    if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX_UNLOCKED(ERROR)
          << "Leader request contained out-of-sequence messages. "
          << "Status: " << s.ToString()
          << ". Request from: " << request->caller_uuid()
          << ", term: " << request->caller_term()
          << ", preceding: " << SecureShortDebugString(request->preceding_id())
          << ". Deduped: " << dedupedReq->precedingOpId << "->"
          << dedupedReq->opsRangeString();
      break;
    }
    prev = &message->get()->id();
  }

  RETURN_NOT_OK(s);

  RETURN_NOT_OK(handleLeaderRequestTermUnlocked(request, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  RETURN_NOT_OK(
      enforceLogMatchingPropertyMatchesUnlocked(*dedupedReq, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  // If the first of the messages to apply is not in our log, either it follows
  // the last received message or it replaces some in-flight.
  if (!dedupedReq->messages.empty()) {
    bool termMismatch;
    CHECK(!pending_->isOpCommittedOrPending(
        dedupedReq->messages[0]->get()->id(), &termMismatch));

    // If the index is in our log but the terms are not the same abort down to
    // the leader's preceding id.
    if (termMismatch) {
      truncateAndAbortOpsAfterUnlocked(dedupedReq->precedingOpId.index());
    }
  }

  // If all of the above logic was successful then we can consider this to be
  // the effective leader of the configuration. If they are not currently marked
  // as the leader locally, mark them as leader now.
  const string& callerUuid = request->caller_uuid();
  if (PREDICT_FALSE(
          HasLeaderUnlocked() && getLeaderUuidUnlocked() != callerUuid)) {
    LOG_WITH_PREFIX_UNLOCKED(FATAL)
        << "Unexpected new leader in same term! "
        << "Existing leader UUID: " << getLeaderUuidUnlocked() << ", "
        << "new leader UUID: " << callerUuid;
  }
  if (PREDICT_FALSE(!HasLeaderUnlocked())) {
    RETURN_NOT_OK(setLeaderUuidUnlocked(request->caller_uuid()));
  }

  return Status::OK();
}

Status RaftConsensus::updateReplica(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::updateReplica",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);
  Synchronizer logSynchronizer;
  StatusCallback syncStatusCb = logSynchronizer.asStatusCallback();

  // The ordering of the following operations is crucial, read on for details.
  //
  // The main requirements explained in more detail below are:
  //
  //   1) We must enqueue the prepares before we write to our local log.
  //   2) If we were able to enqueue a prepare then we must be able to log it.
  //   3) If we fail to enqueue a prepare, we must not attempt to enqueue any
  //      later-indexed prepare or apply.
  //
  // See below for detailed rationale.
  //
  // The steps are:
  //
  // 0 - Split/Dedup
  //
  // We split the operations into replicates and commits and make sure that we
  // don't do anything on operations we've already received in a previous
  // call. This essentially makes this method idempotent.
  //
  // 1 - We mark as many pending transactions as committed as we can.
  //
  // We may have some pending transactions that, according to the leader, are
  // now committed. We Apply them early, because:
  // - Soon (step 2) we may reject the call due to excessive memory pressure.
  // One
  //   way to relieve the pressure is by flushing the MRS, and applying these
  //   transactions may unblock an in-flight Flush().
  // - The Apply and subsequent Prepares (step 2) can take place concurrently.
  //
  // 2 - We enqueue the Prepare of the transactions.
  //
  // The actual prepares are enqueued in order but happen asynchronously so we
  // don't have decoding/acquiring locks on the critical path.
  //
  // We need to do this now for a number of reasons:
  // - Prepares, by themselves, are inconsequential, i.e. they do not mutate the
  //   state machine so, were we to crash afterwards, having the prepares
  //   in-flight won't hurt.
  // - Prepares depend on factors external to consensus (the transaction drivers
  // and
  //   the TabletReplica) so if for some reason they cannot be enqueued we must
  //   know before we try write them to the WAL. Once enqueued, we assume that
  //   prepare will always succeed on a replica transaction (because the leader
  //   already prepared them successfully, and thus we know they are valid).
  // - The prepares corresponding to every operation that was logged must be
  // in-flight
  //   first. This because should we need to abort certain transactions (say a
  //   new leader says they are not committed) we need to have those prepares
  //   in-flight so that the transactions can be continued (in the abort path).
  // - Failure to enqueue prepares is OK, we can continue and let the leader
  // know that
  //   we only went so far. The leader will re-send the remaining messages.
  // - Prepares represent new transactions, and transactions consume memory.
  // Thus, if the
  //   overall memory pressure on the server is too high, we will reject the
  //   prepares.
  //
  // 3 - We enqueue the writes to the WAL.
  //
  // We enqueue writes to the WAL, but only the operations that were
  // successfully enqueued for prepare (for the reasons introduced above). This
  // means that even if a prepare fails to enqueue, if any of the previous
  // prepares were successfully submitted they must be written to the WAL. If
  // writing to the WAL fails, we're in an inconsistent state and we crash. In
  // this case, no one will ever know of the transactions we previously prepared
  // so those are inconsequential.
  //
  // 4 - We mark the transactions as committed.
  //
  // For each transaction which has been committed by the leader, we update the
  // transaction state to reflect that. If the logging has already succeeded for
  // that transaction, this will trigger the Apply phase. Otherwise, Apply will
  // be triggered when the logging completes. In both cases the Apply phase
  // executes asynchronously. This must, of course, happen after the prepares
  // have been triggered as the same batch can both replicate/prepare and
  // commit/apply an operation.
  //
  // Currently, if a prepare failed to enqueue we still trigger all applies for
  // operations with an id lower than it (if we have them). This is important
  // now as the leader will not re-send those commit messages. This will be moot
  // when we move to the commit commitIndex way of doing things as we can simply
  // ignore the applies as we know they will be triggered with the next
  // successful batch.
  //
  // 5 - We wait for the writes to be durable.
  //
  // Before replying to the leader we wait for the writes to be durable. We then
  // just update the last replicated watermark and respond.
  //
  // TODO - These failure scenarios need to be exercised in an unit
  //        test. Moreover we need to add more fault injection spots (well that
  //        and actually use the) for each of these steps.
  //        This will be done in a follow up patch.
  TRACE("Updating replica for $0 ops", request->ops_size());

  // The deduplicated request.
  LeaderRequest dedupedReq;
  auto& messages = dedupedReq.messages;

  // Snooze at the end, leader only starts heartbeating again after we respond
  // If this particular instance is banned from cluster manager,
  // then we snooze for longer to give other instances an opportunity to win
  // the election
  // We only activate this after the proper snooze point below
  auto snoozeGuard = folly::makeDismissedGuard(
      [this]() { SnoozeFailureDetector({}, minimumElectionTimeoutWithBan()); });

  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked());
    if (!cmeta_->isMemberInConfig(peer_uuid(), ACTIVE_CONFIG)) {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Allowing update even though not a member of the config";
    }

    dedupedReq.leaderUuid = request->caller_uuid();

    RETURN_NOT_OK(checkLeaderRequestUnlocked(request, response, &dedupedReq));
    if (response->status().has_error()) {
      // We had an error, like an invalid term, we still fill the response.
      FillConsensusResponseOKUnlocked(response);
      return Status::OK();
    }

    // Snooze the failure detector as soon as we decide to accept the message.
    // We are guaranteed to be acting as a FOLLOWER at this point by the above
    // sanity check.
    // We snooze for a longer timeout to allow for processing. snoozeGuard here
    // overwrites it to election timeout again at the end.
    snoozeGuard.rehire();
    SnoozeFailureDetector({}, updateReplicaSnoozeTimeout());

    STATS_raft_num_leader_heartbeat_received.add(1, KUDU_STATS_TAG);
    lastLeaderCommunicationTimeMicros_ = getMonoTimeMicros();

    // Reset the 'failedElectionsSinceStableLeader_' metric now that we've
    // accepted an update from the established leader. This is done in addition
    // to the reset of the value in setLeaderUuidUnlocked() because there is
    // a potential race between resetting the failed elections count in
    // setLeaderUuidUnlocked() and incrementing after a failed election
    // if another replica was elected leader in an election concurrent with
    // the one called by this replica.
    failedElectionsSinceStableLeader_ = 0;
    failedElectionsCandidateNotInConfig_ = 0;
    STATS_failed_elections_since_stable_leader.addValue(
        failedElectionsSinceStableLeader_, KUDU_STATS_TAG);

    // We update the lag metrics here in addition to after appending to the
    // queue so the metrics get updated even when the operation is rejected.
    queue_->UpdateLastIndexAppendedToLeader(
        request->last_idx_appended_to_leader());

    // Also prohibit voting for anyone for the minimum election timeout.
    // Recognize the assymmetry. Since this member has heard from LEADER it
    // will try to keep ring stable for next MinElectionTimeout.
    // However it will allow itself to solicit votes only after a Random
    // interval from 1x -> 2X of election timeout.
    withholdVotesUntil_ = MonoTime::Now() + minimumElectionTimeout();

    if (FLAGS_enable_raft_leader_lease) {
      // Renew the Leader Lease
      if ((request->ops_size() == 1 &&
           request->ops(0).op_type() == NO_OP) /* No-op */
          || leaderLeaseTerm_ == -1) {
        leaderLeaseTerm_ = request->caller_term();
        queue_->SetLeaderLeaseUntil(
            MonoTime::Now() +
            MonoDelta::FromMilliseconds(request->requested_lease_duration()));
        response->set_lease_granted(true);
      } else if (leaderLeaseTerm_ == request->caller_term()) {
        if (request->requested_lease_duration() == 0 /* Revoke Lease */) {
          queue_->SetLeaderLeaseUntil(MonoTime::Now());
        } else /* Extend Lease */ {
          queue_->SetLeaderLeaseUntil(
              MonoTime::Now() +
              MonoDelta::FromMilliseconds(request->requested_lease_duration()));
        }
        response->set_lease_granted(true);
      } else {
        response->set_lease_granted(false);
      }
    }

    // 1 - Early commit pending (and committed) transactions

    // What should we commit?
    // 1. As many pending transactions as we can, except...
    // 2. ...if we commit beyond the preceding index, we'd regress KUDU-639,
    // and...
    // 3. ...the leader's committed index is always our upper bound.
    const int64_t earlyApplyUpTo = std::min(
        {pending_->getLastPendingTransactionOpId().index(),
         dedupedReq.precedingOpId.index(),
         request->committed_index()});

    VLOG_WITH_PREFIX_UNLOCKED(1)
        << "Early marking committed up to " << earlyApplyUpTo
        << ", Last pending opid index: "
        << pending_->getLastPendingTransactionOpId().index()
        << ", preceding opid index: " << dedupedReq.precedingOpId.index()
        << ", requested index: " << request->committed_index();
    TRACE("Early marking committed up to index $0", earlyApplyUpTo);
    CHECK_OK(pending_->advanceCommittedIndex(earlyApplyUpTo));

    // 2 - Enqueue the prepares

    TRACE("Triggering prepare for $0 ops", messages.size());

    // Even for empty heartbeats, check if the replica can accept appends.
    // Without this, the leader flip-flops: the degraded (empty) heartbeat
    // succeeds because there is nothing to prepare, resetting the peer's
    // failure count, and the next full request fails again.
    if (messages.empty()) {
      Status checkStatus = roundHandler_->canAppend();
      if (PREDICT_FALSE(!checkStatus.ok())) {
        FillConsensusResponseError(
            response, ConsensusErrorPB::CANNOT_PREPARE, checkStatus);
        FillConsensusResponseOKUnlocked(response);
        return Status::OK();
      }
    }

    if (PREDICT_TRUE(!messages.empty())) {
      // This request contains at least one message, and is likely to increase
      // our memory pressure.
      double capacityPct;
      if (process_memory::softLimitExceeded(&capacityPct)) {
        STATS_follower_memory_pressure_rejections.add(1, KUDU_STATS_TAG);
        string msg = fmt::format(
            "Soft memory limit exceeded (at {:.2f}% of capacity)", capacityPct);
        if (capacityPct >= FLAGS_memory_limit_warn_threshold_percentage) {
          KLOG_EVERY_N_SECS(WARNING, 1)
              << "Rejecting consensus request [EVERY 1 second]: " << msg
              << THROTTLE_MSG;
        } else {
          KLOG_EVERY_N_SECS(INFO, 1)
              << "Rejecting consensus request [EVERY 1 second]: " << msg
              << THROTTLE_MSG;
        }
        return Status::ServiceUnavailable(msg);
      }
    }

    std::vector<ReplicateMsgWrapper> msgWrappers;
    msgWrappers.reserve(messages.size());
    // This is a best-effort way of isolating safe and expected failures
    // from true warnings.
    bool expectedRotationDelay = false;
    Status prepareStatus;
    auto iter = messages.begin();
    if (request->has_compression_dictionary()) {
      KLOG_EVERY_N_SECS(INFO, 180)
          << "[EVERY 3 mins] Received compression dictionary from leader";
      const std::string& compressionDict = request->compression_dictionary();
      RETURN_NOT_OK(CompressionCodecManager::setDictionary(compressionDict));
      persistentVars_->setCompressionDictionary(compressionDict);
      RETURN_NOT_OK(persistentVars_->flush());
    }
    while (iter != messages.end()) {
      // Create a ReplicateMsgWrapper which handles compression, here we'll be
      // decompressing the msg
      ReplicateMsgWrapper msgWrapper(*iter);
      prepareStatus = msgWrapper.init(&compressionBuffer_);

      if (prepareStatus.ok()) {
        prepareStatus = StartFollowerTransactionUnlocked(msgWrapper);
      }

      if (PREDICT_FALSE(!prepareStatus.ok())) {
        expectedRotationDelay = prepareStatus.IsIllegalState() &&
            (prepareStatus.ToString().find("Previous Rotate Event with") !=
             std::string::npos);
        break;
      }
      // TODO(dralves) Without leader leases this shouldn't be allowed to fail.
      // Once we have that functionality we'll have to revisit this.
      CHECK_OK(timeManager_->messageReceivedFromLeader(*(*iter)->get()));
      ++iter;
      msgWrappers.push_back(msgWrapper);
    }

    // If we stopped before reaching the end we failed to prepare some
    // message(s) and need to perform cleanup, namely trimming
    // deduped_req.messages to only contain the messages that were actually
    // prepared, and deleting the other ones since we've taken ownership when we
    // first deduped.
    if (iter != messages.end()) {
      if (!expectedRotationDelay) {
        LOG_WITH_PREFIX_UNLOCKED(WARNING) << fmt::format(
            "Could not prepare transaction for op '{}' and following {} ops. "
            "Status for this op: {}",
            (*iter)->get()->id().ShortDebugString(),
            std::distance(iter, messages.end()) - 1,
            prepareStatus.ToString());
      }
      iter = messages.erase(iter, messages.end());

      // If this is empty, it means we couldn't prepare a single de-duped
      // message. There is nothing else we can do. The leader will detect this
      // and retry later.
      if (messages.empty()) {
        string msg = fmt::format(
            "Rejecting Update request from peer {} for term {}. "
            "Could not prepare a single transaction due to: {}",
            request->caller_uuid(),
            request->caller_term(),
            prepareStatus.ToString());

        // Log the message only when there is no rotation message in this batch
        if (!expectedRotationDelay) {
          LOG_WITH_PREFIX_UNLOCKED(INFO) << msg;
        }

        Status s;
        if (prepareStatus.IsCompressionDictMismatch()) {
          s = Status::CompressionDictMismatch(std::move(msg));
        } else if (prepareStatus.IsCorruption()) {
          s = Status::Corruption(std::move(msg));
        } else {
          s = Status::IllegalState(std::move(msg));
        }

        FillConsensusResponseError(
            response, ConsensusErrorPB::CANNOT_PREPARE, s);
        FillConsensusResponseOKUnlocked(response);
        return Status::OK();
      }
    }

    // All transactions that are going to be prepared were started, advance the
    // safe timestamp.
    // TODO(dralves) This is only correct because the queue only sets safe time
    // when the request is an empty heartbeat. If we actually start setting this
    // on a consensus request along with actual messages we need to be careful
    // to ignore it if any of the messages fails to prepare.
    if (request->has_safe_timestamp()) {
      timeManager_->advanceSafeTime(Timestamp(request->safe_timestamp()));
    }

    OpId lastFromLeader;
    // 3 - Enqueue the writes.
    // Now that we've triggered the prepares enqueue the operations to be
    // written to the WAL.
    if (PREDICT_TRUE(!messages.empty())) {
      int64_t precedingTerm = dedupedReq.precedingOpId.term();
      lastFromLeader = messages.back()->get()->id();
      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      //
      // Since we've prepared, we need to be able to append (or we risk trying
      // to apply later something that wasn't logged). We crash if we can't.
      CHECK_OK(queue_->AppendOperations(msgWrappers, syncStatusCb));
      if (cmeta_->lastKnownLeader().uuid().empty() ||
          lastFromLeader.term() != precedingTerm) {
        HandleNewTermAppendedUnlocked(lastFromLeader.term());
      }
    } else {
      lastFromLeader = dedupedReq.precedingOpId;
    }

    // 4 - Mark transactions as committed

    // Choose the last operation to be applied. This will either be
    // 'committed_index', if no prepare enqueuing failed, or the minimum between
    // 'committed_index' and the id of the last successfully enqueued prepare,
    // if some prepare failed to enqueue.
    int64_t applyUpTo;
    if (lastFromLeader.index() < request->committed_index()) {
      // we should never apply anything later than what we received in this
      // request
      applyUpTo = lastFromLeader.index();

      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Received commit index " << request->committed_index()
          << " from the leader but only" << " marked up to " << applyUpTo
          << " as committed.";
    } else {
      applyUpTo = request->committed_index();
    }

    VLOG_WITH_PREFIX_UNLOCKED(1) << "Marking committed up to " << applyUpTo;
    TRACE("Marking committed up to $0", applyUpTo);
    CHECK_OK(pending_->advanceCommittedIndex(applyUpTo));
    queue_->UpdateFollowerWatermarks(
        applyUpTo,
        request->all_replicated_index(),
        request->region_durable_index());

    // If any messages failed to be started locally, then we already have
    // removed them from 'deduped_req' at this point. So, 'lastFromLeader' is
    // the last one that we might apply.
    lastReceivedCurLeader_ = lastFromLeader;

    // Fill the response with the current state. We will not mutate anymore
    // state until we actually reply to the leader, we'll just wait for the
    // messages to be durable.
    FillConsensusResponseOKUnlocked(response);
    if (!haveQueuedLdcbOrNorcb_) {
      ScheduleLeaderDetectedCallback(currentTermUnlocked());
    }
  }
  // Release the lock while we wait for the log append to finish so that commits
  // can go through. We'll re-acquire it before we update the state again.

  // Update the last replicated op id
  if (!messages.empty()) {
    // 5 - We wait for the writes to be durable.

    // Note that this is safe because dist consensus now only supports a single
    // outstanding request at a time and this way we can allow commits to
    // proceed while we wait.
    TRACE("Waiting on the replicates to finish logging");
    TRACE_EVENT0("consensus", "Wait for log");
    Status s;
    do {
      // If just waiting for our log append to finish lets snooze the timer.
      // We don't want to fire leader election because we're waiting on our own
      // log.
      SnoozeFailureDetector();
      s = logSynchronizer.waitFor(
          MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms));
    } while (s.IsTimedOut());
    RETURN_NOT_OK(s);

    TRACE("finished");
  }

  VLOG_WITH_PREFIX(2) << "Replica updated. " << ToString()
                      << ". Request: " << SecureShortDebugString(*request);

  TRACE("UpdateReplicas() finished");
  return Status::OK();
}

void RaftConsensus::FillConsensusResponseOKUnlocked(
    ConsensusResponsePB* response) {
  DCHECK(lock_.is_locked());
  TRACE("Filling consensus response to leader.");
  response->set_responder_term(currentTermUnlocked());

  // if RESPONSE STATUS does not have error - i.e. common case
  // and there are messages in the request, then lastReceivedCurLeader_
  // = last_from_leader
  // and AppendOperations also uses the same OpId (last_id) to
  // update queue_state_.last_appended.
  // So in COMMON case both the first and second OpId's should be the same.
  response->mutable_status()->mutable_last_received()->CopyFrom(
      queue_->GetLastOpIdInLog());
  response->mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      lastReceivedCurLeader_);
  response->mutable_status()->set_last_committed_idx(
      queue_->GetCommittedIndex());
}

void RaftConsensus::FillConsensusResponseError(
    ConsensusResponsePB* response,
    ConsensusErrorPB::Code error_code,
    const Status& status) {
  ConsensusErrorPB* error = response->mutable_status()->mutable_error();
  error->set_code(error_code);
  statusToPb(status, error->mutable_status());
}

Status RaftConsensus::requestVote(
    const VoteRequestPB* request,
    TabletVotingState tabletVotingState,
    VoteResponsePB* response) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::requestVote",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);
  response->set_responder_uuid(peer_uuid());

  // We must acquire the update lock in order to ensure that this vote action
  // takes place between requests.
  // Lock ordering: updateLock_ must be acquired before lock_.
  std::unique_lock<simple_mutexlock> updateGuard(updateLock_, std::defer_lock);
  if (FLAGS_enable_leader_failure_detection &&
      request->mode() != ElectionMode::ELECT_EVEN_IF_LEADER_IS_ALIVE &&
      request->mode() != ElectionMode::MOCK_ELECTION) {
    updateGuard.try_lock();
  } else {
    // If failure detection is not enabled, then we can't just reject the vote,
    // because there will be no automatic retry later. So, block for the lock.
    updateGuard.lock();
  }
  if (!updateGuard.owns_lock()) {
    // There is another vote or update concurrent with the vote. In that case,
    // that other request is likely to reset the timer, and we'll end up just
    // voting "NO" after waiting. To avoid starving RPC handlers and causing
    // cascading timeouts, just vote a quick NO.
    //
    // We still need to take the state lock in order to respond with term info,
    // etc.
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    return RequestVoteRespondIsBusy(request, response);
  }

  // Acquire the replica state lock so we can read / modify the consensus state.
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);

  // Ensure our lifecycle state is compatible with voting.
  // If RaftConsensus is running, we use the latest OpId from the WAL to vote.
  // Otherwise, we must be voting while tombstoned.
  OpId localLastLoggedOpId;
  switch (state_) {
    case kShutdown:
      return Status::IllegalState("cannot vote while shut down");
    case kRunning:
      // Note: it is (theoretically) possible for 'tombstone_last_logged_opid'
      // to be passed in and by the time we reach here the state is kRunning.
      // That may occur when a vote request comes in at the end of a tablet
      // copy and then tablet bootstrap completes quickly. In that case, we
      // ignore the passed-in value and use the latest OpId from our queue.
      localLastLoggedOpId = queue_->GetLastOpIdInLog();
      break;
    default:
      if (!tabletVotingState.tombstoneLastLoggedOpId) {
        return Status::IllegalState(
            "must be running to vote when last-logged opid is not known");
      }
      if (!FLAGS_raft_enable_tombstoned_voting) {
        return Status::IllegalState(
            "must be running to vote when tombstoned voting is disabled");
      }
      localLastLoggedOpId = *(tabletVotingState.tombstoneLastLoggedOpId);
      break;
  }
  DCHECK(localLastLoggedOpId.IsInitialized());

  if (request->mode() == MOCK_ELECTION) {
    if (!request->has_mock_election_snapshot_op_id()) {
      return RequestVoteRespondInvalidClientRequest(
          response,
          "mock_election_snapshot_op_id must be provided in a Mock Election");
    }
    localLastLoggedOpId =
        MinOpId(localLastLoggedOpId, request->mock_election_snapshot_op_id());
  }

  // If the node is not in the configuration, allow the vote (this is required
  // by Raft) but log an informational message anyway.
  std::string hostnamePort("[NOT-IN-CONFIG]");
  response->mutable_voter_context()->set_is_candidate_removed(false);

  // to be used later for lag check.
  std::string candidateQuorumId;
  bool isCandidateVoter = false;

  // Check if the CANDIDATE is in current config.
  if (FLAGS_enable_flexi_raft &&
      !cmeta_->isMemberInConfigWithDetail(
          request->candidate_uuid(),
          ACTIVE_CONFIG,
          &hostnamePort,
          &isCandidateVoter,
          &candidateQuorumId)) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Handling vote request from an unknown peer "
        << request->candidate_uuid();
    if (cmeta_->IsPeerRemoved(request->candidate_uuid())) {
      response->mutable_voter_context()->set_is_candidate_removed(true);
    }

    // Now try to see if Candidate Context was sent in order to populate
    // candidateQuorumId and hostnamePort. This is best effort since
    // CANDIDATEs are not guaranteed to send their context, however in current
    // use cases @Meta, it is always sent.
    std::string hnamePort;
    if (request->has_candidate_context() &&
        request->candidate_context().has_candidate_peer_pb()) {
      const RaftPeerPB& candidatePeerPb =
          request->candidate_context().candidate_peer_pb();
      getRaftPeerDetail(
          candidatePeerPb,
          &hnamePort,
          &isCandidateVoter,
          &candidateQuorumId,
          cmeta_->ActiveConfig().commit_rule());
      hostnamePort = fmt::format("{} ({})", hostnamePort, hnamePort);
    }
  }

  // If we've heard recently from the leader, then we should ignore the request.
  // It might be from a "disruptive" server. This could happen in a few cases:
  //
  // 1) Network partitions
  // If the leader can talk to a majority of the nodes, but is partitioned from
  // a bad node, the bad node's failure detector will trigger. If the bad node
  // is able to reach other nodes in the cluster, it will continuously trigger
  // elections.
  //
  // 2) An abandoned node
  // It's possible that a node has fallen behind the log GC mark of the leader.
  // In that case, the leader will stop sending it requests. Eventually, the the
  // configuration will change to eject the abandoned node, but until that
  // point, we don't want the abandoned follower to disturb the other nodes.
  //
  // See also https://ramcloud.stanford.edu/~ongaro/thesis.pdf
  // section 4.2.3.

  if (withholdVotes_) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Rejecting vote request from peer " << request->candidate_uuid()
        << " " << hostnamePort << " for testing.";
    return RequestVoteRespondVoteWitheld(
        request, hostnamePort, "votes are being witheld for testing", response);
  }

  if (request->mode() != ELECT_EVEN_IF_LEADER_IS_ALIVE &&
      request->mode() != MOCK_ELECTION &&
      (FLAGS_enable_raft_leader_lease
           ? MonoTime::Now() <
               std::max<MonoTime>(
                   withholdVotesUntil_, queue_->GetLeaderLeaseUntil())
           : MonoTime::Now() < withholdVotesUntil_)) {
    return RequestVoteRespondLeaderIsAlive(request, hostnamePort, response);
  }

  // Candidate is running behind.
  if (request->candidate_term() < currentTermUnlocked()) {
    return RequestVoteRespondInvalidTerm(request, hostnamePort, response);
  }

  // We already voted this term.
  if (request->candidate_term() == currentTermUnlocked() &&
      HasVotedCurrentTermUnlocked()) {
    // Already voted for the same candidate in the current term.
    if (GetVotedForCurrentTermUnlocked() == request->candidate_uuid()) {
      return RequestVoteRespondVoteAlreadyGranted(
          request, hostnamePort, response);
    }

    // Voted for someone else in current term.
    return RequestVoteRespondAlreadyVotedForOther(
        request, hostnamePort, response);
  }

  // Candidate must have last-logged OpId at least as large as our own to get
  // our vote.

  // New Non Standard Voting Heuristic: In Flexi Raft, since quorum sizes are
  // typically small, we have much lesser tolerance for failed nodes. A
  // partially failed node can be a node which is lagging (say) 10 million opids
  // behind the CANDIDATE. In normal raft, it will give votes to the CANDIDATE,
  // but if the VOTER is in same region as CANDIDATE, the VOTER will also soon
  // be part of the write quorum for the No-Op and needs to be caught up. Hence
  // giving VOTES eagerly when SELF is much much behind the CANDIDATE, will not
  // help No-Op commit. So we add another heuristic to address this. Here is how
  // it works
  // 1. It withholds votes if SELF is much behind CANDIDATE.
  // 2. Only applies to flexi raft
  // 3. Heuristic has a kill switch
  // 4. Only applies to VOTERs which are in the same region as CANDIDATE
  bool checkSrdLag = false;
  if ((FLAGS_lag_threshold_for_request_vote != -1) && FLAGS_enable_flexi_raft) {
    // single region dynamic mode, where quorum is in LEADER's region.
    checkSrdLag = !candidateQuorumId.empty() &&
        (peer_quorum_id(/*need_lock=*/false) == candidateQuorumId);
  }

  // Regular Raft protocol: Give vote if CANDIDATE is ahead of VOTER.
  bool voteYes = !OpIdLessThan(
      request->candidate_status().last_received(), localLastLoggedOpId);
  // MODIFIED heuristic explained above.
  if (voteYes && checkSrdLag) {
    int64_t lag =
        (request->candidate_status().last_received().index() -
         localLastLoggedOpId.index());
    int64_t lagThreshold = request->mode() == MOCK_ELECTION
        ? 0
        : FLAGS_lag_threshold_for_request_vote;
    if (lag > lagThreshold) {
      return RequestVoteRespondVoteWitheld(
          request,
          hostnamePort,
          fmt::format(
              "votes are being witheld for huge lag "
              "{} > {}, candidate at: {}, voter at: {}",
              lag,
              lagThreshold,
              SecureShortDebugString(
                  request->candidate_status().last_received()),
              SecureShortDebugString(localLastLoggedOpId)),
          response);
    }
  }

  // Record the term advancement if necessary. We don't do so in the case of
  // pre or mock elections because it's possible that the node who called the
  // pre or mock election has actually now successfully become leader of the
  // prior term, in which case bumping our term here would disrupt it.
  if (request->mode() != ElectionMode::PRE_ELECTION &&
      request->mode() != ElectionMode::MOCK_ELECTION &&
      request->candidate_term() > currentTermUnlocked()) {
    // If we are going to vote for this peer, then we will flush the consensus
    // metadata to disk below when we record the vote, and we can skip flushing
    // the term advancement to disk here.
    auto flush = voteYes ? kSkipFlushToDisk : kFlushToDisk;
    RETURN_NOT_OK_PREPEND(
        HandleTermAdvanceUnlocked(request->candidate_term(), flush),
        fmt::format(
            "Could not step down in RequestVote. Current term: {}, candidate term: {}",
            currentTermUnlocked(),
            request->candidate_term()));
  }

  if (!voteYes) {
    return RequestVoteRespondLastOpIdTooOld(
        localLastLoggedOpId, request, hostnamePort, response);
  }

  // Passed all our checks. Vote granted.
  return RequestVoteRespondVoteGranted(request, hostnamePort, response);
}

Status RaftConsensus::changeConfig(
    const ChangeConfigRequestPB& req,
    StdStatusCallback clientCb,
    std::optional<ServerErrorPB::Code>* errorCode) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::changeConfig",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);

  BulkChangeConfigRequestPB bulkReq;
  getBulkConfigChangeRequest(req, &bulkReq);

  return bulkChangeConfig(bulkReq, std::move(clientCb), errorCode);
}

void RaftConsensus::getBulkConfigChangeRequest(
    const ChangeConfigRequestPB& req,
    BulkChangeConfigRequestPB* bulkReq) {
  *(bulkReq->mutable_tablet_id()) = req.tablet_id();

  if (req.has_dest_uuid()) {
    *(bulkReq->mutable_dest_uuid()) = req.dest_uuid();
  }
  if (req.has_cas_config_opid_index()) {
    bulkReq->set_cas_config_opid_index(req.cas_config_opid_index());
  }
  if (req.has_external_version()) {
    *bulkReq->mutable_external_version() = req.external_version();
  }
  auto* change = bulkReq->add_config_changes();
  if (req.has_type()) {
    change->set_type(req.type());
  }
  if (req.has_server()) {
    *change->mutable_peer() = req.server();
  }
}

Status RaftConsensus::bulkChangeConfig(
    const BulkChangeConfigRequestPB& req,
    StdStatusCallback clientCb,
    std::optional<ServerErrorPB::Code>* errorCode) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::bulkChangeConfig",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);
  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    RaftConfigPB newConfig;
    checkBulkConfigChangeAndGetNewConfigUnlocked(req, errorCode, &newConfig);
    const RaftConfigPB committedConfig = cmeta_->committedConfig();

    RETURN_NOT_OK(replicateConfigChangeUnlocked(
        committedConfig,
        std::move(newConfig),
        std::bind(
            &RaftConsensus::MarkDirtyOnSuccess,
            this,
            string("Config change replication complete"),
            std::move(clientCb),
            std::placeholders::_1)));
  } // Release lock before signaling request.

  peerManager_->signalRequest();
  return Status::OK();
}

Status RaftConsensus::checkAndPopulateChangeConfigMessage(
    const ChangeConfigRequestPB& req,
    std::optional<ServerErrorPB::Code>* errorCode,
    ReplicateMsg* replicateMsg) {
  BulkChangeConfigRequestPB bulkReq;
  getBulkConfigChangeRequest(req, &bulkReq);

  LockGuard l(lock_);
  RaftConfigPB newConfig;
  RETURN_NOT_OK(checkBulkConfigChangeAndGetNewConfigUnlocked(
      bulkReq, errorCode, &newConfig));
  const RaftConfigPB committedConfig = cmeta_->committedConfig();

  RETURN_NOT_OK(createReplicateMsgFromConfigsUnlocked(
      committedConfig, std::move(newConfig), replicateMsg));

  return Status::OK();
}

Status RaftConsensus::checkAndPopulateChangeConfigMessage(
    const JointConsensusConfigChangeRequestPB& req,
    ReplicateMsg* replicateMsg,
    JointConsensusPhase jcStage) {
  if (req.new_peers_size() == 0) {
    return Status::InvalidArgument(
        "All peers in the intended new config cannot be empty");
  }

  // Create ReplicateMsg containing the transitional config for
  // joint-consensus phase.
  if (jcStage == JointConsensusPhase::START_JOINT_CONSENSUS) {
    LockGuard l(lock_);

    // Get the current committed config
    const RaftConfigPB committedConfig = cmeta_->committedConfig();

    // Create the transitional config for joint-consensus: C_old_new
    RaftConfigPB transitionalConfig;
    transitionalConfig.CopyFrom(committedConfig);
    transitionalConfig.mutable_next_config_peers()->CopyFrom(req.new_peers());

    // Combine both the current config and the transitional config into
    // ReplicateMsg: C_old => C_old_new
    RETURN_NOT_OK(createReplicateMsgFromConfigsUnlocked(
        committedConfig, std::move(transitionalConfig), replicateMsg));

  } else if (jcStage == JointConsensusPhase::FINISH_JOINT_CONSENSUS) {
    LockGuard l(lock_);

    // Phase-2 of joint-consenus (FINISH_JOINT_CONSENSUS) is valid only when
    // the currently committed config is C_old_new. Here, we validate that the
    // C_new's peers in the C_old_new is indeed the intended peers in `req`.
    const RaftConfigPB& committedConfig = cmeta_->committedConfig();
    const std::vector<RaftPeerPB> committedNewPeers =
        copyPeersIntoVector(committedConfig.next_config_peers());
    const std::vector<RaftPeerPB> intendedNewPeers =
        copyPeersIntoVector(req.new_peers());
    if (committedNewPeers.size() == 0) {
      return Status::IllegalState(
          "Expecting the committed config to be transitional config "
          "with non-empty next peers");
    }
    if (!isPeersEqual(committedNewPeers, intendedNewPeers)) {
      return Status::IllegalState(
          "Expecting the committed config to be transitional config whose "
          "next peers is the peers from the intended config in the request");
    }

    // Create the next config after joint-consensus: C_new
    RaftConfigPB nextConfig;
    nextConfig.CopyFrom(committedConfig);
    nextConfig.clear_next_config_peers();
    nextConfig.mutable_peers()->CopyFrom(committedConfig.next_config_peers());

    // Create the ReplicateMsg, having ConfigChangeRecordPB: C_old_new => C_new
    RETURN_NOT_OK(createReplicateMsgFromConfigsUnlocked(
        committedConfig, std::move(nextConfig), replicateMsg));

  } else {
    return Status::InvalidArgument("Unsupported joint-consensus phase");
  }

  return Status::OK();
}

Status RaftConsensus::checkAndSetExternalVersion(
    const ConfigExternalVersionPB& externalVersionReq,
    RaftConfigPB* newConfig,
    std::optional<ServerErrorPB::Code>* errorCode) {
  if (newConfig->external_version() != externalVersionReq.current_version()) {
    *errorCode = ServerErrorPB::CAS_FAILED;
    return Status::IllegalState(
        fmt::format(
            "Request specified external_version "
            "of {} but the committed config has external_version "
            "of {}",
            externalVersionReq.current_version(),
            newConfig->external_version()));
  }

  if (externalVersionReq.next_version() <= newConfig->external_version() &&
      !externalVersionReq.backdoor_allow_arbitrary_next_version()) {
    *errorCode = ServerErrorPB::INVALID_CONFIG;
    return Status::IllegalState(
        fmt::format(
            "Request specified next_version of {} is smaller "
            "and equal to committed config external_version "
            "of {}",
            externalVersionReq.next_version(),
            newConfig->external_version()));
  }

  // CAS and validation pass, set the new config external version
  newConfig->set_external_version(externalVersionReq.next_version());

  return Status::OK();
}

Status RaftConsensus::checkBulkConfigChangeAndGetNewConfigUnlocked(
    const BulkChangeConfigRequestPB& req,
    std::optional<ServerErrorPB::Code>* errorCode,
    RaftConfigPB* newConfig) {
  {
    DCHECK(lock_.is_locked());
    RETURN_NOT_OK(CheckRunningUnlocked());
    RETURN_NOT_OK(CheckActiveLeaderUnlocked());
    RETURN_NOT_OK(CheckNoConfigChangePendingUnlocked());

    // We are required by Raft to reject config change operations until we have
    // committed at least one operation in our current term as leader.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (!queue_->IsCommittedIndexInCurrentTerm()) {
      return Status::IllegalState(
          "Leader has not yet committed an operation in its own term");
    }

    const RaftConfigPB committedConfig = cmeta_->committedConfig();

    // Support atomic ChangeConfig requests.
    if (req.has_cas_config_opid_index()) {
      if (committedConfig.opid_index() != req.cas_config_opid_index()) {
        *errorCode = ServerErrorPB::CAS_FAILED;
        return Status::IllegalState(
            fmt::format(
                "Request specified cas_config_opid_index "
                "of {} but the committed config has opid_index "
                "of {}",
                req.cas_config_opid_index(),
                committedConfig.opid_index()));
      }
    }

    // 'newConfig' will be modified in-place and validated before being used
    // as the new Raft configuration.
    *newConfig = committedConfig;

    // CAS and validation for external version
    if (req.has_external_version()) {
      RETURN_NOT_OK(checkAndSetExternalVersion(
          req.external_version(), newConfig, errorCode));
    }

    // Enforce the "one by one" config change rules, even with the bulk API.
    // Keep track of total voters added, including non-voters promoted to
    // voters, and removed, including voters demoted to non-voters.
    int numVotersModified = 0;

    // A record of the peers being modified so that we can enforce only one
    // change per peer per request.
    unordered_set<string> peersModified;

    for (const auto& item : req.config_changes()) {
      if (PREDICT_FALSE(!item.has_type())) {
        *errorCode = ServerErrorPB::INVALID_CONFIG;
        return Status::InvalidArgument(
            "Must specify 'type' argument", SecureShortDebugString(req));
      }
      if (PREDICT_FALSE(!item.has_peer())) {
        *errorCode = ServerErrorPB::INVALID_CONFIG;
        return Status::InvalidArgument(
            "Must specify 'peer' argument", SecureShortDebugString(req));
      }

      ChangeConfigType type = item.type();
      const RaftPeerPB& peer = item.peer();

      if (PREDICT_FALSE(!peer.has_permanent_uuid())) {
        return Status::InvalidArgument(
            "peer must have permanent_uuid specified",
            SecureShortDebugString(req));
      }

      if (!peersModified.insert(peer.permanent_uuid()).second) {
        return Status::InvalidArgument(
            fmt::format(
                "only one change allowed per peer: peer {} appears more "
                "than once in the config change request",
                peer.permanent_uuid()),
            SecureShortDebugString(req));
      }

      const string& serverUuid = peer.permanent_uuid();
      bool peerBbd = peer.has_attrs() &&
          peer.attrs().has_backing_db_present() &&
          peer.attrs().backing_db_present();
      switch (type) {
        case ADD_PEER:
          // Ensure the peer we are adding is not already a member of the
          // configuration.
          if (isRaftConfigMember(serverUuid, committedConfig)) {
            return Status::InvalidArgument(
                fmt::format(
                    "Server with UUID {} is already a member of the config. RaftConfig: {}",
                    serverUuid,
                    SecureShortDebugString(committedConfig)));
          }
          if (!peer.has_member_type()) {
            return Status::InvalidArgument(
                "peer must have member_type specified",
                SecureShortDebugString(req));
          }
          if (!peer.has_last_known_addr()) {
            return Status::InvalidArgument(
                "peer must have last_known_addr specified",
                SecureShortDebugString(req));
          }
          if (FLAGS_enable_flexi_raft &&
              isUseQuorumId(committedConfig.commit_rule())) {
            if (!peerHasValidQuorumId(peer)) {
              return Status::InvalidArgument(
                  "Peer must have a non-empty quorum_id for voter and empty quorum_id "
                  "for non-voter",
                  SecureShortDebugString(req));
            }
          }
          if (peer.member_type() == RaftPeerPB::VOTER &&
              isStandbyMember(peer)) {
            return Status::InvalidArgument(
                "Peer can not be a VOTER and a standby member at the same time",
                SecureShortDebugString(req));
          }

          // In quorum_id is enabled, we have an option to disallow multiple
          // MySQL instances being added to the same quorum
          if (FLAGS_enable_flexi_raft &&
              isUseQuorumId(committedConfig.commit_rule()) &&
              !FLAGS_allow_multiple_backed_by_db_per_quorum && peerBbd) {
            std::string leaderUuidUnused;
            // A map from quorum id to actual number of backed_by_db voters in
            // config
            std::map<std::string, int> actualBbdVoterCounts;
            GetActualVoterCountsFromConfig(
                committedConfig,
                leaderUuidUnused,
                &actualBbdVoterCounts,
                /* leader_quorum_id */ nullptr,
                /* backed_by_db_only */ true);
            std::string peerQuorumIdVal =
                getQuorumId(peer, /* use_quorum_id */ true);
            auto it = actualBbdVoterCounts.find(peerQuorumIdVal);
            int count = (it != actualBbdVoterCounts.end()) ? it->second : 0;
            if (count >= 1) {
              return Status::AlreadyPresent(
                  "Not allow multiple backed_by_db instance "
                  "added to the same quorum.",
                  SecureShortDebugString(req));
            }
          }

          if (peer.member_type() == RaftPeerPB::VOTER) {
            numVotersModified++;
          }
          *(newConfig->add_peers()) = peer;
          break;

        case REMOVE_PEER:
          if (serverUuid == peer_uuid()) {
            return Status::InvalidArgument(
                fmt::format(
                    "Cannot remove peer {} from the config because it is the leader. "
                    "Force another leader to be elected to remove this peer. "
                    "Consensus state: {}",
                    serverUuid,
                    SecureShortDebugString(cmeta_->ToConsensusStatePB())));
          }
          if (!removeFromRaftConfig(newConfig, serverUuid)) {
            return Status::NotFound(
                fmt::format(
                    "Server with UUID {} not a member of the config. RaftConfig: {}",
                    serverUuid,
                    SecureShortDebugString(committedConfig)));
          }
          if (isRaftConfigVoter(serverUuid, committedConfig)) {
            numVotersModified++;

            // If we are in flexi-raft mode, we want to make sure that the
            // number of voters does not dip below quorum
            // requirements/min-rep-factor So if we have 6 voters in LEADER
            // region and min-replication-factor/ quorum = Majority(6) = 4, then
            // in healthy state we are expecting 6 voters. We can safely remove
            // 2 voters and we will still have 4 voters, having enough for
            // write-availability, but we can't remove another one as that will
            // make #voters=3 which will prevent commit with a MIN-REP-FACTOR=4
            // We are also currently only enforcing this requirement in current
            // LEADER region for single region dynami mode. In SRD mode we allow
            // other regions to go below this requirement, because it gives
            // flexibility to automation to replace nodes without impacting the
            // write availability.
            if (FLAGS_enable_flexi_raft) {
              std::map<std::string, int> vdMap;
              GetVoterDistributionForQuorumId(committedConfig, &vdMap);

              std::map<std::string, int> votersInConfigPerQuorum;
              std::string unusedLeaderQuorum;
              std::string unusedLeaderUuid;
              // Get number of voters in each region
              GetActualVoterCountsFromConfig(
                  committedConfig,
                  unusedLeaderUuid,
                  &votersInConfigPerQuorum,
                  &unusedLeaderQuorum);

              // single region dynamic mode.
              for (const RaftPeerPB& configPeer : committedConfig.peers()) {
                if (configPeer.permanent_uuid() != serverUuid) {
                  continue;
                }

                // Zeroed in on the peer we are about to remove.
                const std::string& quorumId = getQuorumId(
                    configPeer, cmeta_->ActiveConfig().commit_rule());

                // In SINGLE REGION DYANMIC mode, we only do this extra check
                // in current LEADER region. the local peer is the LEADER
                // because of CheckActiveLeaderUnlocked above
                if (quorumId != peer_quorum_id(/* need_lock */ false)) {
                  break;
                }
                int currentCount = votersInConfigPerQuorum[quorumId];
                // reduce count by 1
                int futureCount = currentCount - 1;
                auto vdItr = vdMap.find(quorumId);
                if (vdItr != vdMap.end()) {
                  int expectedVoters = (*vdItr).second;
                  int quorum = majoritySize(expectedVoters);
                  if (futureCount < quorum) {
                    return Status::InvalidArgument(
                        fmt::format(
                            "Cannot remove a voter in quorum: {}"
                            " which will make future voter count: {} dip below expected voters: {}",
                            quorumId,
                            futureCount,
                            quorum));
                  }
                }
                break;
              }
            }
          }
          break;

        case MODIFY_PEER: {
          LOG(INFO) << "modifying peer" << peer.ShortDebugString();
          if (FLAGS_enable_flexi_raft &&
              isUseQuorumId(committedConfig.commit_rule())) {
            if (!peerHasValidQuorumId(peer)) {
              return Status::InvalidArgument(
                  "Peer must have a non-empty quorum_id for voter and empty quorum_id "
                  "for non-voter",
                  SecureShortDebugString(req));
            }
          }
          if (peer.member_type() == RaftPeerPB::VOTER &&
              isStandbyMember(peer)) {
            return Status::InvalidArgument(
                "Peer can not be a VOTER and a standby member at the same time",
                SecureShortDebugString(req));
          }

          RaftPeerPB* modifiedPeer;
          RETURN_NOT_OK(
              getRaftConfigMember(newConfig, serverUuid, &modifiedPeer));
          const RaftPeerPB origPeer(*modifiedPeer);
          // Override 'member_type' and items within 'attrs' only if they are
          // explicitly passed in the request. At least one field must be
          // modified to be a valid request.
          if (peer.has_member_type() &&
              peer.member_type() != modifiedPeer->member_type()) {
            if (modifiedPeer->member_type() == RaftPeerPB::VOTER ||
                peer.member_type() == RaftPeerPB::VOTER) {
              // This is a 'member_type' change involving a VOTER, i.e. a
              // promotion or demotion.
              numVotersModified++;
            }
            // A leader must be forced to step down before demoting it.
            if (serverUuid == peer_uuid()) {
              return Status::InvalidArgument(
                  fmt::format(
                      "Cannot modify member type of peer {} because it is the leader. "
                      "Cause another leader to be elected to modify this peer. "
                      "Consensus state: {}",
                      serverUuid,
                      SecureShortDebugString(cmeta_->ToConsensusStatePB())));
            }
            modifiedPeer->set_member_type(peer.member_type());
          }
          modifiedPeer->mutable_attrs()->CopyFrom(peer.attrs());
          // Ensure that MODIFY_PEER actually modified something.
          if (MessageDifferencer::Equals(origPeer, *modifiedPeer)) {
            return Status::InvalidArgument(
                "must modify a field when calling MODIFY_PEER");
          }
          break;
        }

        default:
          return Status::NotSupported(
              fmt::format(
                  "{}: unsupported type of configuration change",
                  ChangeConfigType_Name(type)));
      }
    }

    // Don't allow no-op config changes to be committed.
    if (MessageDifferencer::Equals(committedConfig, *newConfig)) {
      return Status::InvalidArgument(
          "requested configuration change does not "
          "actually modify the config",
          SecureShortDebugString(req));
    }

    // Ensure this wasn't an illegal bulk change.
    if (numVotersModified > 1) {
      return Status::InvalidArgument(
          "it is not safe to modify the VOTER status "
          "of more than one peer at a time",
          SecureShortDebugString(req));
    }

    // We'll assign a new opid_index to this config change.
    newConfig->clear_opid_index();
  }
  return Status::OK();
}

Status RaftConsensus::unsafeChangeConfig(
    const UnsafeChangeConfigRequestPB& req,
    std::optional<ServerErrorPB::Code>* errorCode) {
  if (PREDICT_FALSE(!req.has_new_config())) {
    *errorCode = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(
        "Request must contain 'new_config' argument "
        "to unsafeChangeConfig()",
        SecureShortDebugString(req));
  }
  if (PREDICT_FALSE(!req.has_caller_id())) {
    *errorCode = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(
        "Must specify 'caller_id' argument to unsafeChangeConfig()",
        SecureShortDebugString(req));
  }

  // Grab the committed config and current term on this node.
  int64_t currentTerm;
  RaftConfigPB committedConfig;
  int64_t allReplicatedIndex;
  int64_t lastCommittedIndex;
  OpId precedingOpId;
  uint64_t msgTimestamp;
  {
    // Take the snapshot of the replica state and queue state so that
    // we can stick them in the consensus update request later.
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    currentTerm = currentTermUnlocked();
    committedConfig = cmeta_->committedConfig();
    if (cmeta_->hasPendingConfig()) {
      LOG_WITH_PREFIX_UNLOCKED(WARNING)
          << "Replica has a pending config, but the new config "
          << "will be unsafely changed anyway. "
          << "Currently pending config on the node: "
          << SecureShortDebugString(cmeta_->PendingConfig());
    }
    allReplicatedIndex = queue_->GetAllReplicatedIndex();
    lastCommittedIndex = queue_->GetCommittedIndex();
    precedingOpId = queue_->GetLastOpIdInLog();
    msgTimestamp = timeManager_->getSerialTimestamp().value();
  }

  // Validate that passed replica uuids are part of the committed config
  // on this node.  This allows a manual recovery tool to only have to specify
  // the uuid of each replica in the new config without having to know the
  // addresses of each server (since we can get the address information from
  // the committed config). Additionally, only a subset of the committed config
  // is required for typical cluster repair scenarios.
  std::unordered_set<string> retainedPeerUuids;
  const RaftConfigPB& config = req.new_config();
  for (const RaftPeerPB& new_peer : config.peers()) {
    const string& peerUuid = new_peer.permanent_uuid();
    retainedPeerUuids.insert(peerUuid);
    if (!isRaftConfigMember(peerUuid, committedConfig)) {
      *errorCode = ServerErrorPB::INVALID_CONFIG;
      return Status::InvalidArgument(
          fmt::format(
              "Peer with uuid {} is not in the committed  "
              "config on this replica, rejecting the  "
              "unsafe config change request for tablet {}. "
              "Committed config: {}",
              peerUuid,
              req.tablet_id(),
              SecureShortDebugString(committedConfig)));
    }
  }

  RaftConfigPB newConfig = committedConfig;
  for (const auto& peer : committedConfig.peers()) {
    const string& peerUuid = peer.permanent_uuid();
    if (!retainedPeerUuids.contains(peerUuid)) {
      CHECK(removeFromRaftConfig(&newConfig, peerUuid));
    }
  }
  // Check that local peer is part of the new config and is a VOTER.
  // Although it is valid for a local replica to not have itself
  // in the committed config, it is rare and a replica without itself
  // in the latest config is definitely not caught up with the latest leader's
  // log.
  if (!isRaftConfigVoter(peer_uuid(), newConfig)) {
    *errorCode = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(
        fmt::format(
            "Local replica uuid {} is not "
            "a VOTER in the new config, "
            "rejecting the unsafe config "
            "change request for tablet {}. "
            "Rejected config: {}",
            peer_uuid(),
            req.tablet_id(),
            SecureShortDebugString(newConfig)));
  }
  newConfig.set_unsafe_config_change(true);
  int64_t replicateOpIdIndex = precedingOpId.index() + 1;
  newConfig.set_opid_index(replicateOpIdIndex);

  // Sanity check the new config. 'type' is irrelevant here.
  Status s = verifyRaftConfig(newConfig);
  if (!s.ok()) {
    *errorCode = ServerErrorPB::INVALID_CONFIG;
    return Status::InvalidArgument(
        fmt::format(
            "The resulting new config for tablet {}  "
            "from passed parameters has failed raft "
            "config sanity check: {}",
            req.tablet_id(),
            s.ToString()));
  }

  // Prepare the consensus request as if the request is being generated
  // from a different leader.
  ConsensusRequestPB consensusReq;
  consensusReq.set_caller_uuid(req.caller_id());
  // Bumping up the term for the consensus request being generated.
  // This makes this request appear to come from a new leader that
  // the local replica doesn't know about yet. If the local replica
  // happens to be the leader, this will cause it to step down.
  const int64_t newTerm = currentTerm + 1;
  consensusReq.set_caller_term(newTerm);
  consensusReq.mutable_preceding_id()->CopyFrom(precedingOpId);
  consensusReq.set_committed_index(lastCommittedIndex);
  consensusReq.set_all_replicated_index(allReplicatedIndex);

  // Prepare the replicate msg to be replicated.
  ReplicateMsg* replicate = consensusReq.add_ops();
  ChangeConfigRecordPB* ccReq = replicate->mutable_change_config_record();
  ccReq->set_tablet_id(req.tablet_id());
  *ccReq->mutable_old_config() = committedConfig;
  *ccReq->mutable_new_config() = newConfig;
  OpId* id = replicate->mutable_id();
  // Bumping up both the term and the opid_index from what's found in the log.
  id->set_term(newTerm);
  id->set_index(replicateOpIdIndex);
  replicate->set_op_type(CHANGE_CONFIG_OP);
  replicate->set_timestamp(msgTimestamp);

  VLOG_WITH_PREFIX(3) << "unsafeChangeConfig: Generated consensus request: "
                      << SecureShortDebugString(consensusReq);

  LOG_WITH_PREFIX(WARNING)
      << "PROCEEDING WITH UNSAFE CONFIG CHANGE ON THIS SERVER, "
      << "COMMITTED CONFIG: " << SecureShortDebugString(committedConfig)
      << "NEW CONFIG: " << SecureShortDebugString(newConfig);

  ConsensusResponsePB consensusResp;
  return update(&consensusReq, &consensusResp).andThen([&consensusResp] {
    return consensusResp.has_error()
        ? statusFromPb(consensusResp.error().status())
        : Status::OK();
  });
}

Status RaftConsensus::changeProxyTopology(
    const ProxyTopologyPB& proxy_topology) {
  LockGuard l(lock_);
  return routingTableContainer_->updateProxyTopology(
      proxy_topology, cmeta_->ActiveConfig(), cmeta_->leaderUuid());
}

Status RaftConsensus::updateProxyRegionGroup(
    const std::vector<std::unordered_set<std::string>>& region_groups) {
  LockGuard l(lock_);
  return routingTableContainer_->updateProxyRegionGroup(
      region_groups, cmeta_->ActiveConfig(), cmeta_->leaderUuid());
}

std::vector<std::unordered_set<std::string>>
RaftConsensus::getProxyRegionGroup() {
  LockGuard l(lock_);
  return routingTableContainer_->getProxyRegionGroup();
}

ProxyTopologyPB RaftConsensus::getProxyTopology() const {
  LockGuard l(lock_);
  return routingTableContainer_->getProxyTopology();
}

void RaftConsensus::stop() {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::Shutdown",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);

  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    if (state_ == kStopping || state_ == kStopped || state_ == kShutdown) {
      return;
    }
    // Transition to kStopping state.
    setStateUnlocked(kStopping);
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Raft consensus shutting down.";
  }

  // Close the peer manager.
  if (peerManager_) {
    peerManager_->close();
  }

  // We must close the queue after we close the peers.
  if (queue_) {
    queue_->Close();
  }

  {
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    if (pending_)
      CHECK_OK(pending_->cancelPendingTransactions());
    setStateUnlocked(kStopped);

    // Clear leader status on Stop(), in case this replica was the leader. If
    // we don't do this, the log messages still show this node as the leader.
    // No need to sync it since it's not persistent state.
    if (cmeta_) {
      ClearLeaderUnlocked();
    }

    // If we were the leader, stop withholding votes.
    if (withholdVotesUntil_ == MonoTime::Max()) {
      withholdVotesUntil_ = MonoTime::Min();
    }

    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Raft consensus is shut down!";
  }

  // Shut down things that might acquire locks during destruction.
  if (raftPoolToken_) {
    raftPoolToken_->Shutdown();
  }
  if (failureDetector_) {
    disableFailureDetector();
  }
}

void RaftConsensus::shutdown() {
  // Avoid taking locks if already shut down so we don't violate
  // ThreadRestrictions assertions in the case where the RaftConsensus
  // destructor runs on the reactor thread due to an election callback being
  // the last outstanding reference.
  if (shutdown_.load(kMemOrderAcquire)) {
    return;
  }

  stop();
  {
    LockGuard l(lock_);
    setStateUnlocked(kShutdown);
  }
  shutdown_.store(true, kMemOrderRelease);
}

Status RaftConsensus::StartConsensusOnlyRoundUnlocked(
    const ReplicateRefPtr& msg) {
  DCHECK(lock_.is_locked());
  OperationType op_type = msg->get()->op_type();
  CHECK(IsConsensusOnlyOperation(op_type))
      << "Expected a consensus-only op type, got "
      << OperationType_Name(op_type) << ": "
      << SecureShortDebugString(*msg->get());
  if (op_type == NO_OP) {
    ScheduleNoOpReceivedCallback(msg);
  }
  VLOG_WITH_PREFIX_UNLOCKED(1) << "Starting consensus round: "
                               << SecureShortDebugString(msg->get()->id());
  std::shared_ptr<ConsensusRound> round(new ConsensusRound(this, msg));
  RETURN_NOT_OK(roundHandler_->startConsensusOnlyRound(round));

  // Using disableNoop_ mode as a proxy for special NORCB handling
  // When in disableNoop_ mode, the SetConsensusReplicatedCallback
  // will be enqueued in the MySQL plugin.
  // In this case we should not enqueue nonTxRoundReplicationFinished
  // below, as it also does unsupported things, e.g. enqueing a CommitMsg
  if (!disableNoop_) {
    StdStatusCallback client_cb = std::bind(
        &RaftConsensus::MarkDirtyOnSuccess,
        this,
        string("Replicated consensus-only round"),
        &doNothingStatusCb,
        std::placeholders::_1);
    round->SetConsensusReplicatedCallback(
        std::bind(
            &RaftConsensus::nonTxRoundReplicationFinished,
            this,
            round.get(),
            std::move(client_cb),
            std::placeholders::_1));
  }
  return AddPendingOperationUnlocked(round);
}

Status RaftConsensus::advanceTermForTests(int64_t new_term) {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  CHECK_OK(CheckRunningUnlocked());
  return HandleTermAdvanceUnlocked(new_term);
}

std::string RaftConsensus::getRequestVoteLogPrefixUnlocked(
    const VoteRequestPB& request) const {
  DCHECK(lock_.is_locked());
  return fmt::format(
      "{}Leader {} vote request",
      logPrefixUnlocked(),
      ElectionMode_Name(request.mode()));
}

void RaftConsensus::FillVoteResponsePreviousVoteHistory(
    VoteResponsePB* response) {
  CHECK(response);

  // Populate previous vote history and last pruned term.
  const std::map<int64_t, PreviousVotePB>& previous_vote_history =
      cmeta_->previousVoteHistory();
  std::map<int64_t, PreviousVotePB>::const_iterator it =
      previous_vote_history.begin();
  while (it != previous_vote_history.end()) {
    response->add_previous_vote_history()->CopyFrom(it->second);
    it++;
  }
  response->set_last_pruned_term(cmeta_->lastPrunedTerm());
}

void RaftConsensus::FillVoteResponseLastKnownLeader(VoteResponsePB* response) {
  CHECK(response);
  response->mutable_last_known_leader()->CopyFrom(cmeta_->lastKnownLeader());
}

void RaftConsensus::FillVoteResponseVoteGranted(VoteResponsePB* response) {
  response->set_responder_term(currentTermUnlocked());
  response->set_vote_granted(true);
  FillVoteResponsePreviousVoteHistory(response);
  FillVoteResponseLastKnownLeader(response);
}

void RaftConsensus::FillVoteResponseVoteDenied(
    ConsensusErrorPB::Code error_code,
    VoteResponsePB* response) {
  response->set_responder_term(currentTermUnlocked());
  response->set_vote_granted(false);
  response->mutable_consensus_error()->set_code(error_code);
  FillVoteResponsePreviousVoteHistory(response);
  FillVoteResponseLastKnownLeader(response);
}

Status RaftConsensus::RequestVoteRespondInvalidTerm(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::INVALID_TERM, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} {} for earlier term {}. "
      "Current term is {}. Candidate context {}. ",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      request->candidate_term(),
      currentTermUnlocked(),
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::InvalidArgument(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteAlreadyGranted(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  FillVoteResponseVoteGranted(response);
  LOG(INFO) << fmt::format(
      "{}: Already granted yes {} for candidate {} {} in term {}. "
      "Candidate context {}. "
      "Re-sending same reply.",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      request->candidate_term(),
      GetCandidateContextString(request));
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondAlreadyVotedForOther(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::ALREADY_VOTED, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} {} in current term {}: "
      "Already voted for candidate {} in this term. "
      "Candidate context {}.",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      currentTermUnlocked(),
      GetVotedForCurrentTermUnlocked(),
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::InvalidArgument(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondLastOpIdTooOld(
    const OpId& localLastLoggedOpId,
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::LAST_OPID_TOO_OLD, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} {} for term {} because "
      "replica has last-logged OpId of {}, which is greater than that of the "
      "candidate, which has last-logged OpId of {}. "
      "Candidate context: {}.",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      request->candidate_term(),
      SecureShortDebugString(localLastLoggedOpId),
      SecureShortDebugString(request->candidate_status().last_received()),
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::InvalidArgument(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteWitheld(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    const std::string& withholdReason,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::UNKNOWN, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} {} for term {} "
      "because of reason: {}. Candidate context: {}.",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      request->candidate_term(),
      withholdReason,
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::InvalidArgument(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondLeaderIsAlive(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::LEADER_IS_ALIVE, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} {} for term {} because "
      "replica is either leader or believes a valid leader to "
      "be alive. Candidate context: {}.",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      hostnamePort,
      request->candidate_uuid(),
      request->candidate_term(),
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::InvalidArgument(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondIsBusy(
    const VoteRequestPB* request,
    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::CONSENSUS_BUSY, response);
  string msg = fmt::format(
      "{}: Denying {} to candidate {} for term {} because "
      "replica is already servicing an update from a current leader "
      "or another vote. Candidate context: {}. ",
      getRequestVoteLogPrefixUnlocked(*request),
      ElectionMode_Name(request->mode()),
      request->candidate_uuid(),
      request->candidate_term(),
      GetCandidateContextString(request));
  LOG(INFO) << msg;
  statusToPb(
      Status::ServiceUnavailable(msg),
      response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteGranted(
    const VoteRequestPB* request,
    const std::string& hostnamePort,
    VoteResponsePB* response) {
  DCHECK(lock_.is_locked());
  // We know our vote will be "yes", so avoid triggering an election while we
  // persist our vote to disk. We use an exponential backoff to avoid too much
  // split-vote contention when nodes display high latencies.
  MonoDelta backoff = LeaderElectionExpBackoffDeltaUnlocked();
  SnoozeFailureDetector(string("vote granted"), backoff);

  ElectionMode mode = request->mode();
  if (mode != ElectionMode::PRE_ELECTION &&
      mode != ElectionMode::MOCK_ELECTION) {
    // Persist our vote to disk.
    RETURN_NOT_OK(SetVotedForCurrentTermUnlocked(request->candidate_uuid()));
  }

  FillVoteResponseVoteGranted(response);

  // Give peer time to become leader. Snooze one more time after persisting our
  // vote. When disk latency is high, this should help reduce churn.
  SnoozeFailureDetector(/*reason_for_log=*/{}, backoff);

  LOG(INFO) << fmt::format(
      "{}: Granting yes vote for candidate {} {} in term {}. "
      "Candidate context: {}.",
      getRequestVoteLogPrefixUnlocked(*request),
      hostnamePort,
      request->candidate_uuid(),
      currentTermUnlocked(),
      GetCandidateContextString(request));
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondInvalidClientRequest(
    VoteResponsePB* response,
    const std::string& errorMessage) {
  LOG(INFO) << "Invalid client request in RequestVote: " << errorMessage;
  response->mutable_error()->set_code(ServerErrorPB::INVALID_CLIENT_REQUEST);
  statusToPb(
      Status::InvalidArgument(errorMessage),
      response->mutable_error()->mutable_status());
  return Status::OK();
}

std::string RaftConsensus::GetCandidateContextString(
    const VoteRequestPB* request) {
  std::string msg = "No candidate context";

  if (request && request->has_candidate_context() &&
      request->candidate_context().has_candidate_peer_pb()) {
    const RaftPeerPB& candidate_peer_pb =
        request->candidate_context().candidate_peer_pb();

    if (candidate_peer_pb.has_last_known_addr()) {
      const HostPortPB& host_port = candidate_peer_pb.last_known_addr();
      msg = fmt::format(
          "Candidate host {}.  Candidate port {}.",
          host_port.host(),
          host_port.port());
    }
  }

  return msg;
}

RaftPeerPB::Role RaftConsensus::role(bool lock) const {
  ThreadRestrictions::assertWaitAllowed();
  std::optional<UniqueLock> opt_lock;
  if (lock) {
    opt_lock.emplace(lock_);
  } else {
    DCHECK(lock_.is_locked());
  }
  return cmeta_->activeRole();
}

int64_t RaftConsensus::currentTerm() const {
  LockGuard l(lock_);
  return currentTermUnlocked();
}

string RaftConsensus::getLeaderUuid() const {
  LockGuard l(lock_);
  return getLeaderUuidUnlocked();
}

std::pair<string, unsigned int> RaftConsensus::getLeaderHostPort() const {
  LockGuard l(lock_);
  return cmeta_->leaderHostport();
}

void RaftConsensus::setStateUnlocked(State new_state) {
  switch (new_state) {
    case kInitialized:
      CHECK_EQ(kNew, state_);
      break;
    case kRunning:
      CHECK_EQ(kInitialized, state_);
      break;
    case kStopping:
      CHECK(state_ != kStopped && state_ != kShutdown)
          << "State = " << stateName(state_);
      break;
    case kStopped:
      CHECK_EQ(kStopping, state_);
      break;
    case kShutdown:
      CHECK(state_ == kStopped || state_ == kShutdown)
          << "State = " << stateName(state_);
      break;
    default:
      LOG(FATAL) << "Disallowed transition to state = " << stateName(new_state);
  }
  state_ = new_state;
}

const char* RaftConsensus::stateName(State state) {
  switch (state) {
    case kNew:
      return "New";
    case kInitialized:
      return "Initialized";
    case kRunning:
      return "Running";
    case kStopping:
      return "Stopping";
    case kStopped:
      return "Stopped";
    case kShutdown:
      return "Shut down";
    default:
      LOG(DFATAL) << "Unknown State value: " << state;
      return "Unknown";
  }
}

Status RaftConsensus::setLeaderUuidUnlocked(const string& uuid) {
  DCHECK(lock_.is_locked());
  failedElectionsSinceStableLeader_ = 0;
  failedElectionsCandidateNotInConfig_ = 0;
  STATS_failed_elections_since_stable_leader.addValue(
      failedElectionsSinceStableLeader_, KUDU_STATS_TAG);
  cmeta_->setLeaderUuid(uuid);

  Status s = Status::OK();
  routingTableContainer_->updateLeader(uuid);
  MarkDirty(fmt::format("New leader {}", uuid));
  return s;
}

Status RaftConsensus::replicateConfigChangeUnlocked(
    RaftConfigPB old_config,
    RaftConfigPB new_config,
    StdStatusCallback clientCb) {
  DCHECK(lock_.is_locked());
  auto cc_replicate = std::make_unique<ReplicateMsg>();
  RETURN_NOT_OK(createReplicateMsgFromConfigsUnlocked(
      std::move(old_config), std::move(new_config), cc_replicate.get()));

  std::shared_ptr<ConsensusRound> round(new ConsensusRound(
      this,
      std::make_shared<RefCountedReplicate>(
          std::move(cc_replicate), Source::Memory)));
  round->SetConsensusReplicatedCallback(
      std::bind(
          &RaftConsensus::nonTxRoundReplicationFinished,
          this,
          round.get(),
          std::move(clientCb),
          std::placeholders::_1));

  return AppendNewRoundToQueueUnlocked(round);
}

Status RaftConsensus::createReplicateMsgFromConfigsUnlocked(
    RaftConfigPB old_config,
    RaftConfigPB new_config,
    ReplicateMsg* cc_replicate) {
  DCHECK(lock_.is_locked());
  cc_replicate->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRecordPB* cc_req = cc_replicate->mutable_change_config_record();
  cc_req->set_tablet_id(options_.tablet_id);
  *cc_req->mutable_old_config() = std::move(old_config);
  *cc_req->mutable_new_config() = std::move(new_config);
  CHECK_OK(timeManager_->assignTimestamp(cc_replicate));
  return Status::OK();
}

Status RaftConsensus::refreshConsensusQueueAndPeersUnlocked() {
  DCHECK(lock_.is_locked());
  DCHECK_EQ(RaftPeerPB::LEADER, cmeta_->activeRole());
  const RaftConfigPB& active_config = cmeta_->ActiveConfig();

  // Change the peers so that we're able to replicate messages remotely and
  // locally. The peer manager must be closed before updating the active config
  // in the queue -- when the queue is in LEADER mode, it checks that all
  // registered peers are a part of the active config.
  peerManager_->close();
  // TODO(todd): should use queue committed index here? in that case do
  // we need to pass it in at all?
  queue_->SetLeaderMode(
      pending_->getCommittedIndex(), currentTermUnlocked(), active_config);
  RETURN_NOT_OK(peerManager_->updateRaftConfig(active_config));
  return Status::OK();
}

const string& RaftConsensus::peer_uuid() const {
  return localPeerPb_.permanent_uuid();
}

std::string RaftConsensus::peer_region() const {
  bool has_region =
      localPeerPb_.has_attrs() && localPeerPb_.attrs().has_region();

  if (!has_region) {
    return "";
  }

  return localPeerPb_.attrs().region();
}

std::string RaftConsensus::peer_quorum_id(bool need_lock) const {
  if (need_lock) {
    LockGuard l(lock_);
  }
  return cmeta_->ActiveConfig().has_commit_rule()
      ? getQuorumId(localPeerPb_, cmeta_->ActiveConfig().commit_rule())
      : "";
}

std::pair<string, unsigned int> RaftConsensus::peer_hostport() const {
  if (localPeerPb_.has_last_known_addr()) {
    const ::kudu::HostPortPB& host_port = localPeerPb_.last_known_addr();
    std::string host = host_port.host();
    return std::make_pair(host_port.host(), host_port.port());
  }
  return {};
}

bool RaftConsensus::peer_is_standby_member() const {
  return isStandbyMember(localPeerPb_);
}

uint32_t RaftConsensus::peer_standby_start_timestamp() const {
  return localPeerPb_.has_attrs() &&
          localPeerPb_.attrs().has_standby_start_timestamp()
      ? localPeerPb_.attrs().standby_start_timestamp()
      : 0;
}

const string& RaftConsensus::tablet_id() const {
  return options_.tablet_id;
}

Status RaftConsensus::ConsensusState(
    ConsensusStatePB* cstate,
    IncludeHealthReport report_health,
    bool lock) const {
  ThreadRestrictions::assertWaitAllowed();
  std::optional<UniqueLock> opt_lock;
  if (lock) {
    opt_lock.emplace(lock_);
  } else {
    DCHECK(lock_.is_locked());
  }

  if (state_ == kShutdown) {
    return Status::IllegalState("Tablet replica is shutdown");
  }
  ConsensusStatePB cstate_tmp = cmeta_->ToConsensusStatePB();

  // If we need to include the health report, merge it into the committed
  // config iff we believe we are the current leader of the config.
  if (report_health == INCLUDE_HEALTH_REPORT &&
      cmeta_->activeRole() == RaftPeerPB::LEADER) {
    auto reports = queue_->ReportHealthOfPeers();

    // We don't need to access the queue anymore, so drop the consensus lock.
    if (opt_lock) {
      opt_lock->unlock();
    }

    // Iterate through each peer in the committed config and attach the health
    // report to it.
    RaftConfigPB* committed_raft_config = cstate_tmp.mutable_committed_config();
    for (int i = 0; i < committed_raft_config->peers_size(); i++) {
      RaftPeerPB* peer = committed_raft_config->mutable_peers(i);
      auto it = reports.find(peer->permanent_uuid());
      if (it == reports.end()) {
        continue; // Only attach details if we know about the peer.
      }
      *peer->mutable_health_report() = it->second;
    }
  }
  *cstate = std::move(cstate_tmp);
  return Status::OK();
}

RaftConfigPB RaftConsensus::CommittedConfig() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->committedConfig();
}

Status RaftConsensus::PendingConfig(RaftConfigPB* pendingConfig) const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  if (cmeta_->hasPendingConfig()) {
    *pendingConfig = cmeta_->PendingConfig();
    return Status::OK();
  }
  return Status::NotFound("No pending config found");
}

void RaftConsensus::ElectionCallback(
    ElectionContext context,
    const ElectionResult& result,
    std::function<void(const ElectionResult&)> callback) {
  if (callback) {
    callback(result);
  }

  // We stop here if we're doing mock elections as we do not want to incur any
  // side effects.
  if (result.vote_request.mode() == ElectionMode::MOCK_ELECTION) {
    return;
  }

  // The election callback runs on a reactor thread, so we need to defer to
  // our threadpool. If the threadpool is already shut down for some reason,
  // it's OK
  // -- we're OK with the callback never running.
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::NestedElectionDecisionCallback,
              shared_from_this(),
              std::move(context),
              result)),
      LogPrefixThreadSafe() + "Unable to run election callback");
}

void RaftConsensus::DoElectionCallback(
    const ElectionContext& context,
    const ElectionResult& result) {
  DCHECK(result.vote_request.mode() != MOCK_ELECTION);
  const int64_t electionTerm = result.vote_request.candidate_term();
  const bool wasPreElection =
      result.vote_request.mode() == ElectionMode::PRE_ELECTION;
  const char* electionType = wasPreElection ? "pre-election" : "election";

  // The vote was granted, become leader.
  ThreadRestrictions::assertWaitAllowed();
  UniqueLock lock(lock_);
  Status s = CheckRunningUnlocked();
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Received " << electionType << " callback for term " << electionTerm
        << " while not running: " << s.ToString();
    return;
  }

  // Snooze to avoid the election timer firing again as much as possible.
  // We need to snooze when we win and when we lose:
  // - When we win because we're about to disable the timer and become leader.
  // - When we lose or otherwise we can fall into a cycle, where everyone
  // keeps
  //   triggering elections but no election ever completes because by the time
  //   they finish another one is triggered already.
  if (result.decision == VOTE_DENIED && result.is_candidate_removed) {
    // TODO: disable detector after a few attempts
    SnoozeFailureDetector(
        string("election complete - candidate not in config"),
        LeaderElectionExpBackoffNotInConfig());
    failedElectionsCandidateNotInConfig_++;
  } else {
    SnoozeFailureDetector(
        string("election complete"), LeaderElectionExpBackoffDeltaUnlocked());
  }

  if (result.decision == VOTE_DENIED) {
    failedElectionsSinceStableLeader_++;
    STATS_failed_elections_since_stable_leader.addValue(
        failedElectionsSinceStableLeader_, KUDU_STATS_TAG);
    STATS_raft_num_failed_elections.add(1, KUDU_STATS_TAG);

    // If we called an election and one of the voters had a higher term than
    // we did, we should bump our term before we potentially try again. This
    // is particularly important with pre-elections to avoid getting "stuck"
    // in a case like:
    //    Peer A: has ops through 1.10, term = 2, voted in term 2 for peer C
    //    Peer B: has ops through 1.15, term = 1
    // In this case, Peer B will reject peer A's pre-elections for term 3
    // because the local log is longer. Peer A will reject B's pre-elections
    // for term 2 because it already voted in term 2. The check below
    // ensures that peer B will bump to term 2 when it gets the vote
    // rejection, such that its next pre-election (for term 3) would
    // succeed.
    if (result.highest_voter_term > currentTermUnlocked()) {
      HandleTermAdvanceUnlocked(result.highest_voter_term);
    }

    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leader " << electionType << " lost for term " << electionTerm
        << ". Reason: "
        << (!result.message.empty() ? result.message : "None given");
    return;
  }

  // In a pre-election, we collected votes for the _next_ term.
  // So, we need to adjust our expectations of what the current term should
  // be.
  int64_t electionStartedInTerm = electionTerm;
  if (wasPreElection) {
    electionStartedInTerm--;
  }

  if (electionStartedInTerm != currentTermUnlocked()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Leader " << electionType << " decision vote started in "
        << "defunct term " << electionStartedInTerm << ": "
        << (result.decision == VOTE_GRANTED ? "won" : "lost");
    return;
  }

  if (!cmeta_->isVoterInConfig(peer_uuid(), ACTIVE_CONFIG)) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Leader " << electionType << " decision while not in active config. "
        << "Result: Term " << electionTerm << ": "
        << (result.decision == VOTE_GRANTED ? "won" : "lost")
        << ". RaftConfig: " << SecureShortDebugString(cmeta_->ActiveConfig());
    return;
  }

  if (cmeta_->activeRole() == RaftPeerPB::LEADER) {
    // If this was a pre-election, it's possible to see the following
    // interleaving:
    //
    //  1. Term N (follower): send a real election for term N
    //  2. Election callback expires again
    //  3. Term N (follower): send a pre-election for term N+1
    //  4. Election callback for real election from term N completes.
    //     Peer is now leader for term N.
    //  5. Pre-election callback from term N+1 completes, even though
    //     we are currently a leader of term N.
    // In this case, we should just ignore the pre-election, since we're
    // happily the leader of the prior term.
    if (wasPreElection) {
      return;
    }
    LOG_WITH_PREFIX_UNLOCKED(DFATAL)
        << "Leader " << electionType << " callback while already leader! "
        << "Result: Term " << electionTerm << ": "
        << (result.decision == VOTE_GRANTED ? "won" : "lost");
    return;
  }

  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Leader " << electionType << " won for term " << electionTerm;

  if (wasPreElection) {
    // We just won the pre-election. So, we need to call a real election.
    lock.unlock();
    WARN_NOT_OK(
        startElection(NORMAL_ELECTION, context),
        "Couldn't start leader election after successful pre-election");
  } else {
    // We won a real election. Convert role to LEADER.
    CHECK_OK(setLeaderUuidUnlocked(peer_uuid()));

    // TODO(todd): becomeLeaderUnlocked() can fail due to state checks during
    // shutdown. It races with the above state check. This could be a problem
    // during tablet deletion.
    CHECK_OK(becomeLeaderUnlocked());
  }
}

void RaftConsensus::NestedElectionDecisionCallback(
    const ElectionContext& context,
    const ElectionResult& result) {
  DCHECK(result.vote_request.mode() != MOCK_ELECTION);
  DoElectionCallback(context, result);
  if (result.vote_request.mode() != ElectionMode::PRE_ELECTION && edcb_) {
    edcb_(result, std::move(context));
  }
}

std::optional<OpId> RaftConsensus::getNextOpId() const {
  LockGuard l(lock_);
  if (!queue_) {
    return {};
  }
  return queue_->GetNextOpId();
}

std::optional<OpId> RaftConsensus::getLastOpId(OpIdType type) {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return getLastOpIdUnlocked(type);
}

std::optional<OpId> RaftConsensus::getLastOpIdUnlocked(OpIdType type) {
  // Return early if this method is called on an instance of RaftConsensus
  // that has not yet been started, failed during Init(), or failed during
  // Start().
  if (!queue_ || !pending_) {
    return {};
  }

  switch (type) {
    case RECEIVED_OPID:
      return queue_->GetLastOpIdInLog();
    case COMMITTED_OPID:
      return MakeOpId(
          pending_->getTermWithLastCommittedOp(),
          pending_->getCommittedIndex());
    default:
      LOG(DFATAL) << logPrefixUnlocked() << "Invalid OpIdType " << type;
      return {};
  }
}

log::RetentionIndexes RaftConsensus::getRetentionIndexes() {
  // Grab the watermarks from the queue. It's OK to fetch these two watermarks
  // separately -- the worst case is we see a relatively "out of date"
  // watermark which just means we'll retain slightly more than necessary in
  // this invocation of log GC.
  return log::RetentionIndexes(
      queue_->GetCommittedIndex(), // for durability
      queue_->GetAllReplicatedIndex(), // for peers
      queue_->GetRegionDurableIndex()); // for region based durability
}

void RaftConsensus::MarkDirty(const std::string& reason) {
  WARN_NOT_OK(
      raftPoolToken_->SubmitClosure(Bind(markDirtyClbk_, reason)),
      LogPrefixThreadSafe() + "Unable to run MarkDirty callback");
}

void RaftConsensus::MarkDirtyOnSuccess(
    const string& reason,
    const StdStatusCallback& clientCb,
    const Status& status) {
  if (PREDICT_TRUE(status.ok())) {
    MarkDirty(reason);
  }
  clientCb(status);
}

void RaftConsensus::nonTxRoundReplicationFinished(
    ConsensusRound* round,
    const StdStatusCallback& clientCb,
    const Status& status) {
  // NOTE: lock_ is held here because this is triggered by
  // PendingRounds::abortOpsAfter() and advanceCommittedIndex().
  DCHECK(lock_.is_locked());
  OperationType op_type = round->replicate_msg()->op_type();
  const string& op_type_str = OperationType_Name(op_type);
  CHECK(IsConsensusOnlyOperation(op_type))
      << "Unexpected op type: " << op_type_str;

  if (op_type == CHANGE_CONFIG_OP) {
    CompleteConfigChangeRoundUnlocked(round, status);
    // Fall through to the generic handling.
  }

  // TODO(mpercy): May need some refactoring to unlock 'lock_' before invoking
  // the client callback.

  if (!status.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << op_type_str << " replication failed: " << status.ToString();
    clientCb(status);
    return;
  }
  VLOG_WITH_PREFIX_UNLOCKED(1)
      << "Committing " << op_type_str << " with op id " << round->id();
  roundHandler_->finishConsensusOnlyRound(round);

  // Using disableNoop_ mode as a proxy for not pushing commit messages
  // after config change success.
  // The call stack should be.
  // StartConsensusOnlyRound in plugin
  //   -> calls StartFollowerTransaction in plugin
  //            which enques commitDoneCb
  // commitDoneCB gets fired which calls
  //     -> nonTxRoundReplicationFinished for OP_TYPE=CONFIG_CHANGE
  //         which should not call commit msg
  if (!disableNoop_) {
    unique_ptr<CommitMsg> commit_msg(new CommitMsg);
    commit_msg->set_op_type(round->replicate_msg()->op_type());
    *commit_msg->mutable_commited_op_id() = round->id();
  }

  clientCb(status);
}

void RaftConsensus::CompleteConfigChangeRoundUnlocked(
    ConsensusRound* round,
    const Status& status) {
  DCHECK(lock_.is_locked());
  const OpId& opId = round->replicate_msg()->id();

  if (!status.ok()) {
    // If the config change being aborted is the current pending one, abort
    // it.
    if (cmeta_->hasPendingConfig() &&
        cmeta_->getConfigOpIdIndex(PENDING_CONFIG) == opId.index()) {
      LOG_WITH_PREFIX_UNLOCKED(INFO) << "Aborting config change with OpId "
                                     << opId << ": " << status.ToString();
      cmeta_->clearPendingConfig();
      // We should not forget to "abort" the config change in the routing
      // table as well.
      RaftConfigPB activeConfig = cmeta_->ActiveConfig();
      CHECK_OK(routingTableContainer_->updateRaftConfig(activeConfig));
      UpdateLocalPeerUnlocked(activeConfig);

      // Disable leader failure detection if transitioning from VOTER to
      // NON_VOTER and vice versa.
      UpdateFailureDetectorState();
    } else {
      LOG_WITH_PREFIX_UNLOCKED(INFO)
          << "Skipping abort of non-pending config change with OpId " << opId
          << ": " << status.ToString();
    }

    // It's possible to abort a config change which isn't the pending one in
    // the following sequence:
    // - replicate a config change
    // - it gets committed, so we write the new config to disk as the
    // Committed configuration
    // - we crash before the COMMIT message hits the WAL
    // - we restart the server, and the config change is added as a pending
    // round again,
    //   but isn't set as Pending because it's already committed.
    // - we delete the tablet before committing it
    // See KUDU-1735.
    return;
  }

  // Commit the successful config change.

  DCHECK(round->replicate_msg()->change_config_record().has_old_config());
  DCHECK(round->replicate_msg()->change_config_record().has_new_config());
  const RaftConfigPB& oldConfig =
      round->replicate_msg()->change_config_record().old_config();
  const RaftConfigPB& newConfig =
      round->replicate_msg()->change_config_record().new_config();
  DCHECK(oldConfig.has_opid_index());
  DCHECK(newConfig.has_opid_index());
  // Check if the pending Raft config has an OpId less than the committed
  // config. If so, this is a replay at startup in which the COMMIT
  // messages were delayed.
  int64_t committedConfigOpIdIndex =
      cmeta_->getConfigOpIdIndex(COMMITTED_CONFIG);
  if (newConfig.opid_index() > committedConfigOpIdIndex) {
    std::vector<std::string> removedPeers;
    std::string configDiff =
        diffRaftConfigs(oldConfig, newConfig, &removedPeers);
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Committing config change with OpId " << opId << ": " << configDiff
        << ". New config: { " << SecureShortDebugString(newConfig) << " }";
    CHECK_OK(SetCommittedConfigUnlocked(newConfig));

    if (FLAGS_track_removed_peers) {
      cmeta_->InsertIntoRemovedPeersList(removedPeers);
    }
  } else {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Ignoring commit of config change with OpId " << opId
        << " because the committed config has OpId index "
        << committedConfigOpIdIndex
        << ". The config change we are ignoring is: " << "Old config: { "
        << SecureShortDebugString(oldConfig) << " }. " << "New config: { "
        << SecureShortDebugString(newConfig) << " }";
  }
}

void RaftConsensus::setAllowStartElection(bool val) {
  if (PREDICT_FALSE(persistentVars_->isStartElectionAllowed() != val)) {
    persistentVars_->setAllowStartElection(val);
    CHECK_OK(persistentVars_->flush());
  }
}

bool RaftConsensus::isStartElectionAllowed() const {
  return persistentVars_->isStartElectionAllowed();
}

Status RaftConsensus::setRaftRpcToken(std::optional<std::string> token) {
  LockGuard guard(lock_);

  if (shouldEnforceRaftRpcToken()) {
    return Status::IllegalState(
        "Raft RPC token cannot be changed when "
        "we're enforcing token matches");
  }

  persistentVars_->setRaftRpcToken(token);
  CHECK_OK(persistentVars_->flush());

  LOG_WITH_PREFIX_UNLOCKED(INFO)
      << "Raft RPC token has been changed to: " << token.value_or("<empty>");
  return Status::OK();
}

std::shared_ptr<const std::string> RaftConsensus::getRaftRpcToken() const {
  return persistentVars_->raftRpcToken();
}

bool RaftConsensus::shouldEnforceRaftRpcToken() const {
  return FLAGS_raft_enforce_rpc_token;
}

void RaftConsensus::enableFailureDetector(std::optional<MonoDelta> delta) {
  if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
    failureDetectorLastSnoozed_.store(
        std::chrono::system_clock::now(), std::memory_order_relaxed);
    failureDetector_->Start(std::move(delta));
  }
}

void RaftConsensus::disableFailureDetector() {
  failureDetector_->Stop();
}

void RaftConsensus::setWithholdVotesForTests(bool withhold_votes) {
  withholdVotes_ = withhold_votes;
}

void RaftConsensus::setRejectAppendEntriesForTests(bool reject_append_entries) {
  rejectAppendEntries_ = reject_append_entries;
}

void RaftConsensus::setAdjustVoterDistribution(bool val) {
  LockGuard l(lock_);
  queue_->SetAdjustVoterDistribution(val);
  adjustVoterDistribution_ = val;
}

void RaftConsensus::UpdateFailureDetectorState(std::optional<MonoDelta> delta) {
  DCHECK(lock_.is_locked());
  const auto& uuid = peer_uuid();
  if (uuid != cmeta_->leaderUuid() &&
      cmeta_->isVoterInConfig(uuid, ACTIVE_CONFIG)) {
    // A voter that is not the leader should run the failure detector.
    enableFailureDetector(std::move(delta));
  } else {
    // Otherwise, the local peer should not start leader elections
    // (e.g. if it is the leader, a non-voter, a non-participant, etc).
    disableFailureDetector();
  }
}

void RaftConsensus::SnoozeFailureDetector(
    std::optional<string> reason_for_log,
    std::optional<MonoDelta> delta) {
  if (PREDICT_TRUE(failureDetector_ && FLAGS_enable_leader_failure_detection)) {
    if (reason_for_log) {
      LOG(INFO) << LogPrefixThreadSafe()
                << fmt::format(
                       "Snoozing failure detection for {} ({})",
                       delta ? delta->ToString() : "election timeout",
                       *reason_for_log);
    }

    if (!delta) {
      delta = minimumElectionTimeout();
    }
    failureDetector_->Snooze(std::move(delta));
    failureDetectorLastSnoozed_.store(
        std::chrono::system_clock::now(), std::memory_order_relaxed);
  }
}

void RaftConsensus::PauseFailureDetector(std::optional<MonoDelta> delta) {
  if (PREDICT_TRUE(failureDetector_ && FLAGS_enable_leader_failure_detection)) {
    if (!delta) {
      delta = updateReplicaSnoozeTimeout();
    }

    if (std::optional<MonoDelta> time_left = failureDetector_->TimeLeft()) {
      VLOG(2) << "Pausing failure detector for " << delta->ToString()
              << " with " << time_left->ToString() << " left";
      *(failureDetectorTimeLeft_.wlock()) = std::move(time_left);
      failureDetector_->Snooze(std::move(delta));
    }
  }
}

void RaftConsensus::ResumeFailureDetector() {
  if (PREDICT_TRUE(failureDetector_ && FLAGS_enable_leader_failure_detection)) {
    std::optional<MonoDelta> time_left = failureDetectorTimeLeft_.withWLock(
        [](std::optional<MonoDelta>& time_left) {
          std::optional<MonoDelta> return_val = std::move(time_left);
          time_left = {};
          return return_val;
        });

    if (time_left) {
      VLOG(2) << "Resuming failure detector with " << time_left->ToString()
              << " left";
      failureDetector_->Snooze(*std::move(time_left));
    }
  }
}

MonoDelta RaftConsensus::updateReplicaSnoozeTimeout() const {
  int32_t failure_timeout = FLAGS_update_replica_snooze_heartbeat_periods *
      FLAGS_raft_heartbeat_interval_ms;
  return MonoDelta::FromMilliseconds(failure_timeout);
}

MonoDelta RaftConsensus::minimumElectionTimeout() const {
  int32_t failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
      FLAGS_raft_heartbeat_interval_ms;
  return MonoDelta::FromMilliseconds(failure_timeout);
}

Status RaftConsensus::setLeaseRenewStateUnlocked(LeaderLeaseState lease_state) {
  leaderLeaseState_ = lease_state;
  return Status::OK();
}

bool RaftConsensus::isLeaderLeaseSetForRevoke() const {
  return leaderLeaseState_ == LeaderLeaseState::kRevoke;
}

MonoDelta RaftConsensus::minimumElectionTimeoutWithBan() {
  // its double so approx comparison
  // Add a randomized window from Min-Election-Timeout to
  // 1.5 times Min-Election-Timeout i.e. from 3 HBs to
  // 4.5 HBs ( 1.5 seconds in default to 2.25 seconds )
  if (fabs(FLAGS_snooze_for_leader_ban_ratio - 1.0) < 0.001) {
    return TimeoutBackoffHelper(1.5);
  }

  int32_t failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
      FLAGS_raft_heartbeat_interval_ms * FLAGS_snooze_for_leader_ban_ratio;
  return MonoDelta::FromMilliseconds(failure_timeout);
}

// Value of 5 here will cap backoff at around 1 hour
constexpr int64_t kMaxBackOffExponent = 5;
MonoDelta RaftConsensus::LeaderElectionExpBackoffNotInConfig() {
  DCHECK(lock_.is_locked());
  // Compute a backoff factor based on how many leader elections have
  // failed since a stablie leader with a 'not-in-config' indicator
  // This is aggressive backoff starting with 5 seconds, 25 seconds, 125
  // seconds and so on
  double duration = pow(
      5,
      std::min(failedElectionsCandidateNotInConfig_ + 1, kMaxBackOffExponent));
  return MonoDelta::FromSeconds(duration);
}

MonoDelta RaftConsensus::LeaderElectionExpBackoffDeltaUnlocked() {
  DCHECK(lock_.is_locked());
  // Compute a backoff factor based on how many leader elections have
  // failed since a stable leader was last seen.
  double backoff_factor = pow(1.5, failedElectionsSinceStableLeader_ + 1);
  return TimeoutBackoffHelper(backoff_factor);
}

MonoDelta RaftConsensus::TimeoutBackoffHelper(double backoff_factor) {
  double min_timeout = minimumElectionTimeout().ToMilliseconds();
  double max_timeout = std::min<double>(
      min_timeout * backoff_factor,
      FLAGS_leader_failure_exp_backoff_max_delta_ms);

  // Randomize the timeout between the minimum and the calculated value.
  // We do this after the above capping to the max. Otherwise, after a
  // churny period, we'd end up highly likely to backoff exactly the max
  // amount.
  double timeout =
      min_timeout + (max_timeout - min_timeout) * rng_.nextDoubleFraction();
  DCHECK_GE(timeout, min_timeout);

  return MonoDelta::FromMilliseconds(timeout);
}

Status RaftConsensus::HandleTermAdvanceUnlocked(
    ConsensusTerm new_term,
    FlushToDisk flush) {
  DCHECK(lock_.is_locked());
  if (new_term <= currentTermUnlocked()) {
    return Status::IllegalState(
        fmt::format(
            "Can't advance term to: {} current term: {} is higher.",
            new_term,
            currentTermUnlocked()));
  }
  if (cmeta_->activeRole() == RaftPeerPB::LEADER) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Stepping down as leader of term " << currentTermUnlocked();
    RETURN_NOT_OK(becomeReplicaUnlocked());
  }

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Advancing to term " << new_term;
  RETURN_NOT_OK(SetCurrentTermUnlocked(new_term, flush));
  STATS_raft_term.addValue(new_term, KUDU_STATS_TAG);
  lastReceivedCurLeader_ = MinimumOpId();
  return Status::OK();
}

Status RaftConsensus::CheckSafeToReplicateUnlocked(
    const ReplicateMsg& /* msg */) const {
  DCHECK(lock_.is_locked());
  RETURN_NOT_OK(CheckRunningUnlocked());
  return CheckActiveLeaderUnlocked();
}

Status RaftConsensus::CheckRunningUnlocked() const {
  DCHECK(lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState(
        "RaftConsensus is not running",
        fmt::format("State = {}", stateName(state_)));
  }
  return Status::OK();
}

Status RaftConsensus::CheckActiveLeaderUnlocked() const {
  DCHECK(lock_.is_locked());
  RaftPeerPB::Role role = cmeta_->activeRole();
  switch (role) {
    case RaftPeerPB::LEADER:
      // Check for the consistency of the information in the consensus
      // metadata and the state of the consensus queue.
      DCHECK(queue_->IsInLeaderMode());
      if (leaderTransferInProgress_.load()) {
        return Status::ServiceUnavailable("leader transfer in progress");
      }
      return Status::OK();

    default:
      // Check for the consistency of the information in the consensus
      // metadata and the state of the consensus queue.
      DCHECK(!queue_->IsInLeaderMode());
      return Status::IllegalState(
          fmt::format(
              "Replica {} is not leader of this config. Role: {}. "
              "Consensus state: {}",
              peer_uuid(),
              RaftPeerPB::Role_Name(role),
              SecureShortDebugString(cmeta_->ToConsensusStatePB())));
  }
}

Status RaftConsensus::CheckNoConfigChangePendingUnlocked() const {
  DCHECK(lock_.is_locked());
  if (cmeta_->hasPendingConfig()) {
    return Status::IllegalState(
        fmt::format(
            "RaftConfig change currently pending. Only one is allowed at a time.\n"
            "  Committed config: {}.\n  Pending config: {}",
            SecureShortDebugString(cmeta_->committedConfig()),
            SecureShortDebugString(cmeta_->PendingConfig())));
  }
  return Status::OK();
}

Status RaftConsensus::SetPendingConfigUnlocked(const RaftConfigPB& new_config) {
  DCHECK(lock_.is_locked());
  RETURN_NOT_OK_PREPEND(
      verifyRaftConfig(new_config), "Invalid config to set as pending");
  if (adjustVoterDistribution_ && !new_config.unsafe_config_change()) {
    K_CHECK(
        !cmeta_->hasPendingConfig(),
        set_pending_config,
        "Attempt to set pending config while another is already pending! "
        "Existing pending config: {}. Attempted new pending config: {}",
        SecureShortDebugString(cmeta_->PendingConfig()),
        SecureShortDebugString(new_config));
  } else if (cmeta_->hasPendingConfig()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Allowing unsafe config change even though there is a pending config! "
        << "Existing pending config: "
        << SecureShortDebugString(cmeta_->PendingConfig()) << "; "
        << "New pending config: " << SecureShortDebugString(new_config);
  }
  cmeta_->setPendingConfig(new_config);
  RaftConfigPB active_config = cmeta_->ActiveConfig();
  RETURN_NOT_OK(routingTableContainer_->updateRaftConfig(active_config));
  UpdateLocalPeerUnlocked(active_config);

  UpdateFailureDetectorState();

  return Status::OK();
}

Status RaftConsensus::changeVoterDistribution(
    const TopologyConfigPB& topology_config,
    bool force) {
  TRACE_EVENT2(
      "consensus",
      "RaftConsensus::changeVoterDistribution",
      "peer",
      peer_uuid(),
      "tablet",
      options_.tablet_id);
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);

  // When force is true we're most likely running an unsafe config change
  // operation to regain availability so we have to live with pending config
  // changes and force apply a voter distribution to run an election
  if (!force) {
    // Do not allow any voter distribution changes on a unsquelched ring.
    Status s = CheckNoConfigChangePendingUnlocked();
    RETURN_NOT_OK(s);
  }

  RaftConfigPB config = cmeta_->ActiveConfig();
  config.clear_voter_distribution();
  config.mutable_voter_distribution()->insert(
      topology_config.voter_distribution().begin(),
      topology_config.voter_distribution().end());

  cmeta_->setActiveConfig(config);
  CHECK_OK(cmeta_->Flush());
  // NB: Not calling the Proxy routing table update as the
  // proxy routing table does not deal with Voter Distribution.
  // If this changes, please make sure this call is uncommented.
  // RETURN_NOT_OK(routingTableContainer_->updateRaftConfig(
  // cmeta_->ActiveConfig()));

  // Since voter distribution has changed, we need to refresh
  // consensus queue to make sure watermark calculation changes.
  if (cmeta_->activeRole() == RaftPeerPB::LEADER) {
    RETURN_NOT_OK(refreshConsensusQueueAndPeersUnlocked());
  }
  return Status::OK();
}

Status RaftConsensus::getVoterDistribution(
    std::map<std::string, int32_t>* vd) const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->voterDistribution(vd);
}

QuorumType RaftConsensus::getQuorumType() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return cmeta_->ActiveConfig().has_commit_rule() &&
          cmeta_->ActiveConfig().commit_rule().has_quorum_type()
      ? cmeta_->ActiveConfig().commit_rule().quorum_type()
      : QuorumType::REGION;
}

Status RaftConsensus::SetCommittedConfigUnlocked(
    const RaftConfigPB& config_to_commit) {
  TRACE_EVENT0("consensus", "RaftConsensus::SetCommittedConfigUnlocked");
  DCHECK(lock_.is_locked());
  DCHECK(config_to_commit.IsInitialized());
  RETURN_NOT_OK_PREPEND(
      verifyRaftConfig(config_to_commit), "Invalid config to set as committed");

  // Compare committed with pending configuration, ensure that they are the
  // same. In the event of an unsafe config change triggered by an
  // administrator, it is possible that the config being committed may not
  // match the pending config because
  // 1. Unsafe config change allows multiple pending configs to exist.
  // 2. When voter distribution adjustment is disabled there might be a
  // different in voter dist.
  // Therefore we only need to validate that 'config_to_commit' matches the
  // pending config if the pending config does not have its
  // 'unsafe_config_change' flag set or when voter distribution adjustment is
  // enabled.
  if (cmeta_->hasPendingConfig()) {
    RaftConfigPB pending_config = cmeta_->PendingConfig();
    if (adjustVoterDistribution_ && !pending_config.unsafe_config_change()) {
      // Quorums must be exactly equal, even w.r.t. peer ordering.
      K_CHECK(
          MessageDifferencer::Equals(pending_config, config_to_commit),
          setCommittedConfig,
          "New committed config must equal pending config, but does not. "
          "Pending config: {}, committed config: {}",
          SecureShortDebugString(pending_config),
          SecureShortDebugString(config_to_commit));
    }
  }
  cmeta_->setCommittedConfig(config_to_commit);
  cmeta_->clearPendingConfig();
  CHECK_OK(cmeta_->Flush());
  RaftConfigPB active_config = cmeta_->ActiveConfig();
  RETURN_NOT_OK(routingTableContainer_->updateRaftConfig(active_config));
  UpdateLocalPeerUnlocked(active_config);
  return Status::OK();
}

void RaftConsensus::ScheduleTermAdvancementCallback(int64_t new_term) {
  WARN_NOT_OK(
      raftPoolToken_->SubmitFunc(
          std::bind(
              &RaftConsensus::DoTermAdvancmentCallback,
              shared_from_this(),
              new_term)),
      LogPrefixThreadSafe() + "Unable to run term advancement callback");
}

void RaftConsensus::DoTermAdvancmentCallback(int64_t new_term) {
  // Simply execute the registered callback for term advancement.
  if (tacb_) {
    tacb_(new_term);
  }
}

void RaftConsensus::ScheduleNoOpReceivedCallback(const ReplicateRefPtr& msg) {
  DCHECK(lock_.is_locked());

  RaftPeerPB current_leader;
  Status s_ok =
      cmeta_->GetConfigMemberCopy(cmeta_->leaderUuid(), &current_leader);
  if (!s_ok.ok()) {
    // In case the leader is not part of current config
    // at the minimum set the uuid of the leader.
    // leader_uuid is expected to be present due to checkLeaderRequestUnlocked
    // implementation
    current_leader.set_permanent_uuid(cmeta_->leaderUuid());
  }

  s_ok = raftPoolToken_->SubmitFunc(
      std::bind(
          &RaftConsensus::DoNoOpReceivedCallback,
          shared_from_this(),
          msg->get()->id(),
          std::move(current_leader)));

  if (!s_ok.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Unable to run no op received callback";
  }

  haveQueuedLdcbOrNorcb_ = s_ok.ok();
}

void RaftConsensus::DoNoOpReceivedCallback(
    const OpId& id,
    const RaftPeerPB& leader_details) {
  if (norcb_) {
    norcb_(id, leader_details);
  }
}

void RaftConsensus::ScheduleLeaderDetectedCallback(int64_t term) {
  DCHECK(lock_.is_locked());
  RaftPeerPB current_leader;
  Status s_ok =
      cmeta_->GetConfigMemberCopy(cmeta_->leaderUuid(), &current_leader);
  if (!s_ok.ok()) {
    // In case the leader is not part of current config
    // at the minimum set the uuid of the leader.
    // leader_uuid is expected to be present due to checkLeaderRequestUnlocked
    // implementation
    current_leader.set_permanent_uuid(cmeta_->leaderUuid());
  }

  s_ok = raftPoolToken_->SubmitFunc(
      std::bind(
          &RaftConsensus::DoLeaderDetectedCallback,
          shared_from_this(),
          term,
          std::move(current_leader)));

  if (!s_ok.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Unable to run leader detected callback";
  }

  haveQueuedLdcbOrNorcb_ = s_ok.ok();
}

void RaftConsensus::DoLeaderDetectedCallback(
    int64_t term,
    const RaftPeerPB& leader_details) {
  if (ldcb_) {
    ldcb_(term, leader_details);
  }
}

Status RaftConsensus::setCurrentTermBootstrap(int64_t new_term) {
  LockGuard l(lock_);
  if (PREDICT_FALSE(new_term <= currentTermUnlocked())) {
    return Status::IllegalState(
        fmt::format(
            "Cannot change term to a term that is lower than or equal to the current one. "
            "Current: {}, Proposed: {}",
            currentTermUnlocked(),
            new_term));
  }
  cmeta_->setCurrentTerm(new_term);
  CHECK_OK(cmeta_->Flush());
  if (voteLogger_) {
    voteLogger_->advanceEpoch(new_term);
  }
  return Status::OK();
}

Status RaftConsensus::SetCurrentTermUnlocked(
    int64_t new_term,
    FlushToDisk flush) {
  TRACE_EVENT1(
      "consensus", "RaftConsensus::SetCurrentTermUnlocked", "term", new_term);
  DCHECK(lock_.is_locked());
  if (PREDICT_FALSE(new_term <= currentTermUnlocked())) {
    return Status::IllegalState(
        fmt::format(
            "Cannot change term to a term that is lower than or equal to the current one. "
            "Current: {}, Proposed: {}",
            currentTermUnlocked(),
            new_term));
  }
  cmeta_->setCurrentTerm(new_term);
  cmeta_->clearVotedFor();
  if (flush == kFlushToDisk) {
    CHECK_OK(cmeta_->Flush());
  }

  ClearLeaderUnlocked();
  if (voteLogger_) {
    voteLogger_->advanceEpoch(new_term);
  }

  // Trigger term advancement callback
  ScheduleTermAdvancementCallback(new_term);

  return Status::OK();
}

const int64_t RaftConsensus::currentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->currentTerm();
}

string RaftConsensus::getLeaderUuidUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->leaderUuid();
}

bool RaftConsensus::HasLeaderUnlocked() const {
  DCHECK(lock_.is_locked());
  return !getLeaderUuidUnlocked().empty();
}

void RaftConsensus::ClearLeaderUnlocked() {
  DCHECK(lock_.is_locked());
  cmeta_->setLeaderUuid("");
}

const bool RaftConsensus::HasVotedCurrentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  return cmeta_->hasVotedFor();
}

Status RaftConsensus::SetVotedForCurrentTermUnlocked(const std::string& uuid) {
  TRACE_EVENT1(
      "consensus",
      "RaftConsensus::SetVotedForCurrentTermUnlocked",
      "uuid",
      uuid);
  DCHECK(lock_.is_locked());
  cmeta_->setVotedFor(uuid);
  CHECK_OK(cmeta_->Flush());
  return Status::OK();
}

const std::string& RaftConsensus::GetVotedForCurrentTermUnlocked() const {
  DCHECK(lock_.is_locked());
  DCHECK(cmeta_->hasVotedFor());
  return cmeta_->votedFor();
}

const ConsensusOptions& RaftConsensus::GetOptions() const {
  return options_;
}

string RaftConsensus::LogPrefix() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return logPrefixUnlocked();
}

string RaftConsensus::logPrefixUnlocked() const {
  DCHECK(lock_.is_locked());
  // 'cmeta_' may not be set if initialization failed.
  string cmeta_info;
  if (cmeta_) {
    cmeta_info = fmt::format(
        " [term {} {}]",
        cmeta_->currentTerm(),
        RaftPeerPB::Role_Name(cmeta_->activeRole()));
  }
  return fmt::format(
      "T {} P {}{}: ", options_.tablet_id, peer_uuid(), cmeta_info);
}

string RaftConsensus::LogPrefixThreadSafe() const {
  return fmt::format("T {} P {}: ", options_.tablet_id, peer_uuid());
}

string RaftConsensus::ToString() const {
  ThreadRestrictions::assertWaitAllowed();
  LockGuard l(lock_);
  return ToStringUnlocked();
}

string RaftConsensus::ToStringUnlocked() const {
  DCHECK(lock_.is_locked());
  return fmt::format(
      "Replica: {}, State: {}, Role: {}",
      peer_uuid(),
      stateName(state_),
      RaftPeerPB::Role_Name(cmeta_->activeRole()));
}

int64_t RaftConsensus::metadataOnDiskSize() const {
  return cmeta_->on_disk_size();
}

ConsensusMetadata* RaftConsensus::consensus_metadata_for_tests() const {
  return cmeta_.get();
}

int64_t RaftConsensus::getMillisSinceLastLeaderHeartbeat() const {
  return lastLeaderCommunicationTimeMicros_ == 0
      ? 0
      : (getMonoTimeMicros() - lastLeaderCommunicationTimeMicros_) / 1000;
}

void RaftConsensus::SetElectionDecisionCallback(ElectionDecisionCallback edcb) {
  CHECK(edcb);
  edcb_ = std::move(edcb);
}

void RaftConsensus::SetTermAdvancementCallback(TermAdvancementCallback tacb) {
  CHECK(tacb);
  tacb_ = std::move(tacb);
}

void RaftConsensus::SetNoOpReceivedCallback(NoOpReceivedCallback norcb) {
  CHECK(norcb);
  norcb_ = std::move(norcb);
}

void RaftConsensus::SetLeaderDetectedCallback(LeaderDetectedCallback ldcb) {
  CHECK(ldcb);
  ldcb_ = std::move(ldcb);
}

void RaftConsensus::SetVoteLogger(
    std::shared_ptr<VoteLoggerInterface> vote_logger) {
  voteLogger_ = std::move(vote_logger);
}

bool RaftConsensus::isProxyRequest(const ConsensusRequestPB* request) const {
  // We expect proxy_uuid to reflect the uuid of the local node if it's a
  // proxy request, or to be empty otherwise.
  return !request->proxy_dest_uuid().empty();
}

// Set an error and respond.
// Stolen (mostly) from tablet_service.cc
static void SetupErrorAndRespond(
    const Status& s,
    ServerErrorPB::Code code,
    ConsensusResponsePB* response,
    rpc::RpcContext* context) {
  // Generic "service unavailable" errors will cause the client to retry
  // later.
  if ((code == ServerErrorPB::UNKNOWN_ERROR /*||
       code == TabletServerErrorPB::THROTTLED */) && s.IsServiceUnavailable()) {
    context->respondRpcFailure(rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  statusToPb(s, response->mutable_error()->mutable_status());
  response->mutable_error()->set_code(code);
  context->respondNoCache();
}

// Respond with an error and return if 's' is not OK.
#define RET_RESPOND_ERROR_NOT_OK(s)                             \
  do {                                                          \
    const kudu::Status& _s = (s);                               \
    if (PREDICT_FALSE(!_s.ok())) {                              \
      SetupErrorAndRespond(                                     \
          _s, ServerErrorPB::UNKNOWN_ERROR, response, context); \
      return;                                                   \
    }                                                           \
  } while (0)

void RaftConsensus::handleProxyRequest(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response,
    rpc::RpcContext* context) {
  // TODO(mpercy): Remove this config lookup when refactoring DRT to return a
  // RaftPeerPB, which will prevent a validation race.
  RaftConfigPB active_config;
  {
    // Snapshot the active Raft config so we know how to route proxied
    // messages.
    ThreadRestrictions::assertWaitAllowed();
    LockGuard l(lock_);
    RET_RESPOND_ERROR_NOT_OK(CheckRunningUnlocked());
    active_config = cmeta_->ActiveConfig();
  }

  STATS_raft_proxy_num_requests_received.add(1, KUDU_STATS_TAG);

  // Initial implementation:
  //
  // Synchronously:
  // 1. Validate that the request is addressed to the local node via
  // 'proxy_dest_uuid'.
  // 2. Reconstitute each message from the local cache.
  //
  // Asynchronously:
  // 4. Deliver the reconstituted request directly to the remote (async).
  // 5. Proxy the response from the remote back to the caller.

  // Validate the request.
  if (request->proxy_dest_uuid() != peer_uuid()) {
    Status s = Status::InvalidArgument(
        fmt::format(
            "Wrong proxy destination UUID requested. "
            "Local UUID: {}. Requested UUID: {}",
            peer_uuid(),
            request->proxy_dest_uuid()));
    LOG_WITH_PREFIX(WARNING)
        << s.ToString() << ": from " << context->requestorString() << ": "
        << SecureShortDebugString(*request);
    SetupErrorAndRespond(
        s, ServerErrorPB::WRONG_SERVER_UUID, response, context);
    return;
  }
  if (request->dest_uuid() == peer_uuid()) {
    LOG_WITH_PREFIX(WARNING)
        << "dest_uuid and proxy_dest_uuid are the same: "
        << request->proxy_dest_uuid() << ": " << request->ShortDebugString();
    context->respondFailure(
        Status::InvalidArgument("proxy and desination must be different"));
    return;
  }

  if (request->proxy_hops_remaining() < 1) {
    LOG_WITH_PREFIX(WARNING)
        << "Proxy hops remaining exhausted (possible routing loop?) "
        << "in request to peer " << request->proxy_dest_uuid() << ": "
        << request->ShortDebugString();
    STATS_raft_proxy_num_requests_hops_remaining_exhausted.add(
        1, KUDU_STATS_TAG);
    context->respondFailure(
        Status::Incomplete(
            "proxy hops remaining exhausted", "possible routing loop"));
    return;
  }

  // Construct the downstream request; copy the relevant fields from the
  // proxied request.
  ConsensusRequestPB downstream_request;
  auto prevent_ops_deletion = folly::makeGuard([&]() {
    // Prevent double-deletion of these requests.
    downstream_request.mutable_ops()->UnsafeArenaExtractSubrange(
        /*start=*/0,
        /*num=*/downstream_request.ops_size(),
        /*elements=*/nullptr);
  });

  downstream_request.set_dest_uuid(request->dest_uuid());
  downstream_request.set_tablet_id(request->tablet_id());
  downstream_request.set_caller_uuid(request->caller_uuid());
  downstream_request.set_caller_term(request->caller_term());
  // Decrement hops remaining.
  downstream_request.set_proxy_hops_remaining(
      request->proxy_hops_remaining() - 1);

  if (request->has_preceding_id()) {
    *downstream_request.mutable_preceding_id() = request->preceding_id();
  }
  if (request->has_committed_index()) {
    downstream_request.set_committed_index(request->committed_index());
  }
  if (request->has_all_replicated_index()) {
    downstream_request.set_all_replicated_index(
        request->all_replicated_index());
  }
  if (request->has_safe_timestamp()) {
    downstream_request.set_safe_timestamp(request->safe_timestamp());
  }
  if (request->has_last_idx_appended_to_leader()) {
    downstream_request.set_last_idx_appended_to_leader(
        request->last_idx_appended_to_leader());
  }
  if (request->has_region_durable_index()) {
    downstream_request.set_region_durable_index(
        request->region_durable_index());
  }
  if (request->has_raft_rpc_token()) {
    downstream_request.set_raft_rpc_token(request->raft_rpc_token());
  }
  if (request->has_compression_dictionary()) {
    downstream_request.set_compression_dictionary(
        request->compression_dictionary());
  }

  downstream_request.set_proxy_caller_uuid(peer_uuid());

  string next_uuid = request->dest_uuid();
  if (FLAGS_raft_enable_multi_hop_proxy_routing) {
    Status s = routingTableContainer_->nextHop(
        peer_uuid(), request->dest_uuid(), &next_uuid);
    if (PREDICT_FALSE(!s.ok())) {
      STATS_raft_proxy_num_requests_unknown_dest.add(1, KUDU_STATS_TAG);
    }
    RET_RESPOND_ERROR_NOT_OK(s);
  }

  // Find the address of the remote given our local config.
  RaftPeerPB* next_peer_pb;
  Status s = getRaftConfigMember(&active_config, next_uuid, &next_peer_pb);
  if (PREDICT_FALSE(!s.ok())) {
    RET_RESPOND_ERROR_NOT_OK(s.cloneAndPrepend(
        fmt::format(
            "unable to proxy to peer {} because it is not in the active config: {}",
            next_uuid,
            SecureShortDebugString(active_config))));
  }
  if (!next_peer_pb->has_last_known_addr()) {
    s = Status::IllegalState("no known address for peer", next_uuid);
    LOG_WITH_PREFIX(ERROR) << s.ToString();
    RET_RESPOND_ERROR_NOT_OK(s);
  }

  std::optional<ServerErrorPB::Code> proxy_error = {};

  vector<ReplicateRefPtr> messages;
  messages.clear();

  if (request->dest_uuid() != next_uuid) {
    // Multi-hop proxy request.
    downstream_request.set_proxy_dest_uuid(next_uuid);
    // Forward the existing PROXY_OP ops.
    for (int i = 0; i < request->ops_size(); i++) {
      *downstream_request.add_ops() = request->ops(i);
    }
    prevent_ops_deletion
        .dismiss(); // The ops we copy here are not pre-allocated
  } else {
    ReadContext read_context;
    read_context.forPeerUuid = &request->dest_uuid();
    read_context.forPeerHost = &next_peer_pb->last_known_addr().host();
    read_context.forPeerPort = next_peer_pb->last_known_addr().port();

    // When we are proxying, we can skip reporting I/O errors (ie. missing log
    // entries) to avoid remediations from replacing the proxy instance
    // because these instances will eventually catch up. Proxy instances
    // automatically disable proxying when there are I/O errors and eventually
    // resume proxying when they're caught up.
    read_context.reportErrors = FLAGS_report_proxy_errors;

    int64_t first_op_index = -1;
    int64_t max_batch_size =
        FLAGS_consensus_max_batch_size_bytes - request->ByteSizeLong();

    // Reconstitute proxied events from the local cache.
    // If the cache does not have all events, we retry up until the specified
    // retry timeout.
    // TODO(mpercy): Switch this from polling to event-triggered.

    for (int i = 0; i < request->ops_size(); i++) {
      auto& msg = request->ops(i);
      if (PREDICT_FALSE(msg.op_type() != PROXY_OP)) {
        RET_RESPOND_ERROR_NOT_OK(
            Status::InvalidArgument(
                fmt::format(
                    "proxy expected PROXY_OP but received opid {} of type {}",
                    OpIdToString(msg.id()),
                    OperationType_Name(msg.op_type()))));
      }
      if (i == 0) {
        first_op_index = msg.id().index();
      } else {
        // TODO(mpercy): It would be nice not to require consecutive indexes
        // in the batch. We should see if we can support it without a big perf
        // penalty in IOPS.
        if (PREDICT_FALSE(msg.id().index() != first_op_index + i)) {
          RET_RESPOND_ERROR_NOT_OK(
              Status::InvalidArgument(
                  fmt::format(
                      "proxy requires consecutive indexes in batch, but received {} after index {}",
                      OpIdToString(msg.id()),
                      first_op_index + i - 1)));
        }
      }
    }

    // Now we know that all ops we are reconstituting are consecutive.
    //
    // Block until the required op is available in local log. This might
    // timeout based on FLAGS_raft_log_cache_proxy_wait_time_ms in which case
    // we return an error
    OpId preceding_id;
    if (request->ops_size() > 0) {
      queue_->log_cache()->blockingReadOps(
          first_op_index - 1,
          max_batch_size,
          read_context,
          FLAGS_raft_log_cache_proxy_wait_time_ms,
          request->ops_size(),
          &messages,
          &preceding_id);
    }

    if (request->ops_size() > 0 && messages.size() == 0) {
      // We timed out and got nothing from the log cache. Send a heartbeat to
      // the destination to prevent it from starting (pre) election
      raftProxyNumRequestsLogReadTimeout_->increment(); // needed for tests
      STATS_raft_proxy_num_requests_log_read_timeout.add(1, KUDU_STATS_TAG);
      proxy_error = ServerErrorPB::PROXY_MISSING_LOG_ENTRIES;
    }

    // Reconstitute the proxied ops. We silently tolerate proxying a subset of
    // the requested batch.
    for (int i = 0; i < request->ops_size() && i < messages.size(); i++) {
      // Ensure that the OpIds match. We don't expect a mismatch to ever
      // happen, so we log an error locally before reponding to the caller.
      if (!OpIdEquals(request->ops(i).id(), messages[i]->get()->id())) {
        string extra_info;
        if (i > 0) {
          extra_info = fmt::format(
              " (previously received OpId: {})",
              OpIdToString(messages[i - 1]->get()->id()));
        }
        Status status = Status::IllegalState(
            fmt::format(
                "log cache returned non-consecutive OpId index for message {} in request: "
                "requested {}, received {}{}",
                i,
                OpIdToString(request->ops(i).id()),
                OpIdToString(messages[i]->get()->id()),
                extra_info));
        LOG_WITH_PREFIX(ERROR) << status.ToString();
        RET_RESPOND_ERROR_NOT_OK(status);
      }
      downstream_request.mutable_ops()->AddAllocated(messages[i]->get());
    }
  }

  VLOG_WITH_PREFIX(3) << "Downstream proxy request: "
                      << SecureShortDebugString(downstream_request);

  // Asynchronously:
  // Send the request to the remote.

  // TODO(mpercy): Cache this proxy object (although they are lightweight).
  // We can use a PeerProxyPool, like we do when sending from the leader.
  shared_ptr<PeerProxy> next_proxy;
  RET_RESPOND_ERROR_NOT_OK(
      peerProxyFactory_->newProxy(*next_peer_pb, &next_proxy));

  ConsensusResponsePB downstream_response;
  rpc::RpcController controller;
  controller.set_timeout(
      MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));

  // Here, we turn an async API into a blocking one with a CountdownLatch.
  // TODO(mpercy): Use an async approach instead.
  CountDownLatch latch(/*count=*/1);
  rpc::ResponseCallback callback = [&latch] { latch.countDown(); };
  next_proxy->updateAsync(
      &downstream_request, &downstream_response, &controller, callback);
  latch.wait();
  if (PREDICT_FALSE(!controller.status().ok())) {
    RET_RESPOND_ERROR_NOT_OK(controller.status().cloneAndPrepend(
        fmt::format(
            "Error proxying request from {} to {}",
            "local peer " + localPeerPb_.permanent_uuid(),
            SecureShortDebugString(*next_peer_pb))));
  }

  if (proxy_error) {
    SetupErrorAndRespond(
        Status::Incomplete(
            "Unable to proxy request. Degraded request to heartbeat."),
        proxy_error.value(),
        response,
        context);
    return;
  }

  // Proxy the response back to the caller.
  if (downstream_response.has_responder_uuid()) {
    response->set_responder_uuid(downstream_response.responder_uuid());
  }
  if (downstream_response.has_responder_term()) {
    response->set_responder_term(downstream_response.responder_term());
  }
  if (downstream_response.has_status()) {
    *response->mutable_status() = downstream_response.status();
  }
  if (downstream_response.has_error()) {
    *response->mutable_error() = downstream_response.error();
  }

  raftProxyNumRequestsSuccess_->increment(); // needed for tests
  STATS_raft_proxy_num_requests_success.add(1, KUDU_STATS_TAG);
  context->respondSuccess();
}

Status RaftConsensus::setCompressionCodec(const std::string& codec) {
  LockGuard l(lock_);
  return CompressionCodecManager::setCurrentCodec(codec);
}

Status RaftConsensus::setCompressionLevel(int level) {
  LockGuard l(lock_);
  return CompressionCodecManager::setCurrentCompressionLevel(level);
}

Status RaftConsensus::setEnableCompressionOnCacheMiss(bool enable) {
  LockGuard l(lock_);
  return queue_->log_cache()->setEnableCompressionOnCacheMiss(enable);
}

Status RaftConsensus::loadCompressionDict(const std::string& filename) {
  std::string dict_buffer;

  if (filename.empty()) {
    return Status::InvalidArgument("Compression dict filename is empty");
  }

  struct stat st;
  if (stat(filename.c_str(), &st) != 0) {
    return Status::InvalidArgument(
        "Could not find compression dict file's size");
  }

  const off_t file_size = st.st_size;
  const size_t size = static_cast<size_t>(file_size);

  if ((file_size < 0) || (file_size != static_cast<size_t>(size))) {
    return Status::InvalidArgument("Compression dict file is too large");
  }

  dict_buffer.resize(size);

  FILE* const dict_file = fopen(filename.c_str(), "rb");
  if (!dict_file) {
    return Status::InvalidArgument("Could not open compression dict file");
  }

  SCOPE_EXIT {
    fclose(dict_file);
  };

  size_t const read_size = fread(dict_buffer.data(), 1, file_size, dict_file);
  if (read_size != size) {
    return Status::InvalidArgument("Could not read compression dict file");
  }

  LockGuard l(lock_);
  RETURN_NOT_OK(queue_->SetCompressionDictionary(dict_buffer));
  persistentVars_->setCompressionDictionary(dict_buffer);
  RETURN_NOT_OK(persistentVars_->flush());
  return Status::OK();
}

std::string RaftConsensus::getCompressionStats() const {
  LockGuard l(lock_);
  auto codec = CompressionCodecManager::getCurrentCodec();
  return codec ? codec->stats() : "";
}

Status RaftConsensus::setProxyPolicy(const ProxyPolicy& proxy_policy) {
  LockGuard l(lock_);
  proxyPolicy_ = proxy_policy;
  return routingTableContainer_->setProxyPolicy(
      proxyPolicy_, cmeta_->leaderUuid(), cmeta_->ActiveConfig());
}

void RaftConsensus::getProxyPolicy(std::string* proxy_policy) {
  LockGuard l(lock_);

  switch (proxyPolicy_) {
    case ProxyPolicy::DISABLE_PROXY:
      *proxy_policy = "DISABLE_PROXY";
      break;
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      *proxy_policy = "SIMPLE_REGION_ROUTING_POLICY";
      break;
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      *proxy_policy = "DURABLE_ROUTING_POLICY";
      break;
    default:
      *proxy_policy = "UNKNOWN";
      break;
  }
}

void RaftConsensus::setProxyFailureThreshold(
    int32_t proxy_failure_threshold_ms) {
  LockGuard l(lock_);
  queue_->SetProxyFailureThreshold(proxy_failure_threshold_ms);
}

void RaftConsensus::setProxyFailureThresholdLag(
    int64_t proxy_failure_threshold_lag) {
  LockGuard l(lock_);
  queue_->SetProxyFailureThresholdLag(proxy_failure_threshold_lag);
}

void RaftConsensus::clearRemovedPeersList() {
  LockGuard l(lock_);
  cmeta_->ClearRemovedPeersList();
}

void RaftConsensus::deleteFromRemovedPeersList(
    const std::vector<std::string>& peer_uuids) {
  LockGuard l(lock_);
  cmeta_->DeleteFromRemovedPeersList(peer_uuids);
}

std::vector<std::string> RaftConsensus::removedPeersList() {
  LockGuard l(lock_);
  return cmeta_->RemovedPeersList();
}

void RaftConsensus::UpdateLocalPeerUnlocked(RaftConfigPB& active_config) {
  DCHECK(lock_.is_locked());
  RaftPeerPB* new_local_peer_pb;
  Status s =
      getRaftConfigMember(&active_config, peer_uuid(), &new_local_peer_pb);
  if (!s.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING)
        << "Unable to find local peer in active config";
    return;
  }

  localPeerPb_.set_member_type(new_local_peer_pb->member_type());

  if (new_local_peer_pb->has_attrs()) {
    localPeerPb_.mutable_attrs()->CopyFrom(new_local_peer_pb->attrs());
  }
}

////////////////////////////////////////////////////////////////////////
// ConsensusRound
////////////////////////////////////////////////////////////////////////

ConsensusRound::ConsensusRound(
    RaftConsensus* consensus,
    unique_ptr<ReplicateMsg> replicate_msg,
    ConsensusReplicatedCallback replicated_cb)
    : consensus_(consensus),
      replicate_msg_(
          std::make_shared<RefCountedReplicate>(
              std::move(replicate_msg),
              Source::Memory)),
      replicated_cb_(std::move(replicated_cb)),
      bound_term_(-1) {}

ConsensusRound::ConsensusRound(
    RaftConsensus* consensus,
    ReplicateRefPtr replicate_msg)
    : consensus_(consensus),
      replicate_msg_(std::move(replicate_msg)),
      bound_term_(-1) {
  DCHECK(replicate_msg_);
}

void ConsensusRound::NotifyReplicationFinished(const Status& status) {
  if (PREDICT_FALSE(!replicated_cb_)) {
    return;
  }
  replicated_cb_(status);
}

Status ConsensusRound::CheckBoundTerm(int64_t current_term) const {
  if (PREDICT_FALSE(bound_term_ != -1 && bound_term_ != current_term)) {
    return Status::Aborted(
        fmt::format(
            "Transaction submitted in term {} cannot be replicated in term {}",
            bound_term_,
            current_term));
  }
  return Status::OK();
}

void RaftConsensus::SetCheckQuorumFailureCallback(
    CheckQuorumFailureCallback failure_callback) {
  LockGuard l(lock_);
  checkQuorumFailureCallback_ = std::move(failure_callback);
  InitCheckQuorumDetectorUnlocked();
}

void RaftConsensus::SetCheckQuorumFailureIntervalHeartbeats(
    int32_t heartbeats) {
  LockGuard l(lock_);
  checkQuorumIntervalHeartbeats_ = heartbeats;
  InitCheckQuorumDetectorUnlocked();
}

void RaftConsensus::InitCheckQuorumDetectorUnlocked() {
  if (!checkQuorumFailureCallback_) {
    LOG(ERROR) << "No Check Quorum Failure Callback set. Unable to initialize "
               << "CheckQuorum.";
    return;
  }

  CHECK(peerProxyFactory_);
  DCHECK(lock_.is_locked());
  PeriodicTimer::Options opts;
  MonoDelta check_interval = MonoDelta::FromMilliseconds(
      static_cast<int64_t>(
          checkQuorumIntervalHeartbeats_ * FLAGS_raft_heartbeat_interval_ms));
  // Capture a weak_ptr reference into the functor so it can safely handle
  // outliving the consensus instance.
  weak_ptr<RaftConsensus> w = shared_from_this();
  checkQuorumTimer_ = PeriodicTimer::Create(
      peerProxyFactory_->messenger(),
      [w]() {
        if (!FLAGS_check_quorum) {
          return;
        }

        if (auto consensus = w.lock()) {
          // We submit to the pool because we should not be waiting for locks
          // or doing anything expensive in this thread.
          Status status = consensus->raftPoolToken_->SubmitFunc([=]() {
            std::unique_lock<std::mutex> lock(
                consensus->checkQuorumRunning_, std::try_to_lock);
            if (!lock.owns_lock()) {
              return;
            }

            if (!consensus->queue_->CheckQuorum()) {
              if (FLAGS_check_quorum_failure_callback &&
                  consensus->checkQuorumFailureCallback_) {
                // If we're running the failure callback, we want to snooze
                // the next check to avoid piling up further callbacks. Also,
                // if we are repeatedly failing to elect a new leader, we want
                // to snooze to avoid starving other operations that require
                // the election mutex.
                consensus->SnoozeCheckQuorumDetector(
                    MonoDelta::FromMilliseconds(
                        FLAGS_check_quorum_cooldown_ms));
                consensus->checkQuorumFailureCallback_();
              }
            }
          });
          if (!status.ok()) {
            LOG(ERROR) << "Unable to schedule check quorum failure callback: "
                       << status.ToString();
          }
        }
      },
      check_interval,
      opts);
  checkQuorumTimer_->Start(check_interval);
}

void RaftConsensus::SnoozeCheckQuorumDetector(MonoDelta snooze_time) {
  LockGuard l(lock_);
  if (checkQuorumTimer_) {
    checkQuorumTimer_->Snooze(snooze_time);
  }
}

void RaftConsensus::StopCheckQuorumDetectorUnlocked() {
  DCHECK(lock_.is_locked());
  checkQuorumTimer_.reset();
}

int32_t RaftConsensus::GetAvailableCommitPeers() {
  LockGuard l(lock_);
  return queue_->GetAvailableCommitPeers();
}

Status RaftConsensus::GetQuorumHealth(PeerMessageQueue::QuorumHealth* health) {
  LockGuard l(lock_);
  return queue_->GetQuorumHealth(health);
}

void RaftConsensus::SetStateMachineMetrics(
    std::shared_ptr<StateMachineMetricsInterface> s) {
  stateMachineMetrics_ = std::move(s);
}

Status RaftConsensus::getAllStateMachineMetrics(
    PeerMessageQueue::AllStateMachineMetrics* metrics) {
  LockGuard l(lock_);
  return queue_->getAllStateMachineMetrics(metrics);
}

bool RaftConsensus::IsStateMachineHealthyForElection(
    const std::string& candidate_uuid,
    std::optional<int> seconds_behind_master_threshold) {
  LockGuard l(lock_);
  return queue_->IsStateMachineHealthyForElection(
      candidate_uuid, seconds_behind_master_threshold);
}

bool RaftConsensus::isHealthyStateMachineForElectionPresent(
    std::optional<int> seconds_behind_master_threshold) {
  LockGuard l(lock_);
  return queue_->isHealthyStateMachineForElectionPresent(
      seconds_behind_master_threshold);
}

} // namespace kudu::consensus
