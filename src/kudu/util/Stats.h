// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
//
// fb303 stats for raft/kudu metrics. All new telemetry counters should be
// added here using DECLARE_dynamic_timeseries or DECLARE_dynamic_quantile_stat,
// with DEFINE in Stats.cpp. Do NOT add new METRIC_DEFINE_* (kudu metrics) —
// the kudu MetricRegistry is being phased out in favor of fb303.
//
// Usage:
//   STATS_my_counter.add(value, KUDU_STATS_TAG);          // timeseries
//   STATS_my_gauge.addValue(value, KUDU_STATS_TAG);       // quantile stat
//
// KUDU_STATS_TAG auto-derives the ODS prefix from the calling file's name.

#pragma once

#include <array>
#include <string_view>

#include <fb303/ExportType.h>
#include <fb303/ThreadCachedServiceData.h>
#include <fb303/detail/QuantileStatWrappers.h>

namespace kudu {

// Quantile buckets for raft metrics: P75, P95, P99
static constexpr const std::array<double, 3> kRaftQuantiles{{.75, .95, .99}};

// Helper to derive a stats tag from __FILE_NAME__ by stripping the extension.
// e.g. "raft_consensus.cc" -> "raft_consensus"
constexpr std::string_view statsFileTag(std::string_view filename) {
  auto pos = filename.rfind('.');
  return pos != std::string_view::npos ? filename.substr(0, pos) : filename;
}

// Expands to the filename stem of the *calling* file (not Stats.h), because
// __FILE_NAME__ is resolved at the macro expansion site.
// e.g. in raft_consensus.cc -> "raft_consensus"
#define KUDU_STATS_TAG kudu::statsFileTag(__FILE_NAME__)

// --- Existing ---
DECLARE_dynamic_timeseries(kuduCheckViolations, 1);

// --- raft_consensus.cc: existing timeseries (converted to dynamic) ---
DECLARE_dynamic_timeseries(raftLogTruncationCounter, 1);
DECLARE_dynamic_timeseries(followerMemoryPressureRejections, 1);
DECLARE_dynamic_timeseries(raftProxyNumRequestsReceived, 1);
DECLARE_dynamic_timeseries(raftProxyNumRequestsSuccess, 1);
DECLARE_dynamic_timeseries(raftProxyNumRequestsUnknownDest, 1);
DECLARE_dynamic_timeseries(raftProxyNumRequestsLogReadTimeout, 1);
DECLARE_dynamic_timeseries(raftProxyNumRequestsHopsRemainingExhausted, 1);
DECLARE_dynamic_timeseries(raftNumFailedElections, 1);
DECLARE_dynamic_timeseries(raftNumLeaderHeartbeatReceived, 1);

// --- raft_consensus.cc: gauges ---
DECLARE_dynamic_quantile_stat(raftTerm, 1);
DECLARE_dynamic_quantile_stat(failedElectionsSinceStableLeader, 1);

// --- consensus_queue.cc: gauges ---
DECLARE_dynamic_quantile_stat(majorityDoneOps, 1);
DECLARE_dynamic_quantile_stat(inProgressOps, 1);
DECLARE_dynamic_quantile_stat(opsBehindLeader, 1);
DECLARE_dynamic_quantile_stat(availableCommitPeers, 1);
DECLARE_dynamic_quantile_stat(availableLeaderLeaseGrantors, 1);
DECLARE_dynamic_quantile_stat(availableBoundedDatalossWindowAckers, 1);

// --- consensus_queue.cc: counters ---
DECLARE_dynamic_timeseries(checkQuorumRuns, 1);
DECLARE_dynamic_timeseries(checkQuorumFailures, 1);
DECLARE_dynamic_timeseries(corruptionCacheDrops, 1);
DECLARE_dynamic_timeseries(singleCorruptionCacheDrops, 1);

// --- log_cache.cc: gauges ---
DECLARE_dynamic_quantile_stat(logCacheNumOps, 1);
DECLARE_dynamic_quantile_stat(logCacheSize, 1);
DECLARE_dynamic_quantile_stat(logCacheMsgSize, 1);

// --- log_cache.cc: counters ---
DECLARE_dynamic_timeseries(logCacheCompressedPayloadSize, 1);
DECLARE_dynamic_timeseries(logCachePayloadSize, 1);

// --- log_metrics.cc: histograms ---
DECLARE_dynamic_quantile_stat(logSyncLatency, 1);
DECLARE_dynamic_quantile_stat(logAppendLatency, 1);
DECLARE_dynamic_quantile_stat(logGroupCommitLatency, 1);
DECLARE_dynamic_quantile_stat(logRollLatency, 1);
DECLARE_dynamic_quantile_stat(logEntryBatchesPerGroup, 1);

// --- log_metrics.cc: counter ---
DECLARE_dynamic_timeseries(logBytesLogged, 1);

// --- consensus_peers.cc: counter ---
DECLARE_dynamic_timeseries(raft_rpc_token_num_response_mismatches, 1);

// --- log_index.cc: counter ---
DECLARE_dynamic_timeseries(log_index_chunk_mmap_for_read, 1);

// --- cache_metrics.cc: counters ---
DECLARE_dynamic_timeseries(blockCacheInserts, 1);
DECLARE_dynamic_timeseries(blockCacheLookups, 1);
DECLARE_dynamic_timeseries(blockCacheEvictions, 1);
DECLARE_dynamic_timeseries(blockCacheMisses, 1);
DECLARE_dynamic_timeseries(blockCacheMissesCaching, 1);
DECLARE_dynamic_timeseries(blockCacheHits, 1);
DECLARE_dynamic_timeseries(blockCacheHitsCaching, 1);

// --- cache_metrics.cc: gauge ---
DECLARE_dynamic_quantile_stat(blockCacheUsage, 1);

// --- RaftInterface.cpp: histogram ---
DECLARE_dynamic_quantile_stat(leader_replicate_latency, 1);

// --- thread.cc: gauges (bumped at thread create/destroy sites) ---
DECLARE_dynamic_quantile_stat(threadsStarted, 1);
DECLARE_dynamic_quantile_stat(threadsRunning, 1);

// --- Process-level gauges (polled every 60s via FunctionScheduler) ---
// Single sample per window, so timeseries (not quantile stat).
DECLARE_dynamic_timeseries(spinlockContentionTime, 1);
DECLARE_dynamic_timeseries(cpuUtime, 1);
DECLARE_dynamic_timeseries(cpuStime, 1);
DECLARE_dynamic_timeseries(voluntary_context_switches, 1);
DECLARE_dynamic_timeseries(involuntary_context_switches, 1);

// --- rpc/reactor.cc: histograms ---
DECLARE_dynamic_quantile_stat(reactor_load_percent, 1);
DECLARE_dynamic_quantile_stat(reactor_active_latency_us, 1);

// --- rpc/inbound_call.cc: histogram ---
DECLARE_dynamic_quantile_stat(rpc_incoming_queue_time_us, 1);

// --- rpc/connection.cc: counter ---
DECLARE_dynamic_timeseries(timeout_connection_kill, 1);

} // namespace kudu
