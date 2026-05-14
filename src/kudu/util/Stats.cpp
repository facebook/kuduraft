// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/util/Stats.h"

namespace kudu {

DEFINE_dynamic_timeseries(
    kuduCheckViolations,
    "kudu_check_violations.{}.count",
    facebook::fb303::ExportType::COUNT);

// ---- raft_consensus.cc: existing timeseries (converted to dynamic) ----

DEFINE_dynamic_timeseries(
    raftLogTruncationCounter,
    "{}.raft_log_truncation_counter",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    followerMemoryPressureRejections,
    "{}.follower_memory_pressure_rejections",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftProxyNumRequestsReceived,
    "{}.raft_proxy_num_requests_received",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftProxyNumRequestsSuccess,
    "{}.raft_proxy_num_requests_success",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftProxyNumRequestsUnknownDest,
    "{}.raft_proxy_num_requests_unknown_dest",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftProxyNumRequestsLogReadTimeout,
    "{}.raft_proxy_num_requests_log_read_timeout",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftProxyNumRequestsHopsRemainingExhausted,
    "{}.raft_proxy_num_requests_hops_remaining_exhausted",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftNumFailedElections,
    "{}.raft_num_failed_elections",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    raftNumLeaderHeartbeatReceived,
    "{}.raft_num_leader_heartbeat_received",
    facebook::fb303::ExportType::SUM);

// ---- raft_consensus.cc: gauges ----

DEFINE_dynamic_quantile_stat(
    raftTerm,
    "{}.raft_term",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    failedElectionsSinceStableLeader,
    "{}.failed_elections_since_stable_leader",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- consensus_queue.cc: gauges ----

DEFINE_dynamic_quantile_stat(
    majorityDoneOps,
    "{}.majority_done_ops",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    inProgressOps,
    "{}.in_progress_ops",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    opsBehindLeader,
    "{}.ops_behind_leader",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    availableCommitPeers,
    "{}.available_commit_peers",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    availableLeaderLeaseGrantors,
    "{}.available_leader_lease_grantors",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    availableBoundedDatalossWindowAckers,
    "{}.available_bounded_dataloss_window_ackers",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- consensus_queue.cc: counters ----

DEFINE_dynamic_timeseries(
    checkQuorumRuns,
    "{}.check_quorum_runs",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    checkQuorumFailures,
    "{}.check_quorum_failures",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    corruptionCacheDrops,
    "{}.corruption_cache_drops",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    singleCorruptionCacheDrops,
    "{}.single_corruption_cache_drops",
    facebook::fb303::ExportType::SUM);

// ---- log_cache.cc: gauges ----

DEFINE_dynamic_quantile_stat(
    logCacheNumOps,
    "{}.log_cache_num_ops",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logCacheSize,
    "{}.log_cache_size_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logCacheMsgSize,
    "{}.log_cache_msg_size_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- log_cache.cc: counters ----

DEFINE_dynamic_timeseries(
    logCacheCompressedPayloadSize,
    "{}.log_cache_compressed_payload_size_bytes",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    logCachePayloadSize,
    "{}.log_cache_payload_size_bytes",
    facebook::fb303::ExportType::SUM);

// ---- log_metrics.cc: histograms ----

DEFINE_dynamic_quantile_stat(
    logSyncLatency,
    "{}.log_sync_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logAppendLatency,
    "{}.log_append_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logGroupCommitLatency,
    "{}.log_group_commit_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logRollLatency,
    "{}.log_roll_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    logEntryBatchesPerGroup,
    "{}.log_entry_batches_per_group",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- log_metrics.cc: counter ----

DEFINE_dynamic_timeseries(
    logBytesLogged,
    "{}.log_bytes_logged_bytes",
    facebook::fb303::ExportType::SUM);

// ---- consensus_peers.cc: counter ----

DEFINE_dynamic_timeseries(
    raftRpcTokenNumResponseMismatches,
    "{}.raft_rpc_token_num_response_mismatches",
    facebook::fb303::ExportType::SUM);

// ---- log_index.cc: counter ----

DEFINE_dynamic_timeseries(
    logIndexChunkMmapForRead,
    "{}.log_index_chunk_mmap_for_read",
    facebook::fb303::ExportType::SUM);

// ---- cache_metrics.cc: counters ----

DEFINE_dynamic_timeseries(
    blockCacheInserts,
    "{}.block_cache_inserts",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheLookups,
    "{}.block_cache_lookups",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheEvictions,
    "{}.block_cache_evictions",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheMisses,
    "{}.block_cache_misses",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheMissesCaching,
    "{}.block_cache_misses_caching",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheHits,
    "{}.block_cache_hits",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    blockCacheHitsCaching,
    "{}.block_cache_hits_caching",
    facebook::fb303::ExportType::SUM);

// ---- cache_metrics.cc: gauge ----

DEFINE_dynamic_quantile_stat(
    blockCacheUsage,
    "{}.block_cache_usage_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- RaftInterface.cpp: histogram ----

DEFINE_dynamic_quantile_stat(
    leaderReplicateLatency,
    "{}.leader_replicate_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- Process-level gauges (polled every 60s via FunctionScheduler) ----
// Single sample per window, so timeseries with AVG (not quantile stat).

DEFINE_dynamic_timeseries(
    spinlockContentionTime,
    "{}.spinlock_contention_time_us",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    cpuUtime,
    "{}.cpu_utime_ms",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    cpuStime,
    "{}.cpu_stime_ms",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    voluntaryContextSwitches,
    "{}.voluntary_context_switches",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    involuntaryContextSwitches,
    "{}.involuntary_context_switches",
    facebook::fb303::ExportType::AVG);

// ---- thread.cc: gauges (bumped at thread create/destroy sites) ----

DEFINE_dynamic_quantile_stat(
    threadsStarted,
    "{}.threads_started",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    threadsRunning,
    "{}.threads_running",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/reactor.cc: histograms ----

DEFINE_dynamic_quantile_stat(
    reactorLoadPercent,
    "{}.reactor_load_percent",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    reactorActiveLatencyUs,
    "{}.reactor_active_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/inbound_call.cc: histogram ----

DEFINE_dynamic_quantile_stat(
    rpcIncomingQueueTimeUs,
    "{}.rpc_incoming_queue_time_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/connection.cc: counter ----

DEFINE_dynamic_timeseries(
    timeoutConnectionKill,
    "{}.timeout_connection_kill",
    facebook::fb303::ExportType::SUM);

} // namespace kudu
