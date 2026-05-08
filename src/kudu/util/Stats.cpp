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
    available_leader_lease_grantors,
    "{}.available_leader_lease_grantors",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    available_bounded_dataloss_window_ackers,
    "{}.available_bounded_dataloss_window_ackers",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- consensus_queue.cc: counters ----

DEFINE_dynamic_timeseries(
    check_quorum_runs,
    "{}.check_quorum_runs",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    check_quorum_failures,
    "{}.check_quorum_failures",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    corruption_cache_drops,
    "{}.corruption_cache_drops",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    single_corruption_cache_drops,
    "{}.single_corruption_cache_drops",
    facebook::fb303::ExportType::SUM);

// ---- log_cache.cc: gauges ----

DEFINE_dynamic_quantile_stat(
    log_cache_num_ops,
    "{}.log_cache_num_ops",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_cache_size,
    "{}.log_cache_size_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_cache_msg_size,
    "{}.log_cache_msg_size_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- log_cache.cc: counters ----

DEFINE_dynamic_timeseries(
    log_cache_compressed_payload_size,
    "{}.log_cache_compressed_payload_size_bytes",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    log_cache_payload_size,
    "{}.log_cache_payload_size_bytes",
    facebook::fb303::ExportType::SUM);

// ---- log_metrics.cc: histograms ----

DEFINE_dynamic_quantile_stat(
    log_sync_latency,
    "{}.log_sync_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_append_latency,
    "{}.log_append_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_group_commit_latency,
    "{}.log_group_commit_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_roll_latency,
    "{}.log_roll_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    log_entry_batches_per_group,
    "{}.log_entry_batches_per_group",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- log_metrics.cc: counter ----

DEFINE_dynamic_timeseries(
    log_bytes_logged,
    "{}.log_bytes_logged_bytes",
    facebook::fb303::ExportType::SUM);

// ---- consensus_peers.cc: counter ----

DEFINE_dynamic_timeseries(
    raft_rpc_token_num_response_mismatches,
    "{}.raft_rpc_token_num_response_mismatches",
    facebook::fb303::ExportType::SUM);

// ---- log_index.cc: counter ----

DEFINE_dynamic_timeseries(
    log_index_chunk_mmap_for_read,
    "{}.log_index_chunk_mmap_for_read",
    facebook::fb303::ExportType::SUM);

// ---- cache_metrics.cc: counters ----

DEFINE_dynamic_timeseries(
    block_cache_inserts,
    "{}.block_cache_inserts",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_lookups,
    "{}.block_cache_lookups",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_evictions,
    "{}.block_cache_evictions",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_misses,
    "{}.block_cache_misses",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_misses_caching,
    "{}.block_cache_misses_caching",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_hits,
    "{}.block_cache_hits",
    facebook::fb303::ExportType::SUM);

DEFINE_dynamic_timeseries(
    block_cache_hits_caching,
    "{}.block_cache_hits_caching",
    facebook::fb303::ExportType::SUM);

// ---- cache_metrics.cc: gauge ----

DEFINE_dynamic_quantile_stat(
    block_cache_usage,
    "{}.block_cache_usage_bytes",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- RaftInterface.cpp: histogram ----

DEFINE_dynamic_quantile_stat(
    leader_replicate_latency,
    "{}.leader_replicate_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- Process-level gauges (polled every 60s via FunctionScheduler) ----
// Single sample per window, so timeseries with AVG (not quantile stat).

DEFINE_dynamic_timeseries(
    spinlock_contention_time,
    "{}.spinlock_contention_time_us",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    cpu_utime,
    "{}.cpu_utime_ms",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    cpu_stime,
    "{}.cpu_stime_ms",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    voluntary_context_switches,
    "{}.voluntary_context_switches",
    facebook::fb303::ExportType::AVG);

DEFINE_dynamic_timeseries(
    involuntary_context_switches,
    "{}.involuntary_context_switches",
    facebook::fb303::ExportType::AVG);

// ---- thread.cc: gauges (bumped at thread create/destroy sites) ----

DEFINE_dynamic_quantile_stat(
    threads_started,
    "{}.threads_started",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    threads_running,
    "{}.threads_running",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/reactor.cc: histograms ----

DEFINE_dynamic_quantile_stat(
    reactor_load_percent,
    "{}.reactor_load_percent",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

DEFINE_dynamic_quantile_stat(
    reactor_active_latency_us,
    "{}.reactor_active_latency_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/inbound_call.cc: histogram ----

DEFINE_dynamic_quantile_stat(
    rpc_incoming_queue_time_us,
    "{}.rpc_incoming_queue_time_us",
    facebook::fb303::ExportTypeConsts::kCountAvg,
    kRaftQuantiles,
    facebook::fb303::SlidingWindowPeriodConsts::kOneMin);

// ---- rpc/connection.cc: counter ----

DEFINE_dynamic_timeseries(
    timeout_connection_kill,
    "{}.timeout_connection_kill",
    facebook::fb303::ExportType::SUM);

} // namespace kudu
