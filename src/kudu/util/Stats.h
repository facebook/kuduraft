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
DECLARE_dynamic_timeseries(raft_log_truncation_counter, 1);
DECLARE_dynamic_timeseries(follower_memory_pressure_rejections, 1);
DECLARE_dynamic_timeseries(raft_proxy_num_requests_received, 1);
DECLARE_dynamic_timeseries(raft_proxy_num_requests_success, 1);
DECLARE_dynamic_timeseries(raft_proxy_num_requests_unknown_dest, 1);
DECLARE_dynamic_timeseries(raft_proxy_num_requests_log_read_timeout, 1);
DECLARE_dynamic_timeseries(raft_proxy_num_requests_hops_remaining_exhausted, 1);
DECLARE_dynamic_timeseries(raft_num_failed_elections, 1);
DECLARE_dynamic_timeseries(raft_num_leader_heartbeat_received, 1);

// --- raft_consensus.cc: gauges ---
DECLARE_dynamic_quantile_stat(raft_term, 1);
DECLARE_dynamic_quantile_stat(failed_elections_since_stable_leader, 1);

// --- consensus_queue.cc: gauges ---
DECLARE_dynamic_quantile_stat(majority_done_ops, 1);
DECLARE_dynamic_quantile_stat(in_progress_ops, 1);
DECLARE_dynamic_quantile_stat(ops_behind_leader, 1);
DECLARE_dynamic_quantile_stat(available_commit_peers, 1);
DECLARE_dynamic_quantile_stat(available_leader_lease_grantors, 1);
DECLARE_dynamic_quantile_stat(available_bounded_dataloss_window_ackers, 1);

// --- consensus_queue.cc: counters ---
DECLARE_dynamic_timeseries(check_quorum_runs, 1);
DECLARE_dynamic_timeseries(check_quorum_failures, 1);
DECLARE_dynamic_timeseries(corruption_cache_drops, 1);
DECLARE_dynamic_timeseries(single_corruption_cache_drops, 1);

// --- log_cache.cc: gauges ---
DECLARE_dynamic_quantile_stat(log_cache_num_ops, 1);
DECLARE_dynamic_quantile_stat(log_cache_size, 1);
DECLARE_dynamic_quantile_stat(log_cache_msg_size, 1);

// --- log_cache.cc: counters ---
DECLARE_dynamic_timeseries(log_cache_compressed_payload_size, 1);
DECLARE_dynamic_timeseries(log_cache_payload_size, 1);

// --- log_metrics.cc: histograms ---
DECLARE_dynamic_quantile_stat(log_sync_latency, 1);
DECLARE_dynamic_quantile_stat(log_append_latency, 1);
DECLARE_dynamic_quantile_stat(log_group_commit_latency, 1);
DECLARE_dynamic_quantile_stat(log_roll_latency, 1);
DECLARE_dynamic_quantile_stat(log_entry_batches_per_group, 1);

// --- log_metrics.cc: counter ---
DECLARE_dynamic_timeseries(log_bytes_logged, 1);

// --- consensus_peers.cc: counter ---
DECLARE_dynamic_timeseries(raft_rpc_token_num_response_mismatches, 1);

// --- log_index.cc: counter ---
DECLARE_dynamic_timeseries(log_index_chunk_mmap_for_read, 1);

// --- cache_metrics.cc: counters ---
DECLARE_dynamic_timeseries(block_cache_inserts, 1);
DECLARE_dynamic_timeseries(block_cache_lookups, 1);
DECLARE_dynamic_timeseries(block_cache_evictions, 1);
DECLARE_dynamic_timeseries(block_cache_misses, 1);
DECLARE_dynamic_timeseries(block_cache_misses_caching, 1);
DECLARE_dynamic_timeseries(block_cache_hits, 1);
DECLARE_dynamic_timeseries(block_cache_hits_caching, 1);

// --- cache_metrics.cc: gauge ---
DECLARE_dynamic_quantile_stat(block_cache_usage, 1);

// --- RaftInterface.cpp: histogram ---
DECLARE_dynamic_quantile_stat(leader_replicate_latency, 1);

// --- thread.cc: gauges (bumped at thread create/destroy sites) ---
DECLARE_dynamic_quantile_stat(threads_started, 1);
DECLARE_dynamic_quantile_stat(threads_running, 1);

// --- Process-level gauges (polled every 60s via FunctionScheduler) ---
// Single sample per window, so timeseries (not quantile stat).
DECLARE_dynamic_timeseries(spinlock_contention_time, 1);
DECLARE_dynamic_timeseries(cpu_utime, 1);
DECLARE_dynamic_timeseries(cpu_stime, 1);
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
