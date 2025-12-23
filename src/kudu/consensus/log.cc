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

#include "kudu/consensus/log.h"

#include <cerrno>
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>

#include <boost/range/adaptor/reversed.hpp>
#include <gflags/gflags.h>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_metrics.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/port.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/thread_pool_builder.h"
#include "kudu/util/threadpool.h"

// Log retention configuration.
// -----------------------------
DEFINE_int32(
    log_min_segments_to_retain,
    1,
    "The minimum number of past log segments to keep at all times,"
    " regardless of what is required for durability. "
    "Must be at least 1.");
TAG_FLAG(log_min_segments_to_retain, runtime);
TAG_FLAG(log_min_segments_to_retain, advanced);

DEFINE_int32(
    log_max_segments_to_retain,
    80,
    "The maximum number of past log segments to keep at all times for "
    "the purposes of catching up other peers.");
TAG_FLAG(log_max_segments_to_retain, runtime);
TAG_FLAG(log_max_segments_to_retain, advanced);
TAG_FLAG(log_max_segments_to_retain, experimental);

// Group commit configuration.
// -----------------------------
DEFINE_int32(
    group_commit_queue_size_bytes,
    4 * 1024 * 1024,
    "Maximum size of the group commit queue in bytes");
TAG_FLAG(group_commit_queue_size_bytes, advanced);

DEFINE_int32(
    log_thread_idle_threshold_ms,
    1000,
    "Number of milliseconds after which the log append thread decides that a "
    "log is idle, and considers shutting down. Used by tests.");
TAG_FLAG(log_thread_idle_threshold_ms, experimental);
TAG_FLAG(log_thread_idle_threshold_ms, hidden);

// Compression configuration.
// -----------------------------
DEFINE_string(
    log_compression_codec,
    "LZ4",
    "Codec to use for compressing WAL segments.");
TAG_FLAG(log_compression_codec, experimental);

// Fault/latency injection flags.
// -----------------------------
DEFINE_bool(
    log_inject_latency,
    false,
    "If true, injects artificial latency in log sync operations. "
    "Advanced option. Use at your own risk -- has a negative effect "
    "on performance for obvious reasons!");

DEFINE_bool(
    skip_remove_old_recovery_dir,
    false,
    "Skip removing WAL recovery dir after startup. (useful for debugging)");
TAG_FLAG(skip_remove_old_recovery_dir, hidden);

TAG_FLAG(log_inject_latency, unsafe);
TAG_FLAG(log_inject_latency, runtime);

DEFINE_int32(
    log_inject_latency_ms_mean,
    100,
    "The number of milliseconds of latency to inject, on average. "
    "Only takes effect if --log_inject_latency is true");
TAG_FLAG(log_inject_latency_ms_mean, unsafe);
TAG_FLAG(log_inject_latency_ms_mean, runtime);

DEFINE_int32(
    log_inject_latency_ms_stddev,
    100,
    "The standard deviation of latency to inject in the log. "
    "Only takes effect if --log_inject_latency is true");
TAG_FLAG(log_inject_latency_ms_stddev, unsafe);
TAG_FLAG(log_inject_latency_ms_stddev, runtime);

DEFINE_int32(
    log_inject_thread_lifecycle_latency_ms,
    0,
    "Injection point for random latency during key thread lifecycle transition "
    "points.");
TAG_FLAG(log_inject_thread_lifecycle_latency_ms, unsafe);
TAG_FLAG(log_inject_thread_lifecycle_latency_ms, runtime);

DEFINE_double(
    fault_crash_before_append_commit,
    0.0,
    "Fraction of the time when the server will crash just before appending a "
    "COMMIT message to the log. (For testing only!)");
TAG_FLAG(fault_crash_before_append_commit, unsafe);
TAG_FLAG(fault_crash_before_append_commit, runtime);

DEFINE_double(
    log_inject_io_error_on_append_fraction,
    0.0,
    "Fraction of the time when the log will fail to append and return an IOError. "
    "(For testing only!)");
TAG_FLAG(log_inject_io_error_on_append_fraction, unsafe);
TAG_FLAG(log_inject_io_error_on_append_fraction, runtime);

DEFINE_double(
    log_inject_io_error_on_preallocate_fraction,
    0.0,
    "Fraction of the time when the log will fail to preallocate and return an IOError. "
    "(For testing only!)");
TAG_FLAG(log_inject_io_error_on_preallocate_fraction, unsafe);
TAG_FLAG(log_inject_io_error_on_preallocate_fraction, runtime);

DEFINE_int64(
    fs_wal_dir_reserved_bytes,
    -1,
    "Number of bytes to reserve on the log directory filesystem for "
    "non-Kudu usage. The default, which is represented by -1, is that "
    "1% of the disk space on each disk will be reserved. Any other "
    "value specified represents the number of bytes reserved and must "
    "be greater than or equal to 0. Explicit percentages to reserve "
    "are not currently supported");
DEFINE_validator(fs_wal_dir_reserved_bytes, [](const char* /*n*/, int64_t v) {
  return v >= -1;
});
TAG_FLAG(fs_wal_dir_reserved_bytes, runtime);
TAG_FLAG(fs_wal_dir_reserved_bytes, evolving);

DEFINE_bool(
    raft_derived_log_mode,
    false,
    "When derived log mode is turned on, certain functions"
    " inside kudu raft become invalid");

// Validate that log_min_segments_to_retain >= 1
static bool ValidateLogsToRetain(const char* flagname, int value) {
  if (value >= 1) {
    return true;
  }
  LOG(ERROR) << fmt::format(
      "{} must be at least 1, value {} is invalid", flagname, value);
  return false;
}
static bool dummy = gflags::RegisterFlagValidator(
    &FLAGS_log_min_segments_to_retain,
    &ValidateLogsToRetain);

namespace kudu::log {

using consensus::OpId;
using consensus::ReplicateMsgWrapper;
using consensus::ReplicateRefPtr;
using env_util::OpenFileForRandom;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

// Manages the thread which drains groups of batches from the log's queue and
// appends them to the underlying log instance.
//
// Rather than being a long-running thread, this instead uses a threadpool with
// size 1 to automatically start and stop a thread on demand. When the log
// is idle for some amount of time, no task will be on the thread pool, and thus
// the underlying thread may exit.
//
// The design of submitting tasks to the threadpool is slightly tricky in order
// to achieve group commit and not have to submit one task per appended batch.
// Instead, a generic 'DoWork()' task is used which loops collecting work until
// it finds that it has been idle for a while, at which point the task finishes.
//
// The trick, then, lies in two areas:
//
// 1) after appending a batch, we need to ensure that a task is already running,
//    and if not, start one. This is done in Wake().
//
// 2) when the task finds no more work to do and wants to go idle, it needs to
//    ensure that it doesn't miss a concurrent wake-up. This is done in
//    GoIdle().
//
// See the implementation comments in Wake() and GoIdle() for details.
class Log::AppendThread {
 public:
  explicit AppendThread(Log* log);

  // Initializes the objects and starts the thread pool.
  Status Init();

  // Waits until the last enqueued elements are processed, sets the
  // Appender thread to closing state. If any entries are added to the
  // queue during the process, invoke their callbacks' 'OnFailure()'
  // method.
  void Shutdown();

  // Wake up the appender task, if it is not already running.
  // This should be called after each time that a new entry is
  // appended to the log's queue.
  void Wake();

  bool active() const {
    return base::subtle::NoBarrier_Load(&worker_state_) == WORKER_ACTIVE;
  }

 private:
  // Tries to transition back to WORKER_STOPPED state. If successful, returns
  // true.
  //
  // Otherwise, returns false to indicate that the task should keep running
  // because a new task was enqueued just as we were trying to go idle.
  bool GoIdle();

  string LogPrefix() const;

  Log* const log_;

  // Atomic state machine for whether there is any worker task currently
  // queued or running on append_pool_. See Wake() and GoIdle() for more
  // details.
  enum WorkerState {
    // No worker task is queued or running.
    WORKER_STOPPED,
    // A worker task is queued or running.
    WORKER_ACTIVE
  };
  Atomic32 worker_state_ = WORKER_STOPPED;

  // Pool with a single thread, which handles shutting down the thread
  // when idle.
  std::unique_ptr<ThreadPool> append_pool_;
};

Log::AppendThread::AppendThread(Log* log) : log_(log) {}

Status Log::AppendThread::Init() {
  DCHECK(!append_pool_) << "Already initialized";
  VLOG_WITH_PREFIX(1) << "Starting log append thread";
  RETURN_NOT_OK(ThreadPoolBuilder("wal-append")
                    .set_min_threads(0)
                    // Only need one thread since we'll only schedule one
                    // task at a time.
                    .set_max_threads(1)
                    // No need for keeping idle threads, since the task itself
                    // handles waiting for work while idle.
                    .set_idle_timeout(MonoDelta::FromSeconds(0))
                    .Build(&append_pool_));
  return Status::OK();
}

bool Log::AppendThread::GoIdle() {
  // Inject latency at key points in this function for the purposes of tests.
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);

  // Stopping is a bit tricky. We have to consider the following race:
  //
  // T1                         AppendThread
  // ------------               -------------
  //                            - state is TRIGGERED
  //                            - BlockingDrainTo returns TimedOut()
  // - queue.Put()
  // - Wake() no-op because
  //   it's already triggered

  // So, we first transition back to STOPPED state, and then re-check to see
  // if there has been something enqueued in the meantime.
  auto old_state =
      base::subtle::NoBarrier_AtomicExchange(&worker_state_, WORKER_STOPPED);
  DCHECK_EQ(old_state, WORKER_ACTIVE);
  if (log_->entry_queue()->empty()) {
    // Nothing got enqueued, which means there must not have been any missed
    // wakeup. We are now in WORKER_STOPPED state.
    return true;
  }

  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
  // Someone enqueued something. We don't know whether their wakeup was
  // successful or not, but we can just try to transition back to ACTIVE mode
  // here.
  if (base::subtle::NoBarrier_CompareAndSwap(
          &worker_state_, WORKER_STOPPED, WORKER_ACTIVE) == WORKER_STOPPED) {
    // Their wake-up was lost, but we've now marked ourselves as running.
    MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
    return false;
  }

  // Their wake-up was successful, meaning that there is another task on the
  // queue behind us now, so we can exit this one.
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
  return true;
}

void Log::AppendThread::Shutdown() {
  log_->entry_queue()->Shutdown();
  if (append_pool_) {
    append_pool_->Wait();
    append_pool_->Shutdown();
  }
}

string Log::AppendThread::LogPrefix() const {
  return log_->LogPrefix();
}

// Return true if the append thread is currently active.
bool Log::append_thread_active_for_tests() const {
  return append_thread_->active();
}

const Status Log::kLogShutdownStatus(
    Status::ServiceUnavailable("WAL is shutting down", "", ESHUTDOWN));

const uint64_t Log::kInitialLogSegmentSequenceNumber = 0L;

Status Log::Open(
    const LogOptions& options,
    FsManager* fs_manager,
    const std::string& tablet_id,
    const std::shared_ptr<MetricEntity>& metric_entity,
    std::shared_ptr<Log>* log) {
  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_id);
  RETURN_NOT_OK(
      env_util::CreateDirIfMissing(fs_manager->env(), tablet_wal_path));

  std::shared_ptr<Log> new_log;
  if (options.log_factory) {
    RETURN_NOT_OK(options.log_factory->createLog(
        options,
        fs_manager,
        tablet_wal_path,
        tablet_id,
        metric_entity,
        &new_log));
  } else {
    return Status::NotSupported("No log factory provided");
  }
  RETURN_NOT_OK(new_log->Init());
  log->swap(new_log);
  return Status::OK();
}

Log::Log(
    LogOptions options,
    FsManager* fs_manager,
    string log_path,
    string tablet_id,
    std::shared_ptr<MetricEntity> metric_entity)
    : options_(options),
      fs_manager_(fs_manager),
      log_dir_(std::move(log_path)),
      tablet_id_(std::move(tablet_id)),
      active_segment_sequence_number_(0),
      log_state_(kLogInitialized),
      max_segment_size_(options_.segment_size_mb * 1024 * 1024),
      entry_batch_queue_(FLAGS_group_commit_queue_size_bytes),
      append_thread_(new AppendThread(this)),
      force_sync_all_(options_.force_fsync_all),
      sync_disabled_(false),
      allocation_state_(kAllocationNotStarted),
      codec_(nullptr),
      metric_entity_(std::move(metric_entity)),
      on_disk_size_(0),
      bootstrap_(std::make_shared<consensus::ConsensusBootstrapInfo>()) {
  CHECK_OK(ThreadPoolBuilder("log-alloc")
               .set_max_threads(1)
               .Build(&allocation_pool_));
  if (metric_entity_) {
    metrics_.reset(new LogMetrics(metric_entity_));
  }
}

Status Log::AsyncAppendReplicates(
    const vector<ReplicateMsgWrapper>& wrappers,
    const StatusCallback& callback) {
  vector<ReplicateRefPtr> uncompressed_msgs;
  uncompressed_msgs.reserve(wrappers.size());

  for (const auto& wrapper : wrappers) {
    uncompressed_msgs.push_back(wrapper.GetUncompressedMsg());
  }
  // By default we write uncompressed msgs to disk but a derived class can
  // choose to write compressed msgs instead
  return AsyncAppendReplicates(uncompressed_msgs, callback);
}

Status Log::AsyncAppendCommit(
    unique_ptr<consensus::CommitMsg>,
    const StatusCallback&) {
  // TODO: Rm
  return Status::OK();
}

FsManager* Log::GetFsManager() {
  return fs_manager_;
}

Status Log::ReadReplicatesInRange(
    int64_t starting_at,
    int64_t up_to,
    int64_t max_bytes_to_read,
    const consensus::ReadContext& /* context */,
    std::vector<consensus::ReplicateRefPtr>* replicates) const {
  return reader()->ReadReplicatesInRange(
      starting_at, up_to, max_bytes_to_read, replicates);
}

Status Log::LookupOpId(int64_t op_index, OpId* op_id) const {
  return reader()->LookupOpId(op_index, op_id);
}

bool Log::HasOnDiskData(FsManager* fs_manager, const string& tablet_id) {
  string wal_dir = fs_manager->GetTabletWalDir(tablet_id);
  return fs_manager->env()->FileExists(wal_dir);
}

std::string Log::LogPrefix() const {
  return fmt::format("T {} P {}: ", tablet_id_, fs_manager_->uuid());
}

Log::~Log() {
  // Close() of log is now called from simple_tablet_manager
}

LogEntryBatch::LogEntryBatch(
    LogEntryTypePB type,
    unique_ptr<LogEntryBatchPB> entry_batch_pb,
    size_t count)
    : type_(type),
      entry_batch_pb_(std::move(entry_batch_pb)),
      total_size_bytes_(
          PREDICT_FALSE(
              count == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER)
              ? 0
              : entry_batch_pb_->ByteSize()),
      count_(count) {}

LogEntryBatch::~LogEntryBatch() {
  if (type_ == REPLICATE && entry_batch_pb_) {
    for (LogEntryPB& entry : *entry_batch_pb_->mutable_entry()) {
      // ReplicateMsg elements are owned by and must be freed by the caller
      // (e.g. the LogCache).
      std::ignore = entry.release_replicate();
    }
  }
}

void LogEntryBatch::Serialize() {
  DCHECK_EQ(buffer_.size(), 0);
  // FLUSH_MARKER LogEntries are markers and are not serialized.
  if (PREDICT_FALSE(
          count() == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER)) {
    return;
  }
  buffer_.reserve(total_size_bytes_);
  pb_util::AppendToString(*entry_batch_pb_, &buffer_);
}

} // namespace kudu::log
