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

#include "kudu/consensus/log_cache.h"

#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/crc.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/mutex.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

DEFINE_int32(
    log_cache_size_limit_mb,
    128,
    "The total per-tablet size of consensus entries which may be kept in memory. "
    "The log cache attempts to keep all entries which have not yet been replicated "
    "to all followers in memory, but if the total size of those entries exceeds "
    "this limit within an individual tablet, the oldest will be evicted.");
TAG_FLAG(log_cache_size_limit_mb, advanced);

DEFINE_int32(
    global_log_cache_size_limit_mb,
    1024,
    "Server-wide version of 'log_cache_size_limit_mb'. The total memory used for "
    "caching log entries across all tablets is kept under this threshold.");
TAG_FLAG(global_log_cache_size_limit_mb, advanced);

DEFINE_uint32(
    ws_max_streams,
    2,
    "Maximum number of concurrent streams to use when reading from warm "
    "storage");

DEFINE_int32(
    log_cache_eviction_headroom_pct,
    0,
    "Percentage of extra cache space to free during eviction. "
    "For example, 10 means evict an extra 10% of the cache limit beyond "
    "what is strictly needed. Must be >= 0 and < 100. "
    "Default 0 disables headroom (evict only what is needed).");
TAG_FLAG(log_cache_eviction_headroom_pct, advanced);

using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::vector;

namespace kudu::consensus {

METRIC_DEFINE_gauge_int64(
    server,
    log_cache_num_ops,
    "Log Cache Operation Count",
    MetricUnit::kOperations,
    "Number of operations in the log cache.");
METRIC_DEFINE_gauge_int64(
    server,
    log_cache_size,
    "Log Cache Memory Usage",
    MetricUnit::kBytes,
    "Amount of memory in use for caching the local log.");
METRIC_DEFINE_gauge_int64(
    server,
    log_cache_msg_size,
    "Log Cache Message Size",
    MetricUnit::kBytes,
    "Size of the incoming uncompressed payload for the "
    "messages in the log_cache");
METRIC_DEFINE_counter(
    server,
    log_cache_compressed_payload_size,
    "Log Cache Compressed Payload Size",
    MetricUnit::kBytes,
    "Size of the compressed msg payload that is sent over the wire");
METRIC_DEFINE_counter(
    server,
    log_cache_payload_size,
    "Log Cache Payload Size",
    MetricUnit::kBytes,
    "Size of the msg payload that is written to the log");

static const char kParentMemTrackerId[] = "log_cache";

LogCache::LogCache(
    const std::shared_ptr<MetricEntity>& metric_entity,
    std::shared_ptr<log::Log> log,
    string local_uuid,
    string tablet_id)
    : log_(std::move(log)),
      localUuid_(std::move(local_uuid)),
      tabletId_(std::move(tablet_id)),
      nextIndexCond_(&lock_),
      nextSequentialOpIndex_(0),
      minPinnedOpIndex_(0),
      metrics_(metric_entity),
      enableCompressionOnCacheMiss_(false) {
  // Validate headroom flag
  CHECK_GE(FLAGS_log_cache_eviction_headroom_pct, 0)
      << "log_cache_eviction_headroom_pct must be >= 0";
  CHECK_LT(FLAGS_log_cache_eviction_headroom_pct, 100)
      << "log_cache_eviction_headroom_pct must be < 100";

  const int64_t max_ops_size_bytes =
      FLAGS_log_cache_size_limit_mb * 1024L * 1024L;
  const int64_t global_max_ops_size_bytes =
      FLAGS_global_log_cache_size_limit_mb * 1024L * 1024L;

  // Set up (or reuse) a tracker with the global limit. It is parented directly
  // to the root tracker so that it's always global.
  parentTracker_ = MemTracker::FindOrCreateGlobalTracker(
      global_max_ops_size_bytes, kParentMemTrackerId);

  // And create a child tracker with the per-tablet limit.
  tracker_ = MemTracker::CreateTracker(
      max_ops_size_bytes,
      fmt::format("{}:{}:{}", kParentMemTrackerId, localUuid_, tabletId_),
      parentTracker_);

  // Put a fake message at index 0, since this simplifies a lot of our
  // code paths elsewhere.
  auto zero_op = new ReplicateMsg();
  *zero_op->mutable_id() = MinimumOpId();
  auto result = cache_.insert(
      {0,
       {makeScopedRefptrReplicate(zero_op, Source::Memory),
        zero_op->SpaceUsed()}});
  CHECK(result.second) << "Failed to insert op at index 0";
}

LogCache::~LogCache() {
  tracker_->Release(tracker_->consumption());
  cache_.clear();
}

void LogCache::Init(const OpId& preceding_op) {
  std::lock_guard<Mutex> l(lock_);
  CHECK_EQ(cache_.size(), 1) << "Cache should have only our special '0' op";
  nextSequentialOpIndex_ = preceding_op.index() + 1;
  minPinnedOpIndex_ = nextSequentialOpIndex_;
}

Status LogCache::EnableCompressionOnCacheMiss(bool enable) {
  enableCompressionOnCacheMiss_ = enable;
  LOG(INFO) << "Compression on cache miss is set to: " << enable;
  return Status::OK();
}

void LogCache::TruncateOpsAfter(int64_t index) {
  {
    std::unique_lock<Mutex> l(lock_);
    TruncateOpsAfterUnlocked(index);
  }

  // In the base kuduraft implementation this is a no-op
  // because trimming is handled by resetting the append index.
  // In the MySQL case, we do actual trimming and this is
  // implemented in the binlog wrapper.
  // We don't append in async mode, so we cannot race with
  // AsyncAppendReplicates, where an append has not finished,
  // but the Truncate comes in.
  Status log_status = log_->truncateOpsAfter(index);

  // We crash the server if Truncate fails, symmetric to
  // what happenes when AsyncAppendReplicates fails.
  CHECK_OK_PREPEND(
      log_status,
      fmt::format(
          "{}: cannot truncate ops after index {}",
          log_status.ToString(),
          index));
}

void LogCache::TruncateOpsAfterUnlocked(int64_t index) {
  int64_t first_to_truncate = index + 1;
  // If the index is not consecutive then it must be lower than or equal
  // to the last index, i.e. we're overwriting.
  CHECK_LE(first_to_truncate, nextSequentialOpIndex_);

  // Now remove the overwritten operations.
  for (int64_t i = first_to_truncate; i < nextSequentialOpIndex_; ++i) {
    auto it = cache_.find(i);
    if (it != cache_.end()) {
      AccountForMessageRemovalUnlocked(it->second);
      cache_.erase(it);
    }
  }
  nextSequentialOpIndex_ = index + 1;
}

namespace {
// Return the payload size as the approximate size of the msg. To get the true
// size we'd have to use msg->SpaceUsedLong() for in-memory size of the msg and
// grpc's WireFormatLite::LengthDelimitedSize() (+ 1 for type tag) for the
// serialized size but both these methods are expensive. So we return the size
// of the payload instead.
int64_t approxMsgSize(const ReplicateRefPtr& msg) {
  return static_cast<int64_t>(msg->get()->write_payload().payload().size());
}
} // anonymous namespace

Status LogCache::AppendOperations(
    const vector<ReplicateRefPtr>& msgs,
    const StatusCallback& callback) {
  CHECK_GT(msgs.size(), 0);

  // SpaceUsed is relatively expensive, so do calculations outside the lock
  // and cache the result with each message.
  int64_t mem_required = 0;
  vector<CacheEntry> entries_to_insert;
  entries_to_insert.reserve(msgs.size());

  for (const auto& msg : msgs) {
    int64_t msg_size = static_cast<int64_t>(msg->get()->SpaceUsedLong());
    CacheEntry e = {msg, msg_size, msg_size};
    mem_required += e.memUsage;
    entries_to_insert.emplace_back(std::move(e));
  }

  int64_t first_idx_in_batch = msgs.front()->get()->id().index();
  int64_t last_idx_in_batch = msgs.back()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (first_idx_in_batch != nextSequentialOpIndex_) {
    TruncateOpsAfterUnlocked(first_idx_in_batch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowed_memory = false;
  if (!tracker_->TryConsume(mem_required)) {
    int spare = tracker_->SpareCapacity();
    int need_to_free = mem_required - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::toString(mem_required)
        << " to log cache (available=" << HumanReadableNumBytes::toString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    EvictSomeUnlocked(minPinnedOpIndex_, CalculateBytesToEvict(need_to_free));

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->Consume(mem_required);

    borrowed_memory = parentTracker_->LimitExceeded();
  }

  for (auto& e : entries_to_insert) {
    auto index = e.msg->get()->id().index();
    auto result = cache_.emplace(index, std::move(e));
    CHECK(result.second) << "Failed to emplace op at index " << index;
    nextSequentialOpIndex_ = index + 1;
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();

  metrics_.log_cache_size->IncrementBy(mem_required);
  metrics_.log_cache_msg_size->IncrementBy(mem_required);
  metrics_.log_cache_num_ops->IncrementBy(msgs.size());
  metrics_.log_cache_payload_size->IncrementBy(mem_required);
  metrics_.log_cache_compressed_payload_size->IncrementBy(mem_required);

  Status log_status = log_->asyncAppendReplicates(
      msgs,
      Bind(
          &LogCache::LogCallback,
          Unretained(this),
          last_idx_in_batch,
          borrowed_memory,
          callback));

  if (!log_status.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(ERROR)
        << "Couldn't append to log: " << log_status.ToString();
    tracker_->Release(mem_required);
    return log_status;
  }

  // Now signal any threads that might be waiting for Ops to be appended to the
  // log
  nextIndexCond_.broadcast();
  return Status::OK();
}

Status LogCache::AppendOperations(
    const vector<ReplicateMsgWrapper>& msg_wrappers,
    const StatusCallback& callback) {
  CHECK_GT(msg_wrappers.size(), 0);

  // SpaceUsed is relatively expensive, so do calculations outside the lock
  // and cache the result with each message.
  int64_t mem_required = 0;
  int64_t total_msg_size = 0;
  int64_t compressed_size = 0;
  int64_t uncompressed_size = 0;
  vector<CacheEntry> entries_to_insert;
  entries_to_insert.reserve(msg_wrappers.size());

  for (const auto& msg_wrapper : msg_wrappers) {
    auto msg = msg_wrapper.GetUncompressedMsg();
    auto compressed_msg = msg_wrapper.GetCompressedMsg();

    CacheEntry e;
    e.msgSize = approxMsgSize(msg);

    uncompressed_size += e.msgSize;

    // We use the compressed msg if available. The compressed msg might
    // not be avaiblable if compression is disabled or the msg doesn't
    // support compression e.g. non write op
    if (compressed_msg) {
      e.memUsage = approxMsgSize(compressed_msg);
      e.msg = compressed_msg;
    } else {
      e.memUsage = e.msgSize;
      e.msg = msg;
    }

    compressed_size +=
        static_cast<int64_t>(e.msg->get()->write_payload().payload().size());

    // Update the crc32 checksum for the payload
    uint32_t payload_crc32 = crc::crc32c(
        e.msg->get()->write_payload().payload().c_str(),
        e.msg->get()->write_payload().payload().size());
    e.msg->get()->mutable_write_payload()->set_crc32(payload_crc32);

    total_msg_size += e.msgSize;
    mem_required += e.memUsage;
    entries_to_insert.emplace_back(std::move(e));
  }

  int64_t first_idx_in_batch =
      msg_wrappers.front().GetOrigMsg()->get()->id().index();
  int64_t last_idx_in_batch =
      msg_wrappers.back().GetOrigMsg()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (first_idx_in_batch != nextSequentialOpIndex_) {
    TruncateOpsAfterUnlocked(first_idx_in_batch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowed_memory = false;
  if (!tracker_->TryConsume(mem_required)) {
    int spare = tracker_->SpareCapacity();
    int need_to_free = mem_required - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::toString(mem_required)
        << " to log cache (available=" << HumanReadableNumBytes::toString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    EvictSomeUnlocked(minPinnedOpIndex_, CalculateBytesToEvict(need_to_free));

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->Consume(mem_required);

    borrowed_memory = parentTracker_->LimitExceeded();
  }

  for (auto& e : entries_to_insert) {
    auto index = e.msg->get()->id().index();
    auto result = cache_.emplace(index, std::move(e));
    CHECK(result.second) << "Failed to emplace op at index " << index;
    nextSequentialOpIndex_ = index + 1;
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();

  metrics_.log_cache_size->IncrementBy(mem_required);
  metrics_.log_cache_msg_size->IncrementBy(total_msg_size);
  metrics_.log_cache_num_ops->IncrementBy(msg_wrappers.size());
  metrics_.log_cache_payload_size->IncrementBy(uncompressed_size);
  metrics_.log_cache_compressed_payload_size->IncrementBy(compressed_size);

  VLOG(2) << "Compressed size: " << compressed_size
          << ", Uncompressed size: " << uncompressed_size
          << ", Total msg size: " << total_msg_size
          << ", Msg Size: " << mem_required;

  Status log_status = log_->asyncAppendReplicates(
      msg_wrappers,
      Bind(
          &LogCache::LogCallback,
          Unretained(this),
          last_idx_in_batch,
          borrowed_memory,
          callback));

  if (!log_status.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(ERROR)
        << "Couldn't append to log: " << log_status.ToString();
    tracker_->Release(mem_required);
    return log_status;
  }

  // Now signal any threads that might be waiting for Ops to be appended to the
  // log
  nextIndexCond_.broadcast();
  return Status::OK();
}

void LogCache::LogCallback(
    int64_t last_idx_in_batch,
    bool borrowed_memory,
    const StatusCallback& user_callback,
    const Status& log_status) {
  if (log_status.ok()) {
    std::lock_guard<Mutex> l(lock_);
    if (minPinnedOpIndex_ <= last_idx_in_batch) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Updating pinned index to " << (last_idx_in_batch + 1);
      minPinnedOpIndex_ = last_idx_in_batch + 1;
    }

    // If we went over the global limit in order to log this batch, evict some
    // to get back down under the limit.
    if (borrowed_memory) {
      int64_t spare_capacity = parentTracker_->SpareCapacity();
      if (spare_capacity < 0) {
        EvictSomeUnlocked(
            minPinnedOpIndex_, CalculateBytesToEvict(-spare_capacity));
      }
    }
  }
  user_callback.Run(log_status);
}

bool LogCache::HasOpBeenWritten(int64_t index) const {
  std::lock_guard<Mutex> l(lock_);
  return index < nextSequentialOpIndex_;
}

Status LogCache::LookupOpId(int64_t op_index, OpId* op_id) const {
  // First check the log cache itself.
  {
    std::lock_guard<Mutex> l(lock_);

    // We sometimes try to look up OpIds that have never been written
    // on the local node. In that case, don't try to read the op from
    // the log reader, since it might actually race against the writing
    // of the op.
    if (op_index >= nextSequentialOpIndex_) {
      return Status::Incomplete(
          fmt::format(
              "Op with index {} is ahead of the local log "
              "(next sequential op: {})",
              op_index,
              nextSequentialOpIndex_));
    }
    auto iter = cache_.find(op_index);
    if (iter != cache_.end()) {
      *op_id = iter->second.msg->get()->id();
      return Status::OK();
    }
  }

  // If it misses, read from the log.
  return log_->lookupOpId(op_index, op_id);
}

Status LogCache::BlockingReadOps(
    int64_t after_op_index,
    int max_size_bytes,
    const ReadContext& context,
    int64_t max_duration_ms,
    size_t max_ops,
    std::vector<ReplicateRefPtr>* messages,
    OpId* preceding_op) {
  MonoTime deadline =
      MonoTime::Now() + MonoDelta::FromMilliseconds(max_duration_ms);

  {
    std::lock_guard<Mutex> l(lock_);

    while ((after_op_index + 1) >= nextSequentialOpIndex_) {
      (void)nextIndexCond_.waitUntil(deadline);

      if (MonoTime::Now() > deadline) {
        break;
      }
    }

    if ((after_op_index + 1) >= nextSequentialOpIndex_) {
      // Waited for max_duration_ms, but 'after_op_index' is still not available
      // in the local log
      return Status::Incomplete(
          fmt::format(
              "Op with index {} is ahead of the local log "
              "(next sequential op: {})",
              after_op_index,
              nextSequentialOpIndex_));
    }
  }

  ReadOpsStatus s = ReadOps(after_op_index, max_size_bytes, context, messages);
  if (s.status.ok()) {
    *preceding_op = std::move(s.precedingOp);
  }

  while (s.status.ok() && s.stoppedEarly && messages->size() < max_ops &&
         MonoTime::Now() < deadline) {
    if (!messages->empty()) {
      after_op_index = messages->back()->get()->id().index();
    }
    s = ReadOps(after_op_index, max_size_bytes, context, messages);
  }

  return std::move(s.status);
}

LogCache::ReadOpsStatus LogCache::ReadOps(
    int64_t after_op_index,
    int max_size_bytes,
    const ReadContext& context,
    std::vector<ReplicateRefPtr>* messages,
    uint32_t limit) {
  DCHECK_GE(after_op_index, 0);
  // Try to lookup the first OpId in index
  OpId preceding_id;
  auto lookUpStatus = LookupOpId(after_op_index, &preceding_id);
  if (!lookUpStatus.ok()) {
    // If warm storage catch up is not enabled and we don't find it in the log,
    // then we have to return not found error.
    // If warm storage catch up is enabled, we will continue on without setting
    // the preceding_id here. We will fill that when we do the warm storage
    // read.
    if (!lookUpStatus.IsNotFound()) {
      return lookUpStatus;
    }

    if (!context.enable_warm_storage_reads) {
      if (context.report_errors) {
        // If it is a NotFound() error, then do a dummy call into
        // ReadReplicatesInRange() to read a single op. This is so that it
        // gets a chance to update the error manager and report the error to
        // upper layer
        vector<ReplicateRefPtr> replicate_ptrs;
        log_->readReplicatesInRange(
            after_op_index,
            after_op_index + 1,
            max_size_bytes,
            context,
            &replicate_ptrs);
      }
      return lookUpStatus;
    }
  }

  std::unique_lock<Mutex> l(lock_);
  int64_t next_index = after_op_index + 1;
  if (!preceding_id.has_index() && context.enable_warm_storage_reads) {
    // If warm storage catchup was enabled, we won't have a preceding_id yet.
    // In that case, we will read set next_index to the  preceding index to
    // retrieve the preceding op id.
    VLOG(1) << "Need to get preceding op id, start_index = " << after_op_index;
    next_index = after_op_index;
  }

  // Return as many operations as we can, up to the limit
  int64_t remaining_space = max_size_bytes;
  while (remaining_space > 0 && next_index < nextSequentialOpIndex_ &&
         (messages->size() < limit || limit <= 0)) {
    // If the messages the peer needs haven't been loaded into the queue yet,
    // load them.
    MessageCache::const_iterator iter =
        context.skip_log_cache ? cache_.end() : cache_.lower_bound(next_index);
    if (iter == cache_.end() || iter->first != next_index) {
      int64_t up_to;
      if (iter == cache_.end()) {
        // Read all the way to the current op
        up_to = minPinnedOpIndex_ - 1;
      } else {
        // Read up to the next entry that's in the cache
        up_to = iter->first - 1;
      }

      // If limit is set, then we need to read only up to the limit
      if (limit > 0) {
        up_to = std::min(up_to, after_op_index + limit);
      }

      l.unlock();

      vector<ReplicateRefPtr> replicate_ptrs;
      auto read_status = log_->readReplicatesInRange(
          next_index, up_to, remaining_space, context, &replicate_ptrs);

      if (read_status.IsUninitialized() && !replicate_ptrs.empty()) {
        // When a Warm Storage stream ends, opening a new stream may result in
        // an Uninitialized status because the stream has to initialize. If we
        // discard any transactions returned from previous stream, ingestion can
        // stall: discarding results causes repeated loops without progress, as
        // each new stream may again yield Uninitialized status and unconsumed
        // transactions. To ensure forward progress, always consume available
        // transactions. This why we return OK() here.
        VLOG(1) << "Return OK on Uninitialized status because we've already "
                << "retrieved" << replicate_ptrs.size() << " transactions";
        return Status::OK();
      }

      RETURN_NOT_OK_PREPEND(
          read_status,
          fmt::format("Failed to read ops {}..{}", next_index, up_to));

      // Compress messages read from the log if:
      // (1) the feature is enabled through
      // enableCompressionOnCacheMiss_ flag
      // (2) the request is not for a proxy host (the payload is discarded for
      // a proxy request and it is wasteful to compress it here)
      const bool should_compress =
          enableCompressionOnCacheMiss_ && !context.route_via_proxy;

      vector<ReplicateMsgWrapper> msg_wrappers;
      faststring buffer;

      for (const auto& replicate : replicate_ptrs) {
        if (!preceding_id.has_index() && context.enable_warm_storage_reads) {
          // When preceding_id was not previously set, it is because warm
          // storage catchup was enabled and we explicitly set the request
          // to retrieve it. In this case, the first entry will be the preceding
          // op and can be skipped from being added to the results.
          // NOLINTNEXTLINE(facebook-hte-LocalUncheckedArrayBounds)
          preceding_id = replicate_ptrs.front()->get()->id();
          VLOG(1) << "Setting preceding opid to "
                  << preceding_id.ShortDebugString();
          next_index++;
          continue;
        }

        ReplicateMsgWrapper msg_wrapper(replicate, should_compress);
        RETURN_NOT_OK(msg_wrapper.Init(&buffer));
        msg_wrappers.push_back(msg_wrapper);
      }

      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Successfully read " << msg_wrappers.size() << " ops "
          << "from disk (" << next_index << ".."
          << (next_index + msg_wrappers.size() - 1) << ")";

      if (!context.route_via_proxy) {
        // Compute crc checksums for the payload that was read from the log
        // Note that this is done _only_ for non-proxy requests because payload
        // is discarded for proxy requests
        for (const auto& msg_wrapper : msg_wrappers) {
          // We use the compressed msg if available. The compressed msg might
          // not be avaiblable if compression is disabled or the msg doesn't
          // support compression e.g. non write op
          ReplicateMsg* msg = msg_wrapper.GetCompressedMsg()
              ? msg_wrapper.GetCompressedMsg()->get()
              : msg_wrapper.GetUncompressedMsg()->get();
          const std::string& payload = msg->write_payload().payload();
          uint32_t payload_crc32 = crc::crc32c(payload.c_str(), payload.size());
          msg->mutable_write_payload()->set_crc32(payload_crc32);
        }
      }

      l.lock();

      for (const auto& msg_wrapper : msg_wrappers) {
        const auto& msg = msg_wrapper.GetCompressedMsg()
            ? msg_wrapper.GetCompressedMsg()
            : msg_wrapper.GetUncompressedMsg();
        CHECK_EQ(next_index, msg->get()->id().index());

        remaining_space -= approxMsgSize(msg);
        if (remaining_space <= 0 && !messages->empty()) {
          break;
        }

        messages->push_back(msg);
        next_index++;
      }
    } else {
      DCHECK(!context.skip_log_cache);
      // Pull contiguous messages from the cache until the size limit is
      // achieved.
      for (; iter != cache_.end(); ++iter) {
        if (limit > 0 && messages->size() >= limit) {
          break;
        }

        const ReplicateRefPtr& msg = iter->second.msg;
        int64_t index = msg->get()->id().index();
        if (index != next_index) {
          continue;
        }

        // The full size of the msg is actually returned by SpaceUsedLong() but
        // that's very expensive, the payload size should be very close to the
        // full msg size
        remaining_space -=
            static_cast<int64_t>(msg->get()->write_payload().payload().size());
        if (remaining_space < 0 && !messages->empty()) {
          break;
        }

        messages->push_back(msg);
        next_index++;
      }
    }
  }
  return {
      Status::OK(),
      std::move(preceding_id),
      next_index < nextSequentialOpIndex_,
      max_size_bytes - remaining_space};
}

Status LogCache::Clear() {
  std::lock_guard<Mutex> lock(lock_);
  // If the next sequential index is not the min pinned index then the cache
  // cannot be cleared. To make sure that they are equal the caller will need to
  // make sure that this method is called when there is no ongoing appends to
  // the log.
  if (nextSequentialOpIndex_ != minPinnedOpIndex_) {
    std::string msg = fmt::format(
        "Log cache cannot be cleared because min "
        "pinned op index {} is not equal to next sequential log index {}",
        minPinnedOpIndex_,
        nextSequentialOpIndex_);
    LOG(ERROR) << msg;
    return Status::RuntimeError(msg);
  }
  EvictSomeUnlocked(
      nextSequentialOpIndex_, MathLimits<int64_t>::kMax, /*force =*/true);
  // Placeholder opid 0 will not be evicted from the cache
  return cache_.size() == 1 ? Status::OK()
                            : Status::RuntimeError("Log cache clearing failed");
}

void LogCache::EvictThroughOp(int64_t index, bool force) {
  std::lock_guard<Mutex> lock(lock_);

  EvictSomeUnlocked(index, MathLimits<int64_t>::kMax, force);
}

int64_t LogCache::CalculateBytesToEvict(int64_t bytes_needed) {
  // If headroom is disabled, just return the bytes needed
  if (FLAGS_log_cache_eviction_headroom_pct <= 0) {
    return bytes_needed;
  }

  // Calculate extra bytes to free based on headroom percentage
  int64_t limit = tracker_->limit();
  int64_t headroom_bytes = limit * FLAGS_log_cache_eviction_headroom_pct / 100;

  // Current spare capacity
  int64_t current_spare = tracker_->SpareCapacity();

  // Evict enough so that spare capacity reaches headroom_bytes
  int64_t target_eviction = headroom_bytes - current_spare;

  // Return the maximum of what's needed and what headroom suggests
  return std::max(bytes_needed, target_eviction);
}

void LogCache::EvictSomeUnlocked(
    int64_t stop_after_index,
    int64_t bytes_to_evict,
    bool force) {
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << "Evicting log cache index <= " << stop_after_index << " or "
      << HumanReadableNumBytes::toString(bytes_to_evict)
      << ": before state: " << ToStringUnlocked();

  int64_t bytes_evicted = 0;
  for (auto iter = cache_.begin(); iter != cache_.end();) {
    const CacheEntry& entry = (*iter).second;
    const ReplicateRefPtr& msg = entry.msg;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "considering for eviction: " << msg->get()->id();
    int64_t msg_index = msg->get()->id().index();
    if (msg_index == 0) {
      // Always keep our special '0' op.
      ++iter;
      continue;
    }

    if (msg_index > stop_after_index || msg_index >= minPinnedOpIndex_) {
      break;
    }

    // If a msg has more than one ref that means it is in flight to some peer.
    // We don't remove it so that memory accounting is accurate. If force is
    // passed then we ignore this.
    if (!force && msg.use_count() > 1) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Evicting cache: cannot remove " << msg->get()->id()
          << " because it is in-use by a peer.";
      ++iter;
      continue;
    }

    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Evicting cache. Removing: " << msg->get()->id();
    AccountForMessageRemovalUnlocked(entry);
    bytes_evicted += entry.memUsage;
    cache_.erase(iter++);

    if (bytes_evicted >= bytes_to_evict) {
      break;
    }
  }
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << "Evicting log cache: after state: " << ToStringUnlocked();
}

void LogCache::AccountForMessageRemovalUnlocked(
    const LogCache::CacheEntry& entry) {
  tracker_->Release(entry.memUsage);
  metrics_.log_cache_size->DecrementBy(entry.memUsage);
  metrics_.log_cache_msg_size->DecrementBy(entry.msgSize);
  metrics_.log_cache_num_ops->Decrement();
}

int64_t LogCache::BytesUsed() const {
  return tracker_->consumption();
}

string LogCache::StatsString() const {
  std::lock_guard<Mutex> lock(lock_);
  return StatsStringUnlocked();
}

string LogCache::StatsStringUnlocked() const {
  return fmt::format(
      "LogCacheStats(num_ops={}, bytes={})",
      metrics_.log_cache_num_ops->value(),
      metrics_.log_cache_size->value());
}

std::string LogCache::ToString() const {
  std::lock_guard<Mutex> lock(lock_);
  return ToStringUnlocked();
}

std::string LogCache::ToStringUnlocked() const {
  return fmt::format(
      "Pinned index: {}, {}", minPinnedOpIndex_, StatsStringUnlocked());
}

std::string LogCache::LogPrefixUnlocked() const {
  return fmt::format("T {} P {}: ", tabletId_, localUuid_);
}

void LogCache::DumpToLog() const {
  vector<string> strings;
  DumpToStrings(&strings);
  for (const string& s : strings) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << s;
  }
}

void LogCache::DumpToStrings(vector<string>* lines) const {
  std::lock_guard<Mutex> lock(lock_);
  int counter = 0;
  lines->push_back(ToStringUnlocked());
  lines->emplace_back("Messages:");
  for (const auto& entry : cache_) {
    const ReplicateMsg* msg = entry.second.msg->get();
    lines->push_back(
        fmt::format(
            "Message[{}] {}.{} : REPLICATE. Type: {}, Size: {}",
            counter++,
            msg->id().term(),
            msg->id().index(),
            OperationType_Name(msg->op_type()),
            msg->ByteSize()));
  }
}

#define INSTANTIATE_METRIC(x) x.Instantiate(metric_entity, 0)
LogCache::Metrics::Metrics(const std::shared_ptr<MetricEntity>& metric_entity)
    : log_cache_num_ops(INSTANTIATE_METRIC(METRIC_log_cache_num_ops)),
      log_cache_size(INSTANTIATE_METRIC(METRIC_log_cache_size)),
      log_cache_msg_size(INSTANTIATE_METRIC(METRIC_log_cache_msg_size)) {
  log_cache_payload_size =
      metric_entity->FindOrCreateCounter(&METRIC_log_cache_payload_size);
  log_cache_compressed_payload_size = metric_entity->FindOrCreateCounter(
      &METRIC_log_cache_compressed_payload_size);
}
#undef INSTANTIATE_METRIC

} // namespace kudu::consensus
