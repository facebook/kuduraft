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
#include <google/protobuf/wire_format_lite.h>
#include <optional>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/compression/compression.pb.h"
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

DEFINE_bool(
    warm_storage_catchup,
    false,
    "Whether to enable warm storage reads when we op id is not found in cache "
    "or disk");

using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::vector;
using strings::Substitute;

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
    const scoped_refptr<MetricEntity>& metric_entity,
    scoped_refptr<log::Log> log,
    string local_uuid,
    string tablet_id)
    : log_(std::move(log)),
      local_uuid_(std::move(local_uuid)),
      tablet_id_(std::move(tablet_id)),
      next_index_cond_(&lock_),
      next_sequential_op_index_(0),
      min_pinned_op_index_(0),
      metrics_(metric_entity),
      enable_compression_on_cache_miss_(false) {
  const int64_t max_ops_size_bytes =
      FLAGS_log_cache_size_limit_mb * 1024L * 1024L;
  const int64_t global_max_ops_size_bytes =
      FLAGS_global_log_cache_size_limit_mb * 1024L * 1024L;

  // Set up (or reuse) a tracker with the global limit. It is parented directly
  // to the root tracker so that it's always global.
  parent_tracker_ = MemTracker::FindOrCreateGlobalTracker(
      global_max_ops_size_bytes, kParentMemTrackerId);

  // And create a child tracker with the per-tablet limit.
  tracker_ = MemTracker::CreateTracker(
      max_ops_size_bytes,
      Substitute("$0:$1:$2", kParentMemTrackerId, local_uuid_, tablet_id_),
      parent_tracker_);

  // Put a fake message at index 0, since this simplifies a lot of our
  // code paths elsewhere.
  auto zero_op = new ReplicateMsg();
  *zero_op->mutable_id() = MinimumOpId();
  InsertOrDie(
      &cache_,
      0,
      {make_scoped_refptr_replicate(zero_op), zero_op->SpaceUsed()});
}

LogCache::~LogCache() {
  tracker_->Release(tracker_->consumption());
  cache_.clear();
}

void LogCache::Init(const OpId& preceding_op) {
  std::lock_guard<Mutex> l(lock_);
  CHECK_EQ(cache_.size(), 1) << "Cache should have only our special '0' op";
  next_sequential_op_index_ = preceding_op.index() + 1;
  min_pinned_op_index_ = next_sequential_op_index_;
}

Status LogCache::EnableCompressionOnCacheMiss(bool enable) {
  enable_compression_on_cache_miss_ = enable;
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
  Status log_status = log_->TruncateOpsAfter(index);

  // We crash the server if Truncate fails, symmetric to
  // what happenes when AsyncAppendReplicates fails.
  CHECK_OK_PREPEND(
      log_status,
      Substitute(
          "$0: cannot truncate ops after index $1",
          log_status.ToString(),
          index));
}

void LogCache::TruncateOpsAfterUnlocked(int64_t index) {
  int64_t first_to_truncate = index + 1;
  // If the index is not consecutive then it must be lower than or equal
  // to the last index, i.e. we're overwriting.
  CHECK_LE(first_to_truncate, next_sequential_op_index_);

  // Now remove the overwritten operations.
  for (int64_t i = first_to_truncate; i < next_sequential_op_index_; ++i) {
    auto it = cache_.find(i);
    if (it != cache_.end()) {
      AccountForMessageRemovalUnlocked(it->second);
      cache_.erase(it);
    }
  }
  next_sequential_op_index_ = index + 1;
}

namespace {
// Return the payload size as the approximate size of the msg. To get the true
// size we'd have to use msg->SpaceUsedLong() for in-memory size of the msg and
// grpc's WireFormatLite::LengthDelimitedSize() (+ 1 for type tag) for the
// serialized size but both these methods are expensive. So we return the size
// of the payload instead.
int64_t ApproxMsgSize(const ReplicateRefPtr& msg) {
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
    mem_required += e.mem_usage;
    entries_to_insert.emplace_back(std::move(e));
  }

  int64_t first_idx_in_batch = msgs.front()->get()->id().index();
  int64_t last_idx_in_batch = msgs.back()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (first_idx_in_batch != next_sequential_op_index_) {
    TruncateOpsAfterUnlocked(first_idx_in_batch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowed_memory = false;
  if (!tracker_->TryConsume(mem_required)) {
    int spare = tracker_->SpareCapacity();
    int need_to_free = mem_required - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::ToString(mem_required)
        << " to log cache (available=" << HumanReadableNumBytes::ToString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    EvictSomeUnlocked(min_pinned_op_index_, need_to_free);

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->Consume(mem_required);

    borrowed_memory = parent_tracker_->LimitExceeded();
  }

  for (auto& e : entries_to_insert) {
    auto index = e.msg->get()->id().index();
    EmplaceOrDie(&cache_, index, std::move(e));
    next_sequential_op_index_ = index + 1;
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

  Status log_status = log_->AsyncAppendReplicates(
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
  next_index_cond_.Broadcast();
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
    e.msg_size = ApproxMsgSize(msg);

    uncompressed_size += e.msg_size;

    // We use the compressed msg if available. The compressed msg might
    // not be avaiblable if compression is disabled or the msg doesn't
    // support compression e.g. non write op
    if (compressed_msg) {
      e.mem_usage = ApproxMsgSize(compressed_msg);
      e.msg = compressed_msg;
    } else {
      e.mem_usage = e.msg_size;
      e.msg = msg;
    }

    compressed_size +=
        static_cast<int64_t>(e.msg->get()->write_payload().payload().size());

    // Update the crc32 checksum for the payload
    uint32_t payload_crc32 = crc::Crc32c(
        e.msg->get()->write_payload().payload().c_str(),
        e.msg->get()->write_payload().payload().size());
    e.msg->get()->mutable_write_payload()->set_crc32(payload_crc32);

    total_msg_size += e.msg_size;
    mem_required += e.mem_usage;
    entries_to_insert.emplace_back(std::move(e));
  }

  int64_t first_idx_in_batch =
      msg_wrappers.front().GetOrigMsg()->get()->id().index();
  int64_t last_idx_in_batch =
      msg_wrappers.back().GetOrigMsg()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (first_idx_in_batch != next_sequential_op_index_) {
    TruncateOpsAfterUnlocked(first_idx_in_batch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowed_memory = false;
  if (!tracker_->TryConsume(mem_required)) {
    int spare = tracker_->SpareCapacity();
    int need_to_free = mem_required - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::ToString(mem_required)
        << " to log cache (available=" << HumanReadableNumBytes::ToString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    EvictSomeUnlocked(min_pinned_op_index_, need_to_free);

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->Consume(mem_required);

    borrowed_memory = parent_tracker_->LimitExceeded();
  }

  for (auto& e : entries_to_insert) {
    auto index = e.msg->get()->id().index();
    EmplaceOrDie(&cache_, index, std::move(e));
    next_sequential_op_index_ = index + 1;
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

  Status log_status = log_->AsyncAppendReplicates(
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
  next_index_cond_.Broadcast();
  return Status::OK();
}

void LogCache::LogCallback(
    int64_t last_idx_in_batch,
    bool borrowed_memory,
    const StatusCallback& user_callback,
    const Status& log_status) {
  if (log_status.ok()) {
    std::lock_guard<Mutex> l(lock_);
    if (min_pinned_op_index_ <= last_idx_in_batch) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Updating pinned index to " << (last_idx_in_batch + 1);
      min_pinned_op_index_ = last_idx_in_batch + 1;
    }

    // If we went over the global limit in order to log this batch, evict some
    // to get back down under the limit.
    if (borrowed_memory) {
      int64_t spare_capacity = parent_tracker_->SpareCapacity();
      if (spare_capacity < 0) {
        EvictSomeUnlocked(min_pinned_op_index_, -spare_capacity);
      }
    }
  }
  user_callback.Run(log_status);
}

bool LogCache::HasOpBeenWritten(int64_t index) const {
  std::lock_guard<Mutex> l(lock_);
  return index < next_sequential_op_index_;
}

Status LogCache::LookupOpId(int64_t op_index, OpId* op_id) const {
  // First check the log cache itself.
  {
    std::lock_guard<Mutex> l(lock_);

    // We sometimes try to look up OpIds that have never been written
    // on the local node. In that case, don't try to read the op from
    // the log reader, since it might actually race against the writing
    // of the op.
    if (op_index >= next_sequential_op_index_) {
      return Status::Incomplete(Substitute(
          "Op with index $0 is ahead of the local log "
          "(next sequential op: $1)",
          op_index,
          next_sequential_op_index_));
    }
    auto iter = cache_.find(op_index);
    if (iter != cache_.end()) {
      *op_id = iter->second.msg->get()->id();
      return Status::OK();
    }
  }

  // If it misses, read from the log.
  return log_->LookupOpId(op_index, op_id);
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

    while ((after_op_index + 1) >= next_sequential_op_index_) {
      (void)next_index_cond_.WaitUntil(deadline);

      if (MonoTime::Now() > deadline) {
        break;
      }
    }

    if ((after_op_index + 1) >= next_sequential_op_index_) {
      // Waited for max_duration_ms, but 'after_op_index' is still not available
      // in the local log
      return Status::Incomplete(Substitute(
          "Op with index $0 is ahead of the local log "
          "(next sequential op: $1)",
          after_op_index,
          next_sequential_op_index_));
    }
  }

  ReadOpsStatus s = ReadOps(after_op_index, max_size_bytes, context, messages);
  if (s.status.ok()) {
    *preceding_op = std::move(s.preceding_op);
  }

  while (s.status.ok() && s.stopped_early && messages->size() < max_ops &&
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
    std::vector<ReplicateRefPtr>* messages) {
  DCHECK_GE(after_op_index, 0);
  bool enabled_warm_storage_catchup = FLAGS_warm_storage_catchup;

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

    if (!enabled_warm_storage_catchup) {
      if (context.report_errors) {
        // If it is a NotFound() error, then do a dummy call into
        // ReadReplicatesInRange() to read a single op. This is so that it
        // gets a chance to update the error manager and report the error to
        // upper layer
        vector<ReplicateRefPtr> replicate_ptrs;
        log_->ReadReplicatesInRange(
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
  if (!preceding_id.has_index() && enabled_warm_storage_catchup) {
    // If warm storae catchup was enabled, it is possible that we won't have a
    // preceding_id yet. In that case, we will read set next_index to the
    // preceding index to retrieve the preceding op id.
    VLOG(1) << "Need to get preceding op id, start_index = " << after_op_index;
    next_index = after_op_index;
  }

  // Return as many operations as we can, up to the limit
  int64_t remaining_space = max_size_bytes;
  while (remaining_space > 0 && next_index < next_sequential_op_index_) {
    // If the messages the peer needs haven't been loaded into the queue yet,
    // load them.
    MessageCache::const_iterator iter = cache_.lower_bound(next_index);
    if (iter == cache_.end() || iter->first != next_index) {
      int64_t up_to;
      if (iter == cache_.end()) {
        // Read all the way to the current op
        up_to = next_sequential_op_index_ - 1;
      } else {
        // Read up to the next entry that's in the cache
        up_to = iter->first - 1;
      }

      l.unlock();

      vector<ReplicateRefPtr> replicate_ptrs;
      RETURN_NOT_OK_PREPEND(
          log_->ReadReplicatesInRange(
              next_index, up_to, remaining_space, context, &replicate_ptrs),
          Substitute("Failed to read ops $0..$1", next_index, up_to));

      // Compress messages read from the log if:
      // (1) the feature is enabled through
      // enable_compression_on_cache_miss_ flag
      // (2) the request is not for a proxy host (the payload is discarded for
      // a proxy request and it is wasteful to compress it here)
      const bool should_compress =
          enable_compression_on_cache_miss_ && !context.route_via_proxy;

      vector<ReplicateMsgWrapper> msg_wrappers;
      faststring buffer;

      for (const auto& replicate : replicate_ptrs) {
        if (!preceding_id.has_index() && enabled_warm_storage_catchup) {
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
          uint32_t payload_crc32 = crc::Crc32c(payload.c_str(), payload.size());
          msg->mutable_write_payload()->set_crc32(payload_crc32);
        }
      }

      l.lock();

      for (const auto& msg_wrapper : msg_wrappers) {
        const auto& msg = msg_wrapper.GetCompressedMsg()
            ? msg_wrapper.GetCompressedMsg()
            : msg_wrapper.GetUncompressedMsg();
        CHECK_EQ(next_index, msg->get()->id().index());

        remaining_space -= ApproxMsgSize(msg);
        if (remaining_space > 0 || messages->empty()) {
          messages->push_back(msg);
          next_index++;
        }
      }
    } else {
      // Pull contiguous messages from the cache until the size limit is
      // achieved.
      for (; iter != cache_.end(); ++iter) {
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
      next_index < next_sequential_op_index_,
      max_size_bytes - remaining_space};
}

Status LogCache::Clear() {
  std::lock_guard<Mutex> lock(lock_);
  // If the next sequential index is not the min pinned index then the cache
  // cannot be cleared. To make sure that they are equal the caller will need to
  // make sure that this method is called when there is no ongoing appends to
  // the log.
  if (next_sequential_op_index_ != min_pinned_op_index_) {
    std::string msg = strings::Substitute(
        "Log cache cannot be cleared because min "
        "pinned op index {} is not equal to next sequential log index {}",
        min_pinned_op_index_,
        next_sequential_op_index_);
    LOG(ERROR) << msg;
    return Status::RuntimeError(msg);
  }
  EvictSomeUnlocked(
      next_sequential_op_index_, MathLimits<int64_t>::kMax, /*force =*/true);
  // Placeholder opid 0 will not be evicted from the cache
  return cache_.size() == 1 ? Status::OK()
                            : Status::RuntimeError("Log cache clearing failed");
}

void LogCache::EvictThroughOp(int64_t index, bool force) {
  std::lock_guard<Mutex> lock(lock_);

  EvictSomeUnlocked(index, MathLimits<int64_t>::kMax, force);
}

void LogCache::EvictSomeUnlocked(
    int64_t stop_after_index,
    int64_t bytes_to_evict,
    bool force) {
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << "Evicting log cache index <= " << stop_after_index << " or "
      << HumanReadableNumBytes::ToString(bytes_to_evict)
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

    if (msg_index > stop_after_index || msg_index >= min_pinned_op_index_) {
      break;
    }

    // If a msg has more than one ref that means it is in flight to some peer.
    // We don't remove it so that memory accounting is accurate. If force is
    // passed then we ignore this.
    if (!force && !msg->HasOneRef()) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Evicting cache: cannot remove " << msg->get()->id()
          << " because it is in-use by a peer.";
      ++iter;
      continue;
    }

    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Evicting cache. Removing: " << msg->get()->id();
    AccountForMessageRemovalUnlocked(entry);
    bytes_evicted += entry.mem_usage;
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
  tracker_->Release(entry.mem_usage);
  metrics_.log_cache_size->DecrementBy(entry.mem_usage);
  metrics_.log_cache_msg_size->DecrementBy(entry.msg_size);
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
  return Substitute(
      "LogCacheStats(num_ops=$0, bytes=$1)",
      metrics_.log_cache_num_ops->value(),
      metrics_.log_cache_size->value());
}

std::string LogCache::ToString() const {
  std::lock_guard<Mutex> lock(lock_);
  return ToStringUnlocked();
}

std::string LogCache::ToStringUnlocked() const {
  return Substitute(
      "Pinned index: $0, $1", min_pinned_op_index_, StatsStringUnlocked());
}

std::string LogCache::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1: ", tablet_id_, local_uuid_);
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
  lines->push_back("Messages:");
  for (const auto& entry : cache_) {
    const ReplicateMsg* msg = entry.second.msg->get();
    lines->push_back(Substitute(
        "Message[$0] $1.$2 : REPLICATE. Type: $3, Size: $4",
        counter++,
        msg->id().term(),
        msg->id().index(),
        OperationType_Name(msg->op_type()),
        msg->ByteSize()));
  }
}

#define INSTANTIATE_METRIC(x) x.Instantiate(metric_entity, 0)
LogCache::Metrics::Metrics(const scoped_refptr<MetricEntity>& metric_entity)
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
