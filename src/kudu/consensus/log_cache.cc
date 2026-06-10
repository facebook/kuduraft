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
#include "kudu/util/Stats.h"
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
    const std::shared_ptr<MetricEntity>& metricEntity,
    std::shared_ptr<log::Log> log,
    string localUuid,
    string tabletId)
    : log_(std::move(log)),
      localUuid_(std::move(localUuid)),
      tabletId_(std::move(tabletId)),
      nextIndexCond_(&lock_),
      nextSequentialOpIndex_(0),
      minPinnedOpIndex_(0),
      metrics_(metricEntity),
      enableCompressionOnCacheMiss_(false) {
  // Validate headroom flag
  CHECK_GE(FLAGS_log_cache_eviction_headroom_pct, 0)
      << "log_cache_eviction_headroom_pct must be >= 0";
  CHECK_LT(FLAGS_log_cache_eviction_headroom_pct, 100)
      << "log_cache_eviction_headroom_pct must be < 100";

  const int64_t maxOpsSizeBytes = FLAGS_log_cache_size_limit_mb * 1024L * 1024L;
  const int64_t globalMaxOpsSizeBytes =
      FLAGS_global_log_cache_size_limit_mb * 1024L * 1024L;

  // Set up (or reuse) a tracker with the global limit. It is parented directly
  // to the root tracker so that it's always global.
  parentTracker_ = MemTracker::findOrCreateGlobalTracker(
      globalMaxOpsSizeBytes, kParentMemTrackerId);

  // And create a child tracker with the per-tablet limit.
  tracker_ = MemTracker::createTracker(
      maxOpsSizeBytes,
      fmt::format("{}:{}:{}", kParentMemTrackerId, localUuid_, tabletId_),
      parentTracker_);

  // Put a fake message at index 0, since this simplifies a lot of our
  // code paths elsewhere.
  auto zeroOp = std::make_unique<ReplicateMsg>();
  *zeroOp->mutable_id() = MinimumOpId();
  auto spaceUsed = zeroOp->SpaceUsed();
  auto result = cache_.insert(
      {0,
       {makeScopedRefptrReplicate(std::move(zeroOp), Source::Memory),
        spaceUsed}});
  CHECK(result.second) << "Failed to insert op at index 0";
}

LogCache::~LogCache() {
  tracker_->release(tracker_->consumption());
  cache_.clear();
}

void LogCache::init(const OpId& precedingOp) {
  std::lock_guard<Mutex> l(lock_);
  CHECK_EQ(cache_.size(), 1) << "Cache should have only our special '0' op";
  nextSequentialOpIndex_ = precedingOp.index() + 1;
  minPinnedOpIndex_ = nextSequentialOpIndex_;
}

Status LogCache::setEnableCompressionOnCacheMiss(bool enable) {
  enableCompressionOnCacheMiss_ = enable;
  LOG(INFO) << "Compression on cache miss is set to: " << enable;
  return Status::OK();
}

void LogCache::truncateOpsAfter(int64_t index) {
  {
    std::unique_lock<Mutex> l(lock_);
    truncateOpsAfterUnlocked(index);
  }

  // In the base kuduraft implementation this is a no-op
  // because trimming is handled by resetting the append index.
  // In the MySQL case, we do actual trimming and this is
  // implemented in the binlog wrapper.
  // We don't append in async mode, so we cannot race with
  // AsyncAppendReplicates, where an append has not finished,
  // but the Truncate comes in.
  Status logStatus = log_->truncateOpsAfter(index);

  // We crash the server if Truncate fails, symmetric to
  // what happenes when AsyncAppendReplicates fails.
  CHECK_OK_PREPEND(
      logStatus,
      fmt::format(
          "{}: cannot truncate ops after index {}",
          logStatus.ToString(),
          index));
}

void LogCache::truncateOpsAfterUnlocked(int64_t index) {
  int64_t firstToTruncate = index + 1;
  // If the index is not consecutive then it must be lower than or equal
  // to the last index, i.e. we're overwriting.
  CHECK_LE(firstToTruncate, nextSequentialOpIndex_);

  // Now remove the overwritten operations.
  for (int64_t i = firstToTruncate; i < nextSequentialOpIndex_; ++i) {
    auto it = cache_.find(i);
    if (it != cache_.end()) {
      accountForMessageRemovalUnlocked(it->second);
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

Status LogCache::appendOperations(
    const vector<ReplicateRefPtr>& msgs,
    const StatusCallback& callback) {
  CHECK_GT(msgs.size(), 0);

  // SpaceUsed is relatively expensive, so do calculations outside the lock
  // and cache the result with each message.
  int64_t memRequired = 0;
  vector<CacheEntry> entriesToInsert;
  entriesToInsert.reserve(msgs.size());

  for (const auto& msg : msgs) {
    int64_t localMsgSize = static_cast<int64_t>(msg->get()->SpaceUsedLong());
    CacheEntry e = {msg, localMsgSize, localMsgSize};
    memRequired += e.memUsage;
    entriesToInsert.emplace_back(std::move(e));
  }

  int64_t firstIdxInBatch = msgs.front()->get()->id().index();
  int64_t lastIdxInBatch = msgs.back()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (firstIdxInBatch != nextSequentialOpIndex_) {
    truncateOpsAfterUnlocked(firstIdxInBatch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowedMemory = false;
  if (!tracker_->tryConsume(memRequired)) {
    int spare = tracker_->spareCapacity();
    int needToFree = memRequired - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::toString(memRequired)
        << " to log cache (available=" << HumanReadableNumBytes::toString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    evictSomeUnlocked(minPinnedOpIndex_, calculateBytesToEvict(needToFree));

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->consume(memRequired);

    borrowedMemory = parentTracker_->limitExceeded();
  }

  for (auto& e : entriesToInsert) {
    auto index = e.msg->get()->id().index();
    auto result = cache_.emplace(index, std::move(e));
    CHECK(result.second) << "Failed to emplace op at index " << index;
    nextSequentialOpIndex_ = index + 1;
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();

  metrics_.logCacheSize->incrementBy(memRequired); // needed for tests
  metrics_.logCacheMsgSize->incrementBy(memRequired); // needed for tests
  metrics_.logCacheNumOps->incrementBy(msgs.size()); // needed for tests
  STATS_logCacheSize.addValue(memRequired, KUDU_STATS_TAG);
  STATS_logCacheMsgSize.addValue(memRequired, KUDU_STATS_TAG);
  STATS_logCacheNumOps.addValue(msgs.size(), KUDU_STATS_TAG);
  STATS_logCachePayloadSize.add(memRequired, KUDU_STATS_TAG);
  STATS_logCacheCompressedPayloadSize.add(memRequired, KUDU_STATS_TAG);

  Status logStatus = log_->asyncAppendReplicates(
      msgs,
      Bind(
          &LogCache::logCallback,
          unretained(this),
          lastIdxInBatch,
          borrowedMemory,
          callback));

  if (!logStatus.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(ERROR)
        << "Couldn't append to log: " << logStatus.ToString();
    tracker_->release(memRequired);
    return logStatus;
  }

  // Now signal any threads that might be waiting for Ops to be appended to the
  // log
  nextIndexCond_.broadcast();
  return Status::OK();
}

Status LogCache::appendOperations(
    const vector<ReplicateMsgWrapper>& msgWrappers,
    const StatusCallback& callback) {
  CHECK_GT(msgWrappers.size(), 0);

  // SpaceUsed is relatively expensive, so do calculations outside the lock
  // and cache the result with each message.
  int64_t memRequired = 0;
  int64_t totalMsgSize = 0;
  int64_t compressedSize = 0;
  int64_t uncompressedSize = 0;
  vector<CacheEntry> entriesToInsert;
  entriesToInsert.reserve(msgWrappers.size());

  for (const auto& msgWrapper : msgWrappers) {
    auto msg = msgWrapper.getUncompressedMsg();
    auto compressedMsg = msgWrapper.getCompressedMsg();

    CacheEntry e;
    e.msgSize = approxMsgSize(msg);

    uncompressedSize += e.msgSize;

    // We use the compressed msg if available. The compressed msg might
    // not be avaiblable if compression is disabled or the msg doesn't
    // support compression e.g. non write op
    if (compressedMsg) {
      e.memUsage = approxMsgSize(compressedMsg);
      e.msg = compressedMsg;
    } else {
      e.memUsage = e.msgSize;
      e.msg = msg;
    }

    compressedSize +=
        static_cast<int64_t>(e.msg->get()->write_payload().payload().size());

    // Update the crc32 checksum for the payload
    uint32_t payloadCrc32 = crc::crc32c(
        e.msg->get()->write_payload().payload().c_str(),
        e.msg->get()->write_payload().payload().size());
    e.msg->get()->mutable_write_payload()->set_crc32(payloadCrc32);

    totalMsgSize += e.msgSize;
    memRequired += e.memUsage;
    entriesToInsert.emplace_back(std::move(e));
  }

  int64_t firstIdxInBatch =
      msgWrappers.front().getOrigMsg()->get()->id().index();
  int64_t lastIdxInBatch = msgWrappers.back().getOrigMsg()->get()->id().index();

  std::unique_lock<Mutex> l(lock_);
  // If we're not appending a consecutive op we're likely overwriting and
  // need to replace operations in the cache.
  if (firstIdxInBatch != nextSequentialOpIndex_) {
    truncateOpsAfterUnlocked(firstIdxInBatch - 1);
  }

  // Try to consume the memory. If it can't be consumed, we may need to evict.
  bool borrowedMemory = false;
  if (!tracker_->tryConsume(memRequired)) {
    int spare = tracker_->spareCapacity();
    int needToFree = memRequired - spare;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "Memory limit would be exceeded trying to append "
        << HumanReadableNumBytes::toString(memRequired)
        << " to log cache (available=" << HumanReadableNumBytes::toString(spare)
        << "): attempting to evict some operations...";

    // TODO: we should also try to evict from other tablets - probably better to
    // evict really old ops from another tablet than evict recent ops from this
    // one.
    evictSomeUnlocked(minPinnedOpIndex_, calculateBytesToEvict(needToFree));

    // Force consuming, so that we don't refuse appending data. We might
    // blow past our limit a little bit (as much as the number of tablets times
    // the amount of in-flight data in the log), but until implementing the
    // above TODO, it's difficult to solve this issue.
    tracker_->consume(memRequired);

    borrowedMemory = parentTracker_->limitExceeded();
  }

  for (auto& e : entriesToInsert) {
    auto index = e.msg->get()->id().index();
    auto result = cache_.emplace(index, std::move(e));
    CHECK(result.second) << "Failed to emplace op at index " << index;
    nextSequentialOpIndex_ = index + 1;
  }

  // We drop the lock during the AsyncAppendReplicates call, since it may block
  // if the queue is full, and the queue might not drain if it's trying to call
  // our callback and blocked on this lock.
  l.unlock();

  metrics_.logCacheSize->incrementBy(memRequired); // needed for tests
  metrics_.logCacheMsgSize->incrementBy(totalMsgSize); // needed for tests
  metrics_.logCacheNumOps->incrementBy(msgWrappers.size()); // needed for tests
  STATS_logCacheSize.addValue(memRequired, KUDU_STATS_TAG);
  STATS_logCacheMsgSize.addValue(totalMsgSize, KUDU_STATS_TAG);
  STATS_logCacheNumOps.addValue(msgWrappers.size(), KUDU_STATS_TAG);
  STATS_logCachePayloadSize.add(uncompressedSize, KUDU_STATS_TAG);
  STATS_logCacheCompressedPayloadSize.add(compressedSize, KUDU_STATS_TAG);

  VLOG(2) << "Compressed size: " << compressedSize
          << ", Uncompressed size: " << uncompressedSize
          << ", Total msg size: " << totalMsgSize
          << ", Msg Size: " << memRequired;

  Status logStatus = log_->asyncAppendReplicates(
      msgWrappers,
      Bind(
          &LogCache::logCallback,
          unretained(this),
          lastIdxInBatch,
          borrowedMemory,
          callback));

  if (!logStatus.ok()) {
    LOG_WITH_PREFIX_UNLOCKED(ERROR)
        << "Couldn't append to log: " << logStatus.ToString();
    tracker_->release(memRequired);
    return logStatus;
  }

  // Now signal any threads that might be waiting for Ops to be appended to the
  // log
  nextIndexCond_.broadcast();
  return Status::OK();
}

void LogCache::logCallback(
    int64_t lastIdxInBatch,
    bool borrowedMemory,
    const StatusCallback& userCallback,
    const Status& logStatus) {
  if (logStatus.ok()) {
    std::lock_guard<Mutex> l(lock_);
    if (minPinnedOpIndex_ <= lastIdxInBatch) {
      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Updating pinned index to " << (lastIdxInBatch + 1);
      minPinnedOpIndex_ = lastIdxInBatch + 1;
    }

    // If we went over the global limit in order to log this batch, evict some
    // to get back down under the limit.
    if (borrowedMemory) {
      int64_t spareCapacity = parentTracker_->spareCapacity();
      if (spareCapacity < 0) {
        evictSomeUnlocked(
            minPinnedOpIndex_, calculateBytesToEvict(-spareCapacity));
      }
    }
  }
  userCallback.Run(logStatus);
}

bool LogCache::hasOpBeenWritten(int64_t index) const {
  std::lock_guard<Mutex> l(lock_);
  return index < nextSequentialOpIndex_;
}

Status LogCache::lookupOpId(int64_t opIndex, OpId* opId) const {
  // First check the log cache itself.
  {
    std::lock_guard<Mutex> l(lock_);

    // We sometimes try to look up OpIds that have never been written
    // on the local node. In that case, don't try to read the op from
    // the log reader, since it might actually race against the writing
    // of the op.
    if (opIndex >= nextSequentialOpIndex_) {
      return Status::Incomplete(
          fmt::format(
              "Op with index {} is ahead of the local log "
              "(next sequential op: {})",
              opIndex,
              nextSequentialOpIndex_));
    }
    auto iter = cache_.find(opIndex);
    if (iter != cache_.end()) {
      *opId = iter->second.msg->get()->id();
      return Status::OK();
    }
  }

  // If it misses, read from the log.
  return log_->lookupOpId(opIndex, opId);
}

Status LogCache::blockingReadOps(
    int64_t afterOpIndex,
    int maxSizeBytes,
    const ReadContext& context,
    int64_t maxDurationMs,
    size_t maxOps,
    std::vector<ReplicateRefPtr>* messages,
    OpId* precedingOp) {
  MonoTime deadline =
      MonoTime::Now() + MonoDelta::FromMilliseconds(maxDurationMs);

  {
    std::lock_guard<Mutex> l(lock_);

    while ((afterOpIndex + 1) >= nextSequentialOpIndex_) {
      (void)nextIndexCond_.waitUntil(deadline);

      if (MonoTime::Now() > deadline) {
        break;
      }
    }

    if ((afterOpIndex + 1) >= nextSequentialOpIndex_) {
      // Waited for maxDurationMs, but 'afterOpIndex' is still not available
      // in the local log
      return Status::Incomplete(
          fmt::format(
              "Op with index {} is ahead of the local log "
              "(next sequential op: {})",
              afterOpIndex,
              nextSequentialOpIndex_));
    }
  }

  ReadOpsStatus s = readOps(afterOpIndex, maxSizeBytes, context, messages);
  if (s.status.ok()) {
    *precedingOp = std::move(s.precedingOp);
  }

  while (s.status.ok() && s.stoppedEarly && messages->size() < maxOps &&
         MonoTime::Now() < deadline) {
    if (!messages->empty()) {
      afterOpIndex = messages->back()->get()->id().index();
    }
    s = readOps(afterOpIndex, maxSizeBytes, context, messages);
  }

  return std::move(s.status);
}

LogCache::ReadOpsStatus LogCache::readOps(
    int64_t afterOpIndex,
    int maxSizeBytes,
    const ReadContext& context,
    std::vector<ReplicateRefPtr>* messages,
    uint32_t limit) {
  DCHECK_GE(afterOpIndex, 0);
  // Try to lookup the first OpId in index
  OpId precedingId;
  auto lookUpStatus = lookupOpId(afterOpIndex, &precedingId);
  if (!lookUpStatus.ok()) {
    // If warm storage catch up is not enabled and we don't find it in the log,
    // then we have to return not found error.
    // If warm storage catch up is enabled, we will continue on without setting
    // the precedingId here. We will fill that when we do the warm storage
    // read.
    if (!lookUpStatus.IsNotFound()) {
      return lookUpStatus;
    }

    if (!context.enableWarmStorageReads) {
      if (context.reportErrors) {
        // If it is a NotFound() error, then do a dummy call into
        // ReadReplicatesInRange() to read a single op. This is so that it
        // gets a chance to update the error manager and report the error to
        // upper layer
        vector<ReplicateRefPtr> replicatePtrs;
        log_->readReplicatesInRange(
            afterOpIndex,
            afterOpIndex + 1,
            maxSizeBytes,
            context,
            &replicatePtrs);
      }
      return lookUpStatus;
    }
  }

  std::unique_lock<Mutex> l(lock_);
  int64_t nextIndex = afterOpIndex + 1;
  if (!precedingId.has_index() && context.enableWarmStorageReads) {
    // If warm storage catchup was enabled, we won't have a precedingId yet.
    // In that case, we will read set nextIndex to the  preceding index to
    // retrieve the preceding op id.
    VLOG(1) << "Need to get preceding op id, start_index = " << afterOpIndex;
    nextIndex = afterOpIndex;
  }

  // Return as many operations as we can, up to the limit
  int64_t remainingSpace = maxSizeBytes;
  while (remainingSpace > 0 && nextIndex < nextSequentialOpIndex_ &&
         (messages->size() < limit || limit <= 0)) {
    // If the messages the peer needs haven't been loaded into the queue yet,
    // load them.
    MessageCache::const_iterator iter =
        context.skipLogCache ? cache_.end() : cache_.lower_bound(nextIndex);
    if (iter == cache_.end() || iter->first != nextIndex) {
      int64_t upTo;
      if (iter == cache_.end()) {
        // Read all the way to the current op
        upTo = minPinnedOpIndex_ - 1;
      } else {
        // Read up to the next entry that's in the cache
        upTo = iter->first - 1;
      }

      // If limit is set, then we need to read only up to the limit
      if (limit > 0) {
        upTo = std::min(upTo, afterOpIndex + limit);
      }

      l.unlock();

      vector<ReplicateRefPtr> replicatePtrs;
      auto readStatus = log_->readReplicatesInRange(
          nextIndex, upTo, remainingSpace, context, &replicatePtrs);

      if (readStatus.isUninitialized() && !replicatePtrs.empty()) {
        // When a Warm Storage stream ends, opening a new stream may result in
        // an Uninitialized status because the stream has to initialize. If we
        // discard any transactions returned from previous stream, ingestion can
        // stall: discarding results causes repeated loops without progress, as
        // each new stream may again yield Uninitialized status and unconsumed
        // transactions. To ensure forward progress, always consume available
        // transactions. This why we return OK() here.
        VLOG(1) << "Return OK on Uninitialized status because we've already "
                << "retrieved" << replicatePtrs.size() << " transactions";
        return Status::OK();
      }

      RETURN_NOT_OK_PREPEND(
          readStatus,
          fmt::format("Failed to read ops {}..{}", nextIndex, upTo));

      // Compress messages read from the log if:
      // (1) the feature is enabled through
      // enableCompressionOnCacheMiss_ flag
      // (2) the request is not for a proxy host (the payload is discarded for
      // a proxy request and it is wasteful to compress it here)
      const bool shouldCompress =
          enableCompressionOnCacheMiss_ && !context.routeViaProxy;

      vector<ReplicateMsgWrapper> msgWrappers;
      faststring buffer;

      for (const auto& replicate : replicatePtrs) {
        if (!precedingId.has_index() && context.enableWarmStorageReads) {
          // When precedingId was not previously set, it is because warm
          // storage catchup was enabled and we explicitly set the request
          // to retrieve it. In this case, the first entry will be the preceding
          // op and can be skipped from being added to the results.
          // NOLINTNEXTLINE(facebook-hte-LocalUncheckedArrayBounds)
          precedingId = replicatePtrs.front()->get()->id();
          VLOG(1) << "Setting preceding opid to "
                  << precedingId.ShortDebugString();
          nextIndex++;
          continue;
        }

        ReplicateMsgWrapper msgWrapper(replicate, shouldCompress);
        RETURN_NOT_OK(msgWrapper.init(&buffer));
        msgWrappers.push_back(msgWrapper);
      }

      VLOG_WITH_PREFIX_UNLOCKED(2)
          << "Successfully read " << msgWrappers.size() << " ops "
          << "from disk (" << nextIndex << ".."
          << (nextIndex + msgWrappers.size() - 1) << ")";

      if (!context.routeViaProxy) {
        // Compute crc checksums for the payload that was read from the log
        // Note that this is done _only_ for non-proxy requests because payload
        // is discarded for proxy requests
        for (const auto& msgWrapper : msgWrappers) {
          // We use the compressed msg if available. The compressed msg might
          // not be avaiblable if compression is disabled or the msg doesn't
          // support compression e.g. non write op
          ReplicateMsg* msg = msgWrapper.getCompressedMsg()
              ? msgWrapper.getCompressedMsg()->get()
              : msgWrapper.getUncompressedMsg()->get();
          const std::string& payload = msg->write_payload().payload();
          uint32_t payloadCrc32 = crc::crc32c(payload.c_str(), payload.size());
          msg->mutable_write_payload()->set_crc32(payloadCrc32);
        }
      }

      l.lock();

      for (const auto& msgWrapper : msgWrappers) {
        const auto& msg = msgWrapper.getCompressedMsg()
            ? msgWrapper.getCompressedMsg()
            : msgWrapper.getUncompressedMsg();
        CHECK_EQ(nextIndex, msg->get()->id().index());

        remainingSpace -= approxMsgSize(msg);
        if (remainingSpace <= 0 && !messages->empty()) {
          break;
        }

        messages->push_back(msg);
        nextIndex++;
      }
    } else {
      DCHECK(!context.skipLogCache);
      // Pull contiguous messages from the cache until the size limit is
      // achieved.
      for (; iter != cache_.end(); ++iter) {
        if (limit > 0 && messages->size() >= limit) {
          break;
        }

        const ReplicateRefPtr& msg = iter->second.msg;
        int64_t index = msg->get()->id().index();
        if (index != nextIndex) {
          continue;
        }

        // The full size of the msg is actually returned by SpaceUsedLong() but
        // that's very expensive, the payload size should be very close to the
        // full msg size
        remainingSpace -=
            static_cast<int64_t>(msg->get()->write_payload().payload().size());
        if (remainingSpace < 0 && !messages->empty()) {
          break;
        }

        messages->push_back(msg);
        nextIndex++;
      }
    }
  }
  return {
      Status::OK(),
      std::move(precedingId),
      nextIndex < nextSequentialOpIndex_,
      maxSizeBytes - remainingSpace};
}

Status LogCache::clear() {
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
  evictSomeUnlocked(
      nextSequentialOpIndex_, MathLimits<int64_t>::kMax, /*force =*/true);
  // Placeholder opid 0 will not be evicted from the cache
  return cache_.size() == 1 ? Status::OK()
                            : Status::RuntimeError("Log cache clearing failed");
}

void LogCache::evictThroughOp(int64_t index, bool force) {
  std::lock_guard<Mutex> lock(lock_);

  evictSomeUnlocked(index, MathLimits<int64_t>::kMax, force);
}

int64_t LogCache::calculateBytesToEvict(int64_t bytesNeeded) {
  // If headroom is disabled, just return the bytes needed
  if (FLAGS_log_cache_eviction_headroom_pct <= 0) {
    return bytesNeeded;
  }

  // Calculate extra bytes to free based on headroom percentage
  int64_t limit = tracker_->limit();
  int64_t headroomBytes = limit * FLAGS_log_cache_eviction_headroom_pct / 100;

  // Current spare capacity
  int64_t currentSpare = tracker_->spareCapacity();

  // Evict enough so that spare capacity reaches headroomBytes
  int64_t targetEviction = headroomBytes - currentSpare;

  // Return the maximum of what's needed and what headroom suggests
  return std::max(bytesNeeded, targetEviction);
}

void LogCache::evictSomeUnlocked(
    int64_t stopAfterIndex,
    int64_t bytesToEvict,
    bool force) {
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << "Evicting log cache index <= " << stopAfterIndex << " or "
      << HumanReadableNumBytes::toString(bytesToEvict)
      << ": before state: " << toStringUnlocked();

  int64_t bytesEvicted = 0;
  for (auto iter = cache_.begin(); iter != cache_.end();) {
    const CacheEntry& entry = (*iter).second;
    const ReplicateRefPtr& msg = entry.msg;
    VLOG_WITH_PREFIX_UNLOCKED(2)
        << "considering for eviction: " << msg->get()->id();
    int64_t msgIndex = msg->get()->id().index();
    if (msgIndex == 0) {
      // Always keep our special '0' op.
      ++iter;
      continue;
    }

    if (msgIndex > stopAfterIndex || msgIndex >= minPinnedOpIndex_) {
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
    accountForMessageRemovalUnlocked(entry);
    bytesEvicted += entry.memUsage;
    cache_.erase(iter++);

    if (bytesEvicted >= bytesToEvict) {
      break;
    }
  }
  VLOG_WITH_PREFIX_UNLOCKED(2)
      << "Evicting log cache: after state: " << toStringUnlocked();
}

void LogCache::accountForMessageRemovalUnlocked(
    const LogCache::CacheEntry& entry) {
  tracker_->release(entry.memUsage);
  metrics_.logCacheSize->decrementBy(entry.memUsage); // needed for tests
  metrics_.logCacheMsgSize->decrementBy(entry.msgSize); // needed for tests
  metrics_.logCacheNumOps->decrement(); // needed for tests
}

int64_t LogCache::bytesUsed() const {
  return tracker_->consumption();
}

string LogCache::statsString() const {
  std::lock_guard<Mutex> lock(lock_);
  return statsStringUnlocked();
}

string LogCache::statsStringUnlocked() const {
  return fmt::format(
      "LogCacheStats(num_ops={}, bytes={})",
      metrics_.logCacheNumOps->value(),
      metrics_.logCacheSize->value());
}

std::string LogCache::toString() const {
  std::lock_guard<Mutex> lock(lock_);
  return toStringUnlocked();
}

std::string LogCache::toStringUnlocked() const {
  return fmt::format(
      "Pinned index: {}, {}", minPinnedOpIndex_, statsStringUnlocked());
}

std::string LogCache::logPrefixUnlocked() const {
  return fmt::format("T {} P {}: ", tabletId_, localUuid_);
}

void LogCache::dumpToLog() const {
  vector<string> strings;
  dumpToStrings(&strings);
  for (const string& s : strings) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << s;
  }
}

void LogCache::dumpToStrings(vector<string>* lines) const {
  std::lock_guard<Mutex> lock(lock_);
  int counter = 0;
  lines->push_back(toStringUnlocked());
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

#define INSTANTIATE_METRIC(x) x.instantiate(metricEntity, 0)
LogCache::Metrics::Metrics(const std::shared_ptr<MetricEntity>& metricEntity)
    : logCacheNumOps(INSTANTIATE_METRIC(METRIC_log_cache_num_ops)),
      logCacheSize(INSTANTIATE_METRIC(METRIC_log_cache_size)),
      logCacheMsgSize(INSTANTIATE_METRIC(METRIC_log_cache_msg_size)) {
  logCachePayloadSize =
      metricEntity->findOrCreateCounter(&METRIC_log_cache_payload_size);
  logCacheCompressedPayloadSize = metricEntity->findOrCreateCounter(
      &METRIC_log_cache_compressed_payload_size);
}
#undef INSTANTIATE_METRIC

} // namespace kudu::consensus
