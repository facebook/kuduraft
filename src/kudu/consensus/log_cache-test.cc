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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
// #include "kudu/common/schema.h"
// #include "kudu/common/wire_protocol-test-util.h"

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/port.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using std::vector;
using testing::StrictMock;

DECLARE_int32(log_cache_size_limit_mb);
DECLARE_int32(global_log_cache_size_limit_mb);

// METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace consensus {

static const char* kPeerUuid = "leader";
static const char* kTestTablet = "test-tablet";

class LogCacheTest : public KuduTest {
 public:
  LogCacheTest()
      : // schema_(getSimpleTestSchema()),

        metric_entity_(METRIC_ENTITY_server.Instantiate(
            &metric_registry_,
            "LogCacheTest")) {}

  virtual void SetUp() override {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    log_ = std::make_shared<StrictMock<StatefulMockLog>>(
        log::LogOptions(), fs_manager_.get(), "", kTestTablet, nullptr);

    closeAndReopenCache(MinimumOpId());
    clock_ = std::make_shared<clock::HybridClock>();
    ASSERT_OK(clock_->Init());
  }

  void closeAndReopenCache(const OpId& precedingId) {
    cache_.reset(new LogCache(metric_entity_, log_, kPeerUuid, kTestTablet));
    cache_->Init(precedingId);
  }

 protected:
  static void fatalOnError(const Status& s) {
    CHECK_OK(s);
  }

  Status appendReplicateMessagesToCache(
      int64_t first,
      int64_t count,
      size_t payloadSize = 0) {
    for (int64_t curIndex = first; curIndex < first + count; curIndex++) {
      int64_t term = curIndex / 7;
      int64_t index = curIndex;
      vector<ReplicateRefPtr> msgs;
      msgs.push_back(make_scoped_refptr_replicate(
          CreateDummyReplicate(term, index, clock_->Now(), payloadSize)
              .release(),
          Source::Memory));
      RETURN_NOT_OK(cache_->AppendOperations(msgs, Bind(&fatalOnError)));
    }
    return Status::OK();
  }

  // const Schema schema_;

  MetricRegistry metric_registry_;
  std::shared_ptr<MetricEntity> metric_entity_;
  unique_ptr<FsManager> fs_manager_;
  unique_ptr<LogCache> cache_;
  std::shared_ptr<kudu::log::Log> log_;
  std::shared_ptr<clock::Clock> clock_;
};

TEST_F(LogCacheTest, TestAppendAndGetMessages) {
  ASSERT_EQ(0, cache_->metrics_.log_cache_num_ops->value());
  ASSERT_EQ(0, cache_->metrics_.log_cache_size->value());
  ASSERT_OK(appendReplicateMessagesToCache(1, 100));
  ASSERT_EQ(100, cache_->metrics_.log_cache_num_ops->value());
  ASSERT_GE(cache_->metrics_.log_cache_size->value(), 500);

  vector<ReplicateRefPtr> messages;
  OpId preceding;
  auto status = cache_->ReadOps(0, 8 * 1024 * 1024, ReadContext(), &messages);
  ASSERT_OK(status.status;)
  EXPECT_EQ(100, messages.size());
  EXPECT_EQ("0.0", OpIdToString(status.preceding_op));

  // Get starting in the middle of the cache.
  messages.clear();
  status = cache_->ReadOps(70, 8 * 1024 * 1024, ReadContext(), &messages);
  ASSERT_OK(status.status;)
  EXPECT_EQ(30, messages.size());
  EXPECT_EQ("10.70", OpIdToString(status.preceding_op));
  EXPECT_EQ("10.71", OpIdToString(messages[0]->get()->id()));

  // Get at the end of the cache
  messages.clear();
  status = cache_->ReadOps(100, 8 * 1024 * 1024, ReadContext(), &messages);
  ASSERT_OK(status.status;)
  EXPECT_EQ(0, messages.size());
  EXPECT_EQ("14.100", OpIdToString(status.preceding_op));

  // Evict some and verify that the eviction took effect.
  cache_->EvictThroughOp(50);
  ASSERT_EQ(50, cache_->metrics_.log_cache_num_ops->value());

  // Can still read data that was evicted, since it got written through.
  messages.clear();
  status = cache_->ReadOps(20, 8 * 1024 * 1024, ReadContext(), &messages);
  ASSERT_OK(status.status;)
  EXPECT_EQ(80, messages.size());
  EXPECT_EQ("2.20", OpIdToString(status.preceding_op));
  EXPECT_EQ("3.21", OpIdToString(messages[0]->get()->id()));
}

// Ensure that the cache always yields at least one message,
// even if that message is larger than the batch size. This ensures
// that we don't get "stuck" in the case that a large message enters
// the cache.
//
// FIXME(mpercy): LogCache uses an optimization to avoid calling into protobuf
// ByteSizeLong() by calling msg->get()->write_payload().payload().size()
// however that assumes a particular PB type. So we can either fix this test to
// use the "real" PB type instead of a NoopRequest with a "payload for tests"
// field, or we can revert the optimization and just use the payload size
// provided by the PB API which may be slower due to use of reflection.
TEST_F(LogCacheTest, DISABLED_TestAlwaysYieldsAtLeastOneMessage) {
  // generate a 2MB dummy payload
  const int kPayloadSize = 2 * 1024 * 1024;
  const int kNumMessages = 4;

  // Append several large ops to the cache
  ASSERT_OK(appendReplicateMessagesToCache(1, kNumMessages, kPayloadSize));

  // We should get one of them, even though we only ask for 100 bytes
  vector<ReplicateRefPtr> messages;
  OpId preceding;
  auto status = cache_->ReadOps(0, 100, ReadContext(), &messages);
  ASSERT_OK(status.status);
  EXPECT_EQ(1, messages.size());

  // Should yield one op also in the 'cache miss' case.
  messages.clear();
  cache_->EvictThroughOp(50);
  status = cache_->ReadOps(0, 100, ReadContext(), &messages);
  ASSERT_OK(status.status);
  EXPECT_EQ(1, messages.size());
}

// Tests that the cache returns Status::NotFound() if queried for messages after
// an index that is higher than it's latest, returns an empty set of messages
// when queried for the the last index and returns all messages when queried for
// MinimumOpId().
TEST_F(LogCacheTest, TestCacheEdgeCases) {
  // Append 1 message to the cache
  ASSERT_OK(appendReplicateMessagesToCache(1, 1));

  std::vector<ReplicateRefPtr> messages;

  // Test when the searched index is MinimumOpId().index().
  auto status = cache_->ReadOps(0, 100, ReadContext(), &messages);
  ASSERT_OK(status.status);
  ASSERT_EQ(1, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 0), status.preceding_op);

  messages.clear();

  // Test when 'after_op_index' is the last index in the cache.
  status = cache_->ReadOps(1, 100, ReadContext(), &messages);
  ASSERT_OK(status.status);
  ASSERT_EQ(0, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 1), status.preceding_op);

  messages.clear();

  // Now test the case when 'after_op_index' is after the last index
  // in the cache.
  status = cache_->ReadOps(2, 100, ReadContext(), &messages);
  auto s = status.status;
  ASSERT_TRUE(s.IsIncomplete()) << "unexpected status: " << s.ToString();
  ASSERT_EQ(0, messages.size());
  ASSERT_FALSE(status.preceding_op.IsInitialized());

  messages.clear();

  // Evict entries from the cache, and ensure that we can still read
  // entries at the beginning of the log.
  cache_->EvictThroughOp(50);
  status = cache_->ReadOps(0, 100, ReadContext(), &messages);
  ASSERT_OK(status.status);
  ASSERT_EQ(1, messages.size());
  ASSERT_OPID_EQ(MakeOpId(0, 0), status.preceding_op);
}

TEST_F(LogCacheTest, TestMemoryLimit) {
  FLAGS_log_cache_size_limit_mb = 1;
  closeAndReopenCache(MinimumOpId());

  const int kPayloadSize = 400 * 1024;
  // Limit should not be violated.
  ASSERT_OK(appendReplicateMessagesToCache(1, 1, kPayloadSize));
  ASSERT_EQ(1, cache_->num_cached_ops());

  // Verify the size is right. It's not exactly kPayloadSize because of
  // in-memory overhead, etc.
  int sizeWithOneMsg = cache_->BytesUsed();
  ASSERT_GT(sizeWithOneMsg, 300 * 1024);
  ASSERT_LT(sizeWithOneMsg, 500 * 1024);

  // Add another operation which fits under the 1MB limit.
  ASSERT_OK(appendReplicateMessagesToCache(2, 1, kPayloadSize));
  ASSERT_EQ(2, cache_->num_cached_ops());

  int sizeWithTwoMsgs = cache_->BytesUsed();
  ASSERT_GT(sizeWithTwoMsgs, 2 * 300 * 1024);
  ASSERT_LT(sizeWithTwoMsgs, 2 * 500 * 1024);

  // Append a third operation, which will push the cache size above the 1MB
  // limit and cause eviction of the first operation.
  LOG(INFO) << "appending op 3";
  // Verify that we have trimmed by appending a message that would
  // otherwise be rejected, since the cache max size limit is 2MB.
  ASSERT_OK(appendReplicateMessagesToCache(3, 1, kPayloadSize));
  ASSERT_EQ(2, cache_->num_cached_ops());
  ASSERT_EQ(sizeWithTwoMsgs, cache_->BytesUsed());

  // Test explicitly evicting one of the ops.
  cache_->EvictThroughOp(2);
  ASSERT_EQ(1, cache_->num_cached_ops());
  ASSERT_EQ(sizeWithOneMsg, cache_->BytesUsed());

  // Explicitly evict the last op.
  cache_->EvictThroughOp(3);
  ASSERT_EQ(0, cache_->num_cached_ops());
  ASSERT_EQ(cache_->BytesUsed(), 0);
}

TEST_F(LogCacheTest, TestGlobalMemoryLimit) {
  // Need to force the global cache memtracker to be destroyed before calling
  // closeAndReopenCache(), otherwise it'll just be reused instead of recreated
  // with a new limit.
  cache_.reset();

  FLAGS_global_log_cache_size_limit_mb = 4;
  closeAndReopenCache(MinimumOpId());

  // Exceed the global hard limit.
  ScopedTrackedConsumption consumption(
      cache_->parent_tracker_, 3 * 1024 * 1024);

  const int kPayloadSize = 768 * 1024;

  // Should succeed, but only end up caching one of the two ops because of the
  // global limit.
  ASSERT_OK(appendReplicateMessagesToCache(1, 2, kPayloadSize));

  ASSERT_EQ(1, cache_->num_cached_ops());
  ASSERT_LE(cache_->BytesUsed(), 1024 * 1024);
}

// Test that the log cache properly replaces messages when an index
// is reused. This is a regression test for a bug where the memtracker's
// consumption wasn't properly managed when messages were replaced.
TEST_F(LogCacheTest, TestReplaceMessages) {
  const int kPayloadSize = 128 * 1024;
  shared_ptr<MemTracker> tracker = cache_->tracker_;
  ASSERT_EQ(0, tracker->consumption());

  ASSERT_OK(appendReplicateMessagesToCache(1, 1, kPayloadSize));
  int sizeWithOneMsg = tracker->consumption();

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(appendReplicateMessagesToCache(1, 1, kPayloadSize));
  }

  EXPECT_EQ(sizeWithOneMsg, tracker->consumption());
  EXPECT_EQ(
      fmt::format(
          "Pinned index: 2, LogCacheStats(num_ops=1, bytes={})",
          sizeWithOneMsg),
      cache_->ToString());
}

// Test that the cache truncates any future messages when either explicitly
// truncated or replacing any earlier message.
TEST_F(LogCacheTest, TestTruncation) {
  enum { TRUNCATE_BY_APPEND, TRUNCATE_EXPLICITLY };

  // Append 1 through 3.
  appendReplicateMessagesToCache(1, 3, 100);

  for (auto mode : {TRUNCATE_BY_APPEND, TRUNCATE_EXPLICITLY}) {
    SCOPED_TRACE(mode == TRUNCATE_BY_APPEND ? "by append" : "explicitly");
    // Append messages 4 through 10.
    appendReplicateMessagesToCache(4, 7, 100);
    ASSERT_EQ(10, cache_->metrics_.log_cache_num_ops->value());

    switch (mode) {
      case TRUNCATE_BY_APPEND:
        appendReplicateMessagesToCache(3, 1, 100);
        break;
      case TRUNCATE_EXPLICITLY:
        cache_->TruncateOpsAfter(3);
        break;
    }

    ASSERT_EQ(3, cache_->metrics_.log_cache_num_ops->value());

    // Op 3 should still be in the cache.
    OpId op;
    ASSERT_OK(cache_->LookupOpId(3, &op));
    ASSERT_TRUE(cache_->HasOpBeenWritten(3));

    // Op 4 should have been removed.
    Status s = cache_->LookupOpId(4, &op);
    ASSERT_TRUE(s.IsIncomplete())
        << "should be truncated, but got: " << s.ToString();
    ASSERT_FALSE(cache_->HasOpBeenWritten(4));
  }
}

TEST_F(LogCacheTest, TestMTReadAndWrite) {
  atomic<bool> stop{false};
  vector<thread> threads;
  SCOPE_EXIT {
    stop = true;
    for (auto& t : threads) {
      t.join();
    }
  };

  // Add a writer thread.
  threads.emplace_back([&] {
    const int kBatch = 10;
    int64_t index = 1;
    while (!stop) {
      CHECK_OK(appendReplicateMessagesToCache(index, kBatch));
      index += kBatch;
    }
  });
  // Add a reader thread.
  threads.emplace_back([&] {
    int64_t index = 0;
    while (!stop) {
      vector<ReplicateRefPtr> messages;
      OpId preceding;
      auto status =
          cache_->ReadOps(index, 1024 * 1024, ReadContext(), &messages);

      CHECK_OK(status.status);
      index += messages.size();
    }
  });

  SleepFor(MonoDelta::FromSeconds(AllowSlowTests() ? 10 : 2));
}

TEST_F(LogCacheTest, TestReadOpsWithLimit) {
  ASSERT_EQ(0, cache_->metrics_.log_cache_num_ops->value());
  ASSERT_EQ(0, cache_->metrics_.log_cache_size->value());
  ASSERT_OK(appendReplicateMessagesToCache(1, 100));
  ASSERT_EQ(100, cache_->metrics_.log_cache_num_ops->value());
  ASSERT_GE(cache_->metrics_.log_cache_size->value(), 500);

  vector<ReplicateRefPtr> messages;
  OpId preceding;
  auto status = cache_->ReadOps(0, 8 * 1024 * 1024, ReadContext(), &messages);
  ASSERT_OK(status.status;)
  EXPECT_EQ(100, messages.size());
  EXPECT_EQ("0.0", OpIdToString(status.preceding_op));

  messages.clear();

  auto limit = 5;
  status = cache_->ReadOps(0, 8 * 1024 * 1024, ReadContext(), &messages, limit);
  ASSERT_OK(status.status;)
  EXPECT_EQ(limit, messages.size());
  EXPECT_EQ("0.0", OpIdToString(status.preceding_op));
}

} // namespace consensus
} // namespace kudu
