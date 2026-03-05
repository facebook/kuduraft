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

#include <memory>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using std::unique_ptr;

class TimeManagerTest : public KuduTest {
 public:
  TimeManagerTest() : clock_(std::make_shared<clock::HybridClock>()) {}

  void SetUp() override {
    CHECK_OK(clock_->init());
  }

  void TearDown() override {
    for (auto& thread : threads_) {
      thread.join();
    }
  }

 protected:
  void initTimeManager(Timestamp initialSafeTime = Timestamp::kMin) {
    timeManager_ = std::make_shared<TimeManager>(clock_, initialSafeTime);
  }

  // Returns a latch that allows to wait for TimeManager to consider 'safeTime'
  // safe.
  CountDownLatch* waitForSafeTimeAsync(Timestamp safeTime) {
    latches_.emplace_back(new CountDownLatch(1));
    CountDownLatch* latch = latches_.back().get();
    threads_.emplace_back([=, this]() {
      CHECK_OK(timeManager_->WaitUntilSafe(safeTime, MonoTime::Max()));
      // When the waiter unblocks safe time should be higher than or equal to
      // 'safeTime'
      CHECK_GE(timeManager_->GetSafeTime(), safeTime);
      latch->countDown();
    });
    return latch;
  }

  std::shared_ptr<clock::HybridClock> clock_;
  std::shared_ptr<TimeManager> timeManager_;
  std::vector<unique_ptr<CountDownLatch>> latches_;
  std::vector<std::thread> threads_;
};

// Tests TimeManager's functionality in non-leader mode and the transition to
// leader mode.
TEST_F(TimeManagerTest, TestTimeManagerNonLeaderMode) {
  // TimeManager should start in non-leader mode and consider the initial
  // timestamp safe.
  Timestamp before = clock_->now();
  Timestamp init(before.value() + 1);
  Timestamp after(init.value() + 1);
  initTimeManager(init);
  ASSERT_EQ(timeManager_->mode_, TimeManager::NON_LEADER);
  ASSERT_EQ(timeManager_->lastSerialTsAssigned_, init);
  ASSERT_EQ(timeManager_->GetSafeTime(), init);

  // Check that 'before' is safe, as is 'init'. 'after' shouldn't be safe.
  ASSERT_TRUE(timeManager_->IsTimestampSafe(before));
  ASSERT_TRUE(timeManager_->IsTimestampSafe(init));
  ASSERT_FALSE(timeManager_->IsTimestampSafe(after));

  // Shouldn't be able to assign timestamps.
  ReplicateMsg message;
  ASSERT_TRUE(timeManager_->AssignTimestamp(&message).IsIllegalState());

  message.set_timestamp(after.value());
  // Should accept messages from the leader.
  ASSERT_OK(timeManager_->MessageReceivedFromLeader(message));
  ASSERT_EQ(timeManager_->lastSerialTsAssigned_, after);
  // .. but shouldn't advance safe time (until we have leader leases).
  ASSERT_EQ(timeManager_->GetSafeTime(), init);

  // Waiting for safe time at this point should time out since we're not moving
  // it.
  MonoTime afterSmall = MonoTime::Now() + MonoDelta::FromMilliseconds(100);
  ASSERT_TRUE(timeManager_->WaitUntilSafe(after, afterSmall).IsTimedOut());

  // Create a latch to wait on 'after' to be safe.
  CountDownLatch* afterLatch = waitForSafeTimeAsync(after);

  // Accepting messages from the leader shouldn't advance safe time.
  ASSERT_EQ(timeManager_->GetSafeTime(), init);
  ASSERT_EQ(afterLatch->count(), 1);

  // Advancing safe time with a message should unblock the waiter and advance
  // safe time.
  message.set_timestamp(after.value());
  timeManager_->AdvanceSafeTimeWithMessage(message);
  afterLatch->wait();
  ASSERT_EQ(timeManager_->GetSafeTime(), after);

  // Committing an old message shouldn't move safe time back.
  message.set_timestamp(before.value());
  timeManager_->AdvanceSafeTimeWithMessage(message);
  ASSERT_EQ(timeManager_->GetSafeTime(), after);

  // Advance 'after' again and test advancing safe time with an explicit
  // timestamp like the leader sends on (empty) heartbeat messages.
  after = clock_->now();
  afterLatch = waitForSafeTimeAsync(after);
  timeManager_->AdvanceSafeTime(after);
  afterLatch->wait();
  ASSERT_EQ(timeManager_->GetSafeTime(), after);

  // Changing to leader mode should advance safe time.
  after = clock_->now();
  afterLatch = waitForSafeTimeAsync(after);
  timeManager_->SetLeaderMode();
  afterLatch->wait();
  ASSERT_GE(timeManager_->GetSafeTime(), after);
}

// Tests the TimeManager's functionality in leader mode and the transition to
// non-leader mode.
TEST_F(TimeManagerTest, TestTimeManagerLeaderMode) {
  Timestamp init = clock_->now();
  initTimeManager(init);
  timeManager_->SetLeaderMode();
  Timestamp safeBefore = timeManager_->GetSafeTime();

  ReplicateMsg message;
  // In leader mode we should be able to assign timestamps and the timestamp
  // should be higher than 'init'.
  ASSERT_OK(timeManager_->AssignTimestamp(&message));
  ASSERT_TRUE(message.has_timestamp());
  Timestamp messageTs(message.timestamp());
  ASSERT_GT(messageTs, safeBefore);

  // In leader mode calling MessageReceivedFromLeader() should cause a CHECK
  // failure.
  EXPECT_DEATH(
      { timeManager_->MessageReceivedFromLeader(message); },
      "Cannot receive messages from a leader in leader mode.");

  // .. as should AdvanceSafeTime()
  EXPECT_DEATH(
      { timeManager_->AdvanceSafeTime(clock_->now()); },
      "Cannot advance safe time by timestamp in leader mode.");

  // Since we haven't appended the message to the queue, safe time should be
  // 'pinned' to 'safeBefore'.
  ASSERT_EQ(timeManager_->GetSafeTime(), safeBefore);

  // When we append the message to the queue safe time should advance again.
  timeManager_->AdvanceSafeTimeWithMessage(message);
  ASSERT_GT(timeManager_->GetSafeTime(), messageTs);

  // 'Now' should be safe.
  Timestamp now = clock_->now();
  ASSERT_TRUE(timeManager_->IsTimestampSafe(now));
  ASSERT_GT(timeManager_->GetSafeTime(), now);

  // When changing to non-leader mode a timestamp after the last safe time
  // shouldn't be safe anymore (even if that time came before the actual
  // change).
  now = clock_->now();
  timeManager_->SetNonLeaderMode();
  Timestamp safeAfter = timeManager_->GetSafeTime();
  ASSERT_LE(safeAfter, now);

  // In leader mode GetSafeTime() usually moves it, but since we changed to
  // non-leader mode safe time shouldn't move anymore ...
  ASSERT_EQ(timeManager_->GetSafeTime(), safeAfter);
  now = clock_->now();
  MonoTime afterSmall = MonoTime::Now();
  afterSmall.AddDelta(MonoDelta::FromMilliseconds(100));
  ASSERT_TRUE(timeManager_->WaitUntilSafe(now, afterSmall).IsTimedOut());

  // ... unless we get a message from the leader.
  now = clock_->now();
  CountDownLatch* afterLatch = waitForSafeTimeAsync(now);
  message.set_timestamp(now.value());
  ASSERT_OK(timeManager_->MessageReceivedFromLeader(message));
  timeManager_->AdvanceSafeTimeWithMessage(message);
  afterLatch->wait();
}

} // namespace consensus
} // namespace kudu
