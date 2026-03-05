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

#include <gtest/gtest.h>

#include "kudu/clock/logical_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace clock {

class LogicalClockTest : public KuduTest {
 public:
  LogicalClockTest()
      : clock_(LogicalClock::createStartingAt(Timestamp::kInitialTimestamp)) {}

 protected:
  std::shared_ptr<LogicalClock> clock_;
};

// Test that two subsequent time reads are monotonically increasing.
TEST_F(LogicalClockTest, TestNow_ValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_->now();
  const Timestamp now2 = clock_->now();
  ASSERT_EQ(now1.value() + 1, now2.value());
}

// Tests that the clock gets updated if the incoming value is higher.
TEST_F(LogicalClockTest, TestUpdate_LogicalValueIncreasesByAmount) {
  Timestamp initial = clock_->now();
  Timestamp future(initial.value() + 10);
  clock_->update(future);
  Timestamp now = clock_->now();
  // now should be 1 after future
  ASSERT_EQ(initial.value() + 11, now.value());
}

// Tests that the clock doesn't get updated if the incoming value is lower.
TEST_F(LogicalClockTest, TestUpdate_LogicalValueDoesNotIncrease) {
  Timestamp ts(1);
  // update the clock to 1, the initial value, should do nothing
  clock_->update(ts);
  Timestamp now = clock_->now();
  ASSERT_EQ(now.value(), 2);
}

TEST_F(LogicalClockTest, TestWaitUntilAfterIsUnavailable) {
  Status status = clock_->waitUntilAfter(Timestamp(10), MonoTime::Now());
  ASSERT_TRUE(status.IsServiceUnavailable());
}

TEST_F(LogicalClockTest, TestIsAfter) {
  Timestamp ts1 = clock_->now();
  ASSERT_TRUE(clock_->isAfter(ts1));

  // Update the clock in the future, make sure it still
  // handles "isAfter" properly even when it's running in
  // "logical" mode.
  Timestamp nowIncreased = Timestamp(1000);
  ASSERT_OK(clock_->update(nowIncreased));
  Timestamp ts2 = clock_->now();

  ASSERT_TRUE(clock_->isAfter(ts1));
  ASSERT_TRUE(clock_->isAfter(ts2));
}

} // namespace clock
} // namespace kudu
