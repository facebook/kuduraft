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

#include <cstdint>
#include <ostream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/util/atomic.h"
#include "kudu/util/monotime.h"
#include "kudu/util/striped64.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

// These flags are used by the multi-threaded tests, can be used for
// microbenchmarking.
DEFINE_int32(num_operations, 10 * 1000, "Number of operations to perform");
DEFINE_int32(num_threads, 2, "Number of worker threads");

namespace kudu {

// Test some basic operations
TEST(Striped64Test, TestBasic) {
  LongAdder adder;
  ASSERT_EQ(adder.value(), 0);
  adder.incrementBy(100);
  ASSERT_EQ(adder.value(), 100);
  adder.increment();
  ASSERT_EQ(adder.value(), 101);
  adder.decrement();
  ASSERT_EQ(adder.value(), 100);
  adder.incrementBy(-200);
  ASSERT_EQ(adder.value(), -100);
  adder.reset();
  ASSERT_EQ(adder.value(), 0);
}

template <class Adder>
class MultiThreadTest {
 public:
  using ThreadVecT = std::vector<std::shared_ptr<Thread>>;

  MultiThreadTest(int64_t numOperations, int64_t numThreads)
      : numOperations_(numOperations), numThreads_(numThreads) {}

  void incrementerThread(const int64_t num) {
    for (int i = 0; i < num; i++) {
      adder_.increment();
    }
  }

  void decrementerThread(const int64_t num) {
    for (int i = 0; i < num; i++) {
      adder_.decrement();
    }
  }

  void run() {
    // Increment
    for (int i = 0; i < numThreads_; i++) {
      std::shared_ptr<Thread> ref;
      Thread::Create(
          "Striped64",
          "Incrementer",
          &MultiThreadTest::incrementerThread,
          this,
          numOperations_,
          &ref);
      threads_.push_back(ref);
    }
    for (const std::shared_ptr<Thread>& t : threads_) {
      t->Join();
    }
    ASSERT_EQ(numThreads_ * numOperations_, adder_.value());
    threads_.clear();

    // Decrement back to zero
    for (int i = 0; i < numThreads_; i++) {
      std::shared_ptr<Thread> ref;
      Thread::Create(
          "Striped64",
          "Decrementer",
          &MultiThreadTest::decrementerThread,
          this,
          numOperations_,
          &ref);
      threads_.push_back(ref);
    }
    for (const std::shared_ptr<Thread>& t : threads_) {
      t->Join();
    }
    ASSERT_EQ(0, adder_.value());
  }

  Adder adder_;

  int64_t numOperations_;
  // This is rounded down to the nearest even number
  int32_t numThreads_;
  ThreadVecT threads_;
};

// Test adder implemented by a single AtomicInt for comparison
class BasicAdder {
 public:
  BasicAdder() : value_(0) {}
  void incrementBy(int64_t x) {
    value_.incrementBy(x);
  }
  inline void increment() {
    incrementBy(1);
  }
  inline void decrement() {
    incrementBy(-1);
  }
  int64_t value() {
    return value_.load();
  }

 private:
  AtomicInt<int64_t> value_;
};

void runMultiTest(int64_t numOperations, int64_t numThreads) {
  MonoTime start = MonoTime::Now();
  MultiThreadTest<BasicAdder> basicTest(numOperations, numThreads);
  basicTest.run();
  MonoTime end1 = MonoTime::Now();
  MultiThreadTest<LongAdder> test(numOperations, numThreads);
  test.run();
  MonoTime end2 = MonoTime::Now();
  MonoDelta basic = end1 - start;
  MonoDelta striped = end2 - end1;
  LOG(INFO) << "Basic counter took   " << basic.ToMilliseconds() << "ms.";
  LOG(INFO) << "Striped counter took " << striped.ToMilliseconds() << "ms.";
}

// Compare a single-thread workload. Demonstrates the overhead of LongAdder over
// AtomicInt.
TEST(Striped64Test, TestSingleIncrDecr) {
  OverrideFlagForSlowTests(
      "num_operations", fmt::format("{}", (FLAGS_num_operations * 100)));
  runMultiTest(FLAGS_num_operations, 1);
}

// Compare a multi-threaded workload. LongAdder should show improvements here.
TEST(Striped64Test, TestMultiIncrDecr) {
  OverrideFlagForSlowTests(
      "num_operations", fmt::format("{}", (FLAGS_num_operations * 100)));
  OverrideFlagForSlowTests(
      "num_threads", fmt::format("{}", (FLAGS_num_threads * 4)));
  runMultiTest(FLAGS_num_operations, FLAGS_num_threads);
}

TEST(Striped64Test, TestSize) {
  ASSERT_EQ(16, sizeof(LongAdder));
}

} // namespace kudu
