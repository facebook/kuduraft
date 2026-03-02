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

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/smart_ptr/shared_ptr.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/ScopeGuard.h>

#include <fmt/core.h>
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/barrier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread_pool_builder.h"
#include "kudu/util/threadpool-test-util.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

using std::atomic;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DECLARE_int32(thread_inject_start_latency_ms);

namespace kudu {

static const char* kDefaultPoolName = "test";

class ThreadPoolTest : public KuduTest {
 public:
  virtual void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ThreadPoolBuilder(kDefaultPoolName).Build(&pool_));
  }

  Status rebuildPoolWithBuilder(const ThreadPoolBuilder& builder) {
    return builder.Build(&pool_);
  }

  Status rebuildPoolWithMinMax(int minThreads, int maxThreads) {
    return ThreadPoolBuilder(kDefaultPoolName)
        .set_min_threads(minThreads)
        .set_max_threads(maxThreads)
        .Build(&pool_);
  }

 protected:
  unique_ptr<ThreadPool> pool_;
};

TEST_F(ThreadPoolTest, TestNoTaskOpenClose) {
  ASSERT_OK(rebuildPoolWithMinMax(4, 4));
  pool_->Shutdown();
}

static void simpleTaskMethod(int n, Atomic32* counter) {
  while (n--) {
    base::subtle::NoBarrier_AtomicIncrement(counter, 1);
    boost::detail::yield(n);
  }
}

class SimpleTask : public Runnable {
 public:
  SimpleTask(int n, Atomic32* counter) : n_(n), counter_(counter) {}

  void run() override {
    simpleTaskMethod(n_, counter_);
  }

 private:
  int n_;
  Atomic32* counter_;
};

TEST_F(ThreadPoolTest, TestSimpleTasks) {
  ASSERT_OK(rebuildPoolWithMinMax(4, 4));

  Atomic32 counter(0);
  std::shared_ptr<Runnable> task(new SimpleTask(15, &counter));

  ASSERT_OK(pool_->SubmitFunc(boost::bind(&simpleTaskMethod, 10, &counter)));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitFunc(boost::bind(&simpleTaskMethod, 20, &counter)));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitClosure(Bind(&simpleTaskMethod, 123, &counter)));
  waitForPool(*pool_);
  ASSERT_EQ(10 + 15 + 20 + 15 + 123, base::subtle::NoBarrier_Load(&counter));
  pool_->Shutdown();
}

static void issueTraceStatement() {
  TRACE("hello from task");
}

// Test that the thread-local trace is propagated to tasks
// submitted to the threadpool.
TEST_F(ThreadPoolTest, TestTracePropagation) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithMinMax(1, 1));

  std::shared_ptr<Trace> t = std::make_shared<Trace>();
  {
    ADOPT_TRACE(t);
    ASSERT_OK(pool_->SubmitFunc(&issueTraceStatement));
  }
  waitForPool(*pool_);
  ASSERT_STR_CONTAINS(t->dumpToString(), "hello from task");
}

TEST_F(ThreadPoolTest, TestSubmitAfterShutdown) {
  ASSERT_OK(rebuildPoolWithMinMax(1, 1));
  pool_->Shutdown();
  Status s = pool_->SubmitFunc(&issueTraceStatement);
  ASSERT_EQ("Service unavailable: The pool has been shut down.", s.ToString());
}

class SlowTask : public Runnable {
 public:
  explicit SlowTask(CountDownLatch* latch) : latch_(latch) {}

  void run() override {
    latch_->Wait();
  }

  static shared_ptr<Runnable> newSlowTask(CountDownLatch* latch) {
    return std::make_shared<SlowTask>(latch);
  }

 private:
  CountDownLatch* latch_;
};

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMinimum) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
          .set_min_threads(0)
          .set_max_threads(3)
          .set_idle_timeout(MonoDelta::FromMilliseconds(1))));

  // There are no threads to start with.
  ASSERT_TRUE(pool_->numThreads() == 0);
  // We get up to 3 threads when submitting work.
  CountDownLatch latch(1);
  SCOPE_EXIT {
    latch.CountDown();
  };
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(2, pool_->numThreads());
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(3, pool_->numThreads());
  // The 4th piece of work gets queued.
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(3, pool_->numThreads());
  // Finish all work
  latch.CountDown();
  waitForPool(*pool_);
  ASSERT_EVENTUALLY([&]() { ASSERT_EQ(0, pool_->activeThreads()); });
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->numThreads());
}

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMaxThreads) {
  FLAGS_use_folly_threadpool = false;
  // By default a threadpool's max_threads is set to the number of CPUs, so
  // this test submits more tasks than that to ensure that the number of CPUs
  // isn't some kind of upper bound.
  const int kNumCpus = base::numCpus();

  // Build a threadpool with no limit on the maximum number of threads.
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
          .set_max_threads(std::numeric_limits<int>::max())));
  CountDownLatch latch(1);
  auto cleanupLatch = folly::makeGuard([&]() { latch.CountDown(); });

  // Submit tokenless tasks. Each should create a new thread.
  for (int i = 0; i < kNumCpus * 2; i++) {
    ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  }
  ASSERT_EQ((kNumCpus * 2), pool_->numThreads());

  // Submit tasks on two tokens. Only two threads should be created.
  unique_ptr<ThreadPoolToken> t1 =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);
  unique_ptr<ThreadPoolToken> t2 =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);
  for (int i = 0; i < kNumCpus * 2; i++) {
    ThreadPoolToken* t = (i % 2 == 0) ? t1.get() : t2.get();
    ASSERT_OK(t->Submit(SlowTask::newSlowTask(&latch)));
  }
  ASSERT_EQ((kNumCpus * 2) + 2, pool_->numThreads());

  // Submit more tokenless tasks. Each should create a new thread.
  for (int i = 0; i < kNumCpus; i++) {
    ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  }
  ASSERT_EQ((kNumCpus * 3) + 2, pool_->numThreads());

  latch.CountDown();
  // Shutdown waits for all tasks to complete.
  pool_->Shutdown();
}

// Regression test for a bug where a task is submitted exactly
// as a thread is about to exit. Previously this could hang forever.
TEST_F(ThreadPoolTest, TestRace) {
  alarm(60);
  auto cleanup = folly::makeGuard([]() {
    alarm(0); // Disable alarm on test exit.
  });
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
          .set_min_threads(0)
          .set_max_threads(1)
          .set_idle_timeout(MonoDelta::FromMicroseconds(1))));

  for (int i = 0; i < 500; i++) {
    CountDownLatch l(1);
    ASSERT_OK(pool_->SubmitFunc(boost::bind(&CountDownLatch::CountDown, &l)));
    l.Wait();
    // Sleeping a different amount in each iteration makes it more likely to hit
    // the bug.
    SleepFor(MonoDelta::FromMicroseconds(i));
  }
}

TEST_F(ThreadPoolTest, TestVariableSizeThreadPool) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
          .set_min_threads(1)
          .set_max_threads(4)
          .set_idle_timeout(MonoDelta::FromMilliseconds(1))));

  // There is 1 thread to start with.
  ASSERT_EQ(1, pool_->numThreads());
  // We get up to 4 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(1, pool_->numThreads());
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(2, pool_->numThreads());
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(3, pool_->numThreads());
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(4, pool_->numThreads());
  // The 5th piece of work gets queued.
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_EQ(4, pool_->numThreads());
  // Finish all work
  latch.CountDown();
  waitForPool(*pool_);
  ASSERT_EVENTUALLY([&]() { ASSERT_EQ(0, pool_->activeThreads()); });
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->numThreads());
}

TEST_F(ThreadPoolTest, TestMaxQueueSize) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                       .set_min_threads(1)
                                       .set_max_threads(1)
                                       .set_max_queue_size(1)));

  CountDownLatch latch(1);
  // We will be able to submit two tasks: one for max_threads == 1 and one for
  // max_queue_size == 1.
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  Status s = pool_->Submit(SlowTask::newSlowTask(&latch));
  CHECK(s.IsServiceUnavailable())
      << "Expected failure due to queue blowout:" << s.ToString();
  latch.CountDown();
  // Shutdown waits for all tasks to complete.
  pool_->Shutdown();
}

// Test that when we specify a zero-sized queue, the maximum number of threads
// running is used for enforcement.
TEST_F(ThreadPoolTest, TestZeroQueueSize) {
  FLAGS_use_folly_threadpool = false;
  const int kMaxThreads = 4;
  ASSERT_OK(rebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                       .set_max_queue_size(0)
                                       .set_max_threads(kMaxThreads)));

  CountDownLatch latch(1);
  for (int i = 0; i < kMaxThreads; i++) {
    ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  }
  Status s = pool_->Submit(SlowTask::newSlowTask(&latch));
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Thread pool is at capacity");
  latch.CountDown();
  // Shutdown waits for all tasks to complete.
  pool_->Shutdown();
}

// Regression test for KUDU-2187:
//
// If a threadpool thread is slow to start up, it shouldn't block progress of
// other tasks on the same pool.
TEST_F(ThreadPoolTest, TestSlowThreadStart) {
  FLAGS_use_folly_threadpool = false;
  // Start a pool of threads from which we'll submit tasks.
  unique_ptr<ThreadPool> submitterPool;
  ASSERT_OK(ThreadPoolBuilder("submitter")
                .set_min_threads(5)
                .set_max_threads(5)
                .Build(&submitterPool));

  // Start the actual test pool, which starts with one thread
  // but will start a second one on-demand.
  ASSERT_OK(rebuildPoolWithMinMax(1, 2));
  // Ensure that the second thread will take a long time to start.
  FLAGS_thread_inject_start_latency_ms = 3000;

  // Now submit 10 tasks to the 'submitter' pool, each of which
  // submits a single task to 'pool_'. The 'pool_' task sleeps
  // for 10ms.
  //
  // Because the 'submitter' tasks submit faster than they can be
  // processed on a single thread (due to the sleep), we expect that
  // this will trigger 'pool_' to start up its second worker thread.
  // The thread startup will have some latency injected.
  //
  // We expect that the thread startup will block only one of the
  // tasks in the 'submitter' pool after it submits its task. Other
  // tasks will continue to be processed by the other (already-running)
  // thread on 'pool_'.
  std::atomic<int32_t> totalQueueTimeMs(0);
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(submitterPool->SubmitFunc([&]() {
      auto submitTime = MonoTime::Now();
      CHECK_OK(pool_->SubmitFunc([&, submitTime]() {
        auto queueTime = MonoTime::Now() - submitTime;
        totalQueueTimeMs += queueTime.ToMilliseconds();
        SleepFor(MonoDelta::FromMilliseconds(10));
      }));
    }));
  }

  waitForPool(*submitterPool);
  waitForPool(*pool_);

  // Since the total amount of work submitted was only 100ms, we expect
  // that the performance would be equivalent to a single-threaded
  // threadpool. So, we expect the total queue time to be approximately
  // 0 + 10 + 20 ... + 80 + 90 = 450ms.
  //
  // If, instead, throughput had been blocked while starting threads,
  // we'd get something closer to 18000ms (3000ms delay * 5 submitter threads).
  ASSERT_GE(totalQueueTimeMs, 400);
  ASSERT_LE(totalQueueTimeMs, 10000);
}

// Test that setting a promise from another thread yields
// a value on the current thread.
TEST_F(ThreadPoolTest, TestPromises) {
  ASSERT_OK(rebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                       .set_min_threads(1)
                                       .set_max_threads(1)
                                       .set_max_queue_size(1)));

  Promise<int> myPromise;
  ASSERT_OK(pool_->SubmitClosure(
      Bind(&Promise<int>::set, Unretained(&myPromise), 5)));
  ASSERT_EQ(5, myPromise.get());
  pool_->Shutdown();
}

METRIC_DEFINE_entity(test_entity);
METRIC_DEFINE_histogram(
    test_entity,
    queue_length,
    "queue length",
    MetricUnit::kTasks,
    "queue length",
    1000,
    1);

METRIC_DEFINE_histogram(
    test_entity,
    queue_time,
    "queue time",
    MetricUnit::kMicroseconds,
    "queue time",
    1000000,
    1);

METRIC_DEFINE_histogram(
    test_entity,
    run_time,
    "run time",
    MetricUnit::kMicroseconds,
    "run time",
    1000,
    1);

TEST_F(ThreadPoolTest, TestMetrics) {
  FLAGS_use_folly_threadpool = false;
  MetricRegistry registry;
  vector<ThreadPoolMetrics> allMetrics;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MetricEntity> entity =
        METRIC_ENTITY_test_entity.Instantiate(
            &registry, fmt::format("test {}", i));
    allMetrics.emplace_back(
        ThreadPoolMetrics{
            METRIC_queue_length.Instantiate(entity),
            METRIC_queue_time.Instantiate(entity),
            METRIC_run_time.Instantiate(entity)});
  }

  // Enable metrics for the thread pool.
  ASSERT_OK(rebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                       .set_min_threads(1)
                                       .set_max_threads(1)
                                       .set_metrics(allMetrics[0])));

  unique_ptr<ThreadPoolToken> t1 = pool_->NewTokenWithMetrics(
      ThreadPool::ExecutionMode::Serial, allMetrics[1]);
  unique_ptr<ThreadPoolToken> t2 = pool_->NewTokenWithMetrics(
      ThreadPool::ExecutionMode::Serial, allMetrics[2]);

  // Submit once to t1, twice to t2, and three times without a token.
  ASSERT_OK(t1->SubmitFunc([]() {}));
  ASSERT_OK(t2->SubmitFunc([]() {}));
  ASSERT_OK(t2->SubmitFunc([]() {}));
  ASSERT_OK(pool_->SubmitFunc([]() {}));
  ASSERT_OK(pool_->SubmitFunc([]() {}));
  ASSERT_OK(pool_->SubmitFunc([]() {}));
  waitForPool(*pool_);

  // The total counts should reflect the number of submissions to each token.
  ASSERT_EQ(1, allMetrics[1].queueLengthHistogram->TotalCount());
  ASSERT_EQ(1, allMetrics[1].queueTimeUsHistogram->TotalCount());
  ASSERT_EQ(1, allMetrics[1].runTimeUsHistogram->TotalCount());
  ASSERT_EQ(2, allMetrics[2].queueLengthHistogram->TotalCount());
  ASSERT_EQ(2, allMetrics[2].queueTimeUsHistogram->TotalCount());
  ASSERT_EQ(2, allMetrics[2].runTimeUsHistogram->TotalCount());

  // And the counts on the pool-wide metrics should reflect all submissions.
  // Note: waitForPool adds 1 additional task for its barrier synchronization.
  ASSERT_EQ(7, allMetrics[0].queueLengthHistogram->TotalCount());
  ASSERT_EQ(7, allMetrics[0].queueTimeUsHistogram->TotalCount());
  ASSERT_EQ(7, allMetrics[0].runTimeUsHistogram->TotalCount());
}

// Test that a thread pool will crash if asked to run its own blocking
// functions in a pool thread.
//
// In a multi-threaded application, TSAN is unsafe to use following a fork().
// After a fork(), TSAN will:
// 1. Disable verification, expecting an exec() soon anyway, and
// 2. Die on future thread creation.
// For some reason, this test triggers behavior #2. We could disable it with
// the TSAN option die_after_fork=0, but this can (supposedly) lead to
// deadlocks, so we'll disable the entire test instead.
#ifndef KUDU_SANITIZE_THREAD
TEST_F(ThreadPoolTest, TestDeadlocks) {
  FLAGS_use_folly_threadpool = false;
  const char* deathMsg = "called pool function that would result in deadlock";
  ASSERT_DEATH(
      {
        ASSERT_OK(rebuildPoolWithMinMax(1, 1));
        ASSERT_OK(pool_->SubmitClosure(
            Bind(&ThreadPool::Shutdown, Unretained(pool_.get()))));
        waitForPool(*pool_);
      },
      deathMsg);
}
#endif

class SlowDestructorRunnable : public Runnable {
 public:
  void run() override {}

  virtual ~SlowDestructorRunnable() {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
};

// Test that if a tasks's destructor is slow, it doesn't cause serialization of
// the tasks in the queue.
TEST_F(ThreadPoolTest, TestSlowDestructor) {
  ASSERT_OK(rebuildPoolWithMinMax(1, 20));
  MonoTime start = MonoTime::Now();
  for (int i = 0; i < 100; i++) {
    shared_ptr<Runnable> task(new SlowDestructorRunnable());
    ASSERT_OK(pool_->Submit(std::move(task)));
  }
  waitForPool(*pool_);
  ASSERT_LT((MonoTime::Now() - start).ToSeconds(), 5);
}

// For test cases that should run with both kinds of tokens.
class ThreadPoolTestTokenTypes
    : public ThreadPoolTest,
      public testing::WithParamInterface<ThreadPool::ExecutionMode> {};

INSTANTIATE_TEST_CASE_P(
    Tokens,
    ThreadPoolTestTokenTypes,
    ::testing::Values(
        ThreadPool::ExecutionMode::Serial,
        ThreadPool::ExecutionMode::Concurrent));

TEST_F(ThreadPoolTest, TestTokenSubmitsProcessedSerially) {
  unique_ptr<ThreadPoolToken> t =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);

  Random r(SeedRandom());
  string result;
  CountDownLatch done(5); // 'a' through 'e' is 5 chars
  for (char c = 'a'; c < 'f'; c++) {
    // Sleep a little first so that there's a higher chance of out-of-order
    // appends if the submissions did execute in parallel.
    int sleepMs = r.Next() % 5;
    ASSERT_OK(t->SubmitFunc([&result, &done, c, sleepMs]() {
      SleepFor(MonoDelta::FromMilliseconds(sleepMs));
      result += c;
      done.CountDown();
    }));
  }
  done.Wait();
  ASSERT_EQ("abcde", result);
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmitsProcessedConcurrently) {
  const int kNumTokens = 5;
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName).set_max_threads(kNumTokens)));
  vector<unique_ptr<ThreadPoolToken>> tokens;

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPE_EXIT {
    alarm(0);
  }; // Disable alarm on test exit.
  shared_ptr<Barrier> b = std::make_shared<Barrier>(kNumTokens + 1);
  for (int i = 0; i < kNumTokens; i++) {
    tokens.emplace_back(pool_->NewToken(GetParam()));
    ASSERT_OK(tokens.back()->SubmitFunc([b]() { b->wait(); }));
  }

  // This will deadlock if the above tasks weren't all running concurrently.
  b->wait();
}

TEST_F(ThreadPoolTest, TestTokenSubmitsNonSequential) {
  const int kNumSubmissions = 5;
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName).set_max_threads(kNumSubmissions)));

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPE_EXIT {
    alarm(0);
  }; // Disable alarm on test exit.
  shared_ptr<Barrier> b = std::make_shared<Barrier>(kNumSubmissions + 1);
  unique_ptr<ThreadPoolToken> t =
      pool_->NewToken(ThreadPool::ExecutionMode::Concurrent);
  for (int i = 0; i < kNumSubmissions; i++) {
    ASSERT_OK(t->SubmitFunc([b]() { b->wait(); }));
  }

  // This will deadlock if the above tasks weren't all running concurrently.
  b->wait();
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenShutdown) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName).set_max_threads(4)));

  unique_ptr<ThreadPoolToken> t1(pool_->NewToken(GetParam()));
  unique_ptr<ThreadPoolToken> t2(pool_->NewToken(GetParam()));
  CountDownLatch l1(1);
  CountDownLatch l2(1);

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPE_EXIT {
    alarm(0);
  }; // Disable alarm on test exit.

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(t1->SubmitFunc([&]() { l1.Wait(); }));
  }
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(t2->SubmitFunc([&]() { l2.Wait(); }));
  }

  // Unblock all of t1's tasks, but not t2's tasks.
  l1.CountDown();

  // If this also waited for t2's tasks, it would deadlock.
  t1->Shutdown();

  // We can no longer submit to t1 but we can still submit to t2.
  ASSERT_TRUE(t1->SubmitFunc([]() {}).IsServiceUnavailable());
  ASSERT_OK(t2->SubmitFunc([]() {}));

  // Unblock t2's tasks.
  l2.CountDown();
  t2->Shutdown();
}

TEST_F(ThreadPoolTest, TestFuzz) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(ThreadPoolBuilder(kDefaultPoolName).Build(&pool_));
  const int kNumOperations = 1000;
  Random r(SeedRandom());
  vector<unique_ptr<ThreadPoolToken>> tokens;

  for (int i = 0; i < kNumOperations; i++) {
    // Operation distribution:
    //
    // - Submit without a token: 45%
    // - Submit with a randomly selected token: 40%
    // - Allocate a new token: 10%
    // - Shutdown a randomly selected token: 3%
    // - Deallocate a randomly selected token: 2%
    int op = r.Next() % 100;
    if (op < 45) {
      // Submit without a token.
      int sleepMs = r.Next() % 5;
      ASSERT_OK(pool_->SubmitFunc([sleepMs]() {
        // Sleep a little first to increase task overlap.
        SleepFor(MonoDelta::FromMilliseconds(sleepMs));
      }));
    } else if (op < 85) {
      // Submit with a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int sleepMs = r.Next() % 5;
      int tokenIdx = r.Next() % tokens.size();
      Status s = tokens[tokenIdx]->SubmitFunc([sleepMs]() {
        // Sleep a little first to increase task overlap.
        SleepFor(MonoDelta::FromMilliseconds(sleepMs));
      });
      ASSERT_TRUE(s.ok() || s.IsServiceUnavailable());
    } else if (op < 95) {
      // Allocate a token with a randomly selected policy.
      ThreadPool::ExecutionMode mode = r.Next() % 2
          ? ThreadPool::ExecutionMode::Serial
          : ThreadPool::ExecutionMode::Concurrent;
      tokens.emplace_back(pool_->NewToken(mode));
    } else if (op < 98) {
      // Shutdown a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int tokenIdx = r.Next() % tokens.size();
      tokens[tokenIdx]->Shutdown();
    } else {
      // Deallocate a randomly selected token.
      ASSERT_LT(op, 100);
      ASSERT_GE(op, 98);
      if (tokens.empty()) {
        continue;
      }
      auto it = tokens.begin();
      int tokenIdx = r.Next() % tokens.size();
      std::advance(it, tokenIdx);
      tokens.erase(it);
    }
  }

  // Some test runs will shut down the pool before the tokens, and some won't.
  // Either way should be safe.
  if (r.Next() % 2 == 0) {
    pool_->Shutdown();
  }
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmissionsAdhereToMaxQueueSize) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(rebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                       .set_min_threads(1)
                                       .set_max_threads(1)
                                       .set_max_queue_size(1)));

  CountDownLatch latch(1);
  unique_ptr<ThreadPoolToken> t = pool_->NewToken(GetParam());
  SCOPE_EXIT {
    latch.CountDown();
  };
  // We will be able to submit two tasks: one for max_threads == 1 and one for
  // max_queue_size == 1.
  ASSERT_OK(t->Submit(SlowTask::newSlowTask(&latch)));
  ASSERT_OK(t->Submit(SlowTask::newSlowTask(&latch)));
  Status s = t->Submit(SlowTask::newSlowTask(&latch));
  ASSERT_TRUE(s.IsServiceUnavailable());
}

TEST_F(ThreadPoolTest, TestTokenConcurrency) {
  FLAGS_use_folly_threadpool = false;
  ASSERT_OK(ThreadPoolBuilder(kDefaultPoolName).Build(&pool_));
  const int kNumTokens = 20;
  const int kTestRuntimeSecs = 1;
  const int kCycleThreads = 2;
  const int kShutdownThreads = 2;
  const int kSubmitThreads = 10;

  vector<shared_ptr<ThreadPoolToken>> tokens;
  Random rng(SeedRandom());

  // Protects 'tokens' and 'rng'.
  simple_spinlock lock;

  // Fetch a token from 'tokens' at random.
  auto getRandomToken = [&]() -> shared_ptr<ThreadPoolToken> {
    std::lock_guard<simple_spinlock> l(lock);
    int idx = rng.Uniform(kNumTokens);
    return tokens[idx];
  };

  // Preallocate all of the tokens.
  for (int i = 0; i < kNumTokens; i++) {
    ThreadPool::ExecutionMode mode;
    {
      std::lock_guard<simple_spinlock> l(lock);
      mode = rng.Next() % 2 ? ThreadPool::ExecutionMode::Serial
                            : ThreadPool::ExecutionMode::Concurrent;
    }
    tokens.emplace_back(pool_->NewToken(mode));
  }

  atomic<int64_t> totalNumTokensCycled(0);
  atomic<int64_t> totalNumTokensShutdown(0);
  atomic<int64_t> totalNumTokensSubmitted(0);

  CountDownLatch latch(1);
  vector<thread> threads;

  for (int i = 0; i < kCycleThreads; i++) {
    // Pick a token at random and replace it.
    //
    // The replaced token is only destroyed when the last ref is dropped,
    // possibly by another thread.
    threads.emplace_back([&]() {
      int numTokensCycled = 0;
      while (latch.count()) {
        {
          std::lock_guard<simple_spinlock> l(lock);
          int idx = rng.Uniform(kNumTokens);
          ThreadPool::ExecutionMode mode = rng.Next() % 2
              ? ThreadPool::ExecutionMode::Serial
              : ThreadPool::ExecutionMode::Concurrent;
          tokens[idx] = pool_->NewToken(mode);
        }
        numTokensCycled++;

        // Sleep a bit, otherwise this thread outpaces the other threads and
        // nothing interesting happens to most tokens.
        SleepFor(MonoDelta::FromMicroseconds(10));
      }
      totalNumTokensCycled += numTokensCycled;
    });
  }

  for (int i = 0; i < kShutdownThreads; i++) {
    // Pick a token at random and shut it down. Submitting a task to a shut
    // down token will return a ServiceUnavailable error.
    threads.emplace_back([&]() {
      int numTokensShutdown = 0;
      while (latch.count()) {
        getRandomToken()->Shutdown();
        numTokensShutdown++;
      }
      totalNumTokensShutdown += numTokensShutdown;
    });
  }

  for (int i = 0; i < kSubmitThreads; i++) {
    // Pick a token at random and submit a task to it.
    threads.emplace_back([&]() {
      int numTokensSubmitted = 0;
      Random localRng(SeedRandom());
      while (latch.count()) {
        int sleepMs = localRng.Next() % 5;
        Status s = getRandomToken()->SubmitFunc([sleepMs]() {
          // Sleep a little first so that tasks are running during other events.
          SleepFor(MonoDelta::FromMilliseconds(sleepMs));
        });
        CHECK(s.ok() || s.IsServiceUnavailable());
        numTokensSubmitted++;
      }
      totalNumTokensSubmitted += numTokensSubmitted;
    });
  }

  SleepFor(MonoDelta::FromSeconds(kTestRuntimeSecs));
  latch.CountDown();
  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << fmt::format(
      "Tokens cycled ({} threads): {}",
      kCycleThreads,
      totalNumTokensCycled.load());
  LOG(INFO) << fmt::format(
      "Tokens shutdown ({} threads): {}",
      kShutdownThreads,
      totalNumTokensShutdown.load());
  LOG(INFO) << fmt::format(
      "Tokens submitted ({} threads): {}",
      kSubmitThreads,
      totalNumTokensSubmitted.load());
}

TEST_F(ThreadPoolTest, TestLIFOThreadWakeUps) {
  FLAGS_use_folly_threadpool = false;
  const int kNumThreads = 10;

  // Test with a pool that allows for kNumThreads concurrent threads.
  ASSERT_OK(rebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName).set_max_threads(kNumThreads)));

  // Submit kNumThreads slow tasks and unblock them, in order to produce
  // kNumThreads worker threads.
  CountDownLatch latch(1);
  SCOPE_EXIT {
    latch.CountDown();
  };
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_OK(pool_->Submit(SlowTask::newSlowTask(&latch)));
  }
  ASSERT_EQ(kNumThreads, pool_->numThreads());
  latch.CountDown();
  waitForPool(*pool_);

  // The kNumThreads threads are idle and waiting for the idle timeout.

  // Submit a slow trickle of lightning fast tasks.
  //
  // If the threads are woken up in FIFO order, this trickle is enough to
  // prevent all of them from idling and the AssertEventually will time out.
  //
  // If LIFO order is used, the same thread will be reused for each task and
  // the other threads will eventually time out.
  AssertEventually(
      [&]() {
        ASSERT_OK(pool_->SubmitFunc([]() {}));
        SleepFor(MonoDelta::FromMilliseconds(10));
        ASSERT_EQ(1, pool_->numThreads());
      },
      MonoDelta::FromSeconds(10),
      AssertBackoff::NONE);
  NO_PENDING_FATALS();
}

} // namespace kudu
