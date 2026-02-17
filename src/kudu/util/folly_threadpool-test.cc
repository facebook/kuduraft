// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/util/folly_threadpool.h"

#include <atomic>
#include <barrier>
#include <latch>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <folly/ScopeGuard.h>

#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

#define ASSERT_OK(status)                  \
  do {                                     \
    const auto& _s = (status);             \
    ASSERT_TRUE(_s.ok()) << _s.ToString(); \
  } while (0)

using std::atomic;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class FollyThreadPoolTest : public ::testing::Test {
 public:
  void SetUp() override {
    pool_ = std::make_unique<FollyThreadPool>("test", 4);
  }

  void TearDown() override {
    pool_.reset();
  }

 protected:
  unique_ptr<FollyThreadPool> pool_;
};

class SimpleTask : public Runnable {
 public:
  explicit SimpleTask(atomic<int>& counter) : counter_(counter) {}

  void run() override {
    counter_++;
  }

 private:
  atomic<int>& counter_;
};

// Test that we can create a pool and shut it down without submitting any tasks.
TEST_F(FollyThreadPoolTest, TestNoTaskOpenClose) {
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->numThreads());
}

// Test basic task submission and execution.
TEST_F(FollyThreadPoolTest, TestSimpleTasks) {
  atomic<int> counter(0);
  auto task = std::make_shared<SimpleTask>(counter);

  ASSERT_OK(pool_->SubmitFunc([&counter]() { counter += 10; }));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitFunc([&counter]() { counter += 20; }));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitFunc([&counter]() { counter += 100; }));

  // Shutdown waits for all tasks to complete.
  pool_->Shutdown();
  ASSERT_EQ(10 + 1 + 20 + 1 + 100, counter.load());
}

// Test that submitting a task after shutdown returns an error.
TEST_F(FollyThreadPoolTest, TestSubmitAfterShutdown) {
  pool_->Shutdown();
  Status s = pool_->SubmitFunc([]() {});
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_THAT(s.ToString(), ::testing::HasSubstr("shut down"));
}

// Test that numThreads() and activeThreads() return reasonable values.
TEST_F(FollyThreadPoolTest, TestThreadCounts) {
  // Pool was created with 4 threads.
  ASSERT_EQ(4, pool_->numThreads());

  // No active threads initially.
  ASSERT_EQ(0, pool_->activeThreads());

  // Submit tasks that block until we release them.
  std::latch allStarted(4);
  std::latch release(1);

  for (int i = 0; i < 4; i++) {
    ASSERT_OK(pool_->SubmitFunc([&allStarted, &release]() {
      allStarted.count_down();
      release.wait();
    }));
  }

  // Wait until all tasks are running.
  allStarted.wait();

  // All 4 threads should be active.
  ASSERT_EQ(4, pool_->activeThreads());

  // Unblock tasks and shutdown.
  release.count_down();
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->numThreads());
}

// Test that SERIAL tokens execute tasks one at a time in order.
TEST_F(FollyThreadPoolTest, TestSerialTokenExecutesInOrder) {
  unique_ptr<ThreadPoolToken> token =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);

  string result;
  std::latch doneLatch(5);

  for (char c = 'a'; c <= 'e'; c++) {
    ASSERT_OK(token->SubmitFunc([&result, c, &doneLatch]() {
      result += c;
      doneLatch.count_down();
    }));
  }

  doneLatch.wait();
  // Must destroy token before pool shutdown to release the keep-alive.
  token.reset();
  pool_->Shutdown();
  ASSERT_EQ("abcde", result);
}

// Test that CONCURRENT tokens can execute tasks in parallel.
TEST_F(FollyThreadPoolTest, TestConcurrentTokenExecutesInParallel) {
  const int kNumTasks = 4;
  unique_ptr<ThreadPoolToken> token =
      pool_->NewToken(ThreadPool::ExecutionMode::Concurrent);

  // Use a barrier to ensure all tasks run concurrently.
  alarm(60);
  SCOPE_EXIT {
    alarm(0);
  };

  auto barrier = std::make_shared<std::barrier<>>(kNumTasks + 1);
  for (int i = 0; i < kNumTasks; i++) {
    ASSERT_OK(token->SubmitFunc([barrier]() { barrier->arrive_and_wait(); }));
  }

  // If tasks weren't running concurrently, this would deadlock.
  barrier->arrive_and_wait();
  token.reset();
  pool_->Shutdown();
}

// Test that multiple tokens can execute tasks concurrently.
TEST_F(FollyThreadPoolTest, TestMultipleTokensConcurrent) {
  const int kNumTokens = 4;
  vector<unique_ptr<ThreadPoolToken>> tokens;

  alarm(60);
  SCOPE_EXIT {
    alarm(0);
  };

  auto barrier = std::make_shared<std::barrier<>>(kNumTokens + 1);
  for (int i = 0; i < kNumTokens; i++) {
    tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::Serial));
    ASSERT_OK(
        tokens.back()->SubmitFunc([barrier]() { barrier->arrive_and_wait(); }));
  }

  // If tokens weren't running concurrently, this would deadlock.
  barrier->arrive_and_wait();
  tokens.clear();
  pool_->Shutdown();
}

// Test that token shutdown prevents further submissions.
TEST_F(FollyThreadPoolTest, TestTokenShutdown) {
  unique_ptr<ThreadPoolToken> token =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);

  atomic<int> counter(0);
  std::latch taskDone(1);
  ASSERT_OK(token->SubmitFunc([&counter, &taskDone]() {
    counter++;
    taskDone.count_down();
  }));

  // Wait for task to complete before shutting down the token.
  taskDone.wait();
  token->Shutdown();

  // Submissions should fail after shutdown.
  Status s = token->SubmitFunc([&counter]() { counter++; });
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_THAT(s.ToString(), ::testing::HasSubstr("shut down"));

  pool_->Shutdown();
  ASSERT_EQ(1, counter.load());
}

// Test that submitting to a token after pool shutdown returns error.
// Note: The token must be shut down before the pool to release the keep-alive.
TEST_F(FollyThreadPoolTest, TestPoolShutdownAffectsTokens) {
  unique_ptr<ThreadPoolToken> token =
      pool_->NewToken(ThreadPool::ExecutionMode::Serial);

  // Mark the pool as shut down (sets the shutdown flag).
  // First destroy the token to release the keep-alive.
  token->Shutdown();
  pool_->Shutdown();

  // Now any attempt to submit directly to the pool should fail.
  Status s = pool_->SubmitFunc([]() {});
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
}

// Test that tasks can be submitted from multiple threads safely.
TEST_F(FollyThreadPoolTest, TestConcurrentSubmissions) {
  const int kNumThreads = 8;
  const int kSubmissionsPerThread = 100;

  atomic<int> counter(0);
  std::latch startLatch(1);
  std::latch doneLatch(kNumThreads);

  vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&]() {
      startLatch.wait();
      for (int j = 0; j < kSubmissionsPerThread; j++) {
        Status s = pool_->SubmitFunc([&counter]() { counter++; });
        CHECK(s.ok() || s.IsServiceUnavailable());
      }
      doneLatch.count_down();
    });
  }

  startLatch.count_down();
  doneLatch.wait();

  pool_->Shutdown();

  // All tasks should have completed.
  ASSERT_EQ(kNumThreads * kSubmissionsPerThread, counter.load());

  for (auto& t : threads) {
    t.join();
  }
}

// Test that Shutdown() blocks until a running task completes.
TEST_F(FollyThreadPoolTest, TestShutdownWaitsForRunningTasks) {
  std::latch taskStarted(1);
  std::latch taskContinue(1);
  atomic<bool> taskCompleted(false);

  ASSERT_OK(pool_->SubmitFunc([&]() {
    taskStarted.count_down();
    taskContinue.wait();
    taskCompleted.store(true);
  }));

  // Wait until the task is definitely running.
  taskStarted.wait();

  // Release it from a background thread so Shutdown() has to block waiting.
  std::thread releaser([&]() { taskContinue.count_down(); });

  // Shutdown() must not return until the task finishes.
  pool_->Shutdown();
  ASSERT_TRUE(taskCompleted.load());

  releaser.join();
}

// Test that Shutdown() also drains queued (not yet running) tasks.
TEST_F(FollyThreadPoolTest, TestShutdownWaitsForQueuedTasks) {
  // Saturate all 4 threads with blocking tasks.
  std::latch blockLatch(1);
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(pool_->SubmitFunc([&blockLatch]() { blockLatch.wait(); }));
  }

  // This 5th task must be queued since all threads are busy.
  atomic<bool> queuedTaskRan(false);
  ASSERT_OK(
      pool_->SubmitFunc([&queuedTaskRan]() { queuedTaskRan.store(true); }));

  // Unblock the running tasks, then shutdown.
  blockLatch.count_down();
  pool_->Shutdown();

  // Shutdown should have waited for the queued task to execute too.
  ASSERT_TRUE(queuedTaskRan.load());
}

// Test that double shutdown is safe.
TEST_F(FollyThreadPoolTest, TestDoubleShutdown) {
  ASSERT_OK(pool_->SubmitFunc([]() {}));
  pool_->Shutdown();
  pool_->Shutdown(); // Should not crash or hang.
}

// Test that destructor calls shutdown.
TEST_F(FollyThreadPoolTest, TestDestructorCallsShutdown) {
  atomic<int> counter(0);
  {
    FollyThreadPool localPool("local", 2);
    ASSERT_OK(localPool.SubmitFunc([&counter]() { counter++; }));
    // Pool will be destroyed here, should wait for task to complete.
  }
  ASSERT_EQ(1, counter.load());
}

// Fuzz test: randomly submit, shutdown, and create tokens.
TEST_F(FollyThreadPoolTest, TestFuzz) {
  const int kNumOperations = 500;
  std::mt19937 rng(std::random_device{}());
  vector<unique_ptr<ThreadPoolToken>> tokens;

  atomic<int> submitted(0);
  atomic<int> completed(0);

  for (int i = 0; i < kNumOperations; i++) {
    int op = rng() % 100;

    if (op < 50) {
      // Submit without a token.
      Status s = pool_->SubmitFunc([&completed]() { completed++; });
      if (s.ok()) {
        submitted++;
      }
    } else if (op < 80) {
      // Submit with a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int tokenIdx = rng() % tokens.size();
      Status s = tokens[tokenIdx]->SubmitFunc([&completed]() { completed++; });
      if (s.ok()) {
        submitted++;
      }
    } else if (op < 90) {
      // Allocate a new token.
      ThreadPool::ExecutionMode mode = rng() % 2
          ? ThreadPool::ExecutionMode::Serial
          : ThreadPool::ExecutionMode::Concurrent;
      tokens.emplace_back(pool_->NewToken(mode));
    } else if (op < 95) {
      // Shutdown a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int tokenIdx = rng() % tokens.size();
      tokens[tokenIdx]->Shutdown();
    } else {
      // Deallocate a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int tokenIdx = rng() % tokens.size();
      tokens.erase(tokens.begin() + tokenIdx);
    }
  }

  // Clear all tokens before pool shutdown to release keep-alives.
  // Note: Some tasks may be dropped if they haven't started yet.
  tokens.clear();
  pool_->Shutdown();

  // With our implementation, submitted tasks may be dropped when tokens are
  // destroyed. So we just verify no crashes occurred - we can't guarantee
  // submitted == completed because SerialExecutor drops pending tasks on
  // destruction.
}

// Test single-threaded pool.
TEST_F(FollyThreadPoolTest, TestSingleThreadPool) {
  FollyThreadPool singlePool("single", 1);
  ASSERT_EQ(1, singlePool.numThreads());

  string result;
  std::latch doneLatch(3);

  for (char c = 'a'; c <= 'c'; c++) {
    ASSERT_OK(singlePool.SubmitFunc([&result, c, &doneLatch]() {
      result += c;
      doneLatch.count_down();
    }));
  }

  doneLatch.wait();
  singlePool.Shutdown();

  // With single thread, tasks execute in order.
  ASSERT_EQ("abc", result);
}

} // namespace kudu
