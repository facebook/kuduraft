// Copyright (c) Meta Platforms, Inc. and affiliates.

#include <gtest/gtest.h>

#include <folly/Function.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/executors/ThreadedExecutor.h>
#include <latch>

#include "kudu/util/debouncer.h"

namespace kudu {
class DebouncerTests : public testing::Test {
 protected:
  DebouncerTests() {}
  ~DebouncerTests() {
    scope_.cleanup().wait();
  }

  void runAsync(folly::Function<folly::coro::Task<void>()> func) {
    scope_.add(folly::coro::co_invoke(std::move(func))
                   .scheduleOn(&threaded_executor_));
  }

  MutexDebouncer debouncer_;

  folly::ThreadedExecutor threaded_executor_;
  folly::coro::AsyncScope scope_;
};

/**
 * Tests that the debouncer lets in a executing entity, blocks on a waiting
 * entity, and rejects additional callers.
 */
TEST_F(DebouncerTests, DebouncerTest) {
  std::latch enqueue_execute_latch{9};
  std::latch failed_latch{8};
  std::latch acquired_latch{1};
  std::atomic_int acquired, failed = 0;

  for (int i = 0; i < 10; ++i) {
    runAsync([&, this]() -> folly::coro::Task<void> {
      std::unique_lock guard(debouncer_, std::try_to_lock);
      enqueue_execute_latch.count_down();
      if (guard.owns_lock()) {
        acquired++;
        acquired.notify_one();
        acquired_latch.wait();
      } else {
        failed++;
        failed_latch.count_down();
      }
      co_return;
    });
  }
  enqueue_execute_latch.wait();
  failed_latch.wait();

  EXPECT_EQ(acquired, 1);
  EXPECT_EQ(failed, 8);

  acquired_latch.count_down();

  acquired.wait(1);

  EXPECT_EQ(acquired, 2);
  EXPECT_EQ(failed, 8);
}
} // namespace kudu
