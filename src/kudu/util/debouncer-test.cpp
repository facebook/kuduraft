// Copyright (c) Meta Platforms, Inc. and affiliates.

#include "kudu/util/debouncer.h"

#include <latch>

#include <gtest/gtest.h>

#include <folly/Function.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/executors/ThreadedExecutor.h>

namespace kudu {
class DebouncerTests : public testing::Test {
 protected:
  MutexDebouncer debouncer_;

  folly::ThreadedExecutor threaded_executor_;
};

/**
 * Tests that the debouncer lets in a executing entity, blocks on a waiting
 * entity, and rejects additional callers.
 */
TEST_F(DebouncerTests, DebouncerTest) {
  folly::coro::AsyncScope scope;

  std::latch enqueue_execute_latch{9};
  std::latch failed_latch{8};
  std::latch acquired_latch{1};
  std::atomic_int acquired, failed = 0;

  for (int i = 0; i < 10; ++i) {
    scope.add(folly::coro::co_invoke(([&, this]() -> folly::coro::Task<void> {
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
              })).scheduleOn(&threaded_executor_));
  }
  enqueue_execute_latch.wait();
  failed_latch.wait();

  EXPECT_EQ(acquired, 1);
  EXPECT_EQ(failed, 8);

  acquired_latch.count_down();

  acquired.wait(1);

  EXPECT_EQ(acquired, 2);
  EXPECT_EQ(failed, 8);

  // Wait for scope to avoid freeing latches before they are complete
  scope.cleanup().wait();
}
} // namespace kudu
