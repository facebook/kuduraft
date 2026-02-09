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

  folly::ThreadedExecutor threadedExecutor_;
};

/**
 * Tests that the debouncer lets in a executing entity, blocks on a waiting
 * entity, and rejects additional callers.
 */
TEST_F(DebouncerTests, DebouncerTest) {
  folly::coro::AsyncScope scope;

  std::latch enqueueExecuteLatch{9};
  std::latch failedLatch{8};
  std::latch acquiredLatch{1};
  std::atomic_int acquired, failed = 0;

  for (int i = 0; i < 10; ++i) {
    scope.add(co_withExecutor(
        &threadedExecutor_,
        folly::coro::co_invoke(([&, this]() -> folly::coro::Task<void> {
          std::unique_lock guard(debouncer_, std::try_to_lock);
          enqueueExecuteLatch.count_down();
          if (guard.owns_lock()) {
            acquired++;
            acquired.notify_one();
            acquiredLatch.wait();
          } else {
            failed++;
            failedLatch.count_down();
          }
          co_return;
        }))));
  }
  enqueueExecuteLatch.wait();
  failedLatch.wait();

  EXPECT_EQ(acquired, 1);
  EXPECT_EQ(failed, 8);

  acquiredLatch.count_down();

  acquired.wait(1);

  EXPECT_EQ(acquired, 2);
  EXPECT_EQ(failed, 8);

  // Wait for scope to avoid freeing latches before they are complete
  scope.cleanup().wait();
}
} // namespace kudu
