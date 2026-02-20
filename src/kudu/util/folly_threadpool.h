// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
#pragma once

#include <atomic>
#include <memory>
#include <string>

#include <folly/Executor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace boost {
template <typename Signature>
class function;
} // namespace boost

namespace kudu {

// Token for FollyThreadPool that supports SERIAL and CONCURRENT execution
// modes. Holds a KeepAlive to either the pool's executor (CONCURRENT) or a
// SerialExecutor wrapping it (SERIAL). Shutdown simply resets the KeepAlive.
class FollyThreadPoolToken : public ThreadPoolToken {
 public:
  FollyThreadPoolToken(
      folly::Executor::KeepAlive<folly::Executor> executor,
      ThreadPool::ExecutionMode mode,
      ThreadPoolMetrics metrics);
  ~FollyThreadPoolToken() override;

  Status SubmitClosure(Closure c) override WARN_UNUSED_RESULT;
  Status SubmitFunc(boost::function<void()> f) override WARN_UNUSED_RESULT;
  Status Submit(std::shared_ptr<Runnable> r) override WARN_UNUSED_RESULT;
  void Shutdown() override;

 private:
  ThreadPoolMetrics metrics_;
  std::atomic<bool> shutdown_{false};
  folly::Executor::KeepAlive<folly::Executor> executor_;

  DISALLOW_COPY_AND_ASSIGN(FollyThreadPoolToken);
};

// Thread pool implementation backed by folly::CPUThreadPoolExecutor.
//
// This implementation provides:
// - Fixed number of threads (set at construction time)
// - FIFO task execution for concurrent submissions
// - Serial execution via tokens with ExecutionMode::SERIAL
// - Graceful shutdown via join() which waits for all tasks to complete
class FollyThreadPool : public ThreadPool {
 public:
  // Creates a new thread pool with the specified name and number of threads.
  FollyThreadPool(std::string name, size_t numThreads);
  ~FollyThreadPool() override;

  // Shuts down the thread pool, waiting for all queued and running tasks to
  // complete. This is equivalent to folly::CPUThreadPoolExecutor::join().
  void Shutdown() override;

  Status SubmitClosure(Closure c) override WARN_UNUSED_RESULT;
  Status SubmitFunc(boost::function<void()> f) override WARN_UNUSED_RESULT;
  Status Submit(std::shared_ptr<Runnable> r) override WARN_UNUSED_RESULT;

  int numThreads() const override;
  int activeThreads() const override;

  std::unique_ptr<ThreadPoolToken> NewToken(ExecutionMode mode) override;
  std::unique_ptr<ThreadPoolToken> NewTokenWithMetrics(
      ExecutionMode mode,
      ThreadPoolMetrics metrics) override;

 private:
  std::string name_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::atomic<bool> shutdown_{false};

  DISALLOW_COPY_AND_ASSIGN(FollyThreadPool);
};

} // namespace kudu
