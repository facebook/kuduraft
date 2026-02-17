// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/util/folly_threadpool.h"

#include <utility>

#include <boost/function.hpp>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/SerialExecutor.h>
#include <glog/logging.h>

#include "kudu/gutil/callback.h"

namespace kudu {

namespace {

// Helper class to wrap a kudu Closure as a Runnable.
class ClosureRunnable : public Runnable {
 public:
  explicit ClosureRunnable(Closure c) : c_(std::move(c)) {}
  void run() override {
    c_.Run();
  }

 private:
  Closure c_;
};

// Helper class to wrap a boost::function as a Runnable.
class FunctionRunnable : public Runnable {
 public:
  explicit FunctionRunnable(boost::function<void()> f) : f_(std::move(f)) {}
  void run() override {
    if (f_) {
      f_();
    }
  }

 private:
  boost::function<void()> f_;
};

} // namespace

////////////////////////////////////////////////////////////
// FollyThreadPoolToken
////////////////////////////////////////////////////////////

FollyThreadPoolToken::FollyThreadPoolToken(
    folly::Executor::KeepAlive<folly::Executor> executor,
    ThreadPool::ExecutionMode mode,
    ThreadPoolMetrics metrics)
    : metrics_(std::move(metrics)) {
  if (mode == ThreadPool::ExecutionMode::Serial) {
    executor_ = folly::SerialExecutor::create(std::move(executor));
  } else {
    executor_ = std::move(executor);
  }
}

FollyThreadPoolToken::~FollyThreadPoolToken() {
  Shutdown();
}

Status FollyThreadPoolToken::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status FollyThreadPoolToken::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status FollyThreadPoolToken::Submit(std::shared_ptr<Runnable> r) {
  if (shutdown_.load(std::memory_order_acquire)) {
    return Status::ServiceUnavailable("Token has been shut down");
  }

  executor_->add([r = std::move(r)]() { r->run(); });
  return Status::OK();
}

void FollyThreadPoolToken::Shutdown() {
  shutdown_.store(true, std::memory_order_release);
  // Release the KeepAlive. For Serial tokens this drops the SerialExecutor,
  // which is necessary so that pool Shutdown (join()) can complete.
  // Any pending tasks on a SerialExecutor will be discarded.
  executor_ = {};
}

////////////////////////////////////////////////////////////
// FollyThreadPool
////////////////////////////////////////////////////////////

FollyThreadPool::FollyThreadPool(std::string name, size_t numThreads)
    : name_(std::move(name)) {
  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      numThreads, std::make_shared<folly::NamedThreadFactory>(name_));
}

FollyThreadPool::~FollyThreadPool() {
  Shutdown();
}

void FollyThreadPool::Shutdown() {
  if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
    // Already shut down.
    return;
  }
  // join() waits for all queued and running tasks to complete.
  executor_->join();
}

Status FollyThreadPool::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status FollyThreadPool::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status FollyThreadPool::Submit(std::shared_ptr<Runnable> r) {
  if (shutdown_.load(std::memory_order_acquire)) {
    return Status::ServiceUnavailable("The pool has been shut down.");
  }

  executor_->add([r = std::move(r)]() { r->run(); });

  return Status::OK();
}

int FollyThreadPool::numThreads() const {
  return executor_->numThreads();
}

int FollyThreadPool::activeThreads() const {
  return executor_->getPoolStats().activeThreadCount;
}

std::unique_ptr<ThreadPoolToken> FollyThreadPool::NewToken(ExecutionMode mode) {
  return NewTokenWithMetrics(mode, {});
}

std::unique_ptr<ThreadPoolToken> FollyThreadPool::NewTokenWithMetrics(
    ExecutionMode mode,
    ThreadPoolMetrics metrics) {
  return std::make_unique<FollyThreadPoolToken>(
      folly::Executor::getKeepAliveToken(executor_.get()),
      mode,
      std::move(metrics));
}

} // namespace kudu
