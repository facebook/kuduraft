// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <memory>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/port.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace boost {
template <typename Signature>
class function;
} // namespace boost

namespace kudu {

class MonoTime;
class MonoDelta;

class Runnable {
 public:
  virtual void Run() = 0;
  virtual ~Runnable() {}
};

// Interesting thread pool metrics. Can be applied to the entire pool
struct ThreadPoolMetrics {
  // Measures the queue length seen by tasks when they enter the queue.
  std::shared_ptr<Histogram> queueLengthHistogram;

  // Measures the amount of time that tasks spend waiting in a queue.
  std::shared_ptr<Histogram> queueTimeUsHistogram;

  // Measures the amount of time that tasks spend running.
  std::shared_ptr<Histogram> runTimeUsHistogram;
};

// Forward declaration of token interface
class ThreadPoolToken {
 public:
  virtual ~ThreadPoolToken() {}

  // Submits a function using the kudu Closure system.
  [[nodiscard]] virtual Status SubmitClosure(Closure c) = 0;

  // Submits a function bound using boost::bind(&FuncName, args...).
  [[nodiscard]] virtual Status SubmitFunc(boost::function<void()> f) = 0;

  // Submits a Runnable class.
  [[nodiscard]] virtual Status Submit(std::shared_ptr<Runnable> r) = 0;

  // Marks the token as unusable for future submissions.
  virtual void Shutdown() = 0;

  // // Waits until all the tasks submitted via this token are completed.
  // virtual void Wait() = 0;

  // // Waits for all submissions using this token are complete, or until
  // 'until'
  // // time is reached.
  // virtual bool WaitUntil(const MonoTime& until) = 0;

  // // Waits for all submissions using this token are complete, or until
  // 'delta'
  // // time elapses.
  // virtual bool WaitFor(const MonoDelta& delta) = 0;
};

// Interface for thread pool implementations.
class ThreadPool {
 public:
  ThreadPool() = default;
  virtual ~ThreadPool() {}

  // Delete copy and move operations
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  // Wait for the running tasks to complete and then shutdown the threads.
  virtual void Shutdown() = 0;

  // Submits a function using the kudu Closure system.
  [[nodiscard]] virtual Status SubmitClosure(Closure c) = 0;

  // Submits a function bound using boost::bind(&FuncName, args...).
  [[nodiscard]] virtual Status SubmitFunc(boost::function<void()> f) = 0;

  // Submits a Runnable class.
  [[nodiscard]] virtual Status Submit(std::shared_ptr<Runnable> r) = 0;

  // Return the number of threads currently running (or in the process of
  // starting up) for this thread pool.
  virtual int numThreads() const = 0;

  // Return the number of threads currently executing tasks.
  virtual int activeThreads() const = 0;

  enum class ExecutionMode {
    // Tasks submitted via this token will be executed serially.
    Serial,

    // Tasks submitted via this token may be executed concurrently.
    Concurrent,
  };

  // Allocates a new token for use in token-based task submission.
  virtual std::unique_ptr<ThreadPoolToken> NewToken(ExecutionMode mode) = 0;

  // Like NewToken(), but lets the caller provide metrics for the token. These
  // metrics are incremented/decremented in addition to the configured
  // pool-wide metrics (if any).
  virtual std::unique_ptr<ThreadPoolToken> NewTokenWithMetrics(
      ThreadPool::ExecutionMode mode,
      ThreadPoolMetrics metrics) = 0;
};

} // namespace kudu
