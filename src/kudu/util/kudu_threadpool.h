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
#ifndef KUDU_UTIL_KUDU_THREAD_POOL_H
#define KUDU_UTIL_KUDU_THREAD_POOL_H

#include <deque>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace boost {
template <typename Signature>
class function;
} // namespace boost

namespace kudu {

class Thread;
class KuduThreadPool;
class KuduThreadPoolToken;
class Trace;

// Thread pool with a variable number of threads.
//
// Tasks submitted directly to the thread pool enter a FIFO queue and are
// dispatched to a worker thread when one becomes free. Tasks may also be
// submitted via KuduThreadPoolTokens. The token Wait() and Shutdown() functions
// can then be used to block on logical groups of tasks.
//
// A token operates in one of two ExecutionModes, determined at token
// construction time:
// 1. Serial: submitted tasks are run one at a time.
// 2. Concurrent: submitted tasks may be run in parallel. This isn't unlike
//    tasks submitted without a token, but the logical grouping that tokens
//    impart can be useful when a pool is shared by many contexts (e.g. to
//    safely shut down one context, to derive context-specific metrics, etc.).
//
// Tasks submitted without a token or via ExecutionMode::Concurrent tokens are
// processed in FIFO order. On the other hand, ExecutionMode::Serial tokens are
// processed in a round-robin fashion, one task at a time. This prevents them
// from starving one another. However, tokenless (and Concurrent token-based)
// tasks can starve Serial token-based tasks.
//
// Usage Example:
//    static void Func(int n) { ... }
//    class Task : public Runnable { ... }
//
//    std::unique_ptr<KuduThreadPool> thread_pool;
//    CHECK_OK(
//        ThreadPoolBuilder("my_pool")
//            .set_min_threads(0)
//            .set_max_threads(5)
//            .set_max_queue_size(10)
//            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
//            .Build(&thread_pool));
//    thread_pool->Submit(shared_ptr<Runnable>(new Task()));
//    thread_pool->SubmitFunc(boost::bind(&Func, 10));
class KuduThreadPool : public ThreadPool {
 public:
  // Creates a new thread pool.
  KuduThreadPool(
      std::string name,
      int min_threads,
      int max_threads,
      int max_queue_size,
      MonoDelta idle_timeout,
      std::string trace_metric_prefix,
      ThreadPoolMetrics metrics);

  virtual ~KuduThreadPool() override;

  // Initializes the thread pool by starting the minimum number of threads.
  // Must be called before submitting any tasks.
  Status Init();

  // Wait for the running tasks to complete and then shutdown the threads.
  // All the other pending tasks in the queue will be removed.
  // NOTE: That the user may implement an external abort logic for the
  //       runnables, that must be called before Shutdown(), if the system
  //       should know about the non-execution of these tasks, or the runnable
  //       require an explicit "abort" notification to exit from the run loop.
  void Shutdown() override;

  // Submits a function using the kudu Closure system.
  Status SubmitClosure(Closure c) override WARN_UNUSED_RESULT;

  // Submits a function bound using boost::bind(&FuncName, args...).
  Status SubmitFunc(boost::function<void()> f) override WARN_UNUSED_RESULT;

  // Submits a Runnable class.
  Status Submit(std::shared_ptr<Runnable> r) override WARN_UNUSED_RESULT;

  // Waits until all the tasks are completed.
  void Wait() override;

  // Waits for the pool to reach the idle state, or until 'until' time is
  // reached. Returns true if the pool reached the idle state, false otherwise.
  bool WaitUntil(const MonoTime& until) override;

  // Waits for the pool to reach the idle state, or until 'delta' time elapses.
  // Returns true if the pool reached the idle state, false otherwise.
  bool WaitFor(const MonoDelta& delta) override;

  // Allocates a new token for use in token-based task submission. All tokens
  // must be destroyed before their KuduThreadPool is destroyed.
  //
  // There is no limit on the number of tokens that may be allocated.
  // Use inherited ExecutionMode from ThreadPool
  std::unique_ptr<ThreadPoolToken> NewToken(
      ThreadPool::ExecutionMode mode) override;

  // Like NewToken(), but lets the caller provide metrics for the token. These
  // metrics are incremented/decremented in addition to the configured
  // pool-wide metrics (if any).
  std::unique_ptr<ThreadPoolToken> NewTokenWithMetrics(
      ThreadPool::ExecutionMode mode,
      ThreadPoolMetrics metrics) override;

  // Return the number of threads currently running (or in the process of
  // starting up) for this thread pool.
  int numThreads() const override {
    MutexLock l(lock_);
    return num_threads_ + num_threads_pending_start_;
  }

  // Return the number of threads currently executing tasks.
  int activeThreads() const override {
    MutexLock l(lock_);
    return active_threads_;
  }

 private:
  FRIEND_TEST(KuduThreadPoolTest, TestThreadPoolWithNoMinimum);
  FRIEND_TEST(KuduThreadPoolTest, TestVariableSizeThreadPool);

  friend class KuduThreadPoolToken;

  // Client-provided task to be executed by this pool.
  struct Task {
    std::shared_ptr<Runnable> runnable;
    std::shared_ptr<Trace> trace;

    // Time at which the entry was submitted to the pool.
    MonoTime submit_time;
  };

  // Dispatcher responsible for dequeueing and executing the tasks
  void dispatchThread();

  // Create new thread.
  //
  // REQUIRES: caller has incremented 'num_threads_pending_start_' ahead of this
  // call. NOTE: For performance reasons, lock_ should not be held.
  Status createThread();

  // Aborts if the current thread is a member of this thread pool.
  void checkNotPoolThreadUnlocked();

  // Submits a task to be run via token.
  Status doSubmit(std::shared_ptr<Runnable> r, KuduThreadPoolToken* token);

  // Releases token 't' and invalidates it.
  void releaseToken(KuduThreadPoolToken* t);

  const std::string name_;
  const int min_threads_;
  const int max_threads_;
  const int max_queue_size_;
  const MonoDelta idle_timeout_;

  // Overall status of the pool. Set to an error when the pool is shut down.
  //
  // Protected by 'lock_'.
  Status pool_status_;

  // Synchronizes many of the members of the pool and all of its
  // condition variables.
  mutable Mutex lock_;

  // Condition variable for "pool is idling". Waiters wake up when
  // active_threads_ reaches zero.
  ConditionVariable idle_cond_;

  // Condition variable for "pool has no threads". Waiters wake up when
  // num_threads_ and num_pending_threads_ are both 0.
  ConditionVariable no_threads_cond_;

  // Number of threads currently running.
  //
  // Protected by lock_.
  int num_threads_;

  // Number of threads which are in the process of starting.
  // When these threads start, they will decrement this counter and
  // accordingly increment 'num_threads_'.
  //
  // Protected by lock_.
  int num_threads_pending_start_;

  // Number of threads currently running and executing client tasks.
  //
  // Protected by lock_.
  int active_threads_;

  // Total number of client tasks queued, either directly (queue_) or
  // indirectly (tokens_).
  //
  // Protected by lock_.
  int total_queued_tasks_;

  // All allocated tokens.
  //
  // Protected by lock_.
  std::unordered_set<KuduThreadPoolToken*> tokens_;

  // FIFO of tokens from which tasks should be executed. Does not own the
  // tokens; they are owned by clients and are removed from the FIFO on
  // shutdown.
  //
  // Protected by lock_.
  std::deque<KuduThreadPoolToken*> queue_;

  // Pointers to all running threads. Raw pointers are safe because a Thread
  // may only go out of scope after being removed from threads_.
  //
  // Protected by lock_.
  std::unordered_set<Thread*> threads_;

  // List of all threads currently waiting for work.
  //
  // A thread is added to the front of the list when it goes idle and is
  // removed from the front and signaled when new work arrives. This produces a
  // LIFO usage pattern that is more efficient than idling on a single
  // ConditionVariable (which yields FIFO semantics).
  //
  // Protected by lock_.
  struct IdleThread : public boost::intrusive::list_base_hook<> {
    explicit IdleThread(Mutex* m) : not_empty(m) {}

    // Condition variable for "queue is not empty". Waiters wake up when a new
    // task is queued.
    ConditionVariable not_empty;

    DISALLOW_COPY_AND_ASSIGN(IdleThread);
  };
  boost::intrusive::list<IdleThread>
      idle_threads_; // NOLINT(build/include_what_you_use)

  // ExecutionMode::Concurrent token used by the pool for tokenless submission.
  std::unique_ptr<ThreadPoolToken> tokenless_;

  // Metrics for the entire thread pool.
  const ThreadPoolMetrics metrics_;

  const char* queue_time_trace_metric_name_;
  const char* run_wall_time_trace_metric_name_;

  DISALLOW_COPY_AND_ASSIGN(KuduThreadPool);
  KuduThreadPool(KuduThreadPool&&) = delete;
  KuduThreadPool& operator=(KuduThreadPool&&) = delete;
};

// Entry point for token-based task submission and blocking for a particular
// thread pool. Tokens can only be created via KuduThreadPool::NewToken().
//
// All functions are thread-safe. Mutable members are protected via the
// KuduThreadPool's lock.
class KuduThreadPoolToken : public ThreadPoolToken {
 public:
  // Destroys the token.
  //
  // May be called on a token with outstanding tasks, as Shutdown() will be
  // called first to take care of them.
  virtual ~KuduThreadPoolToken() override;

  // Submits a function using the kudu Closure system.
  Status SubmitClosure(Closure c) override WARN_UNUSED_RESULT;

  // Submits a function bound using boost::bind(&FuncName, args...).
  Status SubmitFunc(boost::function<void()> f) override WARN_UNUSED_RESULT;

  // Submits a Runnable class.
  Status Submit(std::shared_ptr<Runnable> r) override WARN_UNUSED_RESULT;

  // Marks the token as unusable for future submissions. Any queued tasks not
  // yet running are destroyed. If tasks are in flight, Shutdown() will wait
  // on their completion before returning.
  void Shutdown() override;

  // Waits until all the tasks submitted via this token are completed.
  void Wait();

  // Waits for all submissions using this token are complete, or until 'until'
  // time is reached.
  //
  // Returns true if all submissions are complete, false otherwise.
  bool WaitUntil(const MonoTime& until);

  // Waits for all submissions using this token are complete, or until 'delta'
  // time elapses.
  //
  // Returns true if all submissions are complete, false otherwise.
  bool WaitFor(const MonoDelta& delta);

 private:
  // All possible token states. Legal state transitions:
  //   IDLE      -> RUNNING: task is submitted via token
  //   IDLE      -> QUIESCED: token or pool is shut down
  //   RUNNING   -> IDLE: worker thread finishes executing a task and
  //                      there are no more tasks queued to the token
  //   RUNNING   -> QUIESCING: token or pool is shut down while worker thread
  //                           is executing a task
  //   RUNNING   -> QUIESCED: token or pool is shut down
  //   QUIESCING -> QUIESCED:  worker thread finishes executing a task
  //                           belonging to a shut down token or pool
  enum class State {
    // Token has no queued tasks.
    IDLE,

    // A worker thread is running one of the token's previously queued tasks.
    RUNNING,

    // No new tasks may be submitted to the token. A worker thread is still
    // running a previously queued task.
    QUIESCING,

    // No new tasks may be submitted to the token. There are no active tasks
    // either. At this state, the token may only be destroyed.
    QUIESCED,
  };

  // Writes a textual representation of the token state in 's' to 'o'.
  friend std::ostream& operator<<(
      std::ostream& o,
      KuduThreadPoolToken::State s);

  friend class KuduThreadPool;

  // Returns a textual representation of 's' suitable for debugging.
  static const char* stateToString(State s);

  // Constructs a new token.
  //
  // The token may not outlive its thread pool ('pool').
  KuduThreadPoolToken(
      KuduThreadPool* pool,
      ThreadPool::ExecutionMode mode,
      ThreadPoolMetrics metrics);

  // Changes this token's state to 'newState' taking actions as needed.
  void transition(State newState);

  // Returns true if this token has a task queued and ready to run, or if a
  // task belonging to this token is already running.
  bool isActive() const {
    return state_ == State::RUNNING || state_ == State::QUIESCING;
  }

  // Returns true if new tasks may be submitted to this token.
  bool maySubmitNewTasks() const {
    return state_ != State::QUIESCING && state_ != State::QUIESCED;
  }

  State state() const {
    return state_;
  }
  ThreadPool::ExecutionMode mode() const {
    return mode_;
  }

  // Token's configured execution mode.
  const ThreadPool::ExecutionMode mode_;

  // Metrics for just this token.
  const ThreadPoolMetrics metrics_;

  // Pointer to the token's thread pool.
  KuduThreadPool* pool_;

  // Token state machine.
  State state_;

  // Queued client tasks.
  std::deque<KuduThreadPool::Task> entries_;

  // Condition variable for "token is idle". Waiters wake up when the token
  // transitions to IDLE or QUIESCED.
  ConditionVariable not_running_cond_;

  // Number of worker threads currently executing tasks belonging to this
  // token.
  int active_threads_;

  DISALLOW_COPY_AND_ASSIGN(KuduThreadPoolToken);
  KuduThreadPoolToken(KuduThreadPoolToken&&) = delete;
  KuduThreadPoolToken& operator=(KuduThreadPoolToken&&) = delete;
};

} // namespace kudu
#endif
