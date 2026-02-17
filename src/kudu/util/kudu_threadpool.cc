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

#include "kudu/util/kudu_threadpool.h"

#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

namespace kudu {

using std::shared_ptr;
using std::string;
using std::unique_ptr;

////////////////////////////////////////////////////////
// FunctionRunnable
////////////////////////////////////////////////////////

class FunctionRunnable : public Runnable {
 public:
  explicit FunctionRunnable(boost::function<void()> func)
      : func_(std::move(func)) {}

  void run() override {
    func_();
  }

 private:
  boost::function<void()> func_;
};

////////////////////////////////////////////////////////
// ClosureRunnable
////////////////////////////////////////////////////////

class ClosureRunnable : public Runnable {
 public:
  explicit ClosureRunnable(Closure cl) : cl_(std::move(cl)) {}

  void run() override {
    cl_.Run();
  }

 private:
  Closure cl_;
};

////////////////////////////////////////////////////////
// KuduThreadPoolToken
////////////////////////////////////////////////////////

KuduThreadPoolToken::KuduThreadPoolToken(
    KuduThreadPool* pool,
    ThreadPool::ExecutionMode mode,
    ThreadPoolMetrics metrics)
    : mode_(mode),
      metrics_(std::move(metrics)),
      pool_(pool),
      state_(State::IDLE),
      not_running_cond_(&pool->lock_),
      active_threads_(0) {}

KuduThreadPoolToken::~KuduThreadPoolToken() {
  Shutdown();
  pool_->releaseToken(this);
}

Status KuduThreadPoolToken::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status KuduThreadPoolToken::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status KuduThreadPoolToken::Submit(shared_ptr<Runnable> r) {
  return pool_->doSubmit(std::move(r), this);
}

void KuduThreadPoolToken::Shutdown() {
  MutexLock unique_lock(pool_->lock_);
  pool_->checkNotPoolThreadUnlocked();

  // Clear the queue under the lock, but defer the releasing of the tasks
  // outside the lock, in case there are concurrent threads wanting to access
  // the KuduThreadPool. The task's destructors may acquire locks, etc, so this
  // also prevents lock inversions.
  std::deque<KuduThreadPool::Task> to_release = std::move(entries_);
  pool_->total_queued_tasks_ -= to_release.size();

  switch (state()) {
    case State::IDLE:
      // There were no tasks outstanding; we can quiesce the token immediately.
      transition(State::QUIESCED);
      break;
    case State::RUNNING:
      // There were outstanding tasks. If any are still running, switch to
      // QUIESCING and wait for them to finish (the worker thread executing
      // the token's last task will switch the token to QUIESCED). Otherwise,
      // we can quiesce the token immediately.

      // Note: this is an O(n) operation, but it's expected to be infrequent.
      // Plus doing it this way (rather than switching to QUIESCING and waiting
      // for a worker thread to process the queue entry) helps retain state
      // transition symmetry with KuduThreadPool::Shutdown.
      for (auto it = pool_->queue_.begin(); it != pool_->queue_.end();) {
        if (*it == this) {
          it = pool_->queue_.erase(it);
        } else {
          it++;
        }
      }

      if (active_threads_ == 0) {
        transition(State::QUIESCED);
        break;
      }
      transition(State::QUIESCING);
      FALLTHROUGH_INTENDED;
    case State::QUIESCING:
      // The token is already quiescing. Just wait for a worker thread to
      // switch it to QUIESCED.
      while (state() != State::QUIESCED) {
        not_running_cond_.wait();
      }
      break;
    default:
      break;
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.unlock();
  // to_release contains shared_ptr<Trace>, no manual release needed
}

void KuduThreadPoolToken::transition(State newState) {
#ifndef NDEBUG
  CHECK_NE(state_, newState);

  switch (state_) {
    case State::IDLE:
      CHECK(newState == State::RUNNING || newState == State::QUIESCED);
      if (newState == State::RUNNING) {
        CHECK(!entries_.empty());
      } else {
        CHECK(entries_.empty());
        CHECK_EQ(active_threads_, 0);
      }
      break;
    case State::RUNNING:
      CHECK(
          newState == State::IDLE || newState == State::QUIESCING ||
          newState == State::QUIESCED);
      CHECK(entries_.empty());
      if (newState == State::QUIESCING) {
        CHECK_GT(active_threads_, 0);
      }
      break;
    case State::QUIESCING:
      CHECK(newState == State::QUIESCED);
      CHECK_EQ(active_threads_, 0);
      break;
    case State::QUIESCED:
      CHECK(false); // QUIESCED is a terminal state

    default:
      LOG(FATAL) << "Unknown token state: " << state_;
  }
#endif

  // Take actions based on the state we're entering.
  switch (newState) {
    case State::IDLE:
    case State::QUIESCED:
      not_running_cond_.broadcast();
      break;
    default:
      break;
  }

  state_ = newState;
}

const char* KuduThreadPoolToken::stateToString(State s) {
  switch (s) {
    case State::IDLE:
      return "IDLE";

    case State::RUNNING:
      return "RUNNING";

    case State::QUIESCING:
      return "QUIESCING";

    case State::QUIESCED:
      return "QUIESCED";
  }
  return "<cannot reach here>";
}

////////////////////////////////////////////////////////
// KuduThreadPool
////////////////////////////////////////////////////////

KuduThreadPool::KuduThreadPool(
    std::string name,
    int min_threads,
    int max_threads,
    int max_queue_size,
    MonoDelta idle_timeout,
    std::string trace_metric_prefix,
    ThreadPoolMetrics metrics)
    : name_(std::move(name)),
      min_threads_(min_threads),
      max_threads_(max_threads),
      max_queue_size_(max_queue_size),
      idle_timeout_(idle_timeout),
      pool_status_(Status::Uninitialized("The pool was not initialized.")),
      idle_cond_(&lock_),
      no_threads_cond_(&lock_),
      num_threads_(0),
      num_threads_pending_start_(0),
      active_threads_(0),
      total_queued_tasks_(0),
      tokenless_(NewToken(ThreadPool::ExecutionMode::Concurrent)),
      metrics_(std::move(metrics)) {
  string prefix =
      !trace_metric_prefix.empty() ? std::move(trace_metric_prefix) : name_;

  queue_time_trace_metric_name_ =
      TraceMetrics::internName(prefix + ".queue_time_us");
  run_wall_time_trace_metric_name_ =
      TraceMetrics::internName(prefix + ".run_wall_time_us");
}

KuduThreadPool::~KuduThreadPool() {
  // There should only be one live token: the one used in tokenless submission.
  CHECK_EQ(1, tokens_.size()) << fmt::format(
      "Threadpool {} destroyed with {} allocated tokens",
      name_,
      tokens_.size());
  Shutdown();
}

Status KuduThreadPool::Init() {
  if (!pool_status_.IsUninitialized()) {
    return Status::NotSupported("The thread pool is already initialized");
  }
  pool_status_ = Status::OK();
  num_threads_pending_start_ = min_threads_;
  for (int i = 0; i < min_threads_; i++) {
    Status status = createThread();
    if (!status.ok()) {
      Shutdown();
      return status;
    }
  }
  return Status::OK();
}

void KuduThreadPool::Shutdown() {
  MutexLock unique_lock(lock_);
  checkNotPoolThreadUnlocked();

  // Wait for all queued and running tasks to complete before shutting down.
  while (total_queued_tasks_ > 0 || active_threads_ > 0) {
    idle_cond_.wait();
  }

  // Note: this is the same error seen at submission if the pool is at
  // capacity, so clients can't tell them apart. This isn't really a practical
  // concern though because shutting down a pool typically requires clients to
  // be quiesced first, so there's no danger of a client getting confused.
  pool_status_ = Status::ServiceUnavailable("The pool has been shut down.");

  // Clear the various queues under the lock, but defer the releasing
  // of the tasks outside the lock, in case there are concurrent threads
  // wanting to access the KuduThreadPool. The task's destructors may acquire
  // locks, etc, so this also prevents lock inversions.
  queue_.clear();
  std::deque<std::deque<Task>> to_release;
  for (auto* t : tokens_) {
    if (!t->entries_.empty()) {
      to_release.emplace_back(std::move(t->entries_));
    }
    switch (t->state()) {
      case KuduThreadPoolToken::State::IDLE:
        // The token is idle; we can quiesce it immediately.
        t->transition(KuduThreadPoolToken::State::QUIESCED);
        break;
      case KuduThreadPoolToken::State::RUNNING:
        // The token has tasks associated with it. If they're merely queued
        // (i.e. there are no active threads), the tasks will have been removed
        // above and we can quiesce immediately. Otherwise, we need to wait for
        // the threads to finish.
        t->transition(
            t->active_threads_ > 0 ? KuduThreadPoolToken::State::QUIESCING
                                   : KuduThreadPoolToken::State::QUIESCED);
        break;
      default:
        break;
    }
  }

  // The queues are empty. Wake any sleeping worker threads and wait for all
  // of them to exit. Some worker threads will exit immediately upon waking,
  // while others will exit after they finish executing an outstanding task.
  total_queued_tasks_ = 0;
  while (!idle_threads_.empty()) {
    idle_threads_.front().not_empty.signal();
    idle_threads_.pop_front();
  }
  while (num_threads_ + num_threads_pending_start_ > 0) {
    no_threads_cond_.wait();
  }

  // All the threads have exited. Check the state of each token.
  for (auto* t : tokens_) {
    DCHECK(
        t->state() == KuduThreadPoolToken::State::IDLE ||
        t->state() == KuduThreadPoolToken::State::QUIESCED);
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.unlock();
  // Automatic cleanup via std::shared_ptr, no manual release needed
  to_release.clear();
}

unique_ptr<ThreadPoolToken> KuduThreadPool::NewToken(
    ThreadPool::ExecutionMode mode) {
  return NewTokenWithMetrics(mode, {});
}

unique_ptr<ThreadPoolToken> KuduThreadPool::NewTokenWithMetrics(
    ThreadPool::ExecutionMode mode,
    ThreadPoolMetrics metrics) {
  MutexLock guard(lock_);
  unique_ptr<KuduThreadPoolToken> t(
      new KuduThreadPoolToken(this, mode, std::move(metrics)));
  auto [it, inserted] = tokens_.insert(t.get());
  CHECK(inserted) << "Token already exists in the set";
  return t;
}

void KuduThreadPool::releaseToken(KuduThreadPoolToken* t) {
  MutexLock guard(lock_);
  CHECK(!t->isActive()) << fmt::format(
      "Token with state {} may not be released",
      KuduThreadPoolToken::stateToString(t->state()));
  CHECK_EQ(1, tokens_.erase(t));
}

Status KuduThreadPool::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status KuduThreadPool::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status KuduThreadPool::Submit(shared_ptr<Runnable> r) {
  return doSubmit(
      std::move(r), static_cast<KuduThreadPoolToken*>(tokenless_.get()));
}

Status KuduThreadPool::doSubmit(
    shared_ptr<Runnable> r,
    KuduThreadPoolToken* token) {
  DCHECK(token);
  MonoTime submit_time = MonoTime::Now();

  MutexLock guard(lock_);
  if (PREDICT_FALSE(!pool_status_.ok())) {
    return pool_status_;
  }

  if (PREDICT_FALSE(!token->maySubmitNewTasks())) {
    return Status::ServiceUnavailable("Thread pool token was shut down");
  }

  // Size limit check.
  int64_t capacity_remaining = static_cast<int64_t>(max_threads_) -
      active_threads_ + static_cast<int64_t>(max_queue_size_) -
      total_queued_tasks_;
  if (capacity_remaining < 1) {
    return Status::ServiceUnavailable(
        fmt::format(
            "Thread pool is at capacity ({}/{} tasks running, {}/{} tasks queued)",
            num_threads_ + num_threads_pending_start_,
            max_threads_,
            total_queued_tasks_,
            max_queue_size_));
  }

  // Should we create another thread?

  // We assume that each current inactive thread will grab one item from the
  // queue.  If it seems like we'll need another thread, we create one.
  //
  // Rather than creating the thread here, while holding the lock, we defer
  // it to down below. This is because thread creation can be rather slow
  // (hundreds of milliseconds in some cases) and we'd like to allow the
  // existing threads to continue to process tasks while we do so.
  //
  // In theory, a currently active thread could finish immediately after this
  // calculation but before our new worker starts running. This would mean we
  // created a thread we didn't really need. However, this race is unavoidable
  // and harmless.
  //
  // Of course, we never create more than max_threads_ threads no matter what.
  int threads_from_this_submit =
      token->isActive() && token->mode() == ThreadPool::ExecutionMode::Serial
      ? 0
      : 1;
  int inactive_threads =
      num_threads_ + num_threads_pending_start_ - active_threads_;
  int additional_threads = static_cast<int>(queue_.size()) +
      threads_from_this_submit - inactive_threads;
  bool need_a_thread = false;
  if (additional_threads > 0 &&
      num_threads_ + num_threads_pending_start_ < max_threads_) {
    need_a_thread = true;
    num_threads_pending_start_++;
  }

  Task task;
  task.runnable = std::move(r);
  task.trace = Trace::CurrentTrace() ? Trace::CurrentTrace()->shared_from_this()
                                     : nullptr;
  task.submit_time = submit_time;

  // Add the task to the token's queue.
  KuduThreadPoolToken::State state = token->state();
  DCHECK(
      state == KuduThreadPoolToken::State::IDLE ||
      state == KuduThreadPoolToken::State::RUNNING);
  token->entries_.emplace_back(std::move(task));
  if (state == KuduThreadPoolToken::State::IDLE ||
      token->mode() == ThreadPool::ExecutionMode::Concurrent) {
    queue_.emplace_back(token);
    if (state == KuduThreadPoolToken::State::IDLE) {
      token->transition(KuduThreadPoolToken::State::RUNNING);
    }
  }
  int length_at_submit = total_queued_tasks_++;

  // Wake up an idle thread for this task. Choosing the thread at the front of
  // the list ensures LIFO semantics as idling threads are also added to the
  // front.
  //
  // If there are no idle threads, the new task remains on the queue and is
  // processed by an active thread (or a thread we're about to create) at some
  // point in the future.
  if (!idle_threads_.empty()) {
    idle_threads_.front().not_empty.signal();
    idle_threads_.pop_front();
  }
  guard.unlock();

  if (metrics_.queueLengthHistogram) {
    metrics_.queueLengthHistogram->Increment(length_at_submit);
  }
  if (token->metrics_.queueLengthHistogram) {
    token->metrics_.queueLengthHistogram->Increment(length_at_submit);
  }

  if (need_a_thread) {
    Status status = createThread();
    if (!status.ok()) {
      guard.lock();
      num_threads_pending_start_--;
      if (num_threads_ + num_threads_pending_start_ == 0) {
        // If we have no threads, we can't do any work.
        return status;
      }
      // If we failed to create a thread, but there are still some other
      // worker threads, log a warning message and continue.
      LOG(ERROR) << "Thread pool failed to create thread: "
                 << status.ToString();
    }
  }

  return Status::OK();
}

void KuduThreadPool::dispatchThread() {
  MutexLock unique_lock(lock_);
  auto [it, inserted] = threads_.insert(Thread::currentThread());
  CHECK(inserted) << "Thread already exists in the set";
  DCHECK_GT(num_threads_pending_start_, 0);
  num_threads_++;
  num_threads_pending_start_--;
  // If we are one of the first 'min_threads_' to start, we must be
  // a "permanent" thread.
  bool permanent = num_threads_ <= min_threads_;

  // Owned by this worker thread and added/removed from idle_threads_ as needed.
  IdleThread me(&lock_);

  while (true) {
    // Note: Status::Aborted() is used to indicate normal shutdown.
    if (!pool_status_.ok()) {
      VLOG(2) << "dispatchThread exiting: " << pool_status_.ToString();
      break;
    }

    if (queue_.empty()) {
      // There's no work to do, let's go idle.
      //
      // Note: if FIFO behavior is desired, it's as simple as changing this to
      // push_back().
      idle_threads_.push_front(me);
      SCOPE_EXIT {
        // For some wake ups (i.e. Shutdown or doSubmit) this thread is
        // guaranteed to be unlinked after being awakened. In others (i.e.
        // spurious wake-up or Wait timeout), it'll still be linked.
        if (me.is_linked()) {
          idle_threads_.erase(idle_threads_.iterator_to(me));
        }
      };
      if (permanent) {
        me.not_empty.wait();
      } else {
        if (!me.not_empty.waitFor(idle_timeout_)) {
          // After much investigation, it appears that pthread condition
          // variables have a weird behavior in which they can return ETIMEDOUT
          // from timed_wait even if another thread did in fact signal.
          // Apparently after a timeout there is some brief period during which
          // another thread may actually grab the internal mutex protecting the
          // state, signal, and release again before we get the mutex. So, we'll
          // recheck the empty queue case regardless.
          if (queue_.empty()) {
            VLOG(3) << "Releasing worker thread from pool " << name_
                    << " after " << idle_timeout_.ToMilliseconds()
                    << "ms of idle time.";
            break;
          }
        }
      }
      continue;
    }

    // Get the next token and task to execute.
    KuduThreadPoolToken* token = queue_.front();
    queue_.pop_front();
    DCHECK_EQ(KuduThreadPoolToken::State::RUNNING, token->state());
    DCHECK(!token->entries_.empty());
    Task task = std::move(token->entries_.front());
    token->entries_.pop_front();
    token->active_threads_++;
    --total_queued_tasks_;
    ++active_threads_;

    unique_lock.unlock();

    // Release the reference which was held by the queued item.
    ADOPT_TRACE(task.trace);
    task.trace.reset();

    // Update metrics
    MonoTime now(MonoTime::Now());
    int64_t queue_time_us = (now - task.submit_time).ToMicroseconds();
    TRACE_COUNTER_INCREMENT(queue_time_trace_metric_name_, queue_time_us);
    if (metrics_.queueTimeUsHistogram) {
      metrics_.queueTimeUsHistogram->Increment(queue_time_us);
    }
    if (token->metrics_.queueTimeUsHistogram) {
      token->metrics_.queueTimeUsHistogram->Increment(queue_time_us);
    }

    // Execute the task
    {
      kudu::MicrosecondsInt64 start_wall_us = GetMonoTimeMicros();

      task.runnable->run();

      int64_t wall_us = GetMonoTimeMicros() - start_wall_us;

      if (metrics_.runTimeUsHistogram) {
        metrics_.runTimeUsHistogram->Increment(wall_us);
      }
      if (token->metrics_.runTimeUsHistogram) {
        token->metrics_.runTimeUsHistogram->Increment(wall_us);
      }
      TRACE_COUNTER_INCREMENT(run_wall_time_trace_metric_name_, wall_us);
    }
    // Destruct the task while we do not hold the lock.
    //
    // The task's destructor may be expensive if it has a lot of bound
    // objects, and we don't want to block submission of the threadpool.
    // In the worst case, the destructor might even try to do something
    // with this threadpool, and produce a deadlock.
    task.runnable.reset();
    unique_lock.lock();

    // Possible states:
    // 1. The token was shut down while we ran its task. Transition to QUIESCED.
    // 2. The token has no more queued tasks. Transition back to IDLE.
    // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
    KuduThreadPoolToken::State state = token->state();
    DCHECK(
        state == KuduThreadPoolToken::State::RUNNING ||
        state == KuduThreadPoolToken::State::QUIESCING);
    if (--token->active_threads_ == 0) {
      if (state == KuduThreadPoolToken::State::QUIESCING) {
        DCHECK(token->entries_.empty());
        token->transition(KuduThreadPoolToken::State::QUIESCED);
      } else if (token->entries_.empty()) {
        token->transition(KuduThreadPoolToken::State::IDLE);
      } else if (token->mode() == ThreadPool::ExecutionMode::Serial) {
        queue_.emplace_back(token);
      }
    }
    if (--active_threads_ == 0) {
      idle_cond_.broadcast();
    }
  }

  // It's important that we hold the lock between exiting the loop and dropping
  // num_threads_. Otherwise it's possible someone else could come along here
  // and add a new task just as the last running thread is about to exit.
  CHECK(unique_lock.ownsLock());

  CHECK_EQ(threads_.erase(Thread::currentThread()), 1);
  num_threads_--;
  if (num_threads_ + num_threads_pending_start_ == 0) {
    no_threads_cond_.broadcast();

    // Sanity check: if we're the last thread exiting, the queue ought to be
    // empty. Otherwise it will never get processed.
    CHECK(queue_.empty());
    DCHECK_EQ(0, total_queued_tasks_);
  }
}

Status KuduThreadPool::createThread() {
  return kudu::Thread::Create(
      "thread pool",
      fmt::format("{}_[worker]", name_),
      &KuduThreadPool::dispatchThread,
      this,
      nullptr);
}

void KuduThreadPool::checkNotPoolThreadUnlocked() {
  Thread* current = Thread::currentThread();
  if (threads_.contains(current)) {
    LOG(FATAL) << fmt::format(
        "Thread belonging to thread pool '{}' with "
        "name '{}' called pool function that would result in deadlock",
        name_,
        current->name());
  }
}

std::ostream& operator<<(std::ostream& o, KuduThreadPoolToken::State s) {
  return o << KuduThreadPoolToken::stateToString(s);
}

} // namespace kudu
