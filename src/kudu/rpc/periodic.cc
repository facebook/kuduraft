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

#include "kudu/rpc/periodic.h"

#include <algorithm>
#include <memory>
#include <mutex>

#include <glog/logging.h>

#include "kudu/rpc/messenger.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"

using std::shared_ptr;
using std::weak_ptr;

namespace kudu {
namespace rpc {

PeriodicTimer::Options::Options() : jitterPct(0.25), oneShot(false) {}

shared_ptr<PeriodicTimer> PeriodicTimer::Create(
    shared_ptr<Messenger> messenger,
    RunTaskFunctor functor,
    MonoDelta period,
    Options options) {
  return PeriodicTimer::make_shared(
      std::move(messenger), std::move(functor), period, options);
}

PeriodicTimer::PeriodicTimer(
    shared_ptr<Messenger> messenger,
    RunTaskFunctor functor,
    MonoDelta period,
    Options options)
    : messenger_(std::move(messenger)),
      functor_(std::move(functor)),
      period_(period),
      options_(options),
      rng_(getRandomSeed32()),
      currentCallbackGeneration_(0),
      numCallbacksForTests_(0),
      started_(false) {
  DCHECK_GE(options_.jitterPct, 0);
  DCHECK_LE(options_.jitterPct, 1);
}

PeriodicTimer::~PeriodicTimer() {
  Stop();
}

void PeriodicTimer::Start(std::optional<MonoDelta> nextTaskDelta) {
  std::unique_lock<simple_spinlock> l(lock_);
  if (!started_) {
    started_ = true;
    SnoozeUnlocked(std::move(nextTaskDelta));
    int newCallbackGeneration = ++currentCallbackGeneration_;

    // Invoke Callback() with the lock released.
    l.unlock();
    Callback(newCallbackGeneration);
  }
}

void PeriodicTimer::Stop() {
  std::lock_guard<simple_spinlock> l(lock_);
  StopUnlocked();
}

void PeriodicTimer::StopUnlocked() {
  DCHECK(lock_.is_locked());
  started_ = false;
}

void PeriodicTimer::Snooze(std::optional<MonoDelta> nextTaskDelta) {
  std::lock_guard<simple_spinlock> l(lock_);
  SnoozeUnlocked(std::move(nextTaskDelta));
}

void PeriodicTimer::SnoozeUnlocked(std::optional<MonoDelta> nextTaskDelta) {
  DCHECK(lock_.is_locked());
  if (!started_) {
    return;
  }

  if (!nextTaskDelta) {
    // Given jitter percentage J and period P, this yields a delay somewhere
    // between (1-J)*P and (1+J)*P.
    nextTaskDelta = MonoDelta::FromMilliseconds(
        GetMinimumPeriod().ToMilliseconds() +
        rng_.NextDoubleFraction() * options_.jitterPct *
            (2 * period_.ToMilliseconds()));
  }
  nextTaskTime_ = MonoTime::Now() + *nextTaskDelta;
}

bool PeriodicTimer::started() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return started_;
}

std::optional<MonoDelta> PeriodicTimer::TimeLeft() const {
  std::lock_guard<simple_spinlock> l(lock_);
  if (!started_) {
    return {};
  }
  MonoTime now = MonoTime::Now();
  if (nextTaskTime_ > now) {
    return nextTaskTime_ - now;
  } else {
    return {};
  }
}

MonoDelta PeriodicTimer::GetMinimumPeriod() {
  // Given jitter percentage J and period P, this returns (1-J)*P, which is
  // the lowest possible jittered value.
  return MonoDelta::FromMilliseconds(
      (1.0 - options_.jitterPct) * period_.ToMilliseconds());
}

int64_t PeriodicTimer::NumCallbacksForTests() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return numCallbacksForTests_;
}

void PeriodicTimer::Callback(int64_t myCallbackGeneration) {
  // To simplify the implementation, a timer may have only one outstanding
  // callback scheduled at a time. This means that once the callback is
  // scheduled, the timer's task cannot run any earlier than whenever the
  // callback runs. Thus, the delay used when scheduling the callback dictates
  // the lowest possible value of 'nextTaskDelta' that Snooze() can honor.
  //
  // If the callback's delay is very low, Snooze() can honor a low
  // 'nextTaskDelta', but the callback will run often and burn more CPU
  // cycles. If the delay is very high, the timer will be more efficient but
  // the granularity for 'nextTaskDelta' will rise accordingly.
  //
  // As a "happy medium" we use GetMinimumPeriod() as the delay. This ensures
  // that a no-arg Snooze() on a jittered timer will always be honored, and as
  // long as the caller passes a value of at least GetMinimumPeriod() to
  // Snooze(), that too will be honored.
  MonoDelta delay = GetMinimumPeriod();
  bool runTask = false;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    numCallbacksForTests_++;

    // If the timer was stopped, exit.
    if (!started_) {
      return;
    }

    // If there's a new callback loop in town, exit.
    //
    // We could check again just before calling Messenger::ScheduleOnReactor()
    // (in case someone else restarted the timer while the functor ran, or in
    // case the functor itself restarted the timer), but there's no real reason
    // to do so: the very next iteration of this callback loop will wind up here
    // and exit.
    if (currentCallbackGeneration_ > myCallbackGeneration) {
      return;
    }

    MonoTime now = MonoTime::Now();
    if (now < nextTaskTime_) {
      // It's not yet time to run the task. Reduce the scheduled delay if
      // enough time has elapsed, but don't increase it.
      delay = std::min(delay, nextTaskTime_ - now);
    } else {
      // It's time to run the task. Although the next task time is reset now,
      // it may be reset again by virtue of running the task itself.
      runTask = true;

      if (options_.oneShot) {
        // Stop the timer first, in case the task wants to restart it.
        StopUnlocked();
      }
    }
  }

  if (runTask) {
    functor_();

    if (options_.oneShot) {
      // The task was run; exit the loop. Even if the task restarted the timer,
      // that will have started a new callback loop, so exiting here is always
      // the correct thing to do.
      return;
    }
    Snooze();
  }

  // Capture a weak_ptr reference into the submitted functor so that we can
  // safely handle the functor outliving its timer.
  weak_ptr<PeriodicTimer> w = shared_from_this();
  messenger_->ScheduleOnReactor(
      [w, myCallbackGeneration](const Status& s) {
        if (!s.ok()) {
          // The reactor was shut down.
          return;
        }
        if (auto timer = w.lock()) {
          timer->Callback(myCallbackGeneration);
        }
      },
      delay);
}

} // namespace rpc
} // namespace kudu
