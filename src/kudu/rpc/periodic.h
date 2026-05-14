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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include <gtest/gtest_prod.h>
#include <optional>

#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"

namespace kudu {
namespace rpc {

class Messenger;

// Repeatedly runs a task on a fixed period.
//
// PeriodicTimer's periodicity is maintained via tail recursive calls to
// Messenger::ScheduleOnReactor(). Every time the scheduled callback is
// invoked, it checks the current time, updates some internal bookkeeping,
// runs the user's task if the time is right, and makes another call to
// Messenger::ScheduleOnReactor() to run itself again in the future. This
// looping behavior is called a "callback loop".
//
// Every time stop() and then start() (or just start(), if this is the first
// such call) are invoked, PeriodicTimer will kick off a new callback loop. If
// there was an old loop, it remains intact until its scheduled callback runs,
// at which point it will detect that a new loop was created and exit.
//
// The use of Messenger::ScheduleOnReactor() is merely for convenience;
// PeriodicTimer could also be built on libev, a hashed wheel timer, o
// something equivalent.
//
// PeriodicTimers have shared ownership, but that's largely an implementation
// detail to support asynchronous stopping. Users can treat them as exclusively
// owned (though care must be taken when writing the task functor; see stop()
// for more details).
//
// TODO(adar): eventually we should build directly on libev as it supports
// timer cancelation, which would allow us to implement synchronous stop(), use
// exclusive ownership, and remove the restriction that the delta passed
// into snooze() be greater than getMinimumPeriod().
class PeriodicTimer : public std::enable_shared_from_this<PeriodicTimer>,
                      public EnableMakeShared<PeriodicTimer> {
 public:
  using RunTaskFunctor = std::function<void(void)>;

  struct Options {
    Options();

    // Defines the percentage of the period that will be jittered up or down
    // randomly. Together with the period, the periodicity of the timer will
    // vary between (1-J)*P and (1+J)*P.
    //
    // Must be between 0 and 1.
    //
    // If not set, defaults to 0.25.
    double jitterPct;

    // The timer will automatically stop after running the user's task.
    //
    // Just as with a normal timer, snooze() will postpone the running of the
    // task, and stop() will cancel the task outright. Unlike a normal timer,
    // both operations will no-op if the timer has already fired.
    //
    // If not set, defaults to false.
    bool oneShot;
  };

  // Creates a new PeriodicTimer.
  //
  // A ref is taken on 'messenger', which is used for scheduling callbacks.
  //
  // 'functor' defines the user's task and is owned for the lifetime of the
  // PeriodicTimer. The task will run on the messenger's reactor threads so it
  // should do very little work (i.e. no I/O).
  //
  // 'period' defines the period between tasks.
  //
  // 'options' allows additional (optional) customization of the timer.
  static std::shared_ptr<PeriodicTimer> create(
      std::shared_ptr<Messenger> messenger,
      RunTaskFunctor functor,
      MonoDelta period,
      Options options = {});

  ~PeriodicTimer();

  // Starts the timer.
  //
  // The timer's task will run in accordance with the period and jitter mode
  // provided during timer construction.
  //
  // If 'nextTaskDelta' is set, it is used verbatim as the delay for the very
  // first task, with the configured period and jitter mode only applying to
  // subsequent tasks.
  //
  // Does nothing if the timer was already started.
  void start(std::optional<MonoDelta> nextTaskDelta = {});

  // Snoozes the timer for one period.
  //
  // If 'nextTaskDelta' is set, it is used verbatim as the delay for the next
  // task. Subsequent tasks will revert to the timer's regular period. The
  // value of 'nextTaskDelta' must be greater than getMinimumPeriod();
  // otherwise the task is not guaranteed to run in a timely manner.
  //
  // Note: snooze() is not additive. That is, if called at time X and again at
  // time X + P/2, the timer is snoozed until X+P/2+P, not X+2P.
  //
  // Does nothing if the timer is stopped.
  void snooze(std::optional<MonoDelta> nextTaskDelta = {});

  // Stops the timer.
  //
  // Stopping is asynchronous; that is, it is still possible for the task to
  // run after stop() returns. Because of this, the task's functor should be
  // written to do nothing if objects it depends on have been destroyed.
  //
  // Does nothing if the timer is already stopped.
  void stop();

  // Returns true iff the timer has been started.
  bool started() const;

  // Returns the time left till the next run if running
  std::optional<MonoDelta> timeLeft() const;

 protected:
  PeriodicTimer(
      std::shared_ptr<Messenger> messenger,
      RunTaskFunctor functor,
      MonoDelta period,
      Options options);

 private:
  FRIEND_TEST(PeriodicTimerTest, TestCallbackRestartsTimer);
  // Calculate the minimum period for the timer, which varies depending on
  // 'jitterPct' and the output of the PRNG.
  MonoDelta getMinimumPeriod();

  // Called by Messenger::ScheduleOnReactor when the timer fires.
  // 'myCallbackGeneration' is the callback generation assigned to this loop
  // when it was constructed.
  void callback(int64_t myCallbackGeneration);

  // Like stop() but must be called with 'lock_' held.
  void stopUnlocked();

  // Like snooze() but must be called with 'lock_' held.
  void snoozeUnlocked(std::optional<MonoDelta> nextTaskDelta = {});

  // Returns the number of times that callback() has been called by this timer.
  //
  // Should only be used for tests!
  int64_t numCallbacksForTests() const;

  // Schedules invocations of callback() in the future.
  std::shared_ptr<Messenger> messenger_;

  // User-defined task functor.
  RunTaskFunctor functor_;

  // User-specified task period.
  const MonoDelta period_;

  // User-specified options.
  const Options options_;

  // Protects all mutable state below.
  mutable simple_spinlock lock_;

  // PRNG used when generating jitter.
  Random rng_;

  // The next time at which the task's functor should be run.
  MonoTime nextTaskTime_;

  // The most recent callback generation.
  //
  // When started, a callback loop is assigned a generation, which it remembers
  // for its entire lifespan. If 'currentCallbackGeneration_' exceeds the
  // loop's assigned generation, that means another loop has been created and
  // the (now old) loop should exit.
  int64_t currentCallbackGeneration_;

  // The number of times that callback() has been invoked.
  int64_t numCallbacksForTests_;

  // Whether the timer is running or not.
  bool started_;

  DISALLOW_COPY_AND_ASSIGN(PeriodicTimer);
};

} // namespace rpc
} // namespace kudu
