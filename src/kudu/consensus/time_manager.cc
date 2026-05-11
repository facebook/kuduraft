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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/flag_tags.h"

DEFINE_bool(
    safe_time_advancement_without_writes,
    true,
    "Whether to enable the advancement of \"safe\" time in the absense of write "
    "operations");
TAG_FLAG(safe_time_advancement_without_writes, advanced);

DEFINE_double(
    missed_heartbeats_before_rejecting_snapshot_scans,
    1.5,
    "The maximum raft heartbeat periods since the tablet has seen safe time advanced "
    "before refusing scans at snapshots that aren't yet safe and forcing clients to "
    "try again.");
TAG_FLAG(missed_heartbeats_before_rejecting_snapshot_scans, experimental);

DEFINE_int32(
    safe_time_max_lag_ms,
    30 * 1000,
    "The maximum amount of time we allow safe time to lag behind the requested timestamp"
    "before forcing the client to retry, in milliseconds.");
TAG_FLAG(safe_time_max_lag_ms, experimental);

DEFINE_int32(
    raft_heartbeat_interval_ms,
    500,
    "The heartbeat interval for Raft replication. The leader produces heartbeats "
    "to followers at this interval. The followers expect a heartbeat at this interval "
    "and consider a leader to have failed if it misses several in a row.");
TAG_FLAG(raft_heartbeat_interval_ms, advanced);

DECLARE_int32(scanner_max_wait_ms);

using kudu::clock::Clock;
using std::string;

namespace kudu::consensus {

using Lock = std::lock_guard<simple_spinlock>;

ExternalConsistencyMode TimeManager::getMessageConsistencyMode(
    const ReplicateMsg& /* message */) {
  // TODO(dralves): We should have no-ops (?) and config changes be COMMIT_WAIT
  // transactions. See KUDU-798.
  // TODO(dralves) Move external consistency mode to ReplicateMsg. This will be
  // useful for consistent alter table ops.
  return CLIENT_PROPAGATED;
}

TimeManager::TimeManager(
    std::shared_ptr<Clock> clock,
    Timestamp initialSafeTime)
    : lastSerialTsAssigned_(initialSafeTime),
      lastSafeTs_(initialSafeTime),
      lastAdvancedSafeTime_(MonoTime::Now()),
      mode_(kNonLeader),
      clock_(std::move(clock)) {}

void TimeManager::setLeaderMode() {
  Lock l(lock_);
  mode_ = kLeader;
  advanceSafeTimeAndWakeUpWaitersUnlocked(clock_->now());
}

void TimeManager::setNonLeaderMode() {
  Lock l(lock_);
  mode_ = kNonLeader;
}

Status TimeManager::assignTimestamp(ReplicateMsg* message) {
  Lock l(lock_);
  if (PREDICT_FALSE(mode_ == kNonLeader)) {
    return Status::IllegalState(
        fmt::format(
            "Cannot assign timestamp to transaction. Tablet is not "
            "in leader mode. Last heard from a leader: {} secs ago.",
            lastAdvancedSafeTime_.ToString()));
  }
  Timestamp t;
  switch (getMessageConsistencyMode(*message)) {
    case COMMIT_WAIT:
      t = getSerialTimestampPlusMaxError();
      break;
    case CLIENT_PROPAGATED:
      t = getSerialTimestampUnlocked();
      break;
    default:
      return Status::NotSupported("Unsupported external consistency mode.");
  }
  message->set_timestamp(t.value());
  return Status::OK();
}

Status TimeManager::messageReceivedFromLeader(const ReplicateMsg& message) {
  // NOTE: Currently this method just updates the clock and stores the message's
  // timestamp.
  //       It always returns Status::OK() if the clock returns an OK status on
  //       Update().
  //
  //       When we have leader leases we can trust that the timestamps of
  //       messages sent by any valid leader are safe and we could increase safe
  //       time here. However, since this is not yet the case we will only
  //       increase safe time later, when the message is committed, at the cost
  //       of additional delay in moving safe time.
  //
  //       This greatly reduces the opportunity for non-repeatable reads. On a
  //       busy cluster with a lot of writes (i.e. no empty, "heartbeat"
  //       messages) safe time moves only with committed message timestamps,
  //       which are forcibly 'safe'.
  //
  //       The only opportunity for unrepeatable reads in this setup is if an
  //       old leader sends an (accepted) empty heartbeat message to a follower
  //       that immediately afterwards receives a non-empty message from another
  //       higher term leader but with a lower timestamp than the empty
  //       heartbeat.
  DCHECK(message.has_timestamp());
  Timestamp t(message.timestamp());
  RETURN_NOT_OK(clock_->update(t));
  {
    Lock l(lock_);
    CHECK_EQ(mode_, kNonLeader)
        << "Cannot receive messages from a leader in leader mode.";
    if (getMessageConsistencyMode(message) == CLIENT_PROPAGATED) {
      lastSerialTsAssigned_ = t;
    }
  }
  return Status::OK();
}

void TimeManager::advanceSafeTimeWithMessage(const ReplicateMsg& message) {
  Lock l(lock_);
  if (getMessageConsistencyMode(message) == CLIENT_PROPAGATED) {
    advanceSafeTimeAndWakeUpWaitersUnlocked(Timestamp(message.timestamp()));
  }
}

void TimeManager::advanceSafeTime(Timestamp safeTime) {
  Lock l(lock_);
  CHECK_EQ(mode_, kNonLeader)
      << "Cannot advance safe time by timestamp in leader mode.";
  ;
  advanceSafeTimeAndWakeUpWaitersUnlocked(safeTime);
}

bool TimeManager::hasAdvancedSafeTimeRecentlyUnlocked(string* errorMessage) {
  DCHECK(lock_.is_locked());

  MonoDelta timeSinceLastAdvance = MonoTime::Now() - lastAdvancedSafeTime_;
  int64_t maxLastAdvanced =
      FLAGS_missed_heartbeats_before_rejecting_snapshot_scans *
      FLAGS_raft_heartbeat_interval_ms;
  // Clamp maxLastAdvanced to 100 ms. Some tests set leader election timeouts
  // really low and don't necessarily want to stress scanners.
  maxLastAdvanced = std::max<int64_t>(maxLastAdvanced, 100LL);
  MonoDelta maxDelta = MonoDelta::FromMilliseconds(maxLastAdvanced);
  if (timeSinceLastAdvance > maxDelta) {
    *errorMessage = fmt::format(
        "Tablet hasn't heard from leader, or there hasn't been a stable "
        "leader for: {} secs, (max is {}):",
        timeSinceLastAdvance.ToString(),
        maxDelta.ToString());
    return false;
  }
  return true;
}

bool TimeManager::isSafeTimeLaggingUnlocked(
    Timestamp timestamp,
    string* errorMessage) {
  DCHECK(lock_.is_locked());

  // Can't calculate safe time lag for the logical clock.
  if (PREDICT_FALSE(!clock_->hasPhysicalComponent())) {
    return false;
  }
  MonoDelta safeTimeDiff =
      clock_->getPhysicalComponentDifference(timestamp, lastSafeTs_);
  if (safeTimeDiff.ToMilliseconds() > FLAGS_safe_time_max_lag_ms) {
    *errorMessage = fmt::format(
        "Tablet is lagging too much to be able to serve snapshot scan. "
        "Lagging by: {} ms, (max is {} ms):",
        safeTimeDiff.ToMilliseconds(),
        FLAGS_safe_time_max_lag_ms);
    return true;
  }
  return false;
}

void TimeManager::makeWaiterTimeoutMessageUnlocked(
    Timestamp timestamp,
    string* errorMessage) {
  DCHECK(lock_.is_locked());

  string mode = mode_ == kLeader ? "LEADER" : "NON-LEADER";
  string clockDiff = clock_->hasPhysicalComponent()
      ? clock_->getPhysicalComponentDifference(timestamp, lastSafeTs_)
            .ToString()
      : "None (Logical clock)";
  *errorMessage = fmt::format(
      "Timed out waiting for ts: {} to be safe (mode: {}). Current safe "
      "time: {} Physical time difference: {}",
      clock_->stringify(timestamp),
      mode,
      clock_->stringify(lastSafeTs_),
      clockDiff);
}

Status TimeManager::waitUntilSafe(
    Timestamp timestamp,
    const MonoTime& deadline) {
  string errorMessage;

  // Pre-flight checks:
  // - If this timestamp is before the last safe time return.
  // - If we're not the leader make sure we've heard from the leader recently.
  // - If we're not the leader make sure safe time isn't lagging too much.
  {
    Lock l(lock_);
    if (timestamp < getSafeTimeUnlocked()) {
      return Status::OK();
    }

    if (mode_ == kNonLeader) {
      if (isSafeTimeLaggingUnlocked(timestamp, &errorMessage)) {
        return Status::TimedOut(errorMessage);
      }

      if (!hasAdvancedSafeTimeRecentlyUnlocked(&errorMessage)) {
        return Status::TimedOut(errorMessage);
      }
    }
  }

  // First wait for the clock to be past 'timestamp'.
  RETURN_NOT_OK(clock_->waitUntilAfterLocally(timestamp, deadline));

  if (PREDICT_FALSE(MonoTime::Now() > deadline)) {
    return Status::TimedOut("Timed out waiting for the local clock.");
  }

  CountDownLatch latch(1);
  WaitingState waiter;
  waiter.timestamp = timestamp;
  waiter.latch = &latch;

  // Register a waiter in waiters_
  {
    Lock l(lock_);
    if (isTimestampSafeUnlocked(timestamp)) {
      return Status::OK();
    }
    waiters_.push_back(&waiter);
  }

  // Wait until we get notified or 'deadline' elapses.
  if (waiter.latch->waitUntil(deadline)) {
    return Status::OK();
  }

  // Timed out, clean up.
  {
    Lock l(lock_);
    // Address the case where we were notified after the timeout.
    if (waiter.latch->count() == 0) {
      return Status::OK();
    }

    waiters_.erase(std::find(waiters_.begin(), waiters_.end(), &waiter));

    makeWaiterTimeoutMessageUnlocked(waiter.timestamp, &errorMessage);
    return Status::TimedOut(errorMessage);
  }
}

void TimeManager::advanceSafeTimeAndWakeUpWaitersUnlocked(Timestamp safeTime) {
  DCHECK(lock_.is_locked());

  if (safeTime <= lastSafeTs_) {
    return;
  }
  lastSafeTs_ = safeTime;
  lastAdvancedSafeTime_ = MonoTime::Now();

  if (PREDICT_FALSE(!waiters_.empty())) {
    auto iter = waiters_.begin();
    while (iter != waiters_.end()) {
      WaitingState* waiter = *iter;
      if (isTimestampSafeUnlocked(waiter->timestamp)) {
        iter = waiters_.erase(iter);
        waiter->latch->countDown();
        continue;
      }
      iter++;
    }
  }
}

bool TimeManager::isTimestampSafe(Timestamp timestamp) {
  Lock l(lock_);
  return isTimestampSafeUnlocked(timestamp);
}

bool TimeManager::isTimestampSafeUnlocked(Timestamp timestamp) {
  return timestamp <= getSafeTimeUnlocked();
}

Timestamp TimeManager::getSafeTime() {
  Lock l(lock_);
  return getSafeTimeUnlocked();
}

Timestamp TimeManager::getSafeTimeUnlocked() {
  DCHECK(lock_.is_locked());

  switch (mode_) {
    case kLeader: {
      // In ASCII form, where 'S' represents a safe timestamp, 'A' represents
      // the last assigned timestamp, and 'N' represents the current clock
      // value, the internal state can look like the following diagrams (time
      // moves from left to right):
      //
      // a)
      //   SSSSSSSSSSSSSSSSSS N
      //                  |  \- lastSafeTs_
      //                  |
      //                   \- lastSerialTsAssigned_
      // or like:
      // b)
      //   SSSSSSSSSSSSSSSSSS A N
      //                    |  \- lastSerialTsAssigned_
      //                    |
      //                    \- lastSafeTs_
      //
      // If the current internal state is a), then we can advance safe time to
      // 'N'. We know the leader will never assign a new timestamp lower than
      // it.
      if (PREDICT_TRUE(lastSerialTsAssigned_ <= lastSafeTs_)) {
        lastSafeTs_ = clock_->now();
        lastAdvancedSafeTime_ = MonoTime::Now();
        return lastSafeTs_;
      }
      // If the current state is b), then there might be transaction with a
      // timestamp that is lower than 'N' in between assignment and being
      // appended to the queue. We can't consider 'N' safe and thus have to
      // return the last known safe timestamp. Note that there can be at most
      // one single transaction in this state, because prepare is single
      // threaded.
      return lastSafeTs_;
    }
    case kNonLeader:
      return lastSafeTs_;
  }
  __builtin_unreachable(); // silence gcc warnings
}

Timestamp TimeManager::getSerialTimestamp() {
  Lock l(lock_);
  return getSerialTimestampUnlocked();
}

Timestamp TimeManager::getSerialTimestampUnlocked() {
  DCHECK(lock_.is_locked());

  lastSerialTsAssigned_ = clock_->now();
  return lastSerialTsAssigned_;
}

Timestamp TimeManager::getSerialTimestampPlusMaxError() {
  return clock_->nowLatest();
}

} // namespace kudu::consensus
