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

#include "kudu/clock/hybrid_clock.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/system_ntp.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using std::string;

DEFINE_int32(
    kudu_max_clock_sync_error_usec,
    10 * 1000 * 1000, // 10 secs
    "Maximum allowed clock synchronization error as reported by NTP "
    "before the server will abort.");
TAG_FLAG(kudu_max_clock_sync_error_usec, advanced);
TAG_FLAG(kudu_max_clock_sync_error_usec, runtime);

DEFINE_bool(
    use_hybrid_clock,
    true,
    "Whether HybridClock should be used as the default clock"
    " implementation. This should be disabled for testing purposes only.");
TAG_FLAG(use_hybrid_clock, hidden);

DEFINE_string(
    time_source,
    "system",
    "The clock source that HybridClock should use. Must be one of "
    "'system' or 'mock' (for tests only)");
TAG_FLAG(time_source, experimental);
DEFINE_validator(
    time_source,
    [](const char* /* flag_name */, const string& value) {
      if (boost::iequals(value, "system") || boost::iequals(value, "mock")) {
        return true;
      }
      LOG(ERROR) << "unknown value for 'time_source': '" << value << "'"
                 << " (expected one of 'system' or 'mock')";
      return false;
    });

METRIC_DEFINE_gauge_uint64(
    server,
    hybrid_clock_timestamp,
    "Hybrid Clock Timestamp",
    kudu::MetricUnit::kMicroseconds,
    "Hybrid clock timestamp.");
METRIC_DEFINE_gauge_uint64(
    server,
    hybrid_clock_error,
    "Hybrid Clock Error",
    kudu::MetricUnit::kMicroseconds,
    "Server clock maximum error.");

namespace kudu::clock {

namespace {

Status checkDeadlineNotWithinMicros(
    const MonoTime& deadline,
    int64_t waitForUsec) {
  if (!deadline.Initialized()) {
    // No deadline.
    return Status::OK();
  }
  int64_t usUntilDeadline = (deadline - MonoTime::Now()).ToMicroseconds();
  if (usUntilDeadline <= waitForUsec) {
    return Status::TimedOut(
        fmt::format(
            "specified time is {}us in the future, but deadline expires in {}us",
            waitForUsec,
            usUntilDeadline));
  }
  return Status::OK();
}

} // anonymous namespace

// Left shifting 12 bits gives us 12 bits for the logical value
// and should still keep accurate microseconds time until 2100+
const int HybridClock::kBitsToShift = 12;
// This mask gives us back the logical bits.
const uint64_t HybridClock::kLogicalBitMask = (1 << kBitsToShift) - 1;

HybridClock::HybridClock() : next_timestamp_(0), state_(kNotInitialized) {}

Status HybridClock::init() {
  if (boost::iequals(FLAGS_time_source, "mock")) {
    time_service_.reset(new clock::MockNtp());
  } else if (boost::iequals(FLAGS_time_source, "system")) {
    time_service_.reset(new clock::SystemNtp());
  } else {
    return Status::InvalidArgument("invalid NTP source", FLAGS_time_source);
  }
  RETURN_NOT_OK(time_service_->init());

  state_ = kInitialized;

  return Status::OK();
}

Timestamp HybridClock::Now() {
  Timestamp now;
  uint64_t error;

  std::lock_guard<simple_spinlock> lock(lock_);
  nowWithError(&now, &error);
  return now;
}

Timestamp HybridClock::NowLatest() {
  Timestamp now;
  uint64_t error;

  {
    std::lock_guard<simple_spinlock> lock(lock_);
    nowWithError(&now, &error);
  }

  uint64_t nowLatest = getPhysicalValueMicros(now) + error;
  uint64_t nowLogical = getLogicalValue(now);

  return timestampFromMicrosecondsAndLogicalValue(nowLatest, nowLogical);
}

Status HybridClock::GetGlobalLatest(Timestamp* t) {
  Timestamp now = Now();
  uint64_t nowLatest =
      getPhysicalValueMicros(now) + FLAGS_kudu_max_clock_sync_error_usec;
  uint64_t nowLogical = getLogicalValue(now);
  *t = timestampFromMicrosecondsAndLogicalValue(nowLatest, nowLogical);
  return Status::OK();
}

void HybridClock::nowWithError(Timestamp* timestamp, uint64_t* maxErrorUsec) {
  DCHECK_EQ(state_, kInitialized)
      << "Clock not initialized. Must call init() first.";

  uint64_t nowUsec;
  uint64_t errorUsec;
  walltimeWithErrorOrDie(&nowUsec, &errorUsec);

  // If the physical time from the system clock is higher than our last-returned
  // time, we should use the physical timestamp.
  uint64_t candidatePhysTimestamp = nowUsec << kBitsToShift;
  if (PREDICT_TRUE(candidatePhysTimestamp > next_timestamp_)) {
    next_timestamp_ = candidatePhysTimestamp;
    *timestamp = Timestamp(next_timestamp_++);
    *maxErrorUsec = errorUsec;
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2)
          << "Current clock is higher than the last one. Resetting logical values."
          << " Physical Value: " << nowUsec
          << " usec Logical Value: 0  Error: " << errorUsec;
    }
    return;
  }

  // We don't have the last time read max error since it might have originated
  // in another machine, but we can put a bound on the maximum error of the
  // timestamp we are providing.
  // In particular we know that the "true" time falls within the interval
  // now_usec +- now.maxerror so we get the following situations:
  //
  // 1)
  // --------|----------|----|---------|--------------------------> time
  //     now - e       now  last   now + e
  // 2)
  // --------|----------|--------------|------|-------------------> time
  //     now - e       now         now + e   last
  //
  // Assuming, in the worst case, that the "true" time is now - error we need to
  // always return: last - (now - e) as the new maximum error.
  // This broadens the error interval for both cases but always returns
  // a correct error interval.

  *maxErrorUsec = (next_timestamp_ >> kBitsToShift) - (nowUsec - errorUsec);
  *timestamp = Timestamp(next_timestamp_++);
  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2)
        << "Current clock is lower than the last one. Returning last read and incrementing"
           " logical values. Clock: " +
            Stringify(*timestamp)
        << " Error: " << *maxErrorUsec;
  }
}

Status HybridClock::Update(const Timestamp& to_update) {
  std::lock_guard<simple_spinlock> lock(lock_);
  Timestamp now;
  uint64_t errorIgnored;
  nowWithError(&now, &errorIgnored);

  // If the incoming message is in the past relative to our current
  // physical clock, there's nothing to do.
  if (PREDICT_TRUE(now > to_update)) {
    return Status::OK();
  }

  uint64_t toUpdatePhysical = getPhysicalValueMicros(to_update);
  uint64_t nowPhysical = getPhysicalValueMicros(now);

  // we won't update our clock if to_update is more than
  // 'max_clock_sync_error_usec' into the future as it might have been corrupted
  // or originated from an out-of-sync server.
  if ((toUpdatePhysical - nowPhysical) > FLAGS_kudu_max_clock_sync_error_usec) {
    return Status::InvalidArgument(
        "Tried to update clock beyond the max. error.");
  }

  // Our next timestamp must be higher than the one that we are updating
  // from.
  next_timestamp_ = to_update.value() + 1;
  return Status::OK();
}

bool HybridClock::SupportsExternalConsistencyMode(
    ExternalConsistencyMode /* mode */) {
  return true;
}

bool HybridClock::HasPhysicalComponent() const {
  return true;
}

MonoDelta HybridClock::GetPhysicalComponentDifference(
    Timestamp lhs,
    Timestamp rhs) const {
  return MonoDelta::FromMicroseconds(
      static_cast<int64_t>(getPhysicalValueMicros(lhs)) -
      static_cast<int64_t>(getPhysicalValueMicros(rhs)));
}

Status HybridClock::WaitUntilAfter(
    const Timestamp& then,
    const MonoTime& deadline) {
  TRACE_EVENT0("clock", "HybridClock::WaitUntilAfter");
  Timestamp now;
  uint64_t error;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    nowWithError(&now, &error);
  }

  // "unshift" the timestamps so that we can measure actual time
  uint64_t nowUsec = getPhysicalValueMicros(now);
  uint64_t thenLatestUsec = getPhysicalValueMicros(then);

  uint64_t nowEarliestUsec = nowUsec - error;

  // Case 1, event happened definitely in the past, return
  if (PREDICT_TRUE(thenLatestUsec < nowEarliestUsec)) {
    return Status::OK();
  }

  // Case 2 wait out until we are sure that then has passed

  // We'll sleep thenLatestUsec - nowEarliestUsec so that the new
  // nw.earliest is higher than then.latest.
  uint64_t waitForUsec = (thenLatestUsec - nowEarliestUsec);

  // Additionally adjust the sleep time with the max tolerance adjustment
  // to account for the worst case clock skew while we're sleeping.
  waitForUsec *= (1 + (time_service_->skewPpm() / 1000000.0));

  // Check that sleeping wouldn't sleep longer than our deadline.
  RETURN_NOT_OK(checkDeadlineNotWithinMicros(deadline, waitForUsec));

  SleepFor(MonoDelta::FromMicroseconds(waitForUsec));

  VLOG(1) << "WaitUntilAfter(): Incoming time(latest): " << thenLatestUsec
          << " Now(earliest): " << nowEarliestUsec << " error: " << error
          << " Waiting for: " << waitForUsec;
  return Status::OK();
}

Status HybridClock::WaitUntilAfterLocally(
    const Timestamp& then,
    const MonoTime& deadline) {
  Timestamp now;
  uint64_t error;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    nowWithError(&now, &error);
  }
  if (now > then) {
    return Status::OK();
  }
  uint64_t waitForUsec =
      getPhysicalValueMicros(then) - getPhysicalValueMicros(now);

  // Check that sleeping wouldn't sleep longer than our deadline.
  RETURN_NOT_OK(checkDeadlineNotWithinMicros(deadline, waitForUsec));

  SleepFor(MonoDelta::FromMicroseconds(waitForUsec));

  return Status::OK();
}

bool HybridClock::IsAfter(Timestamp t) {
  // Manually get the time, rather than using Now(), so we don't end up causing
  // a time update.
  uint64_t nowUsec;
  uint64_t errorUsec;
  walltimeWithErrorOrDie(&nowUsec, &errorUsec);

  Timestamp now;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    now = Timestamp(std::max(next_timestamp_, nowUsec << kBitsToShift));
  }
  return t.value() < now.value();
}

void HybridClock::walltimeWithErrorOrDie(
    uint64_t* nowUsec,
    uint64_t* errorUsec) {
  Status s = walltimeWithError(nowUsec, errorUsec);
  if (PREDICT_FALSE(!s.ok())) {
    time_service_->dumpDiagnostics(/*log=*/nullptr);
    CHECK_OK_PREPEND(s, "unable to get current time with error bound");
  }
}

Status HybridClock::walltimeWithError(uint64_t* nowUsec, uint64_t* errorUsec) {
  bool isExtrapolated = false;
  auto readTimeBefore = MonoTime::Now();
  Status s = time_service_->walltimeWithError(nowUsec, errorUsec);
  auto readTimeAfter = MonoTime::Now();

  if (PREDICT_TRUE(s.ok())) {
    // We got a good clock read. Remember this in case the clock later becomes
    // unsynchronized and we need to extrapolate from here.
    //
    // Note that the actual act of reading the clock could have taken some time
    // (eg if we context-switched out) so we need to account for that by adding
    // some extra error.
    //
    //  A         B          C
    //  |---------|----------|
    //
    //  A = readTimeBefore (monotime)
    //  B = nowUsec (walltime reading)
    //  C = readTimeAfter (monotime)
    //
    // We don't know whether 'B' was halfway in between 'A' and 'C' or
    // elsewhere. The max likelihood estimate is that 'B' corresponds to the
    // average of 'A' and 'C'. Then we need to add in this uncertainty (half of
    // C - A) into any future clock readings that we extrapolate from this
    // estimate.
    int64_t readDurationUs = (readTimeAfter - readTimeBefore).ToMicroseconds();
    int64_t readTimeErrorUs = readDurationUs / 2;
    MonoTime readTimeMaxLikelihood =
        readTimeBefore + MonoDelta::FromMicroseconds(readTimeErrorUs);

    std::unique_lock<simple_spinlock> l(last_clock_read_lock_);
    if (!last_clock_read_time_.Initialized() ||
        last_clock_read_time_ < readTimeMaxLikelihood) {
      last_clock_read_time_ = readTimeMaxLikelihood;
      last_clock_read_physical_ = *nowUsec;
      last_clock_read_error_ = *errorUsec + readTimeErrorUs;
    }
  } else {
    // We failed to read the clock. Extrapolate the new time based on our
    // last successful read.
    std::unique_lock<simple_spinlock> l(last_clock_read_lock_);
    if (!last_clock_read_time_.Initialized()) {
      RETURN_NOT_OK_PREPEND(s, "could not read system time source");
    }
    MonoDelta timeSinceLastRead = readTimeAfter - last_clock_read_time_;
    int64_t microsSinceLastRead = timeSinceLastRead.ToMicroseconds();
    int64_t accumErrorUs =
        (microsSinceLastRead * time_service_->skewPpm()) / 1000000;
    *nowUsec = last_clock_read_physical_ + microsSinceLastRead;
    *errorUsec = last_clock_read_error_ + accumErrorUs;
    isExtrapolated = true;
    l.unlock();
    // Log after unlocking to minimize the lock hold time.
    KLOG_EVERY_N_SECS(ERROR, 1)
        << "Unable to read clock for last " << timeSinceLastRead.ToString()
        << ": " << s.ToString();
  }

  // If the clock is synchronized but has max_error beyond
  // max_clock_sync_error_usec we also return a non-ok status.
  if (*errorUsec > FLAGS_kudu_max_clock_sync_error_usec) {
    return Status::ServiceUnavailable(
        fmt::format(
            "clock error estimate ({}us) too high (clock considered {} by the kernel)",
            *errorUsec,
            isExtrapolated ? "unsynchronized" : "synchronized"));
  }
  return kudu::Status::OK();
}

// Used to get the timestamp for metrics.
uint64_t HybridClock::nowForMetrics() {
  return Now().toUint64();
}

// Used to get the current error, for metrics.
uint64_t HybridClock::errorForMetrics() {
  Timestamp now;
  uint64_t error;

  std::lock_guard<simple_spinlock> lock(lock_);
  nowWithError(&now, &error);
  return error;
}

void HybridClock::RegisterMetrics(
    const std::shared_ptr<MetricEntity>& metric_entity) {
  METRIC_hybrid_clock_timestamp
      .InstantiateFunctionGauge(
          metric_entity, Bind(&HybridClock::nowForMetrics, Unretained(this)))
      ->AutoDetachToLastValue(&metric_detacher_);
  METRIC_hybrid_clock_error
      .InstantiateFunctionGauge(
          metric_entity, Bind(&HybridClock::errorForMetrics, Unretained(this)))
      ->AutoDetachToLastValue(&metric_detacher_);
}

string HybridClock::Stringify(Timestamp timestamp) {
  return stringifyTimestamp(timestamp);
}

uint64_t HybridClock::getLogicalValue(const Timestamp& timestamp) {
  return timestamp.value() & kLogicalBitMask;
}

uint64_t HybridClock::getPhysicalValueMicros(const Timestamp& timestamp) {
  return timestamp.value() >> kBitsToShift;
}

Timestamp HybridClock::timestampFromMicroseconds(uint64_t micros) {
  return Timestamp(micros << kBitsToShift);
}

Timestamp HybridClock::timestampFromMicrosecondsAndLogicalValue(
    uint64_t micros,
    uint64_t logicalValue) {
  return Timestamp((micros << kBitsToShift) + logicalValue);
}

Timestamp HybridClock::addPhysicalTimeToTimestamp(
    const Timestamp& original,
    const MonoDelta& toAdd) {
  int64_t newPhysical = static_cast<int64_t>(getPhysicalValueMicros(original)) +
      toAdd.ToMicroseconds();
  int64_t oldLogical = getLogicalValue(original);
  return timestampFromMicrosecondsAndLogicalValue(newPhysical, oldLogical);
}

string HybridClock::stringifyTimestamp(const Timestamp& timestamp) {
  return fmt::format(
      "P: {} usec, L: {}",
      getPhysicalValueMicros(timestamp),
      getLogicalValue(timestamp));
}

} // namespace kudu::clock
