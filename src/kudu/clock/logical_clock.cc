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

#include "kudu/clock/logical_clock.h"

#include <ostream>
#include <string>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu::clock {

METRIC_DEFINE_gauge_uint64(
    server,
    logical_clock_timestamp,
    "Logical Clock Timestamp",
    kudu::MetricUnit::kUnits,
    "Logical clock timestamp.");

using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using base::subtle::NoBarrier_CompareAndSwap;
using base::subtle::NoBarrier_Load;

Timestamp LogicalClock::now() {
  return Timestamp(Barrier_AtomicIncrement(&now_, 1));
}

Timestamp LogicalClock::nowLatest() {
  return now();
}

Status LogicalClock::update(const Timestamp& toUpdate) {
  DCHECK_NE(toUpdate.value(), Timestamp::kInvalidTimestamp.value())
      << "Updating the clock with an invalid timestamp";
  Atomic64 newValue = toUpdate.value();

  while (true) {
    Atomic64 currentValue = NoBarrier_Load(&now_);
    // if the incoming value is less than the current one, or we've failed the
    // CAS because the current clock increased to higher than the incoming
    // value, we can stop the loop now.
    if (newValue <= currentValue) {
      return Status::OK();
    }
    // otherwise try a CAS
    if (PREDICT_TRUE(
            NoBarrier_CompareAndSwap(&now_, currentValue, newValue) ==
            currentValue)) {
      break;
    }
  }
  return Status::OK();
}

Status LogicalClock::waitUntilAfter(
    const Timestamp& /* then */,
    const MonoTime& /* deadline */) {
  return Status::ServiceUnavailable(
      "Logical clock does not support waitUntilAfter()");
}

Status LogicalClock::waitUntilAfterLocally(
    const Timestamp& then,
    const MonoTime& /* deadline */) {
  if (isAfter(then)) {
    return Status::OK();
  }
  return Status::ServiceUnavailable(
      "Logical clock does not support waitUntilAfterLocally()");
}

bool LogicalClock::isAfter(Timestamp t) {
  return base::subtle::Acquire_Load(&now_) >= t.value();
}

LogicalClock* LogicalClock::createStartingAt(const Timestamp& timestamp) {
  // initialize at 'timestamp' - 1 so that the  first output value is
  // 'timestamp'.
  return new LogicalClock(timestamp.value() - 1);
}

uint64_t LogicalClock::getCurrentTime() {
  // We don't want reading metrics to change the clock.
  return NoBarrier_Load(&now_);
}

void LogicalClock::registerMetrics(
    const std::shared_ptr<MetricEntity>& metricEntity) {
  METRIC_logical_clock_timestamp
      .InstantiateFunctionGauge(
          metricEntity, Bind(&LogicalClock::getCurrentTime, Unretained(this)))
      ->AutoDetachToLastValue(&metricDetacher_);
}

std::string LogicalClock::stringify(Timestamp timestamp) {
  return fmt::format("L: {}", timestamp.toUint64());
}

} // namespace kudu::clock
