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

#include "kudu/util/test_graph.h"

#include <mutex>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>

#include "kudu/gutil/walltime.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::shared_ptr;
using std::string;

namespace kudu {

void TimeSeries::addValue(double val) {
  std::lock_guard<simple_spinlock> l(lock_);
  val_ += val;
}

void TimeSeries::setValue(double val) {
  std::lock_guard<simple_spinlock> l(lock_);
  val_ = val;
}

double TimeSeries::value() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return val_;
}

TimeSeriesCollector::~TimeSeriesCollector() {
  if (started_) {
    stopDumperThread();
  }
}

shared_ptr<TimeSeries> TimeSeriesCollector::getTimeSeries(const string& key) {
  MutexLock l(seriesLock_);
  SeriesMap::const_iterator it = seriesMap_.find(key);
  if (it == seriesMap_.end()) {
    shared_ptr<TimeSeries> ts(new TimeSeries());
    seriesMap_[key] = ts;
    return ts;
  } else {
    return (*it).second;
  }
}

void TimeSeriesCollector::startDumperThread() {
  LOG(INFO) << "Starting metrics dumper";
  CHECK(!started_);
  exitLatch_.Reset(1);
  started_ = true;
  CHECK_OK(
      kudu::Thread::Create(
          "time series",
          "dumper",
          &TimeSeriesCollector::dumperThread,
          this,
          &dumperThread_));
}

void TimeSeriesCollector::stopDumperThread() {
  CHECK(started_);
  exitLatch_.CountDown();
  CHECK_OK(ThreadJoiner(dumperThread_.get()).Join());
  started_ = false;
}

void TimeSeriesCollector::dumperThread() {
  CHECK(started_);
  WallTime startTime = wallTimeNow();

  faststring metricsStr;
  while (true) {
    metricsStr.clear();
    metricsStr.append("metrics: ");
    buildMetricsString(wallTimeNow() - startTime, &metricsStr);
    LOG(INFO) << metricsStr.ToString();

    // Sleep until next dump time, or return if we should exit
    if (exitLatch_.WaitFor(MonoDelta::FromMilliseconds(250))) {
      return;
    }
  }
}

void TimeSeriesCollector::buildMetricsString(
    WallTime timeSinceStart,
    faststring* dstBuf) const {
  MutexLock l(seriesLock_);

  dstBuf->append(
      fmt::format(
          "{{ \"scope\": \"{}\", \"time\": {:.3f}",
          scope_.c_str(),
          timeSinceStart));

  for (SeriesMap::const_reference entry : seriesMap_) {
    dstBuf->append(
        fmt::format(
            ", \"{}\": {:.3f}", entry.first.c_str(), entry.second->value()));
  }
  dstBuf->append("}");
}

} // namespace kudu
