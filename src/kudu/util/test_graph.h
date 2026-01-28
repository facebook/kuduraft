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
#ifndef KUDU_TEST_GRAPH_COLLECTOR_H
#define KUDU_TEST_GRAPH_COLLECTOR_H

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/mutex.h"

namespace kudu {

class Thread;
class faststring;

class TimeSeries {
 public:
  void addValue(double val);
  void setValue(double val);

  double value() const;

 private:
  friend class TimeSeriesCollector;

  DISALLOW_COPY_AND_ASSIGN(TimeSeries);

  TimeSeries() : val_(0) {}

  mutable simple_spinlock lock_;
  double val_;
};

class TimeSeriesCollector {
 public:
  explicit TimeSeriesCollector(std::string scope)
      : scope_(std::move(scope)), exitLatch_(0), started_(false) {}

  ~TimeSeriesCollector();

  std::shared_ptr<TimeSeries> getTimeSeries(const std::string& key);
  void startDumperThread();
  void stopDumperThread();

 private:
  DISALLOW_COPY_AND_ASSIGN(TimeSeriesCollector);

  void dumperThread();
  void buildMetricsString(WallTime timeSinceStart, faststring* dstBuf) const;

  std::string scope_;

  using SeriesMap =
      std::unordered_map<std::string, std::shared_ptr<TimeSeries>>;
  SeriesMap seriesMap_;
  mutable Mutex seriesLock_;

  std::shared_ptr<kudu::Thread> dumperThread_;

  // Latch used to stop the dumperThread_. When the thread is started,
  // this is set to 1, and when the thread should exit, it is counted down.
  CountDownLatch exitLatch_;

  bool started_;
};

} // namespace kudu
#endif
