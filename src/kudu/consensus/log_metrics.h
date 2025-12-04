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
#ifndef KUDU_CONSENSUS_LOG_METRICS_H
#define KUDU_CONSENSUS_LOG_METRICS_H

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu::log {

struct LogMetrics {
  explicit LogMetrics(const std::shared_ptr<MetricEntity>& metric_entity);

  // Global stats
  std::shared_ptr<Counter> bytes_logged;

  // Per-group group commit stats
  std::shared_ptr<Histogram> sync_latency;
  std::shared_ptr<Histogram> append_latency;
  std::shared_ptr<Histogram> group_commit_latency;
  std::shared_ptr<Histogram> roll_latency;
  std::shared_ptr<Histogram> entry_batches_per_group;
};

} // namespace kudu::log

#endif // KUDU_CONSENSUS_LOG_METRICS_H
