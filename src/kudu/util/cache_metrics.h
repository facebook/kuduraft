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
#ifndef KUDU_UTIL_CACHE_METRICS_H
#define KUDU_UTIL_CACHE_METRICS_H

#include <cstdint>

#include "kudu/util/metrics.h"

namespace kudu {

struct CacheMetrics {
  explicit CacheMetrics(const std::shared_ptr<MetricEntity>& metricEntity);

  std::shared_ptr<Counter> inserts;
  std::shared_ptr<Counter> lookups;
  std::shared_ptr<Counter> evictions;
  std::shared_ptr<Counter> cacheHits;
  std::shared_ptr<Counter> cacheHitsCaching;
  std::shared_ptr<Counter> cacheMisses;
  std::shared_ptr<Counter> cacheMissesCaching;

  std::shared_ptr<AtomicGauge<uint64_t>> cacheUsage;
};

} // namespace kudu
#endif /* KUDU_UTIL_CACHE_METRICS_H */
