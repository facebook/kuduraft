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

#include "kudu/util/thread_pool_builder.h"

#include <limits>
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/sysinfo.h"
#include "kudu/util/kudu_threadpool.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;

namespace kudu {

ThreadPoolBuilder::ThreadPoolBuilder(string name)
    : name_(std::move(name)),
      min_threads_(0),
      max_threads_(base::numCpus()),
      max_queue_size_(std::numeric_limits<int>::max()),
      idle_timeout_(MonoDelta::FromMilliseconds(500)) {}

ThreadPoolBuilder& ThreadPoolBuilder::set_trace_metric_prefix(
    const string& prefix) {
  trace_metric_prefix_ = prefix;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_min_threads(int min_threads) {
  CHECK_GE(min_threads, 0);
  min_threads_ = min_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_threads(int max_threads) {
  CHECK_GT(max_threads, 0);
  max_threads_ = max_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_queue_size(int max_queue_size) {
  max_queue_size_ = max_queue_size;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_idle_timeout(
    const MonoDelta& idle_timeout) {
  idle_timeout_ = idle_timeout;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_metrics(ThreadPoolMetrics metrics) {
  metrics_ = std::move(metrics);
  return *this;
}

Status ThreadPoolBuilder::Build(unique_ptr<ThreadPool>* pool) const {
  std::unique_ptr kuduPool = std::unique_ptr<KuduThreadPool>(new KuduThreadPool(
      name_,
      min_threads_,
      max_threads_,
      max_queue_size_,
      idle_timeout_,
      trace_metric_prefix_,
      metrics_));
  KuduThreadPool& poolRef = *kuduPool;
  *pool = std::move(kuduPool);
  RETURN_NOT_OK(poolRef.Init());
  return Status::OK();
}

} // namespace kudu
