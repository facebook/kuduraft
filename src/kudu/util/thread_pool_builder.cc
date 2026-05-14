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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/sysinfo.h"
#include "kudu/util/folly_threadpool.h"
#include "kudu/util/kudu_threadpool.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

DEFINE_bool(
    use_folly_threadpool,
    true,
    "Use folly::CPUThreadPoolExecutor instead of the default KuduThreadPool.");

using std::string;
using std::unique_ptr;

namespace kudu {

ThreadPoolBuilder::ThreadPoolBuilder(string name)
    : name_(std::move(name)),
      minThreads_(0),
      maxThreads_(base::numCpus()),
      maxQueueSize_(std::numeric_limits<int>::max()),
      idleTimeout_(MonoDelta::FromMilliseconds(500)) {}

ThreadPoolBuilder& ThreadPoolBuilder::setTraceMetricPrefix(
    const string& prefix) {
  traceMetricPrefix_ = prefix;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::setMinThreads(int minThreads) {
  CHECK_GE(minThreads, 0);
  minThreads_ = minThreads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::setMaxThreads(int maxThreads) {
  CHECK_GT(maxThreads, 0);
  maxThreads_ = maxThreads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::setMaxQueueSize(int maxQueueSize) {
  maxQueueSize_ = maxQueueSize;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::setIdleTimeout(
    const MonoDelta& idleTimeout) {
  idleTimeout_ = idleTimeout;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::setMetrics(ThreadPoolMetrics metrics) {
  metrics_ = std::move(metrics);
  return *this;
}

Status ThreadPoolBuilder::build(unique_ptr<ThreadPool>* pool) const {
  std::unique_ptr<ThreadPool> threadPool;
  if (FLAGS_use_folly_threadpool) {
    threadPool = std::make_unique<FollyThreadPool>(name_, maxThreads_);
    LOG(INFO) << "Using folly::CPUThreadPoolExecutor for " << name_;
  } else {
    LOG(INFO) << "Using KuduThreadPool for " << name_;
    auto kuduPool = std::make_unique<KuduThreadPool>(
        name_,
        minThreads_,
        maxThreads_,
        maxQueueSize_,
        idleTimeout_,
        traceMetricPrefix_,
        metrics_);
    RETURN_NOT_OK(kuduPool->init());
    threadPool = std::move(kuduPool);
  }
  *pool = std::move(threadPool);
  return Status::OK();
}

} // namespace kudu
