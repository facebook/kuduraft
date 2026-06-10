// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>

#include <gflags/gflags_declare.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

DECLARE_bool(use_folly_threadpool);

namespace kudu {

// ThreadPool takes a lot of arguments. We provide sane defaults with a builder.
//
// name: Used for debugging output and default names of the worker threads.
//    Since thread names are limited to 16 characters on Linux, it's good to
//    choose a short name here.
//    Required.
//
// traceMetricPrefix: used to prefix the names of TraceMetric counters.
//    When a task on a thread pool has an associated trace, the thread pool
//    implementation will increment TraceMetric counters to indicate the
//    amount of time spent waiting in the queue as well as the amount of wall
//    and CPU time spent executing. By default, these counters are prefixed
//    with the name of the thread pool. For example, if the pool is named
//    'apply', then counters such as 'apply.queue_time_us' will be
//    incremented.
//
//    The TraceMetrics implementation relies on the number of distinct counter
//    names being small. Thus, if the thread pool name itself is dynamically
//    generated, the default behavior described above would result in an
//    unbounded number of distinct counter names. The 'traceMetricPrefix'
//    setting can be used to override the prefix used in generating the trace
//    metric names.
//
//    For example, the Raft thread pools are named "<tablet id>-raft" which
//    has unbounded cardinality (a server may have thousands of different
//    tablet IDs over its lifetime). In that case, setting the prefix to
//    "raft" will avoid any issues.
//
// minThreads: Minimum number of threads we'll have at any time.
//    Default: 0.
//
// maxThreads: Maximum number of threads we'll have at any time.
//    Default: Number of CPUs detected on the system.
//
// maxQueueSize: Maximum number of items to enqueue before returning a
//    Status::ServiceUnavailable message from Submit().
//    Default: INT_MAX.
//
// idleTimeout: How long we'll keep around an idle thread before timing it out.
//    We always keep at least minThreads.
//    Default: 500 milliseconds.
//
// metrics: Histograms, counters, etc. to update on various threadpool events.
//    Default: not set.
//
class ThreadPoolBuilder {
 public:
  explicit ThreadPoolBuilder(std::string name);

  // Note: We violate the style guide by returning mutable references here
  // in order to provide traditional Builder pattern conveniences.
  ThreadPoolBuilder& setTraceMetricPrefix(const std::string& prefix);
  ThreadPoolBuilder& setMinThreads(int minThreads);
  ThreadPoolBuilder& setMaxThreads(int maxThreads);
  ThreadPoolBuilder& setMaxQueueSize(int maxQueueSize);
  ThreadPoolBuilder& setIdleTimeout(const MonoDelta& idleTimeout);
  ThreadPoolBuilder& setMetrics(ThreadPoolMetrics metrics);

  // Instantiate a new ThreadPool with the existing builder arguments.
  Status build(std::unique_ptr<ThreadPool>* pool) const;

  // Delete copy and move operations
  ThreadPoolBuilder(const ThreadPoolBuilder&) = delete;
  ThreadPoolBuilder& operator=(const ThreadPoolBuilder&) = delete;
  ThreadPoolBuilder(ThreadPoolBuilder&&) = delete;
  ThreadPoolBuilder& operator=(ThreadPoolBuilder&&) = delete;

 private:
  const std::string name_;
  std::string traceMetricPrefix_;
  int minThreads_;
  int maxThreads_;
  int maxQueueSize_;
  MonoDelta idleTimeout_;
  ThreadPoolMetrics metrics_;
};

} // namespace kudu
