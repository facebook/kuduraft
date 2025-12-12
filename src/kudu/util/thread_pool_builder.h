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

#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

// ThreadPool takes a lot of arguments. We provide sane defaults with a builder.
//
// name: Used for debugging output and default names of the worker threads.
//    Since thread names are limited to 16 characters on Linux, it's good to
//    choose a short name here.
//    Required.
//
// trace_metric_prefix: used to prefix the names of TraceMetric counters.
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
//    unbounded number of distinct counter names. The 'trace_metric_prefix'
//    setting can be used to override the prefix used in generating the trace
//    metric names.
//
//    For example, the Raft thread pools are named "<tablet id>-raft" which
//    has unbounded cardinality (a server may have thousands of different
//    tablet IDs over its lifetime). In that case, setting the prefix to
//    "raft" will avoid any issues.
//
// min_threads: Minimum number of threads we'll have at any time.
//    Default: 0.
//
// max_threads: Maximum number of threads we'll have at any time.
//    Default: Number of CPUs detected on the system.
//
// max_queue_size: Maximum number of items to enqueue before returning a
//    Status::ServiceUnavailable message from Submit().
//    Default: INT_MAX.
//
// idle_timeout: How long we'll keep around an idle thread before timing it out.
//    We always keep at least min_threads.
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
  ThreadPoolBuilder& set_trace_metric_prefix(const std::string& prefix);
  ThreadPoolBuilder& set_min_threads(int min_threads);
  ThreadPoolBuilder& set_max_threads(int max_threads);
  ThreadPoolBuilder& set_max_queue_size(int max_queue_size);
  ThreadPoolBuilder& set_idle_timeout(const MonoDelta& idle_timeout);
  ThreadPoolBuilder& set_metrics(ThreadPoolMetrics metrics);

  // Instantiate a new ThreadPool with the existing builder arguments.
  Status Build(std::unique_ptr<ThreadPool>* pool) const;

  // Delete copy and move operations
  ThreadPoolBuilder(const ThreadPoolBuilder&) = delete;
  ThreadPoolBuilder& operator=(const ThreadPoolBuilder&) = delete;
  ThreadPoolBuilder(ThreadPoolBuilder&&) = delete;
  ThreadPoolBuilder& operator=(ThreadPoolBuilder&&) = delete;

 private:
  const std::string name_;
  std::string trace_metric_prefix_;
  int min_threads_;
  int max_threads_;
  int max_queue_size_;
  MonoDelta idle_timeout_;
  ThreadPoolMetrics metrics_;
};

} // namespace kudu
