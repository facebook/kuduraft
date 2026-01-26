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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/function.hpp> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(
    mt_metrics_test_num_threads,
    4,
    "Number of threads to spawn in mt metrics tests");

METRIC_DEFINE_entity(test_entity);

namespace kudu {

using debug::ScopedLeakCheckDisabler;
using std::string;
using std::vector;

class MultiThreadedMetricsTest : public KuduTest {
 public:
  static void registerCounters(
      const std::shared_ptr<MetricEntity>& metricEntity,
      const string& namePrefix,
      int numCounters);

  MetricRegistry registry_;
};

// Call increment on a Counter a bunch of times.
static void countWithCounter(
    std::shared_ptr<Counter> counter,
    int numIncrements) {
  for (int i = 0; i < numIncrements; i++) {
    counter->Increment();
  }
}

// Helper function that spawns and then joins a bunch of threads.
static void runWithManyThreads(boost::function<void()>* f, int numThreads) {
  vector<std::shared_ptr<kudu::Thread>> threads;
  for (int i = 0; i < numThreads; i++) {
    std::shared_ptr<kudu::Thread> newThread;
    CHECK_OK(
        kudu::Thread::Create(
            "test", fmt::format("thread{}", i), *f, &newThread));
    threads.push_back(newThread);
  }
  for (int i = 0; i < numThreads; i++) {
    ASSERT_OK(ThreadJoiner(threads[i].get()).Join());
  }
}

METRIC_DEFINE_counter(
    test_entity,
    test_counter,
    "Test Counter",
    MetricUnit::kRequests,
    "Test counter");

// Ensure that incrementing a counter is thread-safe.
TEST_F(MultiThreadedMetricsTest, CounterIncrementTest) {
  std::shared_ptr<Counter> counter(new Counter(&METRIC_test_counter));
  int numThreads = FLAGS_mt_metrics_test_num_threads;
  int numIncrements = 1000;
  boost::function<void()> f =
      boost::bind(countWithCounter, counter, numIncrements);
  runWithManyThreads(&f, numThreads);
  ASSERT_EQ(numThreads * numIncrements, counter->value());
}

// Helper function to register a bunch of counters in a loop.
void MultiThreadedMetricsTest::registerCounters(
    const std::shared_ptr<MetricEntity>& metricEntity,
    const string& namePrefix,
    int numCounters) {
  uint64_t tid = Env::Default()->gettid();
  for (int i = 0; i < numCounters; i++) {
    // This loop purposefully leaks metrics prototypes, because the metrics
    // system expects the prototypes and their names to live forever. This is
    // the only place we dynamically generate them for the purposes of a test,
    // so it's easier to just leak them than to figure out a way to manage
    // lifecycle of objects that are typically static.
    ScopedLeakCheckDisabler disabler;

    string name = fmt::format("{}-{}-{}", namePrefix, tid, i);
    auto proto = new CounterPrototype(
        MetricPrototype::CtorArgs(
            "test_entity",
            strdup(name.c_str()),
            "Test Counter",
            MetricUnit::kOperations,
            "test counter"));
    proto->Instantiate(metricEntity)->Increment();
  }
}

// Ensure that adding a counter to a registry is thread-safe.
TEST_F(MultiThreadedMetricsTest, AddCounterToRegistryTest) {
  std::shared_ptr<MetricEntity> entity =
      METRIC_ENTITY_test_entity.Instantiate(&registry_, "my-test");
  int numThreads = FLAGS_mt_metrics_test_num_threads;
  int numCounters = 1000;
  boost::function<void()> f =
      boost::bind(registerCounters, entity, "prefix", numCounters);
  runWithManyThreads(&f, numThreads);
  ASSERT_EQ(
      numThreads * numCounters, entity->UnsafeMetricsMapForTests().size());
}

} // namespace kudu
