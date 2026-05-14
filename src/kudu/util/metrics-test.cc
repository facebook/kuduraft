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
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unordered_set;
using std::vector;

DECLARE_int32(metrics_retirement_age_ms);

namespace kudu {

METRIC_DEFINE_entity(test_entity);

class MetricsTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    entity_ = METRIC_ENTITY_test_entity.instantiate(&registry_, "my-test");
  }

 protected:
  MetricRegistry registry_;
  std::shared_ptr<MetricEntity> entity_;
};

METRIC_DEFINE_counter(
    test_entity,
    test_counter,
    "My Test Counter",
    MetricUnit::kRequests,
    "Description of test counter");

TEST_F(MetricsTest, SimpleCounterTest) {
  std::shared_ptr<Counter> requests =
      std::shared_ptr<Counter>(new Counter(&METRIC_test_counter));
  ASSERT_EQ(
      "Description of test counter", requests->prototype()->description());
  ASSERT_EQ(0, requests->value());
  requests->increment();
  ASSERT_EQ(1, requests->value());
  requests->incrementBy(2);
  ASSERT_EQ(3, requests->value());
}

METRIC_DEFINE_gauge_uint64(
    test_entity,
    test_gauge,
    "Test uint64 Gauge",
    MetricUnit::kBytes,
    "Description of Test Gauge");

TEST_F(MetricsTest, SimpleAtomicGaugeTest) {
  std::shared_ptr<AtomicGauge<uint64_t>> memUsage =
      METRIC_test_gauge.instantiate(entity_, 0);
  ASSERT_EQ(
      METRIC_test_gauge.description(), memUsage->prototype()->description());
  ASSERT_EQ(0, memUsage->value());
  memUsage->incrementBy(7);
  ASSERT_EQ(7, memUsage->value());
  memUsage->setValue(5);
  ASSERT_EQ(5, memUsage->value());
}

METRIC_DEFINE_gauge_int64(
    test_entity,
    test_func_gauge,
    "Test Function Gauge",
    MetricUnit::kBytes,
    "Test Gauge 2");

static int64_t myFunction(int* metricVal) {
  return (*metricVal)++;
}

TEST_F(MetricsTest, SimpleFunctionGaugeTest) {
  int metricVal = 1000;
  std::shared_ptr<FunctionGauge<int64_t>> gauge =
      METRIC_test_func_gauge.instantiateFunctionGauge(
          entity_, Bind(&myFunction, Unretained(&metricVal)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());

  gauge->detachToCurrentValue();
  // After detaching, it should continue to return the same constant value.
  ASSERT_EQ(1002, gauge->value());
  ASSERT_EQ(1002, gauge->value());

  // Test resetting to a constant.
  gauge->detachToConstant(2);
  ASSERT_EQ(2, gauge->value());
}

TEST_F(MetricsTest, AutoDetachToLastValue) {
  int metricVal = 1000;
  std::shared_ptr<FunctionGauge<int64_t>> gauge =
      METRIC_test_func_gauge.instantiateFunctionGauge(
          entity_, Bind(&myFunction, Unretained(&metricVal)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());
  {
    FunctionGaugeDetacher detacher;
    gauge->autoDetachToLastValue(&detacher);
    ASSERT_EQ(1002, gauge->value());
    ASSERT_EQ(1003, gauge->value());
  }

  ASSERT_EQ(1004, gauge->value());
  ASSERT_EQ(1004, gauge->value());
}

TEST_F(MetricsTest, AutoDetachToConstant) {
  int metricVal = 1000;
  std::shared_ptr<FunctionGauge<int64_t>> gauge =
      METRIC_test_func_gauge.instantiateFunctionGauge(
          entity_, Bind(&myFunction, Unretained(&metricVal)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());
  {
    FunctionGaugeDetacher detacher;
    gauge->autoDetach(&detacher, 12345);
    ASSERT_EQ(1002, gauge->value());
    ASSERT_EQ(1003, gauge->value());
  }

  ASSERT_EQ(12345, gauge->value());
}

METRIC_DEFINE_gauge_uint64(
    test_entity,
    counter_as_gauge,
    "Gauge exposed as Counter",
    MetricUnit::kBytes,
    "Gauge exposed as Counter",
    kExposeAsCounter);
TEST_F(MetricsTest, TestExposeGaugeAsCounter) {
  ASSERT_EQ(MetricType::kCounter, METRIC_counter_as_gauge.type());
}

METRIC_DEFINE_histogram(
    test_entity,
    test_hist,
    "Test Histogram",
    MetricUnit::kMilliseconds,
    "foo",
    1000000,
    3);

TEST_F(MetricsTest, SimpleHistogramTest) {
  std::shared_ptr<Histogram> hist = METRIC_test_hist.instantiate(entity_);
  hist->increment(2);
  hist->incrementBy(4, 1);
  ASSERT_EQ(2, hist->histogram_->minValue());
  ASSERT_EQ(3, hist->histogram_->meanValue());
  ASSERT_EQ(4, hist->histogram_->maxValue());
  ASSERT_EQ(2, hist->histogram_->totalCount());
  ASSERT_EQ(6, hist->histogram_->totalSum());
  // TODO: Test coverage needs to be improved a lot.
}

TEST_F(MetricsTest, JsonPrintTest) {
  std::shared_ptr<Counter> testCounter =
      METRIC_test_counter.instantiate(entity_);
  testCounter->increment();
  entity_->setAttribute("test_attr", "attr_val");

  // Generate the JSON.
  std::ostringstream out;
  JsonWriter writer(&out, JsonWriter::kPretty);
  ASSERT_OK(entity_->writeAsJson(&writer, {"*"}, MetricJsonOptions()));

  // Now parse it back out.
  JsonReader reader(out.str());
  ASSERT_OK(reader.init());

  vector<const rapidjson::Value*> metrics;
  ASSERT_OK(reader.extractObjectArray(reader.root(), "metrics", &metrics));
  ASSERT_EQ(1, metrics.size());
  string metricName;
  ASSERT_OK(reader.extractString(metrics[0], "name", &metricName));
  ASSERT_EQ("test_counter", metricName);
  int64_t metricValue;
  ASSERT_OK(reader.extractInt64(metrics[0], "value", &metricValue));
  ASSERT_EQ(1L, metricValue);

  const rapidjson::Value* attributes;
  ASSERT_OK(reader.extractObject(reader.root(), "attributes", &attributes));
  string attrValue;
  ASSERT_OK(reader.extractString(attributes, "test_attr", &attrValue));
  ASSERT_EQ("attr_val", attrValue);

  // Verify that metric filtering matches on substrings.
  out.str("");
  ASSERT_OK(entity_->writeAsJson(&writer, {"test count"}, MetricJsonOptions()));
  ASSERT_STR_CONTAINS(METRIC_test_counter.name(), out.str());

  // Verify that, if we filter for a metric that isn't in this entity, we get no
  // result.
  out.str("");
  ASSERT_OK(entity_->writeAsJson(
      &writer, {"not_a_matching_metric"}, MetricJsonOptions()));
  ASSERT_EQ("", out.str());

  // Verify that filtering is case-insensitive.
  out.str("");
  ASSERT_OK(
      entity_->writeAsJson(&writer, {"mY teST coUNteR"}, MetricJsonOptions()));
  ASSERT_STR_CONTAINS(METRIC_test_counter.name(), out.str());
}

// Test that metrics are retired when they are no longer referenced.
TEST_F(MetricsTest, RetirementTest) {
  FLAGS_metrics_retirement_age_ms = 100;

  const string kMetricName = "foo";
  std::shared_ptr<Counter> counter = METRIC_test_counter.instantiate(entity_);
  ASSERT_EQ(1, entity_->unsafeMetricsMapForTests().size());

  // Since we hold a reference to the counter, it should not get retired.
  entity_->retireOldMetrics();
  ASSERT_EQ(1, entity_->unsafeMetricsMapForTests().size());

  // When we de-ref it, it should not get immediately retired, either, because
  // we keep retirable metrics around for some amount of time. We try retiring
  // a number of times to hit all the cases.
  counter = nullptr;
  for (int i = 0; i < 3; i++) {
    entity_->retireOldMetrics();
    ASSERT_EQ(1, entity_->unsafeMetricsMapForTests().size());
  }

  // If we wait for longer than the retirement time, and call retire again,
  // we'll actually retire it.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_metrics_retirement_age_ms * 1.5));
  entity_->retireOldMetrics();
  ASSERT_EQ(0, entity_->unsafeMetricsMapForTests().size());
}

TEST_F(MetricsTest, TestRetiringEntities) {
  ASSERT_EQ(1, registry_.numEntities());

  // Drop the reference to our entity.
  entity_.reset();

  // Retire metrics. Since there is nothing inside our entity, it should
  // retire immediately (no need to loop).
  registry_.retireOldMetrics();

  ASSERT_EQ(0, registry_.numEntities());
}

// Test that we can mark a metric to never be retired.
TEST_F(MetricsTest, NeverRetireTest) {
  entity_->neverRetire(METRIC_test_hist.instantiate(entity_));
  FLAGS_metrics_retirement_age_ms = 0;

  for (int i = 0; i < 3; i++) {
    entity_->retireOldMetrics();
    ASSERT_EQ(1, entity_->unsafeMetricsMapForTests().size());
  }
}

TEST_F(MetricsTest, TestInstantiatingTwice) {
  // Test that re-instantiating the same entity ID returns the same object.
  std::shared_ptr<MetricEntity> newEntity =
      METRIC_ENTITY_test_entity.instantiate(&registry_, entity_->id());
  ASSERT_EQ(newEntity.get(), entity_.get());
}

TEST_F(MetricsTest, TestInstantiatingDifferentEntities) {
  std::shared_ptr<MetricEntity> newEntity =
      METRIC_ENTITY_test_entity.instantiate(&registry_, "some other ID");
  ASSERT_NE(newEntity.get(), entity_.get());
}

TEST_F(MetricsTest, TestDumpJsonPrototypes) {
  // Dump the prototype info.
  std::ostringstream out;
  JsonWriter w(&out, JsonWriter::kPretty);
  MetricPrototypeRegistry::get()->writeAsJson(&w);
  string json = out.str();

  // Quick sanity check for one of our metrics defined in this file.
  const char* expected =
      "        {\n"
      "            \"name\": \"test_func_gauge\",\n"
      "            \"label\": \"Test Function Gauge\",\n"
      "            \"type\": \"gauge\",\n"
      "            \"unit\": \"bytes\",\n"
      "            \"description\": \"Test Gauge 2\",\n"
      "            \"entity_type\": \"test_entity\"\n"
      "        }";
  ASSERT_STR_CONTAINS(json, expected);

  // Parse it.
  rapidjson::Document d;
  d.Parse<0>(json.c_str());

  // Ensure that we got a reasonable number of metrics.
  int numMetrics = d["metrics"].Size();
  int numEntities = d["entities"].Size();
  LOG(INFO) << "Parsed " << numMetrics << " metrics and " << numEntities
            << " entities";
  ASSERT_GT(numMetrics, 5);
  ASSERT_EQ(numEntities, 2);

  // Spot-check that some metrics were properly registered and that the JSON was
  // properly formed.
  unordered_set<string> seenMetrics;
  for (int i = 0; i < d["metrics"].Size(); i++) {
    auto [it, inserted] =
        seenMetrics.insert(d["metrics"][i]["name"].GetString());
    CHECK(inserted);
  }
  ASSERT_TRUE(seenMetrics.contains("threads_started"));
  ASSERT_TRUE(seenMetrics.contains("test_hist"));
}

TEST_F(MetricsTest, TestDumpOnlyChanged) {
  auto getJson = [&](int64_t sinceEpoch) {
    MetricJsonOptions opts;
    opts.onlyModifiedInOrAfterEpoch = sinceEpoch;
    std::ostringstream out;
    JsonWriter writer(&out, JsonWriter::kCompact);
    CHECK_OK(entity_->writeAsJson(&writer, {"*"}, opts));
    return out.str();
  };

  std::shared_ptr<Counter> testCounter =
      METRIC_test_counter.instantiate(entity_);

  int64_t epochWhenModified = Metric::currentEpoch();
  testCounter->increment();

  // If we pass a "since dirty" epoch from before we incremented it, we should
  // see the metric.
  for (int i = 0; i < 2; i++) {
    ASSERT_STR_CONTAINS(
        getJson(epochWhenModified), "{\"name\":\"test_counter\",\"value\":1}");
    Metric::incrementEpoch();
  }

  // If we pass a current epoch, we should see that the metric was not modified.
  int64_t newEpoch = Metric::currentEpoch();
  ASSERT_STR_NOT_CONTAINS(getJson(newEpoch), "test_counter");
  // ... until we modify it again.
  testCounter->increment();
  ASSERT_STR_CONTAINS(
      getJson(newEpoch), "{\"name\":\"test_counter\",\"value\":2}");
}

// Test that 'includeUntouchedMetrics=false' prevents dumping counters and
// histograms which have never been incremented.
TEST_F(MetricsTest, TestDontDumpUntouched) {
  // Instantiate a bunch of metrics.
  int metricVal = 1000;
  std::shared_ptr<Counter> testCounter =
      METRIC_test_counter.instantiate(entity_);
  std::shared_ptr<Histogram> hist = METRIC_test_hist.instantiate(entity_);
  std::shared_ptr<FunctionGauge<int64_t>> functionGauge =
      METRIC_test_func_gauge.instantiateFunctionGauge(
          entity_, Bind(&myFunction, Unretained(&metricVal)));
  std::shared_ptr<AtomicGauge<uint64_t>> atomicGauge =
      METRIC_test_gauge.instantiate(entity_, 0);

  MetricJsonOptions opts;
  opts.includeUntouchedMetrics = false;
  std::ostringstream out;
  JsonWriter writer(&out, JsonWriter::kCompact);
  CHECK_OK(entity_->writeAsJson(&writer, {"*"}, opts));
  // Untouched counters and histograms should not be included.
  ASSERT_STR_NOT_CONTAINS(out.str(), "test_counter");
  ASSERT_STR_NOT_CONTAINS(out.str(), "test_hist");
  // Untouched gauges need to be included, because we don't actually
  // track whether they have been touched.
  ASSERT_STR_CONTAINS(out.str(), "test_func_gauge");
  ASSERT_STR_CONTAINS(out.str(), "test_gauge");
}

} // namespace kudu
