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

#include <gtest/gtest.h>

#include "kudu/util/hdr_histogram.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

static const int kSigDigits = 2;

class HdrHistogramTest : public KuduTest {};

TEST_F(HdrHistogramTest, SimpleTest) {
  uint64_t highestVal = 10000LU;

  HdrHistogram hist(highestVal, kSigDigits);
  ASSERT_EQ(0, hist.CountInBucketForValue(1));
  hist.Increment(1);
  ASSERT_EQ(1, hist.CountInBucketForValue(1));
  hist.IncrementBy(1, 3);
  ASSERT_EQ(4, hist.CountInBucketForValue(1));
  hist.Increment(10);
  ASSERT_EQ(1, hist.CountInBucketForValue(10));
  hist.Increment(20);
  ASSERT_EQ(1, hist.CountInBucketForValue(20));
  ASSERT_EQ(0, hist.CountInBucketForValue(1000));
  hist.Increment(1000);
  hist.Increment(1001);
  ASSERT_EQ(2, hist.CountInBucketForValue(1000));

  ASSERT_EQ(1 + 1 * 3 + 10 + 20 + 1000 + 1001, hist.TotalSum());
}

TEST_F(HdrHistogramTest, TestCoordinatedOmission) {
  uint64_t interval = 1000;
  int loopIters = 100;
  int64_t normalValue = 10;
  HdrHistogram hist(1000000LU, kSigDigits);
  for (int i = 1; i <= loopIters; i++) {
    // Simulate a periodic "large value" that would exhibit coordinated
    // omission were this loop to sleep on 'interval'.
    int64_t value = (i % normalValue == 0) ? interval * 10 : normalValue;

    hist.IncrementWithExpectedInterval(value, interval);
  }
  ASSERT_EQ(
      loopIters - (loopIters / normalValue),
      hist.CountInBucketForValue(normalValue));
  for (int i = interval; i <= interval * 10; i += interval) {
    ASSERT_EQ(loopIters / normalValue, hist.CountInBucketForValue(i));
  }
}

static const int kExpectedSum =
    10 * 80 + 100 * 10 + 1000 * 5 + 10000 * 3 + 100000 * 1 + 1000000 * 1;
static const int kExpectedMax = 1000000;
static const int kExpectedCount = 100;
static const int kExpectedMin = 10;
static void loadPercentiles(HdrHistogram* hist) {
  hist->IncrementBy(10, 80);
  hist->IncrementBy(100, 10);
  hist->IncrementBy(1000, 5);
  hist->IncrementBy(10000, 3);
  hist->IncrementBy(100000, 1);
  hist->IncrementBy(1000000, 1);
}

static void validatePercentiles(HdrHistogram* hist, uint64_t specifiedMax) {
  double expectedMean =
      static_cast<double>(kExpectedSum) / (80 + 10 + 5 + 3 + 1 + 1);

  ASSERT_EQ(kExpectedMin, hist->MinValue());
  ASSERT_EQ(kExpectedMax, hist->MaxValue());
  ASSERT_EQ(kExpectedSum, hist->TotalSum());
  ASSERT_NEAR(expectedMean, hist->MeanValue(), 0.001);
  ASSERT_EQ(kExpectedCount, hist->TotalCount());
  ASSERT_EQ(10, hist->ValueAtPercentile(80));
  ASSERT_EQ(kExpectedCount, hist->ValueAtPercentile(90));
  ASSERT_EQ(
      hist->LowestEquivalentValue(specifiedMax), hist->ValueAtPercentile(99));
  ASSERT_EQ(
      hist->LowestEquivalentValue(specifiedMax),
      hist->ValueAtPercentile(99.99));
  ASSERT_EQ(
      hist->LowestEquivalentValue(specifiedMax), hist->ValueAtPercentile(100));
}

TEST_F(HdrHistogramTest, PercentileAndCopyTest) {
  uint64_t specifiedMax = 10000;
  HdrHistogram hist(specifiedMax, kSigDigits);
  loadPercentiles(&hist);
  NO_FATALS(validatePercentiles(&hist, specifiedMax));

  HdrHistogram copy(hist);
  NO_FATALS(validatePercentiles(&copy, specifiedMax));

  ASSERT_EQ(hist.TotalSum(), copy.TotalSum());
}

} // namespace kudu
