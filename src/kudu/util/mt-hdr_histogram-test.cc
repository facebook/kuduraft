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
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/stl_util.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(
    histogram_test_num_threads,
    16,
    "Number of threads to spawn for mt-hdr_histogram test");
DEFINE_uint64(
    histogram_test_num_increments_per_thread,
    100000LU,
    "Number of times to call Increment() per thread in mt-hdr_histogram test");

using std::vector;

namespace kudu {

class MtHdrHistogramTest : public KuduTest {
 public:
  MtHdrHistogramTest() {
    numThreads_ = FLAGS_histogram_test_num_threads;
    numTimes_ = FLAGS_histogram_test_num_increments_per_thread;
  }

 protected:
  int numThreads_;
  uint64_t numTimes_;
};

// Increment a counter a bunch of times in the same bucket
static void
incrementSameHistValue(HdrHistogram* hist, uint64_t value, uint64_t times) {
  for (uint64_t i = 0; i < times; i++) {
    hist->Increment(value);
  }
}

TEST_F(MtHdrHistogramTest, ConcurrentWriteTest) {
  const uint64_t kValue = 1LU;

  HdrHistogram hist(100000LU, 3);

  auto threads = new std::shared_ptr<kudu::Thread>[numThreads_];
  for (int i = 0; i < numThreads_; i++) {
    CHECK_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("thread-{}", i),
            incrementSameHistValue,
            &hist,
            kValue,
            numTimes_,
            &threads[i]));
  }
  for (int i = 0; i < numThreads_; i++) {
    CHECK_OK(ThreadJoiner(threads[i].get()).Join());
  }

  HdrHistogram snapshot(hist);
  ASSERT_EQ(numThreads_ * numTimes_, snapshot.CountInBucketForValue(kValue));

  delete[] threads;
}

// Copy while writing, then iterate to ensure copies are consistent.
TEST_F(MtHdrHistogramTest, ConcurrentCopyWhileWritingTest) {
  const int kNumCopies = 10;
  const uint64_t kValue = 1;

  HdrHistogram hist(100000LU, 3);

  auto threads = new std::shared_ptr<kudu::Thread>[numThreads_];
  for (int i = 0; i < numThreads_; i++) {
    CHECK_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("thread-{}", i),
            incrementSameHistValue,
            &hist,
            kValue,
            numTimes_,
            &threads[i]));
  }

  // This is somewhat racy but the goal is to catch this issue at least
  // most of the time. At the time of this writing, before fixing a bug where
  // the total count stored in a copied histogram may not match its internal
  // counts (under concurrent writes), this test fails for me on 100/100 runs.
  vector<HdrHistogram*> snapshots;
  ElementDeleter deleter(&snapshots);
  for (int i = 0; i < kNumCopies; i++) {
    snapshots.push_back(new HdrHistogram(hist));
    SleepFor(MonoDelta::FromMicroseconds(100));
  }
  for (int i = 0; i < kNumCopies; i++) {
    snapshots[i]
        ->MeanValue(); // Will crash if underlying iterator is inconsistent.
  }

  for (int i = 0; i < numThreads_; i++) {
    CHECK_OK(ThreadJoiner(threads[i].get()).Join());
  }

  delete[] threads;
}

} // namespace kudu
