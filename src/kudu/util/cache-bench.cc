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

#include <string.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/cache.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

DEFINE_int32(
    num_threads,
    16,
    "The number of threads to access the cache concurrently.");
DEFINE_int32(run_seconds, 1, "The number of seconds to run the benchmark");

using std::atomic;
using std::pair;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {

// Benchmark a 1GB cache.
static constexpr int kCacheCapacity = 1024 * 1024 * 1024;
// Use 4kb entries.
static constexpr int kEntrySize = 4 * 1024;

// Test parameterization.
struct BenchSetup {
  enum class Pattern {
    // Zipfian distribution -- a small number of items make up the
    // vast majority of lookups.
    Zipfian,
    // Every item is equally likely to be looked up.
    Uniform
  };
  Pattern pattern;

  // The ratio between the size of the dataset and the cache.
  //
  // A value smaller than 1 will ensure that the whole dataset fits
  // in the cache.
  double datasetCacheRatio;

  string toString() const {
    string ret;
    switch (pattern) {
      case Pattern::Zipfian:
        ret += "ZIPFIAN";
        break;
      case Pattern::Uniform:
        ret += "UNIFORM";
        break;
    }
    ret +=
        fmt::format(" ratio={:.2f}x n_unique={}", datasetCacheRatio, maxKey());
    return ret;
  }

  // Return the maximum cache key to be generated for a lookup.
  uint32_t maxKey() const {
    return static_cast<int64_t>(kCacheCapacity * datasetCacheRatio) /
        kEntrySize;
  }
};

class CacheBench : public KuduTest,
                   public testing::WithParamInterface<BenchSetup> {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    cache_.reset(NewLRUCache(DRAM_CACHE, kCacheCapacity, "test-cache"));
  }

  // Run queries against the cache until '*done' becomes true.
  // Returns a pair of the number of cache hits and lookups.
  pair<int64_t, int64_t> doQueries(const atomic<bool>* done) {
    const BenchSetup& setup = GetParam();
    Random r(getRandomSeed32());
    int64_t lookups = 0;
    int64_t hits = 0;
    while (!*done) {
      uint32_t intKey;
      if (setup.pattern == BenchSetup::Pattern::Zipfian) {
        intKey = r.Skewed(Bits::Log2Floor(setup.maxKey()));
      } else {
        intKey = r.Uniform(setup.maxKey());
      }
      char keyBuf[sizeof(intKey)];
      memcpy(keyBuf, &intKey, sizeof(intKey));
      Slice keySlice(keyBuf, arraysize(keyBuf));
      Cache::Handle* h = cache_->Lookup(keySlice, Cache::EXPECT_IN_CACHE);
      if (h) {
        hits++;
      } else {
        Cache::PendingHandle* ph = cache_->Allocate(
            keySlice, /* val_len=*/kEntrySize, /* charge=*/kEntrySize);
        h = cache_->Insert(ph, nullptr);
      }

      cache_->Release(h);
      lookups++;
    }
    return {hits, lookups};
  }

  // Starts the given number of threads to concurrently call doQueries.
  // Returns the aggregated number of cache hits and lookups.
  pair<int64_t, int64_t> runQueryThreads(int nThreads, int nSeconds) {
    vector<thread> threads(nThreads);
    atomic<bool> done(false);
    atomic<int64_t> totalLookups(0);
    atomic<int64_t> totalHits(0);
    for (int i = 0; i < nThreads; i++) {
      threads[i] = thread([&]() {
        pair<int64_t, int64_t> hitsLookups = doQueries(&done);
        totalHits += hitsLookups.first;
        totalLookups += hitsLookups.second;
      });
    }
    SleepFor(MonoDelta::FromSeconds(nSeconds));
    done = true;
    for (auto& t : threads) {
      t.join();
    }
    return {totalHits, totalLookups};
  }

 protected:
  unique_ptr<Cache> cache_;
};

// Test both distributions, and for each, test both the case where the data
// fits in the cache and where it is a bit larger.
INSTANTIATE_TEST_CASE_P(
    Patterns,
    CacheBench,
    testing::ValuesIn(
        std::vector<BenchSetup>{
            {BenchSetup::Pattern::Zipfian, 1.0},
            {BenchSetup::Pattern::Zipfian, 3.0},
            {BenchSetup::Pattern::Uniform, 1.0},
            {BenchSetup::Pattern::Uniform, 3.0}}));

TEST_P(CacheBench, RunBench) {
  const BenchSetup& setup = GetParam();

  // Run a short warmup phase to try to populate the cache. Otherwise even if
  // the dataset is smaller than the cache capacity, we would count a bunch of
  // misses during the warm-up phase.
  LOG(INFO) << "Warming up...";
  runQueryThreads(FLAGS_num_threads, 1);

  LOG(INFO) << "Running benchmark...";
  pair<int64_t, int64_t> hitsLookups =
      runQueryThreads(FLAGS_num_threads, FLAGS_run_seconds);
  int64_t hits = hitsLookups.first;
  int64_t lookups = hitsLookups.second;

  int64_t lPerSec = lookups / FLAGS_run_seconds;
  double hitRate = static_cast<double>(hits) / lookups;
  string testCase = setup.toString();
  LOG(INFO) << testCase << ": " << HumanReadableNum::toString(lPerSec)
            << " lookups/sec";
  LOG(INFO) << testCase << ": " << fmt::format("{:.1f}", hitRate * 100.0)
            << "% hit rate";
}

} // namespace kudu
