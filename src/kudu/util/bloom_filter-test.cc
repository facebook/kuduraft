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
#include <cstdlib>
#include <ostream>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/bloom_filter.h"
#include "kudu/util/slice.h"

namespace kudu {

static const int kRandomSeed = 0xdeadbeef;

static void addRandomKeys(int randomSeed, int nKeys, BloomFilterBuilder* bf) {
  srandom(randomSeed);
  for (int i = 0; i < nKeys; i++) {
    uint64_t key = random();
    Slice keySlice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
    BloomKeyProbe probe(keySlice);
    bf->AddKey(probe);
  }
}

static void checkRandomKeys(int randomSeed, int nKeys, const BloomFilter& bf) {
  srandom(randomSeed);
  for (int i = 0; i < nKeys; i++) {
    uint64_t key = random();
    Slice keySlice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
    BloomKeyProbe probe(keySlice);
    ASSERT_TRUE(bf.MayContainKey(probe));
  }
}

TEST(TestBloomFilter, TestInsertAndProbe) {
  int nKeys = 2000;
  BloomFilterBuilder bfb(BloomFilterSizing::ByCountAndFPRate(nKeys, 0.01));

  // Check that the desired false positive rate is achieved.
  double expectedFpRate = bfb.false_positive_rate();
  ASSERT_NEAR(expectedFpRate, 0.01, 0.002);

  // 1% FP rate should need about 9 bits per key
  ASSERT_EQ(9, bfb.n_bits() / nKeys);

  // Enter nKeys random keys into the bloom filter
  addRandomKeys(kRandomSeed, nKeys, &bfb);

  // Verify that the keys we inserted all return true when queried.
  BloomFilter bf(bfb.slice(), bfb.n_hashes());
  checkRandomKeys(kRandomSeed, nKeys, bf);

  // Query a bunch of other keys, and verify the false positive rate
  // is within reasonable bounds.
  uint32_t numQueries = 100000;
  uint32_t numPositives = 0;
  for (int i = 0; i < numQueries; i++) {
    uint64_t key = random();
    Slice keySlice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
    BloomKeyProbe probe(keySlice);
    if (bf.MayContainKey(probe)) {
      numPositives++;
    }
  }

  double fpRate =
      static_cast<double>(numPositives) / static_cast<double>(numQueries);
  LOG(INFO) << "FP rate: " << fpRate << " (" << numPositives << "/"
            << numQueries << ")";
  LOG(INFO) << "Expected FP rate: " << expectedFpRate;

  // Actual FP rate should be within 20% of the estimated FP rate
  ASSERT_NEAR(fpRate, expectedFpRate, 0.20 * expectedFpRate);
}

} // namespace kudu
