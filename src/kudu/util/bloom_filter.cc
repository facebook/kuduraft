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

#include "kudu/util/bloom_filter.h"

#include <cmath>
#include <cstring>
#include <ostream>

#include <glog/logging.h>

namespace kudu {

static double kNaturalLog2 = 0.69314;

static int computeOptimalHashCount(size_t nBits, size_t elems) {
  int nHashes = nBits * kNaturalLog2 / elems;
  if (nHashes < 1) {
    nHashes = 1;
  }
  return nHashes;
}

BloomFilterSizing BloomFilterSizing::byCountAndFpRate(
    size_t expectedCount,
    double fpRate) {
  CHECK_GT(fpRate, 0);
  CHECK_LT(fpRate, 1);

  double nBits = -static_cast<double>(expectedCount) * log(fpRate) /
      kNaturalLog2 / kNaturalLog2;
  int nBytes = static_cast<int>(ceil(nBits / 8));
  CHECK_GT(nBytes, 0) << "expectedCount: " << expectedCount
                      << " fpRate: " << fpRate;
  return BloomFilterSizing(nBytes, expectedCount);
}

BloomFilterSizing BloomFilterSizing::bySizeAndFpRate(
    size_t nBytes,
    double fpRate) {
  size_t nBits = nBytes * 8;
  double expectedElems =
      -static_cast<double>(nBits) * kNaturalLog2 * kNaturalLog2 / log(fpRate);
  DCHECK_GT(expectedElems, 1);
  return BloomFilterSizing(nBytes, (size_t)ceil(expectedElems));
}

BloomFilterBuilder::BloomFilterBuilder(const BloomFilterSizing& sizing)
    : nBits_(sizing.nBytes() * 8),
      bitmap_(new uint8_t[sizing.nBytes()]),
      nHashes_(computeOptimalHashCount(nBits_, sizing.expectedCount())),
      expectedCount_(sizing.expectedCount()),
      nInserted_(0) {
  clear();
}

void BloomFilterBuilder::clear() {
  memset(&bitmap_[0], 0, nBytes());
  nInserted_ = 0;
}

double BloomFilterBuilder::falsePositiveRate() const {
  CHECK_NE(expectedCount_, 0)
      << "expectedCount_ not initialized: can't call this function on "
      << "a BloomFilter initialized from external data";

  return pow(
      1 - exp(-static_cast<double>(nHashes_) * expectedCount_ / nBits_),
      nHashes_);
}

BloomFilter::BloomFilter(const Slice& data, size_t nHashes)
    : nBits_(data.size() * 8),
      bitmap_(reinterpret_cast<const uint8_t*>(data.data())),
      nHashes_(nHashes) {}

} // namespace kudu
