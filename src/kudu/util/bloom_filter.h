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
#ifndef KUDU_UTIL_BLOOM_FILTER_H
#define KUDU_UTIL_BLOOM_FILTER_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/slice.h"

namespace kudu {

// Probe calculated from a given key. This caches the calculated
// hash values which are necessary for probing into a Bloom Filter,
// so that when many bloom filters have to be consulted for a given
// key, we only need to calculate the hashes once.
//
// This is implemented based on the idea of double-hashing from the following
// paper:
//   "Less Hashing, Same Performance: Building a Better Bloom Filter"
//   Kirsch and Mitzenmacher, ESA 2006
//   https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
//
// Currently, the implementation uses the 64-bit City Hash.
// TODO: an SSE CRC32 hash is probably ~20% faster. Come back to this
// at some point.
class BloomKeyProbe {
 public:
  // Default constructor - this is only used to instantiate an object
  // and later reassign by assignment from another instance
  BloomKeyProbe() {}

  // Construct a probe from the given key.
  //
  // NOTE: proper operation requires that the referenced memory remain
  // valid for the lifetime of this object.
  explicit BloomKeyProbe(const Slice& key) : key_(key) {
    uint64_t h = util_hash::cityHash64(
        reinterpret_cast<const char*>(key.data()), key.size());

    // Use the top and bottom halves of the 64-bit hash
    // as the two independent hash functions for mixing.
    h1_ = static_cast<uint32_t>(h);
    h2_ = static_cast<uint32_t>(h >> 32);
  }

  const Slice& key() const {
    return key_;
  }

  // The initial hash value. See mixHash() for usage example.
  uint32_t initialHash() const {
    return h1_;
  }

  // Mix the given hash function with the second calculated hash
  // value. A sequence of independent hashes can be calculated
  // by repeatedly calling mixHash() on its previous result.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  uint32_t mixHash(uint32_t h) const {
    return h + h2_;
  }

 private:
  Slice key_;

  // The two hashes.
  uint32_t h1_;
  uint32_t h2_;
};

// Sizing parameters for the constructor to BloomFilterBuilder.
// This is simply to provide a nicer API than a bunch of overloaded
// constructors.
class BloomFilterSizing {
 public:
  // Size the bloom filter by a fixed size and false positive rate.
  //
  // Picks the number of entries to achieve the above.
  static BloomFilterSizing bySizeAndFpRate(size_t nBytes, double fpRate);

  // Size the bloom filer by an expected count and false positive rate.
  //
  // Picks the number of bytes to achieve the above.
  static BloomFilterSizing byCountAndFpRate(
      size_t expectedCount,
      double fpRate);

  size_t nBytes() const {
    return nBytes_;
  }
  size_t expectedCount() const {
    return expectedCount_;
  }

 private:
  BloomFilterSizing(size_t nBytes, size_t expectedCount)
      : nBytes_(nBytes), expectedCount_(expectedCount) {}

  size_t nBytes_;
  size_t expectedCount_;
};

// Builder for a BloomFilter structure.
class BloomFilterBuilder {
 public:
  // Create a bloom filter.
  // See BloomFilterSizing static methods to specify this argument.
  explicit BloomFilterBuilder(const BloomFilterSizing& sizing);

  // Clear all entries, reset insertion count.
  void clear();

  // Add the given key to the bloom filter.
  void addKey(const BloomKeyProbe& probe);

  // Return an estimate of the false positive rate.
  double falsePositiveRate() const;

  int nBytes() const {
    return nBits_ / 8;
  }

  int nBits() const {
    return nBits_;
  }

  // Return a slice view into this Bloom Filter, suitable for
  // writing out to a file.
  const Slice slice() const {
    return Slice(&bitmap_[0], nBytes());
  }

  // Return the number of hashes that are calculated for each entry
  // in the bloom filter.
  size_t nHashes() const {
    return nHashes_;
  }

  size_t expectedCount() const {
    return expectedCount_;
  }

  // Return the number of keys inserted.
  size_t count() const {
    return nInserted_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFilterBuilder);

  size_t nBits_;
  std::unique_ptr<uint8_t[]> bitmap_;

  // The number of hash functions to compute.
  size_t nHashes_;

  // The expected number of elements, for which the bloom is optimized.
  size_t expectedCount_;

  // The number of elements inserted so far since the last Reset.
  size_t nInserted_;
};

// Wrapper around a byte array for reading it as a bloom filter.
class BloomFilter {
 public:
  BloomFilter() : bitmap_(nullptr) {}
  BloomFilter(const Slice& data, size_t nHashes);

  // Return true if the filter may contain the given key.
  bool mayContainKey(const BloomKeyProbe& probe) const;

 private:
  friend class BloomFilterBuilder;
  static uint32_t pickBit(uint32_t hash, size_t nBits);

  size_t nBits_;
  const uint8_t* bitmap_;

  size_t nHashes_;
};

////////////////////////////////////////////////////////////
// Inline implementations
////////////////////////////////////////////////////////////

inline uint32_t BloomFilter::pickBit(uint32_t hash, size_t nBits) {
  switch (nBits) {
    // Fast path for the default bloom filter block size. Bitwise math
    // is much faster than division.
    case 4096 * 8:
      return hash & (nBits - 1);

    default:
      return hash % nBits;
  }
}

inline void BloomFilterBuilder::addKey(const BloomKeyProbe& probe) {
  uint32_t h = probe.initialHash();
  for (size_t i = 0; i < nHashes_; i++) {
    uint32_t bitPos = BloomFilter::pickBit(h, nBits_);
    bitmapSet(&bitmap_[0], bitPos);
    h = probe.mixHash(h);
  }
  nInserted_++;
}

inline bool BloomFilter::mayContainKey(const BloomKeyProbe& probe) const {
  uint32_t h = probe.initialHash();

  // Basic unrolling by 2s gives a small benefit here since the two bit
  // positions can be calculated in parallel -- it's a 50% chance that the first
  // will be set even if it's a bloom miss, in which case we can parallelize the
  // load.
  int remHashes = nHashes_;
  while (remHashes >= 2) {
    uint32_t bitPos1 = pickBit(h, nBits_);
    h = probe.mixHash(h);
    uint32_t bitPos2 = pickBit(h, nBits_);
    h = probe.mixHash(h);

    if (!bitmapTest(&bitmap_[0], bitPos1) ||
        !bitmapTest(&bitmap_[0], bitPos2)) {
      return false;
    }

    remHashes -= 2;
  }

  while (remHashes) {
    uint32_t bitPos = pickBit(h, nBits_);
    if (!bitmapTest(&bitmap_[0], bitPos)) {
      return false;
    }
    h = probe.mixHash(h);
    remHashes--;
  }
  return true;
}

} // namespace kudu

#endif
