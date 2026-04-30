// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cmath>
#include <cstdint>
#include <mutex>
#include <random>
#include <vector>

#include "kudu/util/locks.h"

namespace kudu {

namespace random_internal {

static const uint32_t kM = 2147483647L; // 2^31-1

} // namespace random_internal

template <class R>
class StdUniformRng;

// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package. This implementation is not thread-safe.
class Random {
 private:
  uint32_t seed_;

 public:
  explicit Random(uint32_t s) {
    reset(s);
  }

  // Reset the RNG to the given seed value.
  void reset(uint32_t s) {
    seed_ = s & 0x7fffffffu;
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == random_internal::kM) {
      seed_ = 1;
    }
  }

  // next pseudo-random 32-bit unsigned integer.
  // FIXME: This currently only generates 31 bits of randomness.
  // The MSB will always be zero.
  uint32_t next() {
    static const uint64_t kA = 16807; // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * kA;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>(
        (product >> 31) + (product & random_internal::kM));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > random_internal::kM) {
      seed_ -= random_internal::kM;
    }
    return seed_;
  }

  // Alias for consistency with next64
  uint32_t next32() {
    return next();
  }

  // next pseudo-random 64-bit unsigned integer.
  uint64_t next64() {
    uint64_t large = next();
    large <<= 31;
    large |= next();
    // Fill in the highest two MSBs.
    large |= static_cast<uint64_t>(next32()) << 62;
    return large;
  }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t uniform(uint32_t n) {
    return next() % n;
  }

  // Returns a uniformly distributed 64-bit value in the range [0..n-1]
  // REQUIRES: n > 0
  uint64_t uniform64(uint64_t n) {
    return next64() % n;
  }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool oneIn(int n) {
    return (next() % n) == 0;
  }

  // Samples a random number from the given normal distribution.
  double normal(double mean, double stdDev);

  // Return a random number between 0.0 and 1.0 inclusive.
  double nextDoubleFraction() {
    return next() / static_cast<double>(random_internal::kM + 1.0);
  }

  // Sample 'k' random elements from the collection 'c' into 'result', taking
  // care not to sample any elements that are already present in 'avoid'.
  //
  // In the case that 'c' has fewer than 'k' elements then all elements in 'c'
  // will be selected.
  //
  // 'c' should be an iterable STL collection such as a vector, set, or list.
  // 'avoid' should be an STL-compatible set.
  //
  // The results are not stored in a randomized order: the order of results will
  // match their order in the input collection.
  template <class Collection, class Set, class T>
  void reservoirSample(
      const Collection& c,
      int k,
      const Set& avoid,
      std::vector<T>* result) {
    result->clear();
    result->reserve(k);
    int i = 0;
    for (const T& elem : c) {
      if (avoid.contains(elem)) {
        continue;
      }
      i++;
      // Fill the reservoir if there is available space.
      if (result->size() < k) {
        result->push_back(elem);
        continue;
      }
      // Otherwise replace existing elements with decreasing probability.
      int j = uniform(i);
      if (j < k) {
        (*result)[j] = elem;
      }
    }
  }
};

// Thread-safe wrapper around Random.
class ThreadSafeRandom {
 public:
  explicit ThreadSafeRandom(uint32_t s) : random_(s) {}

  uint32_t next32() {
    std::lock_guard<SimpleSpinlock> l(lock_);
    return random_.next32();
  }

  uint32_t uniform(uint32_t n) {
    std::lock_guard<SimpleSpinlock> l(lock_);
    return random_.uniform(n);
  }

  uint64_t uniform64(uint64_t n) {
    std::lock_guard<SimpleSpinlock> l(lock_);
    return random_.uniform64(n);
  }

  double normal(double mean, double stdDev) {
    std::lock_guard<SimpleSpinlock> l(lock_);
    return random_.normal(mean, stdDev);
  }

 private:
  SimpleSpinlock lock_;
  Random random_;
};

// Wraps either Random or ThreadSafeRandom as a C++ standard library
// compliant UniformRandomNumberGenerator:
//   http://en.cppreference.com/w/cpp/concept/UniformRandomNumberGenerator
template <class R>
class StdUniformRng {
 public:
  using result_type = uint32_t;

  explicit StdUniformRng(R* r) : r_(r) {}
  uint32_t operator()() {
    return r_->next32();
  }
  constexpr static uint32_t min() {
    return 0;
  }
  constexpr static uint32_t max() {
    return (1L << 31) - 1;
  }

 private:
  R* r_;
};

// Defined outside the class to make use of StdUniformRng above.
inline double Random::normal(double mean, double stdDev) {
  std::normal_distribution<> nd(mean, stdDev);
  StdUniformRng<Random> gen(this);
  return nd(gen);
}

} // namespace kudu
