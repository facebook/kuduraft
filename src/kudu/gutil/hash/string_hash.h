// Copyright 2011 Google Inc. All Rights Reserved.
//
// These are the core hashing routines which operate on strings. We define
// strings loosely as a sequence of bytes, and these routines are designed to
// work with the most fundamental representations of a string of bytes.
//
// These routines provide "good" hash functions in terms of both quality and
// speed. Their values can and will change as their implementations change and
// evolve.

#pragma once

#include <stddef.h>
#include <cstdint>

#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/hash/jenkins.h"
#include "kudu/gutil/hash/jenkins_lookup2.h"
#include "kudu/gutil/port.h"

namespace hash_internal {

// We have some special cases for 64-bit hardware and x86-64 in particular.
// Instead of sprinkling ifdefs through the file, we have one ugly ifdef here.
// Later code can then use "if" instead of "ifdef".
#if defined(__x86_64__)
enum { kX8664 = true, kSixtyFourBit = true };
#elif defined(_LP64)
enum { kX8664 = false, kSixtyFourBit = true };
#else
enum { kX8664 = false, kSixtyFourBit = false };
#endif

// Arbitrary mix constants (pi).
static const uint32_t kMix32 = 0x12b9b0a1UL;
static const uint64_t kMix64 = 0x2b992ddfa23249d6ULL;

} // namespace hash_internal

inline size_t
hashStringThoroughlyWithSeed(const char* s, size_t len, size_t seed) {
  if (hash_internal::kX8664) {
    return static_cast<size_t>(util_hash::CityHash64WithSeed(s, len, seed));
  }

  if (hash_internal::kSixtyFourBit) {
    return Hash64StringWithSeed(s, static_cast<uint32_t>(len), seed);
  }

  return static_cast<size_t>(Hash32StringWithSeed(
      s, static_cast<uint32_t>(len), static_cast<uint32_t>(seed)));
}

inline size_t hashStringThoroughly(const char* s, size_t len) {
  if (hash_internal::kX8664) {
    return static_cast<size_t>(util_hash::CityHash64(s, len));
  }

  if (hash_internal::kSixtyFourBit) {
    return Hash64StringWithSeed(
        s, static_cast<uint32_t>(len), hash_internal::kMix64);
  }

  return static_cast<size_t>(Hash32StringWithSeed(
      s, static_cast<uint32_t>(len), hash_internal::kMix32));
}

inline size_t hashStringThoroughlyWithSeeds(
    const char* s,
    size_t len,
    size_t seed0,
    size_t seed1) {
  if (hash_internal::kX8664) {
    return util_hash::CityHash64WithSeeds(s, len, seed0, seed1);
  }

  if (hash_internal::kSixtyFourBit) {
    uint64_t a = seed0;
    uint64_t b = seed1;
    uint64_t c = hashStringThoroughly(s, len);
    mix(a, b, c);
    return c;
  }

  uint32_t a = static_cast<uint32_t>(seed0);
  uint32_t b = static_cast<uint32_t>(seed1);
  uint32_t c = static_cast<uint32_t>(hashStringThoroughly(s, len));
  mix(a, b, c);
  return c;
}
