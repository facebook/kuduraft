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

inline size_t hashStringThoroughly(const char* s, size_t len) {
  if (hash_internal::kX8664) {
    return static_cast<size_t>(util_hash::cityHash64(s, len));
  }

  if (hash_internal::kSixtyFourBit) {
    return hash64StringWithSeed(
        s, static_cast<uint32_t>(len), hash_internal::kMix64);
  }

  return static_cast<size_t>(hash32StringWithSeed(
      s, static_cast<uint32_t>(len), hash_internal::kMix32));
}
