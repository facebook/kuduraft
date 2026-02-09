// Copyright 2011 Google Inc. All Rights Reserved.
//
// Hash functions for C++ builtin types. These are all of the fundamental
// integral and floating point types in the language as well as pointers. This
// library provides a minimal set of interfaces for hashing these values.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include "kudu/gutil/hash/jenkins_lookup2.h"
#include "kudu/gutil/macros.h"

inline uint32_t hash32NumWithSeed(uint32_t num, uint32_t c) {
  uint32_t b = 0x9e3779b9UL; // the golden ratio; an arbitrary value
  mix(num, b, c);
  return c;
}

inline uint64_t hash64NumWithSeed(uint64_t num, uint64_t c) {
  uint64_t b = 0xe08c1d668b756f82ULL; // more of the golden ratio
  mix(num, b, c);
  return c;
}

// This function hashes pointer sized items and returns a 32b hash,
// convenienty hiding the fact that pointers may be 32b or 64b,
// depending on the architecture.
inline uint32_t hash32PointerWithSeed(const void* p, uint32_t seed) {
  uintptr_t pvalue = reinterpret_cast<uintptr_t>(p);
  uint32_t h = seed;
  // Hash the pointer 32b at a time.
  for (size_t i = 0; i < sizeof(pvalue); i += 4) {
    h = hash32NumWithSeed(static_cast<uint32_t>(pvalue >> (i * 8)), h);
  }
  return h;
}

// ----------------------------------------------------------------------
// hash64FloatWithSeed
// hash64DoubleWithSeed
//   Functions for computing a hash value of floating-point numbers.
//   On systems where float and double comply with IEEE 754, these hashes
//   guarantee that if a == b, hash64FloatWithSeed(a, c) ==
//   hash64FloatWithSeed(b, c). Note that NaN does not compare equal to
//   itself, so two NaN inputs will not necessarily hash to the same value.
//
//   It is often a mistake to compare floating-point values for equality,
//   since floating-point computations do not produce exact values, due to
//   rounding. If equality comparison doesn't make sense in your situation,
//   hashing almost certainly doesn't make sense either.
//
//   Not guaranteed to return the same value in different builds, or to
//   avoid any reserved values.
// ----------------------------------------------------------------------
inline uint64_t hash64FloatWithSeed(float num, uint64_t seed) {
  // +0 and -0 are the only floating point numbers which compare equal but
  // have distinct bitwise representations in IEEE 754. To work around this,
  // we force 0 to be +0.
  if (num == 0) {
    num = 0;
  }
  KUDU_COMPILE_ASSERT(sizeof(float) == sizeof(uint32_t), float_has_wrong_size);

  const uint64_t kMul = 0xc6a4a7935bd1e995ULL;

  uint64_t a = (std::bit_cast<uint32_t>(num) + seed) * kMul;
  a ^= (a >> 47);
  a *= kMul;
  a ^= (a >> 47);
  a *= kMul;
  return a;
}

inline uint64_t hash64DoubleWithSeed(double num, uint64_t seed) {
  if (num == 0) {
    num = 0;
  }
  KUDU_COMPILE_ASSERT(
      sizeof(double) == sizeof(uint64_t), double_has_wrong_size);

  const uint64_t kMul = 0xc6a4a7935bd1e995ULL;

  uint64_t a = (std::bit_cast<uint64_t>(num) + seed) * kMul;
  a ^= (a >> 47);
  a *= kMul;
  a ^= (a >> 47);
  a *= kMul;
  return a;
}
