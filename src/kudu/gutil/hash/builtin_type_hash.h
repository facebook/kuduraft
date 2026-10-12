// Copyright 2011 Google Inc. All Rights Reserved.
//
// Hash functions for C++ builtin types. These are all of the fundamental
// integral and floating point types in the language as well as pointers. This
// library provides a minimal set of interfaces for hashing these values.

#pragma once

#include <cstdint>
#include "kudu/gutil/hash/jenkins_lookup2.h"

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
