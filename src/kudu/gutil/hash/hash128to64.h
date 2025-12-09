// Copyright 2010 Google Inc. All Rights Reserved.
// Authors: jyrki@google.com (Jyrki Alakuijala), gpike@google.com (Geoff Pike)

#pragma once

#include <cstdint>

#include "kudu/gutil/int128.h"
#include "kudu/gutil/port.h"

// Hash 128 input bits down to 64 bits of output.
// This is intended to be a reasonably good hash function.
// It may change from time to time.
ATTRIBUTE_NO_SANITIZE_INTEGER
inline uint64_t Hash128to64(const kudu::uint128& x) {
  // Murmur-inspired hashing.
  const uint64_t kMul = 0xc6a4a7935bd1e995ULL;
  uint64_t a = (Uint128Low64(x) ^ Uint128High64(x)) * kMul;
  a ^= (a >> 47);
  uint64_t b = (Uint128High64(x) ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}
