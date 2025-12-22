// Copyright 2008 Google Inc. All Rights Reserved.
//
// Fast memory copying and comparison routines.
//   strings::fastmemcmpInlined() replaces memcmp()
//   strings::memcpyInlined() replaces memcpy()
//   strings::memeq(a, b, n) replaces memcmp(a, b, n) == 0
//
// strings::*_inlined() routines are inline versions of the
// routines exported by this module.  Sometimes using the inlined
// versions is faster.  Measure before using the inlined versions.
//
// Performance measurement:
//   strings::fastmemcmpInlined
//     Analysis: memcmp, fastmemcmpInlined, fastmemcmp
//     2012-01-30

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <cstdint>

#include "kudu/gutil/port.h"

namespace strings {

// Return true if the n bytes at a equal the n bytes at b.
// The regions are allowed to overlap.
//
// The performance is similar to the performance memcmp(), but faster for
// moderately-sized inputs, or inputs that share a common prefix and differ
// somewhere in their last 8 bytes. Further optimizations can be added later
// if it makes sense to do so.
inline bool memeq(const void* aV, const void* bV, size_t n) {
  const uint8_t* a = reinterpret_cast<const uint8_t*>(aV);
  const uint8_t* b = reinterpret_cast<const uint8_t*>(bV);

  size_t nRoundedDown = n & ~static_cast<size_t>(7);
  if (PREDICT_FALSE(nRoundedDown == 0)) { // n <= 7
    return memcmp(a, b, n) == 0;
  }
  // n >= 8
  uint64_t u = UNALIGNED_LOAD64(a) ^ UNALIGNED_LOAD64(b);
  uint64_t v = UNALIGNED_LOAD64(a + n - 8) ^ UNALIGNED_LOAD64(b + n - 8);
  if ((u | v) != 0) { // The first or last 8 bytes differ.
    return false;
  }
  a += 8;
  b += 8;
  n = nRoundedDown - 8;
  if (n > 128) {
    // As of 2012, memcmp on x86-64 uses a big unrolled loop with SSE2
    // instructions, and while we could try to do something faster, it
    // doesn't seem worth pursuing.
    return memcmp(a, b, n) == 0;
  }
  for (; n >= 16; n -= 16) {
    uint64_t x = UNALIGNED_LOAD64(a) ^ UNALIGNED_LOAD64(b);
    uint64_t y = UNALIGNED_LOAD64(a + 8) ^ UNALIGNED_LOAD64(b + 8);
    if ((x | y) != 0) {
      return false;
    }
    a += 16;
    b += 16;
  }
  // n must be 0 or 8 now because it was a multiple of 8 at the top of the loop.
  return n == 0 || UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b);
}

inline int fastmemcmpInlined(const void* aVoid, const void* bVoid, size_t n) {
  const uint8_t* a = reinterpret_cast<const uint8_t*>(aVoid);
  const uint8_t* b = reinterpret_cast<const uint8_t*>(bVoid);

  if (n >= 64) {
    return memcmp(a, b, n);
  }
  const void* aLimit = a + n;
  const size_t sizeofUint64 = sizeof(uint64_t); // NOLINT(runtime/sizeof)
  while (a + sizeofUint64 <= aLimit &&
         UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b)) {
    a += sizeofUint64;
    b += sizeofUint64;
  }
  const size_t sizeofUint32 = sizeof(uint32_t); // NOLINT(runtime/sizeof)
  if (a + sizeofUint32 <= aLimit &&
      UNALIGNED_LOAD32(a) == UNALIGNED_LOAD32(b)) {
    a += sizeofUint32;
    b += sizeofUint32;
  }
  while (a < aLimit) {
    int d = static_cast<int>(*a++) - static_cast<int>(*b++);
    if (d) {
      return d;
    }
  }
  return 0;
}

// The standard memcpy operation is slow for variable small sizes.
// This implementation inlines the optimal realization for sizes 1 to 16.
// To avoid code bloat don't use it in case of not performance-critical spots,
// nor when you don't expect very frequent values of size <= 16.
inline void memcpyInlined(void* dst, const void* src, size_t size) {
  // Compiler inlines code with minimal amount of data movement when third
  // parameter of memcpy is a constant.
  switch (size) {
    case 1:
      memcpy(dst, src, 1);
      break;
    case 2:
      memcpy(dst, src, 2);
      break;
    case 3:
      memcpy(dst, src, 3);
      break;
    case 4:
      memcpy(dst, src, 4);
      break;
    case 5:
      memcpy(dst, src, 5);
      break;
    case 6:
      memcpy(dst, src, 6);
      break;
    case 7:
      memcpy(dst, src, 7);
      break;
    case 8:
      memcpy(dst, src, 8);
      break;
    case 9:
      memcpy(dst, src, 9);
      break;
    case 10:
      memcpy(dst, src, 10);
      break;
    case 11:
      memcpy(dst, src, 11);
      break;
    case 12:
      memcpy(dst, src, 12);
      break;
    case 13:
      memcpy(dst, src, 13);
      break;
    case 14:
      memcpy(dst, src, 14);
      break;
    case 15:
      memcpy(dst, src, 15);
      break;
    case 16:
      memcpy(dst, src, 16);
      break;
    default:
      memcpy(dst, src, size);
      break;
  }
}

} // namespace strings
