// Copyright 2002 and onwards Google Inc.
//
// Derived from code by Moses Charikar

#include "kudu/gutil/bits.h"

#include <assert.h>

namespace kudu {

// this array gives the number of bits for any number from 0 to 255
// (We could make these ints.  The tradeoff is size (eg does it overwhelm
// the cache?) vs efficiency in referencing sub-word-sized array elements)
const char Bits::numBits_[] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4,
    2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

int Bits::count(const void* m, int numBytes) {
  int nbits = 0;
  const uint8_t* s = static_cast<const uint8_t*>(m);
  for (int i = 0; i < numBytes; i++) {
    nbits += numBits_[*s++];
  }
  return nbits;
}

int Bits::difference(const void* m1, const void* m2, int numBytes) {
  int nbits = 0;
  const uint8_t* s1 = static_cast<const uint8_t*>(m1);
  const uint8_t* s2 = static_cast<const uint8_t*>(m2);
  for (int i = 0; i < numBytes; i++) {
    nbits += numBits_[(*s1++) ^ (*s2++)];
  }
  return nbits;
}

int Bits::cappedDifference(
    const void* m1,
    const void* m2,
    int numBytes,
    int cap) {
  int nbits = 0;
  const uint8_t* s1 = static_cast<const uint8_t*>(m1);
  const uint8_t* s2 = static_cast<const uint8_t*>(m2);
  for (int i = 0; i < numBytes && nbits <= cap; i++) {
    nbits += numBits_[(*s1++) ^ (*s2++)];
  }
  return nbits;
}

int Bits::log2FloorPortable(uint32_t n) {
  if (n == 0) {
    return -1;
  }
  int log = 0;
  uint32_t value = n;
  for (int i = 4; i >= 0; --i) {
    int shift = (1 << i);
    uint32_t x = value >> shift;
    if (x != 0) {
      value = x;
      log += shift;
    }
  }
  assert(value == 1);
  return log;
}

int Bits::log2Ceiling(uint32_t n) {
  int floor = log2Floor(n);
  if (n == (n & ~(n - 1))) { // zero or a power of two
    return floor;
  } else {
    return floor + 1;
  }
}

int Bits::log2Ceiling64(uint64_t n) {
  int floor = log2Floor64(n);
  if (n == (n & ~(n - 1))) { // zero or a power of two
    return floor;
  } else {
    return floor + 1;
  }
}

int Bits::findLsbSetNonZeroPortable(uint32_t n) {
  int rc = 31;
  for (int i = 4, shift = 1 << 4; i >= 0; --i) {
    const uint32_t x = n << shift;
    if (x != 0) {
      n = x;
      rc -= shift;
    }
    shift >>= 1;
  }
  return rc;
}

} // namespace kudu
