// Copyright 2002 and onwards Google Inc.
//
// Derived from code by Moses Charikar

#include "kudu/gutil/bits.h"

namespace kudu {

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

} // namespace kudu
