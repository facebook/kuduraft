// Copyright 2002 and onwards Google Inc.
//
// A collection of useful (static) bit-twiddling functions.

#pragma once

#include <cstdint>
#include "kudu/gutil/macros.h"

namespace kudu {

class Bits {
 public:
  // Return floor(log2(n)) for positive integer n.  Returns -1 iff n == 0.
  static int log2Floor(uint32_t n);
  static int log2Floor64(uint64_t n);

  // Return ceiling(log2(n)) for positive integer n.  Returns -1 iff n == 0.
  static int log2Ceiling(uint32_t n);
  static int log2Ceiling64(uint64_t n);

  // Return the first set least / most significant bit, 0-indexed.  Returns an
  // undefined value if n == 0.  findLsbSetNonZero() is similar to ffs() except
  // that it's 0-indexed, while findMsbSetNonZero() is the same as
  // log2FloorNonZero().
  static int findLsbSetNonZero(uint32_t n);

 private:
  DISALLOW_COPY_AND_ASSIGN(Bits);
};

// ------------------------------------------------------------------------
// Implementation details follow
// ------------------------------------------------------------------------

// use GNU builtins where available
#if defined(__GNUC__) && \
    ((__GNUC__ == 3 && __GNUC_MINOR__ >= 4) || __GNUC__ >= 4)
inline int Bits::log2Floor(uint32_t n) {
  return n == 0 ? -1 : 31 ^ __builtin_clz(n);
}

inline int Bits::findLsbSetNonZero(uint32_t n) {
  return __builtin_ctz(n);
}

inline int Bits::log2Floor64(uint64_t n) {
  return n == 0 ? -1 : 63 ^ __builtin_clzll(n);
}
#elif defined(_MSC_VER)
#include "kudu/gutil/bits-internal-windows.h" // @manual
#else
#include "kudu/gutil/bits-internal-unknown.h" // @manual
#endif

} // namespace kudu
