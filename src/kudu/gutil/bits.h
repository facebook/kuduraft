// Copyright 2002 and onwards Google Inc.
//
// A collection of useful (static) bit-twiddling functions.

#pragma once

#include <cstdint>
#include "kudu/gutil/macros.h"

namespace kudu {

class Bits {
 public:
  // Return the number of one bits in the given integer.
  static int countOnesInByte(unsigned char n);

  static int countOnes(uint32_t n) {
    n -= ((n >> 1) & 0x55555555);
    n = ((n >> 2) & 0x33333333) + (n & 0x33333333);
    return (((n + (n >> 4)) & 0xF0F0F0F) * 0x1010101) >> 24;
  }

  // Count bits using sideways addition [WWG'57]. See Knuth TAOCP v4 7.1.3(59)
  static inline int countOnes64(uint64_t n) {
#if defined(__x86_64__)
    n -= (n >> 1) & 0x5555555555555555ULL;
    n = ((n >> 2) & 0x3333333333333333ULL) + (n & 0x3333333333333333ULL);
    return (((n + (n >> 4)) & 0xF0F0F0F0F0F0F0FULL) * 0x101010101010101ULL) >>
        56;
#else
    return countOnes(n >> 32) + countOnes(n & 0xffffffff);
#endif
  }

  // Count bits using popcnt instruction (available on argo machines).
  // Doesn't check if the instruction exists.
  // Please use TestCPUFeature(POPCNT) from base/cpuid/cpuid.h before using
  // this.
  static inline int countOnes64withPopcount(uint64_t n) {
#if defined(__x86_64__) && defined __GNUC__
    int64_t count = 0;
    asm("popcnt %1,%0" : "=r"(count) : "rm"(n) : "cc");
    return count;
#else
    return countOnes64(n);
#endif
  }

  // Reverse the bits in the given integer.
  static uint8_t reverseBits8(uint8_t n);
  static uint32_t reverseBits32(uint32_t n);
  static uint64_t reverseBits64(uint64_t n);

  // Return the number of one bits in the byte sequence.
  static int count(const void* m, int numBytes);

  // Return the number of different bits in the given byte sequences.
  // (i.e., the Hamming distance)
  static int difference(const void* m1, const void* m2, int numBytes);

  // Return the number of different bits in the given byte sequences,
  // up to a maximum.  Values larger than the maximum may be returned
  // (because multiple bits are checked at a time), but the function
  // may exit early if the cap is exceeded.
  static int
  cappedDifference(const void* m1, const void* m2, int numBytes, int cap);

  // Return floor(log2(n)) for positive integer n.  Returns -1 iff n == 0.
  static int log2Floor(uint32_t n);
  static int log2Floor64(uint64_t n);

  // Potentially faster version of log2Floor() that returns an
  // undefined value if n == 0
  static int log2FloorNonZero(uint32_t n);
  static int log2FloorNonZero64(uint64_t n);

  // Return ceiling(log2(n)) for positive integer n.  Returns -1 iff n == 0.
  static int log2Ceiling(uint32_t n);
  static int log2Ceiling64(uint64_t n);

  // Return the first set least / most significant bit, 0-indexed.  Returns an
  // undefined value if n == 0.  findLsbSetNonZero() is similar to ffs() except
  // that it's 0-indexed, while findMsbSetNonZero() is the same as
  // log2FloorNonZero().
  static int findLsbSetNonZero(uint32_t n);
  static int findLsbSetNonZero64(uint64_t n);
  static int findMsbSetNonZero(uint32_t n) {
    return log2FloorNonZero(n);
  }
  static int findMsbSetNonZero64(uint64_t n) {
    return log2FloorNonZero64(n);
  }

  // Portable implementations
  static int log2FloorPortable(uint32_t n);
  static int log2FloorNonZeroPortable(uint32_t n);
  static int findLsbSetNonZeroPortable(uint32_t n);
  static int log2Floor64Portable(uint64_t n);
  static int log2FloorNonZero64Portable(uint64_t n);
  static int findLsbSetNonZero64Portable(uint64_t n);

  // Viewing bytes as a stream of unsigned bytes, does that stream
  // contain any byte equal to c?
  template <class T>
  static bool bytesContainByte(T bytes, uint8_t c);

  // Viewing bytes as a stream of unsigned bytes, does that stream
  // contain any byte b < c?
  template <class T>
  static bool bytesContainByteLessThan(T bytes, uint8_t c);

  // Viewing bytes as a stream of unsigned bytes, are all elements of that
  // stream in [lo, hi]?
  template <class T>
  static bool bytesAllInRange(T bytes, uint8_t lo, uint8_t hi);

 private:
  static const char numBits_[];
  static const unsigned char bitReverseTable_[];
  DISALLOW_COPY_AND_ASSIGN(Bits);
};

// A utility class for some handy bit patterns.  The names l and h
// were chosen to match Knuth Volume 4: l is 0x010101... and h is 0x808080...;
// half_ones is ones in the lower half only.  We assume sizeof(T) is 1 or even.
template <class T>
struct BitPattern {
  static const T kHalfOnes = (static_cast<T>(1) << (sizeof(T) * 4)) - 1;
  static const T kL =
      (sizeof(T) == 1) ? 1 : (kHalfOnes / 0xff * (kHalfOnes + 2));
  static const T kH = ~(kL * 0x7f);
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

inline int Bits::log2FloorNonZero(uint32_t n) {
  return 31 ^ __builtin_clz(n);
}

inline int Bits::findLsbSetNonZero(uint32_t n) {
  return __builtin_ctz(n);
}

inline int Bits::log2Floor64(uint64_t n) {
  return n == 0 ? -1 : 63 ^ __builtin_clzll(n);
}

inline int Bits::log2FloorNonZero64(uint64_t n) {
  return 63 ^ __builtin_clzll(n);
}

inline int Bits::findLsbSetNonZero64(uint64_t n) {
  return __builtin_ctzll(n);
}
#elif defined(_MSC_VER)
#include "kudu/gutil/bits-internal-windows.h" // @manual
#else
#include "kudu/gutil/bits-internal-unknown.h" // @manual
#endif

inline int Bits::countOnesInByte(unsigned char n) {
  return numBits_[n];
}

inline uint8_t Bits::reverseBits8(unsigned char n) {
  n = ((n >> 1) & 0x55) | ((n & 0x55) << 1);
  n = ((n >> 2) & 0x33) | ((n & 0x33) << 2);
  return ((n >> 4) & 0x0f) | ((n & 0x0f) << 4);
}

inline uint32_t Bits::reverseBits32(uint32_t n) {
  n = ((n >> 1) & 0x55555555) | ((n & 0x55555555) << 1);
  n = ((n >> 2) & 0x33333333) | ((n & 0x33333333) << 2);
  n = ((n >> 4) & 0x0F0F0F0F) | ((n & 0x0F0F0F0F) << 4);
  n = ((n >> 8) & 0x00FF00FF) | ((n & 0x00FF00FF) << 8);
  return (n >> 16) | (n << 16);
}

inline uint64_t Bits::reverseBits64(uint64_t n) {
#if defined(__x86_64__)
  n = ((n >> 1) & 0x5555555555555555ULL) | ((n & 0x5555555555555555ULL) << 1);
  n = ((n >> 2) & 0x3333333333333333ULL) | ((n & 0x3333333333333333ULL) << 2);
  n = ((n >> 4) & 0x0F0F0F0F0F0F0F0FULL) | ((n & 0x0F0F0F0F0F0F0F0FULL) << 4);
  n = ((n >> 8) & 0x00FF00FF00FF00FFULL) | ((n & 0x00FF00FF00FF00FFULL) << 8);
  n = ((n >> 16) & 0x0000FFFF0000FFFFULL) | ((n & 0x0000FFFF0000FFFFULL) << 16);
  return (n >> 32) | (n << 32);
#else
  return reverseBits32(n >> 32) |
      (static_cast<uint64_t>(reverseBits32(n & 0xffffffff)) << 32);
#endif
}

inline int Bits::log2FloorNonZeroPortable(uint32_t n) {
  // Just use the common routine
  return log2Floor(n);
}

// log2Floor64() is defined in terms of log2Floor32(), log2FloorNonZero32()
inline int Bits::log2Floor64Portable(uint64_t n) {
  const uint32_t topBits = static_cast<uint32_t>(n >> 32);
  if (topBits == 0) {
    // Top bits are zero, so scan in bottom bits
    return log2Floor(static_cast<uint32_t>(n));
  } else {
    return 32 + log2FloorNonZero(topBits);
  }
}

// log2FloorNonZero64() is defined in terms of log2FloorNonZero32()
inline int Bits::log2FloorNonZero64Portable(uint64_t n) {
  const uint32_t topBits = static_cast<uint32_t>(n >> 32);
  if (topBits == 0) {
    // Top bits are zero, so scan in bottom bits
    return log2FloorNonZero(static_cast<uint32_t>(n));
  } else {
    return 32 + log2FloorNonZero(topBits);
  }
}

// findLsbSetNonZero64() is defined in terms of findLsbSetNonZero()
inline int Bits::findLsbSetNonZero64Portable(uint64_t n) {
  const uint32_t bottomBits = static_cast<uint32_t>(n);
  if (bottomBits == 0) {
    // Bottom bits are zero, so scan in top bits
    return 32 + findLsbSetNonZero(static_cast<uint32_t>(n >> 32));
  } else {
    return findLsbSetNonZero(bottomBits);
  }
}

template <class T>
inline bool Bits::bytesContainByteLessThan(T bytes, uint8_t c) {
  T patternL = BitPattern<T>::kL;
  T patternH = BitPattern<T>::kH;
  // The c <= 0x80 code is straight out of Knuth Volume 4.
  // Usually c will be manifestly constant.
  return c <= 0x80
      ? ((patternH & (bytes - patternL * c) & ~bytes) != 0)
      : ((((bytes - patternL * c) | (bytes ^ patternH)) & patternH) != 0);
}

template <class T>
inline bool Bits::bytesContainByte(T bytes, uint8_t c) {
  // Usually c will be manifestly constant.
  return Bits::bytesContainByteLessThan<T>(bytes ^ (c * BitPattern<T>::kL), 1);
}

template <class T>
inline bool Bits::bytesAllInRange(T bytes, uint8_t lo, uint8_t hi) {
  T patternL = BitPattern<T>::kL;
  T patternH = BitPattern<T>::kH;
  // In the common case, lo and hi are manifest constants.
  if (lo > hi) {
    return false;
  }
  if (hi - lo < 128) {
    T x = bytes - patternL * lo;
    T y = bytes + patternL * (127 - hi);
    return ((x | y) & patternH) == 0;
  }
  return !Bits::bytesContainByteLessThan(
      bytes + (255 - hi) * patternL, lo + (255 - hi));
}

} // namespace kudu
