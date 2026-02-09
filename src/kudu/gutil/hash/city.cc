// Copyright 2010 Google Inc. All Rights Reserved.
// Authors: gpike@google.com (Geoff Pike), jyrki@google.com (Jyrki Alakuijala)
//
// This file provides cityHash64() and related functions.
//
// The externally visible functions follow the naming conventions of
// hash.h, where the size of the output is part of the name.  For
// example, cityHash64 returns a 64-bit hash.  The internal helpers do
// not have the return type in their name, but instead have names like
// HashLenXX or HashLenXXtoYY, where XX and YY are input string lengths.
//
// Most of the constants and tricks here were copied from murmur.cc or
// hash.h, or discovered by trial and error.  It's probably possible to further
// optimize the code here by writing a program that systematically explores
// more of the space of possible hash functions, or by using SIMD instructions.

#include "kudu/gutil/hash/city.h"

#include <sys/types.h>

#include <algorithm>
#include <iterator>
#include <utility>

#include <cstdint>

#include <glog/logging.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/hash/hash128to64.h"
#include "kudu/gutil/int128.h"
#include "kudu/gutil/port.h"

using std::make_pair;
using std::pair;

namespace util_hash {

using kudu::uint128;

// Some primes between 2^63 and 2^64 for various uses.
static const uint64_t k0 = 0xa5b85c5e198ed849ULL;
static const uint64_t k1 = 0x8d58ac26afe12e47ULL;
static const uint64_t k2 = 0xc47b6e9e3a970ed3ULL;
static const uint64_t k3 = 0xc70f6907e782aa0bULL;

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
static uint64_t rotate(uint64_t val, int shift) {
  DCHECK_GE(shift, 0);
  DCHECK_LE(shift, 63);
  // Avoid shifting by 64: doing so yields an undefined result.
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

// Equivalent to rotate(), but requires the second arg to be non-zero.
// On x86-64, and probably others, it's possible for this to compile
// to a single instruction if both args are already in registers.
static uint64_t rotateByAtLeast1(uint64_t val, int shift) {
  DCHECK_GE(shift, 1);
  DCHECK_LE(shift, 63);
  return (val >> shift) | (val << (64 - shift));
}

static uint64_t shiftMix(uint64_t val) {
  return val ^ (val >> 47);
}

static uint64_t hashLen16(uint64_t u, uint64_t v) {
  return hash128to64(uint128(u, v));
}

ATTRIBUTE_NO_SANITIZE_INTEGER
static uint64_t hashLen0To16(const char* s, size_t len) {
  DCHECK_GE(len, 0);
  DCHECK_LE(len, 16);
  if (len > 8) {
    uint64_t a = LittleEndian::Load64(s);
    uint64_t b = LittleEndian::Load64(s + len - 8);
    return hashLen16(a, rotateByAtLeast1(b + len, len)) ^ b;
  }
  if (len >= 4) {
    uint64_t a = LittleEndian::Load32(s);
    return hashLen16(len + (a << 3), LittleEndian::Load32(s + len - 4));
  }
  if (len > 0) {
    uint8_t a = s[0];
    uint8_t b = s[len >> 1];
    uint8_t c = s[len - 1];
    uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
    uint32_t z = len + (static_cast<uint32_t>(c) << 2);
    return shiftMix(y * k2 ^ z * k3) * k2;
  }
  return k2;
}

// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
ATTRIBUTE_NO_SANITIZE_INTEGER
static uint64_t HashLen17to32(const char* s, size_t len) {
  DCHECK_GE(len, 17);
  DCHECK_LE(len, 32);
  uint64_t a = LittleEndian::Load64(s) * k1;
  uint64_t b = LittleEndian::Load64(s + 8);
  uint64_t c = LittleEndian::Load64(s + len - 8) * k2;
  uint64_t d = LittleEndian::Load64(s + len - 16) * k0;
  return hashLen16(
      rotate(a - b, 43) + rotate(c, 30) + d, a + rotate(b ^ k3, 20) - c + len);
}

// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
// (For more, see the code review discussion of CL 18799087.)
ATTRIBUTE_NO_SANITIZE_INTEGER
static pair<uint64_t, uint64_t> WeakHashLen32WithSeeds(
    uint64_t w,
    uint64_t x,
    uint64_t y,
    uint64_t z,
    uint64_t a,
    uint64_t b) {
  a += w;
  b = rotate(b + a + z, 51);
  uint64_t c = a;
  a += x;
  a += y;
  b += rotate(a, 23);
  return make_pair(a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
static pair<uint64_t, uint64_t>
WeakHashLen32WithSeeds(const char* s, uint64_t a, uint64_t b) {
  return WeakHashLen32WithSeeds(
      LittleEndian::Load64(s),
      LittleEndian::Load64(s + 8),
      LittleEndian::Load64(s + 16),
      LittleEndian::Load64(s + 24),
      a,
      b);
}

// Return an 8-byte hash for 33 to 64 bytes.
ATTRIBUTE_NO_SANITIZE_INTEGER
static uint64_t HashLen33to64(const char* s, size_t len) {
  uint64_t z = LittleEndian::Load64(s + 24);
  uint64_t a =
      LittleEndian::Load64(s) + (len + LittleEndian::Load64(s + len - 16)) * k0;
  uint64_t b = rotate(a + z, 52);
  uint64_t c = rotate(a, 37);
  a += LittleEndian::Load64(s + 8);
  c += rotate(a, 7);
  a += LittleEndian::Load64(s + 16);
  uint64_t vf = a + z;
  uint64_t vs = b + rotate(a, 31) + c;
  a = LittleEndian::Load64(s + 16) + LittleEndian::Load64(s + len - 32);
  z += LittleEndian::Load64(s + len - 8);
  b = rotate(a + z, 52);
  c = rotate(a, 37);
  a += LittleEndian::Load64(s + len - 24);
  c += rotate(a, 7);
  a += LittleEndian::Load64(s + len - 16);
  uint64_t wf = a + z;
  uint64_t ws = b + rotate(a, 31) + c;
  uint64_t r = shiftMix((vf + ws) * k2 + (wf + vs) * k0);
  return shiftMix(r * k0 + vs) * k2;
}

ATTRIBUTE_NO_SANITIZE_INTEGER
uint64_t cityHash64(const char* s, size_t len) {
  if (len <= 32) {
    if (len <= 16) {
      return hashLen0To16(s, len);
    } else {
      return HashLen17to32(s, len);
    }
  } else if (len <= 64) {
    return HashLen33to64(s, len);
  }

  // For strings over 64 bytes we hash the end first, and then as we
  // loop we keep 56 bytes of state: v, w, x, y, and z.
  uint64_t x = LittleEndian::Load64(s + len - 40);
  uint64_t y =
      LittleEndian::Load64(s + len - 16) + LittleEndian::Load64(s + len - 56);
  uint64_t z = hashLen16(
      LittleEndian::Load64(s + len - 48) + len,
      LittleEndian::Load64(s + len - 24));
  pair<uint64_t, uint64_t> v = WeakHashLen32WithSeeds(s + len - 64, len, z);
  pair<uint64_t, uint64_t> w = WeakHashLen32WithSeeds(s + len - 32, y + k1, x);
  x = x * k1 + LittleEndian::Load64(s);

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
  len = (len - 1) & ~static_cast<size_t>(63);
  DCHECK_GT(len, 0);
  DCHECK_EQ(len, len / 64 * 64);
  do {
    x = rotate(x + y + v.first + LittleEndian::Load64(s + 8), 37) * k1;
    y = rotate(y + v.second + LittleEndian::Load64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + LittleEndian::Load64(s + 40);
    z = rotate(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(
        s + 32, z + w.second, y + LittleEndian::Load64(s + 16));
    std::swap(z, x);
    s += 64;
    len -= 64;
  } while (len != 0);
  return hashLen16(
      hashLen16(v.first, w.first) + shiftMix(y) * k1 + z,
      hashLen16(v.second, w.second) + x);
}

uint64_t cityHash64WithSeed(const char* s, size_t len, uint64_t seed) {
  return cityHash64WithSeeds(s, len, k2, seed);
}

uint64_t
cityHash64WithSeeds(const char* s, size_t len, uint64_t seed0, uint64_t seed1) {
  return hashLen16(cityHash64(s, len) - seed0, seed1);
}

// A subroutine for cityHash128().  Returns a decent 128-bit hash for strings
// of any length representable in ssize_t.  Based on City and Murmur128.
static uint128 cityMurmur(const char* s, size_t len, const uint128& seed) {
  uint64_t a = Uint128Low64(seed);
  uint64_t b = Uint128High64(seed);
  uint64_t c = 0;
  uint64_t d = 0;
  ssize_t l = len - 16;
  if (l <= 0) { // len <= 16
    c = b * k1 + hashLen0To16(s, len);
    d = rotate(a + (len >= 8 ? LittleEndian::Load64(s) : c), 32);
  } else { // len > 16
    c = hashLen16(LittleEndian::Load64(s + len - 8) + k1, a);
    d = hashLen16(b + len, c + LittleEndian::Load64(s + len - 16));
    a += d;
    do {
      a ^= shiftMix(LittleEndian::Load64(s) * k1) * k1;
      a *= k1;
      b ^= a;
      c ^= shiftMix(LittleEndian::Load64(s + 8) * k1) * k1;
      c *= k1;
      d ^= c;
      s += 16;
      l -= 16;
    } while (l > 0);
  }
  a = hashLen16(a, c);
  b = hashLen16(d, b);
  return uint128(a ^ b, hashLen16(b, a));
}

uint128 cityHash128WithSeed(const char* s, size_t len, const uint128& seed) {
  // TODO(user): As of February 2011, there's a beta of Murmur3 that would
  // most likely be useful here.  E.g., if (len < 900) return Murmur3(...)
  if (len < 128) {
    return cityMurmur(s, len, seed);
  }

  // We expect len >= 128 to be the common case.  Keep 56 bytes of state:
  // v, w, x, y, and z.
  pair<uint64_t, uint64_t> v, w;
  uint64_t x = Uint128Low64(seed);
  uint64_t y = Uint128High64(seed);
  uint64_t z = len * k1;
  v.first = rotate(y ^ k1, 49) * k1 + LittleEndian::Load64(s);
  v.second = rotate(v.first, 42) * k1 + LittleEndian::Load64(s + 8);
  w.first = rotate(y + z, 35) * k1 + x;
  w.second = rotate(x + LittleEndian::Load64(s + 88), 53) * k1;

  // This is similar to the inner loop of cityHash64(), manually unrolled.
  do {
    x = rotate(x + y + v.first + LittleEndian::Load64(s + 16), 37) * k1;
    y = rotate(y + v.second + LittleEndian::Load64(s + 48), 42) * k1;
    x ^= w.second;
    y ^= v.first;
    z = rotate(z ^ w.first, 33);
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y);
    std::swap(z, x);
    s += 64;
    x = rotate(x + y + v.first + LittleEndian::Load64(s + 16), 37) * k1;
    y = rotate(y + v.second + LittleEndian::Load64(s + 48), 42) * k1;
    x ^= w.second;
    y ^= v.first;
    z = rotate(z ^ w.first, 33);
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y);
    std::swap(z, x);
    s += 64;
    len -= 128;
  } while (PREDICT_TRUE(len >= 128));
  y += rotate(w.first, 37) * k0 + z;
  x += rotate(v.first + z, 49) * k0;
  // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
  for (size_t tail_done = 0; tail_done < len;) {
    tail_done += 32;
    y = rotate(y - x, 42) * k0 + v.second;
    w.first += LittleEndian::Load64(s + len - tail_done + 16);
    x = rotate(x, 49) * k0 + w.first;
    w.first += v.first;
    v = WeakHashLen32WithSeeds(s + len - tail_done, v.first, v.second);
  }
  // At this point our 48 bytes of state should contain more than
  // enough information for a strong 128-bit hash.  We use two
  // different 48-byte-to-8-byte hashes to get a 16-byte final result.
  x = hashLen16(x, v.first);
  y = hashLen16(y, w.first);
  return uint128(
      hashLen16(x + v.second, w.second) + y,
      hashLen16(x + w.second, y + v.second));
}

uint128 cityHash128(const char* s, size_t len) {
  if (len >= 16) {
    return cityHash128WithSeed(
        s + 16,
        len - 16,
        uint128(LittleEndian::Load64(s) ^ k3, LittleEndian::Load64(s + 8)));
  } else if (len >= 8) {
    return cityHash128WithSeed(
        nullptr,
        0,
        uint128(
            LittleEndian::Load64(s) ^ (len * k0),
            LittleEndian::Load64(s + len - 8) ^ k1));
  } else {
    return cityHash128WithSeed(s, len, uint128(k0, k1));
  }
}

} // namespace util_hash
