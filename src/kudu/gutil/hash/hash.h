//
// Copyright (C) 1999 and onwards Google, Inc.
//
//
// This file contains routines for hashing and fingerprinting.
//
// A hash function takes an arbitrary input bitstring (string, char*,
// number) and turns it into a hash value (a fixed-size number) such
// that unequal input values have a high likelihood of generating
// unequal hash values.  A fingerprint is a hash whose design is
// biased towards avoiding hash collisions, possibly at the expense of
// other characteristics such as execution speed.
//
// In general, if you are only using the hash values inside a single
// executable -- you're not writing the values to disk, and you don't
// depend on another instance of your program, running on another
// machine, generating the same hash values as you -- you want to use
// a HASH.  Otherwise, you want to use a FINGERPRINT.
//
// RECOMMENDED HASH FOR STRINGS:    GoodFastHash
//
// It is a functor, so you can use it like this:
//     hash_map<string, xxx, GoodFastHash<string> >
//     hash_set<char *, GoodFastHash<char*> >
//
// RECOMMENDED HASH FOR NUMBERS:    hash<>
//
// Note that this is likely the identity hash, so if your
// numbers are "non-random" (especially in the low bits), another
// choice is better.  You can use it like this:
//     hash_map<int, xxx>
//     hash_set<uint64>
//
// RECOMMENDED HASH FOR POINTERS:    hash<>
//
// This is also likely the identity hash.
//
// RECOMMENDED HASH FOR STRUCTS:    hash<some_fingerprint(struct)>
//
// Take a fingerprint of the struct, and use that as the key.
// For instance: const uint64 hash_data[] = { s.foo, bit_cast<uint64>(s.bar) };
//    uint64 fprint = <fingerprint fn>(reinterpret_cast<const char*>(hash_data),
//                                     sizeof(hash_data));
//    hash_map[fprint] = whatever;
//
// RECOMMENDED FINGERPRINT:         Fingerprint2011
//
// (In util/hash/fingerprint2011.h)
// In particular, do *not* use Fingerprint in new code; it has
// problems with excess collisions.
//
// OTHER HASHES AND FINGERPRINTS:
//
//
// The wiki page also has good advice for when to use a fingerprint vs
// a hash.
//
//
// Note: if your file declares hash_map<string, ...> or
// hash_set<string>, it will use the default hash function,
// hash<string>.  This is not a great choice.  Always provide an
// explicit functor, such as GoodFastHash, as a template argument.
// (Either way, you will need to #include this file to get the
// necessary definition.)
//
// Some of the hash functions below are documented to be fixed
// forever; the rest (whether they're documented as so or not) may
// change over time.  If you require a hash function that does not
// change over time, you should have unittests enforcing this
// property.  We already have several such functions; see
// hash_unittest.cc for the details and unittests.

#pragma once

#include <cstddef>
#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>

#include <cstdint>

#include "kudu/gutil/hash/builtin_type_hash.h"
#include "kudu/gutil/hash/hash128to64.h"
#include "kudu/gutil/hash/jenkins.h"
#include "kudu/gutil/hash/jenkins_lookup2.h"
#include "kudu/gutil/hash/legacy_hash.h"
#include "kudu/gutil/hash/string_hash.h"
#include "kudu/gutil/int128.h"

// ----------------------------------------------------------------------
// Fingerprint()
//   Not recommended for new code.  Instead, use Fingerprint2011(),
//   a higher-quality and faster hash function.  See fingerprint2011.h.
//
//   Fingerprinting a string (or char*) will never return 0 or 1,
//   in case you want a couple of special values.  However,
//   fingerprinting a numeric type may produce 0 or 1.
//
//   The hash mapping of Fingerprint() will never change.
//
//   Note: AVOID USING FINGERPRINT if at all possible.  Use
//   Fingerprint2011 (in fingerprint2011.h) instead.
//   Fingerprint() is susceptible to collisions for even short
//   strings with low edit distance; see
//   Example collisions:
//     "01056/02" vs. "11057/02"
//     "LTA 02" vs. "MTA 12"
//   The same study found only one collision each for CityHash64() and
//   MurmurHash64(), from more than 2^32 inputs, and on medium-length
//   strings with large edit distances.These issues, among others,
//   led to the recommendation that new code should avoid Fingerprint().
// ----------------------------------------------------------------------
extern uint64_t FingerprintReferenceImplementation(const char* s, uint32_t len);
extern uint64_t FingerprintInterleavedImplementation(
    const char* s,
    uint32_t len);
inline uint64_t Fingerprint(const char* s, uint32_t len) {
  if (sizeof(s) == 8) { // 64-bit systems have 8-byte pointers.
    // The better choice when we have a decent number of registers.
    return FingerprintInterleavedImplementation(s, len);
  } else {
    return FingerprintReferenceImplementation(s, len);
  }
}

// Routine that combines together the hi/lo part of a fingerprint
// and changes the result appropriately to avoid returning 0/1.
inline uint64_t CombineFingerprintHalves(uint32_t hi, uint32_t lo) {
  uint64_t result =
      (static_cast<uint64_t>(hi) << 32) | static_cast<uint64_t>(lo);
  if ((hi == 0) && (lo < 2)) {
    result ^= 0x130f9bef94a0a928ULL;
  }
  return result;
}

inline uint64_t Fingerprint(const std::string& s) {
  return Fingerprint(s.data(), static_cast<uint32_t>(s.size()));
}
inline uint64_t hash64StringWithSeed(const std::string& s, uint64_t c) {
  return hash64StringWithSeed(s.data(), static_cast<uint32_t>(s.size()), c);
}
inline uint64_t Fingerprint(int8_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(char c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(uint16_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(int16_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(uint32_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(int32_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(uint64_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}
inline uint64_t Fingerprint(int64_t c) {
  return hash64NumWithSeed(static_cast<uint64_t>(c), kMix64);
}

// This concatenates two 64-bit fingerprints. It is a convenience function to
// get a fingerprint for a combination of already fingerprinted components.
// It assumes that each input is already a good fingerprint itself.
// Note that this is legacy code and new code should use its replacement
// FingerprintCat2011().
//
// Note that in general it's impossible to construct Fingerprint(str)
// from the fingerprints of substrings of str.  One shouldn't expect
// FingerprintCat(Fingerprint(x), Fingerprint(y)) to indicate
// anything about Fingerprint(strCat(x, y)).
inline uint64_t FingerprintCat(uint64_t fp1, uint64_t fp2) {
  return hash64NumWithSeed(fp1, fp2);
}

namespace std {

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct hash<kudu::uint128> {
  size_t operator()(const kudu::uint128& x) const {
    if (sizeof(const kudu::uint128*) ==
        8) { // 64-bit systems have 8-byte pointers.
      return hash128To64(x);
    } else {
      uint32_t a = static_cast<uint32_t>(Uint128Low64(x)) +
          static_cast<uint32_t>(0x9e3779b9UL);
      uint32_t b = static_cast<uint32_t>(Uint128Low64(x) >> 32) +
          static_cast<uint32_t>(0x9e3779b9UL);
      uint32_t c = static_cast<uint32_t>(Uint128High64(x)) + kMix32;
      mix(a, b, c);
      a += static_cast<uint32_t>(Uint128High64(x) >> 32);
      mix(a, b, c);
      return c;
    }
  }
  // Less than operator for MSVC use.
  bool operator()(const kudu::uint128& a, const kudu::uint128& b) const {
    return a < b;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// Hasher for STL pairs. Requires hashers for both members to be defined
template <class First, class Second>
struct hash<pair<First, Second>> {
  size_t operator()(const pair<First, Second>& p) const {
    size_t h1 = std::hash<First>()(p.first);
    size_t h2 = std::hash<Second>()(p.second);
    // The decision below is at compile time
    return (sizeof(h1) <= sizeof(uint32_t)) ? hash32NumWithSeed(h1, h2)
                                            : hash64NumWithSeed(h1, h2);
  }
  // Less than operator for MSVC.
  bool operator()(const pair<First, Second>& a, const pair<First, Second>& b)
      const {
    return a < b;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

} // namespace std

// If you want an excellent string hash function, and you don't mind if it
// might change when you sync and recompile, please use GoodFastHash<>.
// For most applications, GoodFastHash<> is a good choice, better than
// hash<string> or hash<char*> or similar.  GoodFastHash<> can change
// from time to time and may differ across platforms, and we'll strive
// to keep improving it.
//
// By the way, when deleting the contents of a hash_set of pointers, it is
// unsafe to delete *iterator because the hash function may be called on
// the next iterator advance.  Use stlDeleteContainerPointers().

template <class X>
struct GoodFastHash;

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct GoodFastHash<char*> {
  size_t operator()(const char* s) const {
    return hashStringThoroughly(s, strlen(s));
  }
  // Less than operator for MSVC.
  bool operator()(const char* a, const char* b) const {
    return strcmp(a, b) < 0;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <>
struct GoodFastHash<const char*> {
  size_t operator()(const char* s) const {
    return hashStringThoroughly(s, strlen(s));
  }
  // Less than operator for MSVC.
  bool operator()(const char* a, const char* b) const {
    return strcmp(a, b) < 0;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <class CharT, class Traits, class Alloc>
struct GoodFastHash<std::basic_string<CharT, Traits, Alloc>> {
  size_t operator()(const std::basic_string<CharT, Traits, Alloc>& k) const {
    return hashStringThoroughly(k.data(), k.length() * sizeof(k[0]));
  }
  // Less than operator for MSVC.
  bool operator()(
      const std::basic_string<CharT, Traits, Alloc>& a,
      const std::basic_string<CharT, Traits, Alloc>& b) const {
    return a < b;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};

// This intended to be a "good" hash function.  It may change from time to time.
template <class CharT, class Traits, class Alloc>
struct GoodFastHash<const std::basic_string<CharT, Traits, Alloc>> {
  size_t operator()(const std::basic_string<CharT, Traits, Alloc>& k) const {
    return hashStringThoroughly(k.data(), k.length() * sizeof(k[0]));
  }
  // Less than operator for MSVC.
  bool operator()(
      const std::basic_string<CharT, Traits, Alloc>& a,
      const std::basic_string<CharT, Traits, Alloc>& b) const {
    return a < b;
  }
  static const size_t bucket_size = 4; // These are required by MSVC
  static const size_t min_buckets = 8; // 4 and 8 are defaults.
};
