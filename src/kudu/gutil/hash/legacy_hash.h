// Copyright 2011 Google Inc. All Rights Reserved.
//
// This is a library of legacy hashing routines. These routines are still in
// use, but are not encouraged for any new code, and may be removed at some
// point in the future.
//
// New code should use one of the targeted libraries that provide hash
// interfaces for the types needed. See //util/hash/README for details.

#pragma once

#include <cstdint>

#include "kudu/gutil/hash/builtin_type_hash.h"
#include "kudu/gutil/hash/string_hash.h"

// Hash32 is for legacy use only.
using Hash32 = uint32_t;

const Hash32 kIllegalHash32 = static_cast<Hash32>(0xffffffffUL);

static const uint32_t kMix32 = 0x12b9b0a1UL; // pi; an arbitrary number
static const uint64_t kMix64 = 0x2b992ddfa23249d6ULL; // more of pi

// ----------------------------------------------------------------------
// hashTo32()
//    This function takes various types of input (through operator
//    overloading) and returns a 32 bit quantity.
//    The basic rule of our hashing is: always mix().  Thus, even for
//    char outputs we cast to a uint32 and mix with two arbitrary numbers.
//    hashTo32 never returns kIllegalHash32.
//
// Note that these methods avoid returning certain reserved values, while
// the corresponding hashXXStringWithSeed() methods may return any value.
// ----------------------------------------------------------------------

// This macro defines the hashTo32 versions all in one go.
// It takes the argument list and a command that hashes your number.
// Example:
//    HASH_TO((char c), hash32NumWithSeed(c, kMix32_1))
// evaluates to
//    uint32 retval;
//    retval = hash32NumWithSeed(c, kMix32_1);
//    return retval == kIllegalHash32 ? retval-1 : retval;
//

#define HASH_TO(arglist, command)                          \
  inline uint32_t hashTo32 arglist {                       \
    uint32_t retval = command;                             \
    return retval == kIllegalHash32 ? retval - 1 : retval; \
  }

// This defines:
// hashToXX(char *s, int slen);
// hashToXX(char c);
// etc

HASH_TO((const char* s, uint32_t slen), hash32StringWithSeed(s, slen, kMix32))
HASH_TO(
    (const wchar_t* s, uint32_t slen),
    hash32StringWithSeed(
        reinterpret_cast<const char*>(s),
        static_cast<uint32_t>(sizeof(wchar_t) * slen),
        kMix32))
HASH_TO((char c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((int8_t c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((uint16_t c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((int16_t c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((uint32_t c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((int32_t c), hash32NumWithSeed(static_cast<uint32_t>(c), kMix32))
HASH_TO((uint64_t c), static_cast<uint32_t>(hash64NumWithSeed(c, kMix64) >> 32))
HASH_TO((int64_t c), static_cast<uint32_t>(hash64NumWithSeed(c, kMix64) >> 32))

#undef HASH_TO // clean up the macro space
