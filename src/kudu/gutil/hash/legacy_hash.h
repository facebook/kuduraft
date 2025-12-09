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

// Hash8, Hash16 and Hash32 are for legacy use only.
using Hash32 = uint32_t;
using Hash16 = uint16_t;
using Hash8 = uint8_t;

const Hash32 kIllegalHash32 = static_cast<Hash32>(0xffffffffUL);
const Hash16 kIllegalHash16 = static_cast<Hash16>(0xffff);

static const uint32_t MIX32 = 0x12b9b0a1UL; // pi; an arbitrary number
static const uint64_t MIX64 = 0x2b992ddfa23249d6ULL; // more of pi

// ----------------------------------------------------------------------
// HashTo32()
// HashTo16()
//    These functions take various types of input (through operator
//    overloading) and return 32 or 16 bit quantities, respectively.
//    The basic rule of our hashing is: always mix().  Thus, even for
//    char outputs we cast to a uint32 and mix with two arbitrary numbers.
//    HashTo32 never returns kIllegalHash32, and similary,
//    HashTo16 never returns kIllegalHash16.
//
// Note that these methods avoid returning certain reserved values, while
// the corresponding HashXXStringWithSeed() methods may return any value.
// ----------------------------------------------------------------------

// This macro defines the HashTo32 and HashTo16 versions all in one go.
// It takes the argument list and a command that hashes your number.
// (For 16 we just mod retval before returning it.)  Example:
//    HASH_TO((char c), Hash32NumWithSeed(c, MIX32_1))
// evaluates to
//    uint32 retval;
//    retval = Hash32NumWithSeed(c, MIX32_1);
//    return retval == kIllegalHash32 ? retval-1 : retval;
//

#define HASH_TO(arglist, command)                          \
  inline uint32_t HashTo32 arglist {                       \
    uint32_t retval = command;                             \
    return retval == kIllegalHash32 ? retval - 1 : retval; \
  }

// This defines:
// HashToXX(char *s, int slen);
// HashToXX(char c);
// etc

HASH_TO((const char* s, uint32_t slen), Hash32StringWithSeed(s, slen, MIX32))
HASH_TO(
    (const wchar_t* s, uint32_t slen),
    Hash32StringWithSeed(
        reinterpret_cast<const char*>(s),
        static_cast<uint32_t>(sizeof(wchar_t) * slen),
        MIX32))
HASH_TO((char c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((int8_t c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((uint16_t c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((int16_t c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((uint32_t c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((int32_t c), Hash32NumWithSeed(static_cast<uint32_t>(c), MIX32))
HASH_TO((uint64_t c), static_cast<uint32_t>(Hash64NumWithSeed(c, MIX64) >> 32))
HASH_TO((int64_t c), static_cast<uint32_t>(Hash64NumWithSeed(c, MIX64) >> 32))

#undef HASH_TO // clean up the macro space

inline uint16_t HashTo16(const char* s, uint32_t slen) {
  uint16_t retval = Hash32StringWithSeed(s, slen, MIX32) >> 16;
  return retval == kIllegalHash16 ? static_cast<uint16_t>(retval - 1) : retval;
}
