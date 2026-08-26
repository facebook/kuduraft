// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#pragma once

#include <cstdint>
#include <cstring>

#include "kudu/gutil/port.h" // IWYU pragma: keep
#include "kudu/util/slice.h"
// IWYU pragma: no_include <endian.h>

namespace kudu {

class faststring;

extern void putFixed32(faststring* dst, uint32_t value);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed32(uint8_t* dst, uint32_t value);
extern void EncodeFixed64(uint8_t* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern uint8_t* EncodeVarint32(uint8_t* dst, uint32_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t DecodeFixed32(const uint8_t* ptr) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
  // Load the raw bytes
  uint32_t result;
  memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
  return result;
#else
  return (
      (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
      (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
      (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
      (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
#endif
}

inline uint64_t DecodeFixed64(const uint8_t* ptr) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
  // Load the raw bytes
  uint64_t result;
  memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
  return result;
#else
  uint64_t lo = DecodeFixed32(ptr);
  uint64_t hi = DecodeFixed32(ptr + 4);
  return (hi << 32) | lo;
#endif
}

} // namespace kudu
