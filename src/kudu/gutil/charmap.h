// Character Map Class
//
// Originally written by Daniel Dulitz
// Yanked out from url.cc on February 2003 by Wei-Hwa Huang
//
// Copyright (C) Google, 2001.
//
// A fast, bit-vector map for 8-bit unsigned characters.
//
// Internally stores 256 bits in an array of 8 uint32s.
// See changelist history for micro-optimization attempts.
// Does quick bit-flicking to lookup needed characters.
//
// This class is useful for non-character purposes as well.

#pragma once

#include <string.h>
#include <cstdint>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/type_traits.h"

class Charmap {
 public:
  // Initializes with a given char*.  NUL is treated as a terminator
  // and will not be in the charmap.
  explicit Charmap(const char* str) {
    Init(str, strlen(str));
  }

  bool contains(unsigned char c) const {
    return (m_[c >> 5] >> (c & 0x1f)) & 0x1;
  }

 protected:
  uint32_t m_[8];

  void Init(const char* str, int len) {
    memset(&m_, 0, sizeof m_);
    for (int i = 0; i < len; ++i) {
      unsigned char value = static_cast<unsigned char>(str[i]);
      m_[value >> 5] |= 1UL << (value & 0x1f);
    }
  }
};
KDECLARE_POD(Charmap);
