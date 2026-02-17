// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/bitmap.h"

#include <cstring>
#include <string>

#include <glog/logging.h>

#include <fmt/core.h>

namespace kudu {

void bitmapChangeBits(
    uint8_t* bitmap,
    size_t offset,
    size_t numBits,
    bool value) {
  DCHECK_GT(numBits, 0);

  size_t startByte = (offset >> 3);
  size_t endByte = (offset + numBits - 1) >> 3;
  int singleByte = (startByte == endByte);

  // Change the last bits of the first byte
  size_t left = offset & 0x7;
  size_t right = (singleByte) ? (left + numBits) : 8;
  uint8_t mask = ((0xff << left) & (0xff >> (8 - right)));
  if (value) {
    bitmap[startByte++] |= mask;
  } else {
    bitmap[startByte++] &= ~mask;
  }

  // Nothing left... I'm done
  if (singleByte) {
    return;
  }

  // change the middle bits
  if (endByte > startByte) {
    const uint8_t pattern8[2] = {0x00, 0xff};
    memset(bitmap + startByte, pattern8[value], endByte - startByte);
  }

  // change the first bits of the last byte
  right = offset + numBits - (endByte << 3);
  mask = (0xff >> (8 - right));
  if (value) {
    bitmap[endByte] |= mask;
  } else {
    bitmap[endByte] &= ~mask;
  }
}

bool bitmapFindFirst(
    const uint8_t* bitmap,
    size_t offset,
    size_t bitmapLen,
    bool value,
    size_t* idx) {
  const uint64_t pattern64[2] = {0xffffffffffffffff, 0x0000000000000000};
  const uint8_t pattern8[2] = {0xff, 0x00};
  size_t bit;

  DCHECK_LE(offset, bitmapLen);

  // Jump to the byte at specified offset
  const uint8_t* p = bitmap + (offset >> 3);
  size_t numBits = bitmapLen - offset;

  // Find a 'value' bit at the end of the first byte
  if ((bit = offset & 0x7)) {
    for (; bit < 8 && numBits > 0; ++bit) {
      if (bitmapTest(p, bit) == value) {
        *idx = ((p - bitmap) << 3) + bit;
        return true;
      }

      numBits--;
    }

    p++;
  }

  // check 64bit at the time for a 'value' bit
  const uint64_t* u64 = reinterpret_cast<const uint64_t*>(p);
  while (numBits >= 64 && *u64 == pattern64[value]) {
    numBits -= 64;
    u64++;
  }

  // check 8bit at the time for a 'value' bit
  p = reinterpret_cast<const uint8_t*>(u64);
  while (numBits >= 8 && *p == pattern8[value]) {
    numBits -= 8;
    p++;
  }

  // Find a 'value' bit at the beginning of the last byte
  for (bit = 0; numBits > 0; ++bit) {
    if (bitmapTest(p, bit) == value) {
      *idx = ((p - bitmap) << 3) + bit;
      return true;
    }
    numBits--;
  }

  return false;
}

std::string bitmapToString(const uint8_t* bitmap, size_t numBits) {
  std::string s;
  size_t index = 0;
  while (index < numBits) {
    fmt::format_to(std::back_inserter(s), "{:4}: ", index);
    for (int i = 0; i < 8 && index < numBits; ++i) {
      for (int j = 0; j < 8 && index < numBits; ++j) {
        fmt::format_to(
            std::back_inserter(s),
            "{}",
            static_cast<int>(bitmapTest(bitmap, index)));
        index++;
      }
      fmt::format_to(std::back_inserter(s), " ");
    }
    fmt::format_to(std::back_inserter(s), "\n");
  }
  return s;
}

} // namespace kudu
