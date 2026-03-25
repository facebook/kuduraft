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
//
// Utility functions for dealing with a byte array as if it were a bitmap.
#ifndef KUDU_UTIL_BITMAP_H
#define KUDU_UTIL_BITMAP_H

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/fastmem.h"

namespace kudu {

// Return the number of bytes necessary to store the given number of bits.
inline size_t bitmapSize(size_t numBits) {
  return (numBits + 7) / 8;
}

// Set the given bit.
inline void bitmapSet(uint8_t* bitmap, size_t idx) {
  bitmap[idx >> 3] |= 1 << (idx & 7);
}

// Switch the given bit to the specified value.
inline void bitmapChange(uint8_t* bitmap, size_t idx, bool value) {
  bitmap[idx >> 3] =
      (bitmap[idx >> 3] & ~(1 << (idx & 7))) | ((!!value) << (idx & 7));
}

// Clear the given bit.
inline void bitmapClear(uint8_t* bitmap, size_t idx) {
  bitmap[idx >> 3] &= ~(1 << (idx & 7));
}

// Test/get the given bit.
inline bool bitmapTest(const uint8_t* bitmap, size_t idx) {
  return bitmap[idx >> 3] & (1 << (idx & 7));
}

// Set bits from offset to (offset + numBits) to the specified value
void bitmapChangeBits(
    uint8_t* bitmap,
    size_t offset,
    size_t numBits,
    bool value);

// Find the first bit of the specified value, starting from the specified
// offset.
bool bitmapFindFirst(
    const uint8_t* bitmap,
    size_t offset,
    size_t bitmapLen,
    bool value,
    size_t* idx);

// Find the first set bit in the bitmap, at the specified offset.
inline bool bitmapFindFirstSet(
    const uint8_t* bitmap,
    size_t offset,
    size_t bitmapLen,
    size_t* idx) {
  return bitmapFindFirst(bitmap, offset, bitmapLen, true, idx);
}

// Find the first zero bit in the bitmap, at the specified offset.
inline bool bitmapFindFirstZero(
    const uint8_t* bitmap,
    size_t offset,
    size_t bitmapLen,
    size_t* idx) {
  return bitmapFindFirst(bitmap, offset, bitmapLen, false, idx);
}

// Returns true if the bitmap contains only ones.
inline bool
bitmapIsAllSet(const uint8_t* bitmap, size_t offset, size_t bitmapLen) {
  DCHECK_LT(offset, bitmapLen);
  size_t idx;
  return !bitmapFindFirstZero(bitmap, offset, bitmapLen, &idx);
}

// Returns true if the bitmap contains only zeros.
inline bool
bitmapIsAllZero(const uint8_t* bitmap, size_t offset, size_t bitmapLen) {
  DCHECK_LT(offset, bitmapLen);
  size_t idx;
  return !bitmapFindFirstSet(bitmap, offset, bitmapLen, &idx);
}

// Returns true if the two bitmaps are equal.
//
// It is assumed that both bitmaps have 'bitmapLen' number of bits.
inline bool
bitmapEquals(const uint8_t* bm1, const uint8_t* bm2, size_t bitmapLen) {
  // Use memeq() to check all of the full bytes.
  size_t numFullBytes = bitmapLen >> 3;
  if (!strings::memeq(bm1, bm2, numFullBytes)) {
    return false;
  }

  // Check any remaining bits in one extra operation.
  size_t numRemainingBits = bitmapLen - (numFullBytes << 3);
  if (numRemainingBits == 0) {
    return true;
  }
  DCHECK_LT(numRemainingBits, 8);
  uint8_t mask = (1 << numRemainingBits) - 1;
  return (bm1[numFullBytes] & mask) == (bm2[numFullBytes] & mask);
}

std::string bitmapToString(const uint8_t* bitmap, size_t numBits);

// Iterator which yields ranges of set and unset bits.
// Example usage:
//   bool value;
//   size_t size;
//   BitmapIterator iter(bitmap, nBits);
//   while ((size = iter.next(&value))) {
//      printf("bitmap block len=%lu value=%d\n", size, value);
//   }
class BitmapIterator {
 public:
  BitmapIterator(const uint8_t* map, size_t numBits)
      : offset_(0), numBits_(numBits), map_(map) {}

  bool done() const {
    return (numBits_ - offset_) == 0;
  }

  void seekTo(size_t bit) {
    DCHECK_LE(bit, numBits_);
    offset_ = bit;
  }

  size_t next(bool* value) {
    size_t len = numBits_ - offset_;
    if (PREDICT_FALSE(len == 0)) {
      return (0);
    }

    *value = bitmapTest(map_, offset_);

    size_t index;
    if (bitmapFindFirst(map_, offset_, numBits_, !(*value), &index)) {
      len = index - offset_;
    } else {
      index = numBits_;
    }

    offset_ = index;
    return len;
  }

 private:
  size_t offset_;
  size_t numBits_;
  const uint8_t* map_;
};

// Iterator which yields the set bits in a bitmap.
// Example usage:
//   for (TrueBitIterator iter(bitmap, nBits);
//        !iter.done();
//        ++iter) {
//     int nextOnebitPosition = *iter;
//   }
class TrueBitIterator {
 public:
  TrueBitIterator(const uint8_t* bitmap, size_t nBits)
      : bitmap_(bitmap),
        curByte_(0),
        curByteIdx_(0),
        nBits_(nBits),
        nBytes_(bitmapSize(nBits_)),
        bitIdx_(0) {
    if (nBits_ == 0) {
      curByteIdx_ = 1; // sets done
    } else {
      curByte_ = bitmap[0];
      advanceToNextOneBit();
    }
  }

  TrueBitIterator& operator++() {
    DCHECK(!done());
    DCHECK(curByte_ & 1);
    curByte_ &= (~1);
    advanceToNextOneBit();
    return *this;
  }

  bool done() const {
    return curByteIdx_ >= nBytes_;
  }

  size_t operator*() const {
    DCHECK(!done());
    return bitIdx_;
  }

 private:
  void advanceToNextOneBit() {
    while (curByte_ == 0) {
      curByteIdx_++;
      if (curByteIdx_ >= nBytes_) {
        return;
      }
      curByte_ = bitmap_[curByteIdx_];
      bitIdx_ = curByteIdx_ * 8;
    }
    DVLOG(2) << "Found next nonzero byte at " << curByteIdx_
             << " val=" << curByte_;

    DCHECK_NE(curByte_, 0);
    int setBit = Bits::findLsbSetNonZero(curByte_);
    bitIdx_ += setBit;
    curByte_ >>= setBit;
  }

  const uint8_t* bitmap_;
  uint8_t curByte_;
  uint8_t curByteIdx_;

  const size_t nBits_;
  const size_t nBytes_;
  size_t bitIdx_;
};

} // namespace kudu

#endif
