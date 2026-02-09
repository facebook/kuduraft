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

#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"

namespace kudu {

static int
readBackBitmap(uint8_t* bm, size_t bits, std::vector<size_t>* result) {
  int iters = 0;
  for (TrueBitIterator iter(bm, bits); !iter.done(); ++iter) {
    size_t val = *iter;
    result->push_back(val);

    iters++;
  }
  return iters;
}

TEST(TestBitMap, TestIteration) {
  uint8_t bm[8];
  memset(bm, 0, sizeof(bm));
  bitmapSet(bm, 0);
  bitmapSet(bm, 8);
  bitmapSet(bm, 31);
  bitmapSet(bm, 32);
  bitmapSet(bm, 33);
  bitmapSet(bm, 63);

  EXPECT_EQ(
      "   0: 10000000 10000000 00000000 00000001 11000000 00000000 00000000 00000001 \n",
      bitmapToString(bm, sizeof(bm) * 8));

  std::vector<size_t> readBack;

  int iters = readBackBitmap(bm, sizeof(bm) * 8, &readBack);
  ASSERT_EQ(6, iters);
  ASSERT_EQ("0,8,31,32,33,63", JoinElements(readBack, ","));
}

TEST(TestBitMap, TestIteration2) {
  uint8_t bm[1];
  memset(bm, 0, sizeof(bm));
  bitmapSet(bm, 1);

  std::vector<size_t> readBack;

  int iters = readBackBitmap(bm, 3, &readBack);
  ASSERT_EQ(1, iters);
  ASSERT_EQ("1", JoinElements(readBack, ","));
}

TEST(TestBitMap, TestSetAndTestBits) {
  uint8_t bm[1];
  memset(bm, 0, sizeof(bm));

  size_t numBits = sizeof(bm) * 8;
  for (size_t i = 0; i < numBits; i++) {
    ASSERT_FALSE(bitmapTest(bm, i));

    bitmapSet(bm, i);
    ASSERT_TRUE(bitmapTest(bm, i));

    bitmapClear(bm, i);
    ASSERT_FALSE(bitmapTest(bm, i));

    bitmapChange(bm, i, true);
    ASSERT_TRUE(bitmapTest(bm, i));

    bitmapChange(bm, i, false);
    ASSERT_FALSE(bitmapTest(bm, i));
  }

  // Set the other bit: 01010101
  for (size_t i = 0; i < numBits; ++i) {
    ASSERT_FALSE(bitmapTest(bm, i));
    if (i & 1) {
      bitmapSet(bm, i);
    }
  }

  // Check and Clear the other bit: 0000000
  for (size_t i = 0; i < numBits; ++i) {
    ASSERT_EQ(!!(i & 1), bitmapTest(bm, i));
    if (i & 1) {
      bitmapClear(bm, i);
    }
  }

  // Check if bits are zero and change the other to one
  for (size_t i = 0; i < numBits; ++i) {
    ASSERT_FALSE(bitmapTest(bm, i));
    bitmapChange(bm, i, i & 1);
  }

  // Check the bits change them again
  for (size_t i = 0; i < numBits; ++i) {
    ASSERT_EQ(!!(i & 1), bitmapTest(bm, i));
    bitmapChange(bm, i, !(i & 1));
  }

  // Check the last setup
  for (size_t i = 0; i < numBits; ++i) {
    ASSERT_EQ(!(i & 1), bitmapTest(bm, i));
  }
}

TEST(TestBitMap, TestBulkSetAndTestBits) {
  uint8_t bm[16];
  size_t totalSize = sizeof(bm) * 8;

  // Test Bulk change bits and test bits
  for (int i = 0; i < 4; ++i) {
    bool value = i & 1;
    size_t numBits = totalSize;
    while (numBits > 0) {
      for (size_t offset = 0; offset < numBits; ++offset) {
        bitmapChangeBits(bm, 0, totalSize, !value);
        bitmapChangeBits(bm, offset, numBits - offset, value);

        ASSERT_EQ(value, bitmapIsAllSet(bm, offset, numBits));
        ASSERT_EQ(!value, bitmapIsAllZero(bm, offset, numBits));

        if (offset > 1) {
          ASSERT_EQ(value, bitmapIsAllZero(bm, 0, offset - 1));
          ASSERT_EQ(!value, bitmapIsAllSet(bm, 0, offset - 1));
        }

        if ((offset + numBits) < totalSize) {
          ASSERT_EQ(value, bitmapIsAllZero(bm, numBits, totalSize));
          ASSERT_EQ(!value, bitmapIsAllSet(bm, numBits, totalSize));
        }
      }
      numBits--;
    }
  }
}

TEST(TestBitMap, TestFindBit) {
  uint8_t bm[16];

  size_t numBits = sizeof(bm) * 8;
  bitmapChangeBits(bm, 0, numBits, false);
  while (numBits > 0) {
    for (size_t offset = 0; offset < numBits; ++offset) {
      size_t idx;
      ASSERT_FALSE(bitmapFindFirstSet(bm, offset, numBits, &idx));
      ASSERT_TRUE(bitmapFindFirstZero(bm, offset, numBits, &idx));
      ASSERT_EQ(idx, offset);
    }
    numBits--;
  }

  numBits = sizeof(bm) * 8;
  for (int i = 0; i < numBits; ++i) {
    bitmapChange(bm, i, i & 3);
  }

  for (; numBits > 0; numBits--) {
    for (size_t offset = 0; offset < numBits; ++offset) {
      size_t idx;

      // Find a set bit
      bool res = bitmapFindFirstSet(bm, offset, numBits, &idx);
      size_t expectedSetIdx = (offset + !(offset & 3));
      bool expectSetFound = (expectedSetIdx < numBits);
      ASSERT_EQ(expectSetFound, res);
      if (expectSetFound) {
        ASSERT_EQ(expectedSetIdx, idx);
      }

      // Find a zero bit
      res = bitmapFindFirstZero(bm, offset, numBits, &idx);
      size_t expectedZeroIdx = offset + ((offset & 3) ? (4 - (offset & 3)) : 0);
      bool expectZeroFound = (expectedZeroIdx < numBits);
      ASSERT_EQ(expectZeroFound, res);
      if (expectZeroFound) {
        ASSERT_EQ(expectedZeroIdx, idx);
      }
    }
  }
}

TEST(TestBitMap, TestBitmapIteration) {
  uint8_t bm[8];
  memset(bm, 0, sizeof(bm));
  bitmapSet(bm, 0);
  bitmapSet(bm, 8);
  bitmapSet(bm, 31);
  bitmapSet(bm, 32);
  bitmapSet(bm, 33);
  bitmapSet(bm, 63);

  BitmapIterator biter(bm, sizeof(bm) * 8);

  size_t i = 0;
  size_t size;
  bool value = false;
  bool expectedValue = true;
  size_t expectedSizes[] = {1, 7, 1, 22, 3, 29, 1, 0};
  while ((size = biter.next(&value)) > 0) {
    ASSERT_LT(i, 8);
    ASSERT_EQ(expectedValue, value);
    ASSERT_EQ(expectedSizes[i], size);
    expectedValue = !expectedValue;
    i++;
  }
  ASSERT_EQ(expectedSizes[i], size);
}

TEST(TestBitMap, TestEquals) {
  uint8_t bm1[8] = {0};
  uint8_t bm2[8] = {0};
  size_t numBits = sizeof(bm1) * 8;
  ASSERT_TRUE(bitmapEquals(bm1, bm2, numBits));

  // Loop over each bit starting from the end and going to the beginning. In
  // each iteration, set the bit in one bitmap and verify that although the two
  // bitmaps aren't equal, if we were to ignore the changed bits, they are still
  // equal.
  for (int i = numBits - 1; i >= 0; i--) {
    SCOPED_TRACE(i);
    bitmapChange(bm1, i, true);
    ASSERT_FALSE(bitmapEquals(bm1, bm2, numBits));
    ASSERT_TRUE(bitmapEquals(bm1, bm2, i));
  }

  // Now loop in the other direction, setting the second bitmap bit by bit.
  // As before, if we consider the bitmaps in their entirety, they're not equal,
  // but if we consider just the sequences where both are set, they are equal.
  for (int i = 0; i < numBits - 1; i++) {
    SCOPED_TRACE(i);
    bitmapChange(bm2, i, true);
    ASSERT_FALSE(bitmapEquals(bm1, bm2, numBits));
    ASSERT_TRUE(bitmapEquals(bm1, bm2, i + 1));
  }

  // If we set the very last bit, both bitmaps are now equal in their entirety.
  bitmapChange(bm2, numBits - 1, true);
  ASSERT_TRUE(bitmapEquals(bm1, bm2, numBits));

  // Test equality on overlapped bitmaps (i.e. a single underlying bitmap, two
  // subsequences of which are considered to be two separate bitmaps).

  // Set every third bit; the rest are unset.
  uint8_t bm3[8] = {0};
  for (int i = 0; i < numBits; i += 3) {
    bitmapChange(bm3, i, true);
  }

  ASSERT_TRUE(bitmapEquals(bm3, bm3, numBits)); // fully overlapped
  ASSERT_FALSE(bitmapEquals(bm3, bm3 + 1, numBits - 8)); // off by one byte
  ASSERT_TRUE(bitmapEquals(bm3, bm3 + 3, numBits - 24)); // off by three bytes
}

} // namespace kudu
