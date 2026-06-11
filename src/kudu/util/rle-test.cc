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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

// Must come before gtest.h.
#include "kudu/gutil/mathlimits.h"

#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/bit-stream-utils.h"
#include "kudu/util/bit-stream-utils.inline.h"
#include "kudu/util/bit-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/rle-encoding.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

const int kMaxWidth = 64;

class TestRle : public KuduTest {};

TEST(BitArray, TestBool) {
  const int lenBytes = 2;
  faststring buffer(lenBytes);

  BitWriter writer(&buffer);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    writer.putValue(i % 2, 1);
  }
  writer.flush();
  EXPECT_EQ(buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        writer.putValue(0, 1);
        break;
      default:
        writer.putValue(1, 1);
        break;
    }
  }
  writer.flush();

  // Validate the exact bit value
  EXPECT_EQ(buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
  EXPECT_EQ(buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

  // Use the reader and validate
  BitReader reader(buffer.data(), buffer.size());
  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.getValue(1, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % 2);
  }

  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.getValue(1, &val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

// Writes 'numVals' values with width 'bitWidth' and reads them back.
void testBitArrayValues(int bitWidth, int numVals) {
  const int kTestLen = BitUtil::ceil(bitWidth * numVals, 8);
  const uint64_t mod = bitWidth == 64 ? 1 : 1LL << bitWidth;

  faststring buffer(kTestLen);
  BitWriter writer(&buffer);
  for (int i = 0; i < numVals; ++i) {
    writer.putValue(i % mod, bitWidth);
  }
  writer.flush();
  EXPECT_EQ(writer.bytesWritten(), kTestLen);

  BitReader reader(buffer.data(), kTestLen);
  for (int i = 0; i < numVals; ++i) {
    int64_t val = 0;
    bool result = reader.getValue(bitWidth, &val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % mod);
  }
  EXPECT_EQ(reader.bytesLeft(), 0);
}

TEST(BitArray, TestValues) {
  for (int width = 1; width <= kMaxWidth; ++width) {
    testBitArrayValues(width, 1);
    testBitArrayValues(width, 2);
    // Don't write too many values
    testBitArrayValues(width, (width < 12) ? (1 << width) : 4096);
    testBitArrayValues(width, 1024);
  }
}

// Test some mixed values
TEST(BitArray, TestMixed) {
  const int kTestLenBits = 1024;
  faststring buffer(kTestLenBits / 8);
  bool parity = true;

  BitWriter writer(&buffer);
  for (int i = 0; i < kTestLenBits; ++i) {
    if (i % 2 == 0) {
      writer.putValue(parity, 1);
      parity = !parity;
    } else {
      writer.putValue(i, 10);
    }
  }
  writer.flush();

  parity = true;
  BitReader reader(buffer.data(), buffer.size());
  for (int i = 0; i < kTestLenBits; ++i) {
    bool result;
    if (i % 2 == 0) {
      bool val = false;
      result = reader.getValue(1, &val);
      EXPECT_EQ(val, parity);
      parity = !parity;
    } else {
      int val;
      result = reader.getValue(10, &val);
      EXPECT_EQ(val, i);
    }
    EXPECT_TRUE(result);
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expectedEncoding != NULL, also validates that the encoded buffer is
// exactly 'expectedEncoding'.
// if expectedLen is not -1, it will validate the encoded size is correct.
template <typename T>
void validateRle(
    const vector<T>& values,
    int bitWidth,
    uint8_t* expectedEncoding,
    int expectedLen) {
  faststring buffer;
  RleEncoder<T> encoder(&buffer, bitWidth);

  for (const auto& value : values) {
    encoder.put(value);
  }
  int encodedLen = encoder.flush();

  if (expectedLen != -1) {
    EXPECT_EQ(encodedLen, expectedLen);
  }
  if (expectedEncoding != nullptr) {
    EXPECT_EQ(memcmp(buffer.data(), expectedEncoding, expectedLen), 0)
        << "\n"
        << "Expected: " << hexDump(Slice(expectedEncoding, expectedLen)) << "\n"
        << "Got:      " << hexDump(Slice(buffer));
  }

  // Verify read
  RleDecoder<T> decoder(buffer.data(), encodedLen, bitWidth);
  for (const auto& value : values) {
    T val = 0;
    bool result = decoder.get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(value, val);
  }
}

TEST(Rle, SpecificSequences) {
  const int kTestLen = 1024;
  uint8_t expectedBuffer[kTestLen];
  vector<uint64_t> values;

  // Test 50 0' followed by 50 1's
  values.resize(100);
  for (int i = 0; i < 50; ++i) {
    values[i] = 0;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = 1;
  }

  // expectedBuffer valid for bit width <= 1 byte
  expectedBuffer[0] = (50 << 1);
  expectedBuffer[1] = 0;
  expectedBuffer[2] = (50 << 1);
  expectedBuffer[3] = 1;
  for (int width = 1; width <= 8; ++width) {
    validateRle(values, width, expectedBuffer, 4);
  }

  for (int width = 9; width <= kMaxWidth; ++width) {
    validateRle(values, width, nullptr, 2 * (1 + BitUtil::ceil(width, 8)));
  }

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int numGroups = BitUtil::ceil(100, 8);
  expectedBuffer[0] = (numGroups << 1) | 1;
  for (int i = 0; i < 100 / 8; ++i) {
    expectedBuffer[i + 1] = BOOST_BINARY(1 0 1 0 1 0 1 0); // 0xaa
  }
  // Values for the last 4 0 and 1's
  expectedBuffer[1 + 100 / 8] = BOOST_BINARY(0 0 0 0 1 0 1 0); // 0x0a

  // numGroups and expectedBuffer only valid for bit width = 1
  validateRle(values, 1, expectedBuffer, 1 + numGroups);
  for (int width = 2; width <= kMaxWidth; ++width) {
    validateRle(values, width, nullptr, 1 + BitUtil::ceil(width * 100, 8));
  }
}

// validateRle on 'numVals' values with width 'bitWidth'. If 'value' != -1,
// that value is used, otherwise alternating values are used.
void testRleValues(int bitWidth, int numVals, int value = -1) {
  const uint64_t mod = bitWidth == 64 ? 1ULL : 1ULL << bitWidth;
  vector<uint64_t> values;
  for (uint64_t v = 0; v < numVals; ++v) {
    values.push_back((value != -1) ? value : (bitWidth == 64 ? v : (v % mod)));
  }
  validateRle(values, bitWidth, nullptr, -1);
}

TEST(Rle, TestValues) {
  for (int width = 1; width <= kMaxWidth; ++width) {
    testRleValues(width, 1);
    testRleValues(width, 1024);
    testRleValues(width, 1024, 0);
    testRleValues(width, 1024, 1);
  }
}

class BitRle : public KuduTest {};

// Tests all true/false values
TEST_F(BitRle, AllSame) {
  const int kTestLen = 1024;
  vector<bool> values;

  for (int v = 0; v < 2; ++v) {
    values.clear();
    for (int i = 0; i < kTestLen; ++i) {
      values.push_back(v ? true : false);
    }

    validateRle(values, 1, nullptr, 3);
  }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST_F(BitRle, Flush) {
  vector<bool> values;
  for (int i = 0; i < 16; ++i) {
    values.push_back(1);
  }
  values.push_back(false);
  validateRle(values, 1, nullptr, -1);
  values.push_back(true);
  validateRle(values, 1, nullptr, -1);
  values.push_back(true);
  validateRle(values, 1, nullptr, -1);
  values.push_back(true);
  validateRle(values, 1, nullptr, -1);
}

// Test some random bool sequences.
TEST_F(BitRle, RandomBools) {
  int iters = 0;
  const int nIters = allowSlowTests() ? 1000 : 20;
  while (iters < nIters) {
    srand(iters++);
    if (iters % 10000 == 0) {
      LOG(ERROR) << "Seed: " << iters;
    }
    vector<uint64_t> values;
    bool parity = 0;
    for (int i = 0; i < 1000; ++i) {
      int groupSize = rand() % 20 + 1; // NOLINT(*)
      if (groupSize > 16) {
        groupSize = 1;
      }
      for (int i2 = 0; i2 < groupSize; ++i2) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    validateRle(values, (iters % kMaxWidth) + 1, nullptr, -1);
  }
}

// Test some random 64-bit sequences.
TEST_F(BitRle, Random64Bit) {
  int iters = 0;
  const int nIters = allowSlowTests() ? 1000 : 20;
  while (iters < nIters) {
    srand(iters++);
    if (iters % 10000 == 0) {
      LOG(ERROR) << "Seed: " << iters;
    }
    vector<uint64_t> values;
    for (int i = 0; i < 1000; ++i) {
      int groupSize = rand() % 20 + 1; // NOLINT(*)
      uint64_t curValue =
          (static_cast<uint64_t>(rand()) << 32) + static_cast<uint64_t>(rand());
      if (groupSize > 16) {
        groupSize = 1;
      }
      for (int i2 = 0; i2 < groupSize; ++i2) {
        values.push_back(curValue);
      }
    }
    validateRle(values, 64, nullptr, -1);
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST_F(BitRle, RepeatedPattern) {
  vector<bool> values;
  const int minRun = 1;
  const int maxRun = 32;

  for (int i = minRun; i <= maxRun; ++i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = maxRun; i >= minRun; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  validateRle(values, 1, nullptr, -1);
}

TEST_F(TestRle, TestBulkPut) {
  size_t runLength;
  bool val = false;

  faststring buffer(1);
  RleEncoder<bool> encoder(&buffer, 1);
  encoder.put(true, 10);
  encoder.put(false, 7);
  encoder.put(true, 5);
  encoder.put(true, 15);
  encoder.flush();

  RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);
  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, runLength);

  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(7, runLength);

  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(20, runLength);

  ASSERT_EQ(0, decoder.getNextRun(&val, MathLimits<size_t>::kMax));
}

TEST_F(TestRle, TestGetNextRun) {
  // Repeat the test with different number of items
  for (int numItems = 7; numItems < 200; numItems += 13) {
    // Test different block patterns
    //    1: 01010101 01010101
    //    2: 00110011 00110011
    //    3: 00011100 01110001
    //    ...
    for (int block = 1; block <= 20; ++block) {
      faststring buffer(1);
      RleEncoder<bool> encoder(&buffer, 1);
      for (int j = 0; j < numItems; ++j) {
        encoder.put(!!(j & 1), block);
      }
      encoder.flush();

      RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);
      size_t count = numItems * block;
      for (int j = 0; j < numItems; ++j) {
        size_t runLength;
        bool val = false;
        DCHECK_GT(count, 0);
        runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
        runLength = std::min(runLength, count);

        ASSERT_EQ(!!(j & 1), val);
        ASSERT_EQ(block, runLength);
        count -= runLength;
      }
      DCHECK_EQ(count, 0);
    }
  }
}

// Generate a random bit string which consists of 'numRuns' runs,
// each with a random length between 1 and 100. Returns the number
// of values encoded (i.e the sum run length).
static size_t
generateRandomBitString(int numRuns, faststring* encBuf, string* stringRep) {
  RleEncoder<bool> enc(encBuf, 1);
  int numBits = 0;
  for (int i = 0; i < numRuns; i++) {
    int runLength = random() % 100;
    bool value = static_cast<bool>(i & 1);
    enc.put(value, runLength);
    stringRep->append(runLength, value ? '1' : '0');
    numBits += runLength;
  }
  enc.flush();
  return numBits;
}

TEST_F(TestRle, TestRoundTripRandomSequencesWithRuns) {
  seedRandom();

  // Test the limiting function of GetNextRun.
  const int kMaxToReadAtOnce = (random() % 20) + 1;

  // Generate a bunch of random bit sequences, and "round-trip" them
  // through the encode/decode sequence.
  for (int rep = 0; rep < 100; rep++) {
    faststring buf;
    string stringRep;
    int numBits = generateRandomBitString(10, &buf, &stringRep);
    RleDecoder<bool> decoder(buf.data(), buf.size(), 1);
    string roundtripStr;
    int remToRead = numBits;
    size_t runLen;
    bool val;
    while (remToRead > 0 &&
           (runLen = decoder.getNextRun(
                &val, std::min(kMaxToReadAtOnce, remToRead))) != 0) {
      ASSERT_LE(runLen, kMaxToReadAtOnce);
      roundtripStr.append(runLen, val ? '1' : '0');
      remToRead -= runLen;
    }

    ASSERT_EQ(stringRep, roundtripStr);
  }
}
TEST_F(TestRle, TestSkip) {
  faststring buffer(1);
  RleEncoder<bool> encoder(&buffer, 1);

  // 0101010[1] 01010101 01
  //        "A"
  for (int j = 0; j < 18; ++j) {
    encoder.put(!!(j & 1));
  }

  // 0011[00] 11001100 11001100 11001100 11001100
  //      "B"
  for (int j = 0; j < 19; ++j) {
    encoder.put(!!(j & 1), 2);
  }

  // 000000000000 11[1111111111] 000000000000 111111111111
  //                   "C"
  // 000000000000 111111111111 0[00000000000] 111111111111
  //                                  "D"
  // 000000000000 111111111111 000000000000 111111111111
  for (int j = 0; j < 12; ++j) {
    encoder.put(!!(j & 1), 12);
  }
  encoder.flush();

  bool val = false;
  size_t runLength;
  RleDecoder<bool> decoder(buffer.data(), encoder.len(), 1);

  // position before "A"
  ASSERT_EQ(3, decoder.skip(7));
  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(1, runLength);

  // position before "B"
  ASSERT_EQ(7, decoder.skip(14));
  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(2, runLength);

  // position before "C"
  ASSERT_EQ(18, decoder.skip(46));
  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, runLength);

  // position before "D"
  ASSERT_EQ(24, decoder.skip(49));
  runLength = decoder.getNextRun(&val, MathLimits<size_t>::kMax);
  ASSERT_FALSE(val);
  ASSERT_EQ(11, runLength);

  encoder.flush();
}
} // namespace kudu
