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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#ifdef NDEBUG
#include <vector>
#endif

#include <gtest/gtest.h>

#include "kudu/util/faststring.h"
#include "kudu/util/group_varint-inl.h"
#ifdef NDEBUG
#include "kudu/util/stopwatch.h"
#endif

namespace kudu {
namespace coding {

extern void dumpSseTable();

// Encodes the given four ints as group-varint, then
// decodes and ensures the result is the same.
static void doTestRoundTripGvi32(
    uint32_t a,
    uint32_t b,
    uint32_t c,
    uint32_t d,
    bool useSse = false) {
  faststring buf;
  appendGroupVarInt32(&buf, a, b, c, d);

  int realSize = buf.size();

  // The implementations actually read past the group varint,
  // so append some extra padding data to ensure that it's not reading
  // uninitialized memory. The SSE implementation uses 128-bit reads
  // and the non-SSE one uses 32-bit reads.
  buf.append(std::string(useSse ? 16 : 4, 'x'));

  uint32_t ret[4];

  const uint8_t* end;

  if (useSse) {
    end =
        decodeGroupVarInt32Sse(buf.data(), &ret[0], &ret[1], &ret[2], &ret[3]);
  } else {
    end = decodeGroupVarInt32(buf.data(), &ret[0], &ret[1], &ret[2], &ret[3]);
  }

  ASSERT_EQ(a, ret[0]);
  ASSERT_EQ(b, ret[1]);
  ASSERT_EQ(c, ret[2]);
  ASSERT_EQ(d, ret[3]);
  ASSERT_EQ(end, buf.data() + realSize);
}

TEST(TestGroupVarInt, TestSseTable) {
  dumpSseTable();
  faststring buf;
  appendGroupVarInt32(&buf, 0, 0, 0, 0);
  doTestRoundTripGvi32(0, 0, 0, 0, true);
  doTestRoundTripGvi32(1, 2, 3, 4, true);
  doTestRoundTripGvi32(1, 2000, 3, 200000, true);
}

TEST(TestGroupVarInt, TestGroupVarInt) {
  faststring buf;
  appendGroupVarInt32(&buf, 0, 0, 0, 0);
  ASSERT_EQ(5UL, buf.size());
  ASSERT_EQ(0, memcmp("\x00\x00\x00\x00\x00", buf.data(), 5));
  buf.clear();

  // All 1-byte
  appendGroupVarInt32(&buf, 1, 2, 3, 254);
  ASSERT_EQ(5UL, buf.size());
  ASSERT_EQ(0, memcmp("\x00\x01\x02\x03\xfe", buf.data(), 5));
  buf.clear();

  // Mixed 1-byte and 2-byte
  appendGroupVarInt32(&buf, 256, 2, 3, 65535);
  ASSERT_EQ(7UL, buf.size());
  ASSERT_EQ(BOOST_BINARY(01 00 00 01), buf.at(0));
  ASSERT_EQ(256, *reinterpret_cast<const uint16_t*>(&buf[1]));
  ASSERT_EQ(2, *reinterpret_cast<const uint8_t*>(&buf[3]));
  ASSERT_EQ(3, *reinterpret_cast<const uint8_t*>(&buf[4]));
  ASSERT_EQ(65535, *reinterpret_cast<const uint16_t*>(&buf[5]));
}

// Round-trip encode/decodes using group varint
TEST(TestGroupVarInt, TestRoundTrip) {
  // A few simple tests.
  doTestRoundTripGvi32(0, 0, 0, 0);
  doTestRoundTripGvi32(1, 2, 3, 4);
  doTestRoundTripGvi32(1, 2000, 3, 200000);

  // Then a randomized test.
  for (int i = 0; i < 10000; i++) {
    doTestRoundTripGvi32(random(), random(), random(), random());
  }
}

#ifdef NDEBUG
TEST(TestGroupVarInt, EncodingBenchmark) {
  int nInts = 1000000;

  std::vector<uint32_t> ints;
  ints.reserve(nInts);
  for (int i = 0; i < nInts; i++) {
    ints.push_back(i);
  }

  faststring s;
  // conservative reservation
  s.reserve(ints.size() * 4);

  LOG_TIMING(INFO, "Benchmark") {
    for (int i = 0; i < 100; i++) {
      s.clear();
      appendGroupVarInt32Sequence(&s, 0, &ints[0], nInts);
    }
  }
}
#endif
} // namespace coding
} // namespace kudu
