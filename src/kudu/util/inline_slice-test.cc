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

#include <cstddef>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "kudu/util/inline_slice.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

namespace kudu {

template <size_t N>
static void
testRoundTrip(InlineSlice<N>* slice, Arena* arena, size_t testSize) {
  std::unique_ptr<uint8_t[]> buf(new uint8_t[testSize]);
  for (int i = 0; i < testSize; i++) {
    buf[i] = i & 0xff;
  }

  Slice testInput(buf.get(), testSize);

  slice->set(testInput, arena);
  Slice ret = slice->asSlice();
  ASSERT_TRUE(ret == testInput)
      << "testSize  =" << testSize << "\n"
      << "ret        = " << ret.ToDebugString() << "\n"
      << "testInput = " << testInput.ToDebugString();

  // If the data is small enough to fit inline, then
  // the returned slice should point directly into the
  // InlineSlice object.
  if (testSize < N) {
    ASSERT_EQ(reinterpret_cast<const uint8_t*>(slice) + 1, ret.data());
  }
}

// Sweep a variety of inputs for a given size of inline
// data
template <size_t N>
static void doTest() {
  Arena arena(1024);

  // Test a range of inputs both growing and shrinking
  InlineSlice<N> mySlice;
  ASSERT_EQ(N, sizeof(mySlice));

  for (size_t toTest = 0; toTest < 1000; toTest++) {
    testRoundTrip(&mySlice, &arena, toTest);
  }
  for (size_t toTest = 1000; toTest > 0; toTest--) {
    testRoundTrip(&mySlice, &arena, toTest);
  }
}

TEST(TestInlineSlice, Test8ByteInline) {
  doTest<8>();
}

TEST(TestInlineSlice, Test12ByteInline) {
  doTest<12>();
}

TEST(TestInlineSlice, Test16ByteInline) {
  doTest<16>();
}

} // namespace kudu
