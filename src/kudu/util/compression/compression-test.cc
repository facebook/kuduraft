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
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using std::vector;

class TestCompression : public KuduTest {};

static void testCompressionCodec(CompressionType compression) {
  const int kInputSize = 64;

  std::shared_ptr<CompressionCodec> codec;
  uint8_t iBuffer[kInputSize];
  uint8_t uBuffer[kInputSize];
  size_t compressed;

  // Fill the test input buffer
  memset(iBuffer, 'Z', kInputSize);

  // Get the specified compression codec
  ASSERT_OK(CompressionCodecManager::getCodec(compression, &codec));

  // Allocate the compression buffer
  size_t maxCompressed = codec->MaxCompressedLength(kInputSize);
  ASSERT_LT(maxCompressed, (kInputSize * 2));
  std::unique_ptr<uint8_t[]> cBuffer(new uint8_t[maxCompressed]);

  // Compress and uncompress
  ASSERT_OK(
      codec->Compress(Slice(iBuffer, kInputSize), cBuffer.get(), &compressed));
  ASSERT_OK(
      codec->Uncompress(Slice(cBuffer.get(), compressed), uBuffer, kInputSize));
  ASSERT_EQ(0, memcmp(iBuffer, uBuffer, kInputSize));

  // Compress slices and uncompress
  vector<Slice> v;
  v.emplace_back(iBuffer, 1);
  for (int i = 1; i <= kInputSize; i += 7)
    v.emplace_back(iBuffer + i, 7);
  ASSERT_OK(
      codec->Compress(Slice(iBuffer, kInputSize), cBuffer.get(), &compressed));
  ASSERT_OK(
      codec->Uncompress(Slice(cBuffer.get(), compressed), uBuffer, kInputSize));
  ASSERT_EQ(0, memcmp(iBuffer, uBuffer, kInputSize));
}

TEST_F(TestCompression, TestNoCompressionCodec) {
  const CompressionCodec* codec;
  ASSERT_OK(CompressionCodecManager::getCodec(NO_COMPRESSION, &codec));
  ASSERT_EQ(nullptr, codec);
}

TEST_F(TestCompression, TestSnappyCompressionCodec) {
  testCompressionCodec(SNAPPY);
}

TEST_F(TestCompression, TestLz4CompressionCodec) {
  testCompressionCodec(LZ4);
}

TEST_F(TestCompression, TestZlibCompressionCodec) {
  testCompressionCodec(ZLIB);
}

} // namespace kudu
