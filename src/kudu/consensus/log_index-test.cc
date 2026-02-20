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
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "kudu/consensus/log_index.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu::log {

using consensus::MakeOpId;
using consensus::OpId;

class LogIndexTest : public KuduTest {
 public:
  virtual void SetUp() override {
    KuduTest::SetUp();
    index_ = std::make_shared<LogIndex>(test_dir_);
  }

 protected:
  Status addEntry(const OpId& opId, int64_t segment, int64_t offset) {
    LogIndexEntry entry;
    entry.opId = opId;
    entry.segmentSequenceNumber = segment;
    entry.offsetInSegment = offset;
    return index_->addEntry(entry);
  }

  void verifyEntry(const OpId& opId, int64_t segment, int64_t offset) {
    SCOPED_TRACE(opId);
    LogIndexEntry result;
    EXPECT_OK(index_->getEntry(opId.index(), &result));
    EXPECT_EQ(opId.term(), result.opId.term());
    EXPECT_EQ(opId.index(), result.opId.index());
    EXPECT_EQ(segment, result.segmentSequenceNumber);
    EXPECT_EQ(offset, result.offsetInSegment);
  }

  void verifyNotFound(int64_t index) {
    SCOPED_TRACE(index);
    LogIndexEntry result;
    Status s = index_->getEntry(index, &result);
    EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  }

  std::shared_ptr<LogIndex> index_;
};

TEST_F(LogIndexTest, TestBasic) {
  // Insert three entries.
  ASSERT_OK(addEntry(MakeOpId(1, 1), 1, 12345));
  ASSERT_OK(addEntry(MakeOpId(1, 999999), 1, 999));
  ASSERT_OK(addEntry(MakeOpId(1, 1500000), 1, 54321));
  verifyEntry(MakeOpId(1, 1), 1, 12345);
  verifyEntry(MakeOpId(1, 999999), 1, 999);
  verifyEntry(MakeOpId(1, 1500000), 1, 54321);

  // Overwrite one.
  ASSERT_OK(addEntry(MakeOpId(5, 1), 1, 50000));
  verifyEntry(MakeOpId(5, 1), 1, 50000);
}

TEST_F(LogIndexTest, TestMultiSegmentWithGC) {
  ASSERT_OK(addEntry(MakeOpId(1, 1), 1, 12345));
  ASSERT_OK(addEntry(MakeOpId(1, 1000000), 1, 54321));
  ASSERT_OK(addEntry(MakeOpId(1, 1500000), 1, 54321));
  ASSERT_OK(addEntry(MakeOpId(1, 2500000), 1, 12345));

  // GCing indexes < 1,000,000 shouldn't have any effect, because we can't
  // remove any whole segment.
  for (int gc = 0; gc < 1000000; gc += 100000) {
    SCOPED_TRACE(gc);
    index_->gc(gc);
    verifyEntry(MakeOpId(1, 1), 1, 12345);
    verifyEntry(MakeOpId(1, 1000000), 1, 54321);
    verifyEntry(MakeOpId(1, 1500000), 1, 54321);
    verifyEntry(MakeOpId(1, 2500000), 1, 12345);
  }

  // If we GC index 1000000, we should lose the first op.
  index_->gc(1000000);
  verifyNotFound(1);
  verifyEntry(MakeOpId(1, 1000000), 1, 54321);
  verifyEntry(MakeOpId(1, 1500000), 1, 54321);
  verifyEntry(MakeOpId(1, 2500000), 1, 12345);

  // GC everything
  index_->gc(9000000);
  verifyNotFound(1);
  verifyNotFound(1000000);
  verifyNotFound(1500000);
  verifyNotFound(2500000);
}

TEST(LogIndexEntry, Comparison) {
  LogIndexEntry a;
  LogIndexEntry b;
  a.opId = MakeOpId(1, 1);
  b.opId = MakeOpId(1, 1);
  a.segmentSequenceNumber = 10;
  b.segmentSequenceNumber = 10;
  a.offsetInSegment = 5;
  b.offsetInSegment = 5;
  EXPECT_EQ(a, b);

  b.segmentSequenceNumber = 12;
  EXPECT_NE(a, b);

  b = a;
  b.offsetInSegment = 6;
  EXPECT_NE(a, b);

  b = a;
  b.opId = MakeOpId(1, 2);
  EXPECT_NE(a, b);

  b = a;
  b.opId = MakeOpId(2, 1);
  EXPECT_NE(a, b);
}

} // namespace kudu::log
