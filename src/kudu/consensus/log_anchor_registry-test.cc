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

#include "kudu/consensus/log_anchor_registry.h"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {
namespace log {

class LogAnchorRegistryTest : public KuduTest {};

TEST_F(LogAnchorRegistryTest, TestUpdateRegistration) {
  const string testName = CURRENT_TEST_NAME();
  std::shared_ptr<LogAnchorRegistry> reg(new LogAnchorRegistry());

  LogAnchor anchor;
  const int64_t kInitialIndex = 12345;

  ASSERT_FALSE(anchor.isRegistered_);
  ASSERT_FALSE(anchor.whenRegistered_.Initialized());
  reg->registerAnchor(kInitialIndex, testName, &anchor);
  ASSERT_TRUE(anchor.isRegistered_);
  ASSERT_TRUE(anchor.whenRegistered_.Initialized());
  ASSERT_OK(reg->updateRegistration(kInitialIndex + 1, testName, &anchor));
  ASSERT_OK(reg->unregister(&anchor));
}

TEST_F(LogAnchorRegistryTest, TestDuplicateInserts) {
  const string testName = CURRENT_TEST_NAME();
  std::shared_ptr<LogAnchorRegistry> reg(new LogAnchorRegistry());

  // Register a bunch of anchors at log index 1.
  const int numAnchors = 10;
  LogAnchor anchors[numAnchors];
  for (auto& anchor : anchors) {
    reg->registerAnchor(1, testName, &anchor);
  }

  // We should see index 1 as the earliest registered.
  int64_t firstIndex = -1;
  ASSERT_OK(reg->getEarliestRegisteredLogIndex(&firstIndex));
  ASSERT_EQ(1, firstIndex);

  // Unregister them all.
  for (auto& anchor : anchors) {
    ASSERT_OK(reg->unregister(&anchor));
  }

  // We should see none registered.
  Status s = reg->getEarliestRegisteredLogIndex(&firstIndex);
  ASSERT_TRUE(s.IsNotFound()) << fmt::format(
      "Should have empty OpId registry. Status: {}, anchor: {}, Num anchors: {}",
      s.ToString(),
      firstIndex,
      reg->getAnchorCountForTests());

  ASSERT_EQ(0, reg->getAnchorCountForTests());
}

// Ensure that the correct results are returned when anchors are added/removed
// out of order.
TEST_F(LogAnchorRegistryTest, TestOrderedEarliestOpId) {
  std::shared_ptr<LogAnchorRegistry> reg(new LogAnchorRegistry());
  const int kNumAnchors = 4;
  const string testName = CURRENT_TEST_NAME();

  LogAnchor anchors[kNumAnchors];

  reg->registerAnchor(2, testName, &anchors[0]);
  reg->registerAnchor(3, testName, &anchors[1]);
  reg->registerAnchor(1, testName, &anchors[2]);
  reg->registerAnchor(4, testName, &anchors[3]);

  ASSERT_STR_CONTAINS(reg->dumpAnchorInfo(), "LogAnchor[index=1");

  int64_t anchorIdx = -1;
  ASSERT_OK(reg->getEarliestRegisteredLogIndex(&anchorIdx));
  ASSERT_EQ(1, anchorIdx);

  ASSERT_OK(reg->unregister(&anchors[2]));
  ASSERT_OK(reg->getEarliestRegisteredLogIndex(&anchorIdx));
  ASSERT_EQ(2, anchorIdx);

  ASSERT_OK(reg->unregister(&anchors[3]));
  ASSERT_OK(reg->getEarliestRegisteredLogIndex(&anchorIdx));
  ASSERT_EQ(2, anchorIdx);

  ASSERT_OK(reg->unregister(&anchors[0]));
  ASSERT_OK(reg->getEarliestRegisteredLogIndex(&anchorIdx));
  ASSERT_EQ(3, anchorIdx);

  ASSERT_OK(reg->unregister(&anchors[1]));
  Status s = reg->getEarliestRegisteredLogIndex(&anchorIdx);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

} // namespace log
} // namespace kudu
