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

#include "kudu/util/rolling_log.h"

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class RollingLogTest : public KuduTest {
 public:
  RollingLogTest() : logDir_(GetTestPath("log_dir")) {}

  virtual void SetUp() override {
    ASSERT_OK(env_->CreateDir(logDir_));
  }

 protected:
  void AssertLogCount(int expectedCount, vector<string>* children) {
    vector<string> dirEntries;
    ASSERT_OK(env_->GetChildren(logDir_, &dirEntries));
    children->clear();

    for (const string& child : dirEntries) {
      if (child == "." || child == "..") {
        continue;
      }
      children->push_back(child);
      ASSERT_TRUE(hasPrefixString(child, "rolling_log-test."));
      ASSERT_STR_CONTAINS(child, ".mylog.");

      string pidSuffix = fmt::format("{}", getpid());
      ASSERT_TRUE(
          hasSuffixString(child, pidSuffix) ||
          hasSuffixString(child, pidSuffix + ".gz"))
          << "bad child: " << child;
    }
    std::sort(children->begin(), children->end());
    ASSERT_EQ(children->size(), expectedCount) << *children;
  }

  const string logDir_;
};

// Test with compression off.
TEST_F(RollingLogTest, TestLog) {
  RollingLog log(env_, logDir_, "mylog");
  log.setCompressionEnabled(false);
  log.setRollThresholdBytes(100);

  // Before writing anything, we shouldn't open a log file.
  vector<string> children;
  NO_FATALS(AssertLogCount(0, &children));

  // Appending some data should write a new segment.
  const string kTestString = "Hello world\n";
  ASSERT_OK(log.append(kTestString));
  NO_FATALS(AssertLogCount(1, &children));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(log.append(kTestString));
  }
  NO_FATALS(AssertLogCount(2, &children));

  faststring data;
  string path = JoinPathSegments(logDir_, children[0]);
  ASSERT_OK(ReadFileToString(env_, path, &data));
  ASSERT_TRUE(hasPrefixString(data.ToString(), kTestString)) << "Data missing";
  ASSERT_LE(data.size(), 100 + kTestString.length())
      << "Roll threshold not respected";
}

// Test with compression on.
TEST_F(RollingLogTest, TestCompression) {
  RollingLog log(env_, logDir_, "mylog");
  ASSERT_OK(log.open());

  StringPiece data = "Hello world\n";
  int rawSize = 0;
  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(log.append(data));
    rawSize += data.size();
  }
  ASSERT_OK(log.close());

  vector<string> children;
  NO_FATALS(AssertLogCount(1, &children));
  ASSERT_TRUE(hasSuffixString(children[0], ".gz"));

  // Ensure that the output is actually gzipped.
  uint64_t size;
  ASSERT_OK(env_->GetFileSize(JoinPathSegments(logDir_, children[0]), &size));
  ASSERT_LT(size, rawSize / 10);
  ASSERT_GT(size, 0);
}

TEST_F(RollingLogTest, TestFileCountLimit) {
  RollingLog log(env_, logDir_, "mylog");
  ASSERT_OK(log.open());
  log.setRollThresholdBytes(100);
  log.setMaxNumSegments(3);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(log.append("hello world\n"));
  }
  ASSERT_OK(log.close());

  vector<string> children;
  NO_FATALS(AssertLogCount(3, &children));
}

} // namespace kudu
