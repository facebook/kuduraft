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

#include "kudu/util/os-util.h"

#include <unistd.h>

#include <string>

#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/util/test_macros.h"

using std::string;

namespace kudu {

void runTest(const string& name, int userTicks, int kernelTicks, int ioWait) {
  string buf = fmt::format(
      "0 ({}) S 0 0 0 0 0 0 0 0 0 0 {} {} 0 0 0 0 0"
      " 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
      " 0 {} 0 0 0 0 0 0 0 0 0 0",
      name,
      userTicks,
      kernelTicks,
      ioWait);
  ThreadStats stats;
  string extractedName;
  ASSERT_OK(parseStat(buf, &extractedName, &stats));
  ASSERT_EQ(name, extractedName);
  ASSERT_EQ(userTicks * (1e9 / sysconf(_SC_CLK_TCK)), stats.userNs);
  ASSERT_EQ(kernelTicks * (1e9 / sysconf(_SC_CLK_TCK)), stats.kernelNs);
  ASSERT_EQ(ioWait * (1e9 / sysconf(_SC_CLK_TCK)), stats.iowaitNs);
}

TEST(OsUtilTest, TestSelf) {
  runTest("test", 111, 222, 333);
}

TEST(OsUtilTest, TestSelfNameWithSpace) {
  runTest("a space", 111, 222, 333);
}

TEST(OsUtilTest, TestSelfNameWithParens) {
  runTest("a(b(c((d))e)", 111, 222, 333);
}

} // namespace kudu
