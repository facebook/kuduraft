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

#include <folly/ScopeGuard.h>

#include <gtest/gtest.h>

TEST(ScopedCleanup, TestCleanup) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = folly::makeGuard([&]() { var = saved; });
    var = 42;
  }
  ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, TestCleanupViaLambda) {
  int executed = 0;
  {
    SCOPE_EXIT {
      executed = 1;
    };
    ASSERT_EQ(0, executed);
  }
  ASSERT_EQ(1, executed);
}

TEST(ScopedCleanup, TestCancelCleanup) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = folly::makeGuard([&]() { var = saved; });
    var = 42;
    cleanup.dismiss();
  }
  ASSERT_EQ(42, var);
}
