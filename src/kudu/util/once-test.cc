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

#include <ostream>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/util/once.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::vector;

namespace kudu {

namespace {

template <class KuduOnceType>
struct Thing {
  explicit Thing(bool shouldFailArg) : shouldFail(shouldFailArg), value(0) {}

  Status init();

  Status initOnce() {
    if (shouldFail) {
      return Status::IllegalState("Whoops!");
    }
    value = 1;
    return Status::OK();
  }

  const bool shouldFail;
  int value;
  KuduOnceType once;
};

template <>
Status Thing<KuduOnceLambda>::init() {
  return once.init([this] { return initOnce(); });
}

template <class KuduOnceType>
static void initOrGetInitted(Thing<KuduOnceType>* t, int i) {
  if (i % 2 == 0) {
    LOG(INFO) << "Thread " << i << " initting";
    t->init();
  } else {
    LOG(INFO) << "Thread " << i << " value: " << t->once.initSucceeded();
  }
}

} // anonymous namespace

using KuduOnceTypes = ::testing::Types<KuduOnceLambda>;
TYPED_TEST_CASE(TestOnce, KuduOnceTypes);

template <class KuduOnceType>
class TestOnce : public KuduTest {};

TYPED_TEST(TestOnce, KuduOnceTest) {
  {
    Thing<TypeParam> t(false);
    ASSERT_EQ(0, t.value);
    ASSERT_FALSE(t.once.initSucceeded());

    for (int i = 0; i < 2; i++) {
      ASSERT_OK(t.init());
      ASSERT_EQ(1, t.value);
      ASSERT_TRUE(t.once.initSucceeded());
    }
  }

  {
    Thing<TypeParam> t(true);
    for (int i = 0; i < 2; i++) {
      ASSERT_TRUE(t.init().IsIllegalState());
      ASSERT_EQ(0, t.value);
      ASSERT_FALSE(t.once.initSucceeded());
    }
  }
}

TYPED_TEST(TestOnce, KuduOnceThreadSafeTest) {
  Thing<TypeParam> thing(false);

  // The threads will read and write to thing.once.initted. If access to
  // it is not synchronized, TSAN will flag the access as data races.
  vector<std::shared_ptr<Thread>> threads;
  for (int i = 0; i < 10; i++) {
    std::shared_ptr<Thread> t;
    ASSERT_OK(
        Thread::Create(
            "test",
            fmt::format("thread {}", i),
            &initOrGetInitted<TypeParam>,
            &thing,
            i,
            &t));
    threads.push_back(t);
  }

  for (const std::shared_ptr<Thread>& t : threads) {
    t->Join();
  }
}

} // namespace kudu
