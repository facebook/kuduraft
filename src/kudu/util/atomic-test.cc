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

#include "kudu/util/atomic.h"

#include <cstdint>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/test_util.h"

namespace kudu {

using std::numeric_limits;
using std::vector;

// TODO Add some multi-threaded tests; currently AtomicInt is just a
// wrapper around 'atomicops.h', but should the underlying
// implemention change, it would help to have tests that make sure
// invariants are preserved in a multi-threaded environment.

template <typename T>
class AtomicIntTest : public KuduTest {
 public:
  AtomicIntTest()
      : max_(numeric_limits<T>::max()), min_(numeric_limits<T>::min()) {
    acquireRelease_ = {kMemOrderNoBarrier, kMemOrderAcquire, kMemOrderRelease};
    barrier_ = {kMemOrderNoBarrier, kMemOrderBarrier};
  }

  vector<MemoryOrder> acquireRelease_;
  vector<MemoryOrder> barrier_;

  T max_;
  T min_;
};

using IntTypes = ::testing::Types<int32_t, int64_t, uint32_t, uint64_t>;
TYPED_TEST_CASE(AtomicIntTest, IntTypes);

TYPED_TEST(AtomicIntTest, LoadStore) {
  for (const MemoryOrder memOrder : this->acquireRelease_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_EQ(0, i.Load(memOrder));
    i.Store(42, memOrder);
    EXPECT_EQ(42, i.Load(memOrder));
    i.Store(this->min_, memOrder);
    EXPECT_EQ(this->min_, i.Load(memOrder));
    i.Store(this->max_, memOrder);
    EXPECT_EQ(this->max_, i.Load(memOrder));
  }
}

TYPED_TEST(AtomicIntTest, SetSwapExchange) {
  for (const MemoryOrder memOrder : this->acquireRelease_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_TRUE(i.CompareAndSet(0, 5, memOrder));
    EXPECT_EQ(5, i.Load(memOrder));
    EXPECT_FALSE(i.CompareAndSet(0, 10, memOrder));

    EXPECT_EQ(5, i.CompareAndSwap(5, this->max_, memOrder));
    EXPECT_EQ(this->max_, i.CompareAndSwap(42, 42, memOrder));
    EXPECT_EQ(this->max_, i.CompareAndSwap(this->max_, this->min_, memOrder));

    EXPECT_EQ(this->min_, i.Exchange(this->max_, memOrder));
    EXPECT_EQ(this->max_, i.Load(memOrder));
  }
}

TYPED_TEST(AtomicIntTest, MinMax) {
  for (const MemoryOrder memOrder : this->acquireRelease_) {
    AtomicInt<TypeParam> i(0);

    i.StoreMax(100, memOrder);
    EXPECT_EQ(100, i.Load(memOrder));
    i.StoreMin(50, memOrder);
    EXPECT_EQ(50, i.Load(memOrder));

    i.StoreMax(25, memOrder);
    EXPECT_EQ(50, i.Load(memOrder));
    i.StoreMin(75, memOrder);
    EXPECT_EQ(50, i.Load(memOrder));

    i.StoreMax(this->max_, memOrder);
    EXPECT_EQ(this->max_, i.Load(memOrder));
    i.StoreMin(this->min_, memOrder);
    EXPECT_EQ(this->min_, i.Load(memOrder));
  }
}

TYPED_TEST(AtomicIntTest, Increment) {
  for (const MemoryOrder memOrder : this->barrier_) {
    AtomicInt<TypeParam> i(0);
    EXPECT_EQ(1, i.Increment(memOrder));
    EXPECT_EQ(3, i.IncrementBy(2, memOrder));
    EXPECT_EQ(3, i.IncrementBy(0, memOrder));
  }
}

TEST(Atomic, AtomicBool) {
  vector<MemoryOrder> memoryOrders = {
      kMemOrderNoBarrier, kMemOrderRelease, kMemOrderAcquire};
  for (const MemoryOrder memOrder : memoryOrders) {
    AtomicBool b(false);
    EXPECT_FALSE(b.Load(memOrder));
    b.Store(true, memOrder);
    EXPECT_TRUE(b.Load(memOrder));
    EXPECT_TRUE(b.CompareAndSet(true, false, memOrder));
    EXPECT_FALSE(b.Load(memOrder));
    EXPECT_FALSE(b.CompareAndSet(true, false, memOrder));
    EXPECT_FALSE(b.CompareAndSwap(false, true, memOrder));
    EXPECT_TRUE(b.Load(memOrder));
    EXPECT_TRUE(b.Exchange(false, memOrder));
    EXPECT_FALSE(b.Load(memOrder));
  }
}

} // namespace kudu
