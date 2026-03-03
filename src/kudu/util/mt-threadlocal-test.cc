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
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadlocal.h"
#include "kudu/util/threadlocal_cache.h"

using std::string;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace threadlocal {

class ThreadLocalTest : public KuduTest {};

const int kTargetCounterVal = 1000000;

class Counter;
typedef unordered_set<Counter*> CounterPtrSet;
typedef Mutex RegistryLockType;
typedef simple_spinlock CounterLockType;

// Registry to provide reader access to the thread-local Counters.
// The methods are only thread-safe if the calling thread holds the lock.
class CounterRegistry {
 public:
  CounterRegistry() {}

  RegistryLockType* getLock() const {
    return &lock_;
  }

  bool registerUnlocked(Counter* counter) {
    LOG(INFO) << "Called registerUnlocked()";
    return InsertIfNotPresent(&counters_, counter);
  }

  bool unregisterUnlocked(Counter* counter) {
    LOG(INFO) << "Called unregisterUnlocked()";
    return counters_.erase(counter) > 0;
  }

  CounterPtrSet* getCountersUnlocked() {
    return &counters_;
  }

 private:
  mutable RegistryLockType lock_;
  CounterPtrSet counters_;
  DISALLOW_COPY_AND_ASSIGN(CounterRegistry);
};

// A simple Counter class that registers itself with a CounterRegistry.
class Counter {
 public:
  Counter(CounterRegistry* registry, int val)
      : tid_(Env::Default()->gettid()),
        registry_(CHECK_NOTNULL(registry)),
        val_(val) {
    LOG(INFO) << "Counter::~Counter(): tid = " << tid_ << ", addr = " << this
              << ", val = " << val_;
    std::lock_guard<RegistryLockType> regLock(*registry_->getLock());
    CHECK(registry_->registerUnlocked(this));
  }

  ~Counter() {
    LOG(INFO) << "Counter::~Counter(): tid = " << tid_ << ", addr = " << this
              << ", val = " << val_;
    std::lock_guard<RegistryLockType> regLock(*registry_->getLock());
    std::lock_guard<CounterLockType> selfLock(lock_);
    LOG(INFO) << tid_ << ": deleting self from registry...";
    CHECK(registry_->unregisterUnlocked(this));
  }

  uint64_t tid() {
    return tid_;
  }

  CounterLockType* getLock() const {
    return &lock_;
  }

  void incrementUnlocked() {
    val_++;
  }

  int getValueUnlocked() {
    return val_;
  }

 private:
  // We expect that most of the time this lock will be uncontended.
  mutable CounterLockType lock_;

  // TID of thread that constructed this object.
  const uint64_t tid_;

  // Register / unregister ourselves with this on construction / destruction.
  CounterRegistry* const registry_;

  // Current value of the counter.
  int val_;

  DISALLOW_COPY_AND_ASSIGN(Counter);
};

// Create a new THREAD_LOCAL Counter and loop an increment operation on it.
static void registerCounterAndLoopIncr(
    CounterRegistry* registry,
    CountDownLatch* countersReady,
    CountDownLatch* readerReady,
    CountDownLatch* countersDone,
    CountDownLatch* readerDone) {
  BLOCK_STATIC_THREAD_LOCAL(Counter, counter, registry, 0);
  // Inform the reader that we are alive.
  countersReady->countDown();
  // Let the reader initialize before we start counting.
  readerReady->wait();
  // Now rock & roll on the counting loop.
  for (int i = 0; i < kTargetCounterVal; i++) {
    std::lock_guard<CounterLockType> l(*counter->getLock());
    counter->incrementUnlocked();
  }
  // Let the reader know we're ready for him to verify our counts.
  countersDone->countDown();
  // Wait until the reader is done before we exit the thread, which will call
  // delete on the Counter.
  readerDone->wait();
}

// Iterate over the registered counters and their values.
static uint64_t iterate(CounterRegistry* registry, int expectedCounters) {
  uint64_t sum = 0;
  int seenCounters = 0;
  std::lock_guard<RegistryLockType> l(*registry->getLock());
  for (Counter* counter : *registry->getCountersUnlocked()) {
    uint64_t value;
    {
      std::lock_guard<CounterLockType> l(*counter->getLock());
      value = counter->getValueUnlocked();
    }
    LOG(INFO) << "tid " << counter->tid() << " (counter " << counter
              << "): " << value;
    sum += value;
    seenCounters++;
  }
  CHECK_EQ(expectedCounters, seenCounters);
  return sum;
}

static void testThreadLocalCounters(
    CounterRegistry* registry,
    const int numThreads) {
  LOG(INFO) << "Starting threads...";
  vector<std::shared_ptr<kudu::Thread>> threads;

  CountDownLatch countersReady(numThreads);
  CountDownLatch readerReady(1);
  CountDownLatch countersDone(numThreads);
  CountDownLatch readerDone(1);
  for (int i = 0; i < numThreads; i++) {
    std::shared_ptr<kudu::Thread> newThread;
    CHECK_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("t{}", i),
            &registerCounterAndLoopIncr,
            registry,
            &countersReady,
            &readerReady,
            &countersDone,
            &readerDone,
            &newThread));
    threads.push_back(newThread);
  }

  // Wait for all threads to start and register their Counters.
  countersReady.wait();
  CHECK_EQ(0, iterate(registry, numThreads));
  LOG(INFO) << "--";

  // Let the counters start spinning.
  readerReady.countDown();

  // Try to catch them in the act, just for kicks.
  for (int i = 0; i < 2; i++) {
    iterate(registry, numThreads);
    LOG(INFO) << "--";
    SleepFor(MonoDelta::FromMicroseconds(1));
  }

  // Wait until they're done and assure they sum up properly.
  countersDone.wait();
  LOG(INFO) << "Checking Counter sums...";
  CHECK_EQ(kTargetCounterVal * numThreads, iterate(registry, numThreads));
  LOG(INFO) << "Counter sums add up!";
  readerDone.countDown();

  LOG(INFO) << "Joining & deleting threads...";
  for (std::shared_ptr<kudu::Thread> thread : threads) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }
  LOG(INFO) << "Done.";
}

TEST_F(ThreadLocalTest, TestConcurrentCounters) {
  // Run this multiple times to ensure we don't leave remnants behind in the
  // CounterRegistry.
  CounterRegistry registry;
  for (int i = 0; i < 3; i++) {
    testThreadLocalCounters(&registry, 8);
  }
}

// Test class that stores a string in a static thread local member.
// This class cannot be instantiated. The methods are all static.
class ThreadLocalString {
 public:
  static void set(std::string value);
  static const std::string& get();

 private:
  ThreadLocalString() {}
  DECLARE_STATIC_THREAD_LOCAL(std::string, value_);
  DISALLOW_COPY_AND_ASSIGN(ThreadLocalString);
};

DEFINE_STATIC_THREAD_LOCAL(std::string, ThreadLocalString, value_);

void ThreadLocalString::set(std::string value) {
  INIT_STATIC_THREAD_LOCAL(std::string, value_);
  *value_ = value;
}

const std::string& ThreadLocalString::get() {
  INIT_STATIC_THREAD_LOCAL(std::string, value_);
  return *value_;
}

static void runAndAssign(
    CountDownLatch* writersReady,
    CountDownLatch* readersReady,
    CountDownLatch* allDone,
    CountDownLatch* threadsExiting,
    const std::string& in,
    std::string* out) {
  writersReady->wait();
  // Ensure it starts off as an empty string.
  CHECK_EQ("", ThreadLocalString::get());
  ThreadLocalString::set(in);

  readersReady->wait();
  out->assign(ThreadLocalString::get());
  allDone->wait();
  threadsExiting->countDown();
}

TEST_F(ThreadLocalTest, TestTLSMember) {
  const int numThreads = 8;

  vector<CountDownLatch*> writersReady;
  vector<CountDownLatch*> readersReady;
  vector<std::string*> outStrings;
  vector<std::shared_ptr<kudu::Thread>> threads;

  ElementDeleter writersDeleter(&writersReady);
  ElementDeleter readersDeleter(&readersReady);
  ElementDeleter outStringsDeleter(&outStrings);

  CountDownLatch allDone(1);
  CountDownLatch threadsExiting(numThreads);

  LOG(INFO) << "Starting threads...";
  for (int i = 0; i < numThreads; i++) {
    writersReady.push_back(new CountDownLatch(1));
    readersReady.push_back(new CountDownLatch(1));
    outStrings.push_back(new std::string());
    std::shared_ptr<kudu::Thread> newThread;
    CHECK_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("t{}", i),
            &runAndAssign,
            writersReady[i],
            readersReady[i],
            &allDone,
            &threadsExiting,
            fmt::format("{}", i),
            outStrings[i],
            &newThread));
    threads.push_back(newThread);
  }

  // Unlatch the threads in order.
  LOG(INFO) << "Writing to thread locals...";
  for (int i = 0; i < numThreads; i++) {
    writersReady[i]->countDown();
  }
  LOG(INFO) << "Reading from thread locals...";
  for (int i = 0; i < numThreads; i++) {
    readersReady[i]->countDown();
  }
  allDone.countDown();
  // threadsExiting acts as a memory barrier.
  threadsExiting.wait();
  for (int i = 0; i < numThreads; i++) {
    ASSERT_EQ(fmt::format("{}", i), *outStrings[i]);
    LOG(INFO) << "Read " << *outStrings[i];
  }

  LOG(INFO) << "Joining & deleting threads...";
  for (std::shared_ptr<kudu::Thread> thread : threads) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }
}

TEST_F(ThreadLocalTest, TestThreadLocalCache) {
  using TLC = ThreadLocalCache<int, string>;
  TLC* tlc = TLC::getInstance();

  // Lookup in an empty cache should return nullptr.
  ASSERT_EQ(nullptr, tlc->lookup(0));

  // Insert more items than the cache capacity.
  const int kLastItem = TLC::kItemCapacity * 2;
  for (int i = 1; i <= kLastItem; i++) {
    auto* item = tlc->emplaceNew(i);
    ASSERT_NE(nullptr, item);
    *item = fmt::format("item {}", i);
  }

  // Looking up the most recent items should return them.
  string* item = tlc->lookup(kLastItem);
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(*item, fmt::format("item {}", kLastItem));

  // Looking up evicted items should return nullptr.
  ASSERT_EQ(nullptr, tlc->lookup(1));
}

} // namespace threadlocal
} // namespace kudu
