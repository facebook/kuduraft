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
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/blocking_queue.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::thread;
using std::vector;

namespace kudu {

BlockingQueue<int32_t> test1Queue(5);

void insertSomeThings() {
  ASSERT_EQ(test1Queue.put(1), kQueueSuccess);
  ASSERT_EQ(test1Queue.put(2), kQueueSuccess);
  ASSERT_EQ(test1Queue.put(3), kQueueSuccess);
}

TEST(BlockingQueueTest, Test1) {
  thread inserterThread(insertSomeThings);
  int32_t i;
  ASSERT_TRUE(test1Queue.blockingGet(&i));
  ASSERT_EQ(1, i);
  ASSERT_TRUE(test1Queue.blockingGet(&i));
  ASSERT_EQ(2, i);
  ASSERT_TRUE(test1Queue.blockingGet(&i));
  ASSERT_EQ(3, i);
  inserterThread.join();
}

TEST(BlockingQueueTest, TestBlockingDrainTo) {
  BlockingQueue<int32_t> testQueue(3);
  ASSERT_EQ(testQueue.put(1), kQueueSuccess);
  ASSERT_EQ(testQueue.put(2), kQueueSuccess);
  ASSERT_EQ(testQueue.put(3), kQueueSuccess);
  vector<int32_t> out;
  ASSERT_OK(testQueue.blockingDrainTo(
      &out, MonoTime::Now() + MonoDelta::FromSeconds(30)));
  ASSERT_EQ(1, out[0]);
  ASSERT_EQ(2, out[1]);
  ASSERT_EQ(3, out[2]);

  // Set a deadline in the past and ensure we time out.
  Status s = testQueue.blockingDrainTo(
      &out, MonoTime::Now() - MonoDelta::FromSeconds(1));
  ASSERT_TRUE(s.IsTimedOut());

  // Ensure that if the queue is shut down, we get Aborted status.
  testQueue.shutdown();
  s = testQueue.blockingDrainTo(
      &out, MonoTime::Now() - MonoDelta::FromSeconds(1));
  ASSERT_TRUE(s.IsAborted());
}

// Test that, when the queue is shut down with elements still pending,
// Drain still returns OK until the elements are all gone.
TEST(BlockingQueueTest, TestGetAndDrainAfterShutdown) {
  // Put some elements into the queue and then shut it down.
  BlockingQueue<int32_t> q(3);
  ASSERT_EQ(q.put(1), kQueueSuccess);
  ASSERT_EQ(q.put(2), kQueueSuccess);

  q.shutdown();

  // Get() should still return an element.
  int i;
  ASSERT_TRUE(q.blockingGet(&i));
  ASSERT_EQ(1, i);

  // Drain should still return OK, since it yielded elements.
  vector<int32_t> out;
  ASSERT_OK(q.blockingDrainTo(&out));
  ASSERT_EQ(2, out[0]);

  // Now that it's empty, it should return Aborted.
  Status s = q.blockingDrainTo(&out);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  ASSERT_FALSE(q.blockingGet(&i));
}

TEST(BlockingQueueTest, TestTooManyInsertions) {
  BlockingQueue<int32_t> testQueue(2);
  ASSERT_EQ(testQueue.put(123), kQueueSuccess);
  ASSERT_EQ(testQueue.put(123), kQueueSuccess);
  ASSERT_EQ(testQueue.put(123), kQueueFull);
}

namespace {

struct LengthLogicalSize {
  static size_t logicalSize(const string& s) {
    return s.length();
  }
};

} // anonymous namespace

TEST(BlockingQueueTest, TestLogicalSize) {
  BlockingQueue<string, LengthLogicalSize> testQueue(4);
  ASSERT_EQ(testQueue.put("a"), kQueueSuccess);
  ASSERT_EQ(testQueue.put("bcd"), kQueueSuccess);
  ASSERT_EQ(testQueue.put("e"), kQueueFull);
}

TEST(BlockingQueueTest, TestNonPointerParamsMayBeNonEmptyOnDestruct) {
  BlockingQueue<int32_t> testQueue(1);
  ASSERT_EQ(testQueue.put(123), kQueueSuccess);
  // No DCHECK failure on destruct.
}

#ifndef NDEBUG
TEST(BlockingQueueDeathTest, TestPointerParamsMustBeEmptyOnDestruct) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_DEATH(
      {
        BlockingQueue<int32_t*> testQueue(1);
        int32_t element = 123;
        ASSERT_EQ(testQueue.put(&element), kQueueSuccess);
        // Debug assertion triggered on queue destruction since type is a
        // pointer.
      },
      "BlockingQueue holds bare pointers");
}
#endif // NDEBUG

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
  BlockingQueue<int64_t> testQueue(2);
  ASSERT_EQ(testQueue.put(123), kQueueSuccess);
  testQueue.shutdown();
  ASSERT_EQ(testQueue.put(456), kQueueShutdown);
  int64_t i;
  ASSERT_TRUE(testQueue.blockingGet(&i));
  ASSERT_EQ(123, i);
  ASSERT_FALSE(testQueue.blockingGet(&i));
}

TEST(BlockingQueueTest, TestGscopedPtrMethods) {
  BlockingQueue<int*> testQueue(2);
  std::unique_ptr<int> inputInt(new int(123));
  ASSERT_EQ(testQueue.put(&inputInt), kQueueSuccess);
  std::unique_ptr<int> outputInt;
  ASSERT_TRUE(testQueue.blockingGet(&outputInt));
  ASSERT_EQ(123, *outputInt.get());
  testQueue.shutdown();
}

class MultiThreadTest {
 public:
  MultiThreadTest()
      : puts(4),
        blockingPuts(4),
        nThreads(5),
        queue(nThreads * puts),
        numInserters(nThreads),
        syncLatch(nThreads) {}

  void inserterThread(int arg) {
    for (int i = 0; i < puts; i++) {
      ASSERT_EQ(queue.put(arg), kQueueSuccess);
    }
    syncLatch.countDown();
    syncLatch.wait();
    for (int i = 0; i < blockingPuts; i++) {
      ASSERT_TRUE(queue.blockingPut(arg));
    }
    MutexLock guard(lock);
    if (--numInserters == 0) {
      queue.shutdown();
    }
  }

  void removerThread() {
    for (int i = 0; i < puts + blockingPuts; i++) {
      int32_t arg = 0;
      bool got = queue.blockingGet(&arg);
      if (!got) {
        arg = -1;
      }
      MutexLock guard(lock);
      gotten[arg] = gotten[arg] + 1;
    }
  }

  void run() {
    for (int i = 0; i < nThreads; i++) {
      threads.emplace_back(&MultiThreadTest::inserterThread, this, i);
      threads.emplace_back(&MultiThreadTest::removerThread, this);
    }
    // We add an extra thread to ensure that there aren't enough elements in
    // the queue to go around.  This way, we test removal after shutdown.
    threads.emplace_back(&MultiThreadTest::removerThread, this);
    for (auto& thread : threads) {
      thread.join();
    }
    // Let's check to make sure we got what we should have.
    MutexLock guard(lock);
    for (int i = 0; i < nThreads; i++) {
      ASSERT_EQ(puts + blockingPuts, gotten[i]);
    }
    // And there were nThreads * (puts + blockingPuts)
    // elements removed, but only nThreads * puts +
    // blockingPuts elements added.  So some removers hit the
    // shutdown case.
    ASSERT_EQ(puts + blockingPuts, gotten[-1]);
  }

  int puts;
  int blockingPuts;
  int nThreads;
  BlockingQueue<int32_t> queue;
  Mutex lock;
  std::map<int32_t, int> gotten;
  vector<thread> threads;
  int numInserters;
  CountDownLatch syncLatch;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
  MultiThreadTest test;
  test.run();
}

} // namespace kudu
