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

#include "kudu/util/rwc_lock.h"

#include <atomic>
#include <thread>
#include <vector>

#include <folly/synchronization/test/Barrier.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

namespace kudu {

using base::subtle::NoBarrier_Load;
using base::subtle::Release_Store;
using std::string;
using std::thread;
using std::vector;

class RwcLockTest : public KuduTest {};

// Holds counters of how many threads hold the lock in each of the
// provided modes.
struct LockHoldersCount {
  LockHoldersCount() : num_readers(0), num_writers(0), num_committers(0) {}

  // Check the invariants of the lock counts.
  void CheckInvariants() {
    // At no time should we have more than one writer or committer.
    CHECK_LE(num_writers, 1);
    CHECK_LE(num_committers, 1);

    // If we have any readers, then we should not have any committers.
    if (num_readers > 0) {
      CHECK_EQ(num_committers, 0);
    }
  }

  void AdjustReaders(int delta) {
    std::lock_guard<simple_spinlock> l(lock);
    num_readers += delta;
    CheckInvariants();
  }

  void AdjustWriters(int delta) {
    std::lock_guard<simple_spinlock> l(lock);
    num_writers += delta;
    CheckInvariants();
  }

  void AdjustCommitters(int delta) {
    std::lock_guard<simple_spinlock> l(lock);
    num_committers += delta;
    CheckInvariants();
  }

  int num_readers;
  int num_writers;
  int num_committers;
  simple_spinlock lock;
};

struct SharedState {
  LockHoldersCount counts;
  RwcLock rwcLock;
  Atomic32 stop;
};

void ReaderThread(SharedState* state) {
  while (!NoBarrier_Load(&state->stop)) {
    state->rwcLock.readLock();
    state->counts.AdjustReaders(1);
    state->counts.AdjustReaders(-1);
    state->rwcLock.readUnlock();
  }
}

void WriterThread(SharedState* state) {
  string local_str;
  while (!NoBarrier_Load(&state->stop)) {
    state->rwcLock.writeLock();
    state->counts.AdjustWriters(1);

    state->rwcLock.upgradeToCommitLock();
    state->counts.AdjustWriters(-1);
    state->counts.AdjustCommitters(1);

    state->counts.AdjustCommitters(-1);
    state->rwcLock.commitUnlock();
  }
}

TEST_F(RwcLockTest, TestCorrectBehavior) {
  SharedState state;
  Release_Store(&state.stop, 0);

  vector<thread> threads;

  const int kNumWriters = 5;
  const int kNumReaders = 5;

  for (int i = 0; i < kNumWriters; i++) {
    threads.emplace_back(WriterThread, &state);
  }
  for (int i = 0; i < kNumReaders; i++) {
    threads.emplace_back(ReaderThread, &state);
  }

  if (AllowSlowTests()) {
    SleepFor(MonoDelta::FromSeconds(1));
  } else {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  Release_Store(&state.stop, 1);

  for (thread& t : threads) {
    t.join();
  }
}

// Test that writeLock (upgrade lock) doesn't block readers.
// This is critical for the copy-on-write pattern used in routing tables.
TEST_F(RwcLockTest, WriteLockDoesNotBlockReaders) {
  RwcLock lock;
  folly::test::Barrier barrier(2);
  std::atomic<bool> reader_acquired{false};
  std::atomic<bool> writer_done{false};

  // Writer thread holds writeLock
  std::thread writer([&]() {
    lock.writeLock();

    // Signal writer has acquired writeLock
    barrier.wait();

    // Wait for reader to verify it can acquire readLock
    barrier.wait();

    writer_done = true;
    lock.writeUnlock();
  });

  // Reader thread should be able to acquire readLock even though writer has
  // writeLock
  std::thread reader([&]() {
    // Wait for writer to acquire writeLock
    barrier.wait();

    lock.readLock();
    reader_acquired = true;

    // Verify writer still holds writeLock
    EXPECT_FALSE(writer_done);

    lock.readUnlock();

    // Signal reader is done
    barrier.wait();
  });

  reader.join();
  writer.join();

  // Verify reader successfully acquired lock while writer held writeLock
  EXPECT_TRUE(reader_acquired);
}

// Test that commitLock (exclusive lock) blocks readers.
TEST_F(RwcLockTest, CommitLockBlocksReaders) {
  RwcLock lock;
  folly::test::Barrier barrier(2);
  std::atomic<bool> reader_acquired{false};
  std::atomic<bool> committer_done{false};

  // Committer thread holds commitLock
  std::thread committer([&]() {
    lock.writeLock();
    lock.upgradeToCommitLock();

    // Signal committer has acquired commitLock
    barrier.wait();

    // Wait for reader to attempt to acquire (reader will block)
    barrier.wait();

    committer_done = true;
    lock.commitUnlock();
  });

  // Reader thread should block until committer releases
  std::thread reader([&]() {
    // Wait for committer to acquire commitLock
    barrier.wait();

    // Signal that reader is about to attempt lock acquisition
    barrier.wait();

    // This will block until committer releases
    lock.readLock();
    reader_acquired = true;

    // We should only acquire after committer is done
    EXPECT_TRUE(committer_done);

    lock.readUnlock();
  });

  reader.join();
  committer.join();

  EXPECT_TRUE(reader_acquired);
}

// Test that only one writer can hold writeLock at a time.
TEST_F(RwcLockTest, OnlyOneWriterAllowed) {
  RwcLock lock;
  std::atomic<int> concurrent_writers{0};
  std::atomic<int> max_concurrent_writers{0};
  const int kNumWriters = 5;
  folly::test::Barrier barrier(kNumWriters);

  std::vector<std::thread> writers;

  for (int i = 0; i < kNumWriters; i++) {
    writers.emplace_back([&]() {
      // Wait for all threads to be ready
      barrier.wait();

      lock.writeLock();

      int current = ++concurrent_writers;
      int max = max_concurrent_writers.load();
      while (current > max &&
             !max_concurrent_writers.compare_exchange_weak(max, current)) {
      }

      --concurrent_writers;
      lock.writeUnlock();
    });
  }

  for (auto& t : writers) {
    t.join();
  }

  // Verify only one writer held writeLock at a time
  EXPECT_EQ(max_concurrent_writers, 1);
}

} // namespace kudu
