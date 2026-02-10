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
#ifndef KUDU_UTIL_RW_SEMAPHORE_H
#define KUDU_UTIL_RW_SEMAPHORE_H

// Uncomment for extra debugging information. See below for details.
//   #define RW_SEMAPHORE_TRACK_HOLDER 1

#include <boost/smart_ptr/detail/yield_k.hpp>
#include <glog/logging.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#ifdef RW_SEMAPHORE_TRACK_HOLDER
#include "kudu/util/debug-util.h"
#endif
#include "kudu/util/thread.h"

namespace kudu {

// Read-Write semaphore. 32bit uint that contains the number of readers.
// When someone wants to write, tries to set the 32bit, and waits until
// the readers have finished. Readers are spinning while the write flag is set.
//
// This rw-semaphore makes no attempt at fairness, though it does avoid write
// starvation (no new readers may obtain the lock if a write is waiting).
//
// NOTE: this means that it is not safe to reentrantly acquire the read lock,
// due to the following deadlock:
//   - T1: acquire read lock
//   - T2: wait for write lock
//     (blocks waiting for readers)
//   - T1: try to acquire read-lock reentrantly
//     (blocks to avoid starving writers)
//
// Given that this is currently based only on spinning (and not futex),
// it should only be used in cases where the lock is held for very short
// time intervals.
//
// If the semaphore is expected to always be released from the same thread
// that acquired it, use rw_spinlock instead.
//
// In order to support easier debugging of leaked locks, this class can track
// the stack trace of the last thread to lock it in write mode. To do so,
// uncomment the definition of RW_SEMAPHORE_TRACK_HOLDER at the top of this
// file. Then, in gdb, print the contents of the semaphore, and you should see
// the collected stack trace.
class RwSemaphore {
 public:
  RwSemaphore() : state_(0) {}
  ~RwSemaphore() {}

  void lock_shared() {
    int loopCount = 0;
    Atomic32 curState = base::subtle::NoBarrier_Load(&state_);
    while (true) {
      Atomic32 expected = curState & kNumReadersMask; // I expect no write lock
      Atomic32 tryNewState = expected + 1; // Add me as reader
      curState =
          base::subtle::Acquire_CompareAndSwap(&state_, expected, tryNewState);
      if (curState == expected) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loopCount++);
    }
  }

  void unlock_shared() {
    int loopCount = 0;
    Atomic32 curState = base::subtle::NoBarrier_Load(&state_);
    while (true) {
      DCHECK_GT(curState & kNumReadersMask, 0)
          << "unlock_shared() called when there are no shared locks held";
      Atomic32 expected = curState; // I expect a write lock and other readers
      Atomic32 tryNewState = expected - 1; // Drop me as reader
      curState =
          base::subtle::Release_CompareAndSwap(&state_, expected, tryNewState);
      if (curState == expected) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loopCount++);
    }
  }

  // Tries to acquire a write lock, if no one else has it.
  // This function retries on CAS failure and waits for readers to complete.
  bool try_lock() {
    int loopCount = 0;
    Atomic32 curState = base::subtle::NoBarrier_Load(&state_);
    while (true) {
      // someone else has already the write lock
      if (curState & kWriteFlag) {
        return false;
      }

      Atomic32 expected =
          curState & kNumReadersMask; // I expect some 0+ readers
      Atomic32 tryNewState =
          kWriteFlag | expected; // I want to lock the other writers
      curState =
          base::subtle::Acquire_CompareAndSwap(&state_, expected, tryNewState);
      if (curState == expected) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loopCount++);
    }

    waitPendingReaders();
    recordLockHolderStack();
    return true;
  }

  void lock() {
    int loopCount = 0;
    Atomic32 curState = base::subtle::NoBarrier_Load(&state_);
    while (true) {
      Atomic32 expected =
          curState & kNumReadersMask; // I expect some 0+ readers
      Atomic32 tryNewState =
          kWriteFlag | expected; // I want to lock the other writers
      // Note: we use NoBarrier here because we'll do the Acquire barrier down
      // below in waitPendingReaders
      curState = base::subtle::NoBarrier_CompareAndSwap(
          &state_, expected, tryNewState);
      if (curState == expected) {
        break;
      }
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loopCount++);
    }

    waitPendingReaders();

    recordLockHolderStack();
  }

  void unlock() {
    // I expect to be the only writer
    DCHECK_EQ(base::subtle::NoBarrier_Load(&state_), kWriteFlag);

    resetLockHolderStack();
    // Reset: no writers & no readers.
    Release_Store(&state_, 0);
  }

  // Return true if the lock is currently held for write by any thread.
  // See simple_semaphore::is_locked() for details about where this is useful.
  bool is_write_locked() const {
    return base::subtle::NoBarrier_Load(&state_) & kWriteFlag;
  }

  // Return true if the lock is currently held, either for read or write
  // by any thread.
  // See simple_semaphore::is_locked() for details about where this is useful.
  bool is_locked() const {
    return base::subtle::NoBarrier_Load(&state_);
  }

 private:
  static const uint32_t kNumReadersMask = 0x7fffffff;
  static const uint32_t kWriteFlag = 1 << 31;

#ifdef RW_SEMAPHORE_TRACK_HOLDER
  StackTrace writer_stack_;
  void recordLockHolderStack() {
    writer_stack_.Collect();
  }
  void resetLockHolderStack() {
    writer_stack_.Reset();
  }
#else
  void recordLockHolderStack() {}
  void resetLockHolderStack() {}
#endif

  void waitPendingReaders() {
    int loopCount = 0;
    while ((base::subtle::Acquire_Load(&state_) & kNumReadersMask) > 0) {
      boost::detail::yield(loopCount++);
    }
  }

 private:
  volatile Atomic32 state_;
};

} // namespace kudu
#endif /* KUDU_UTIL_RW_SEMAPHORE_H */
