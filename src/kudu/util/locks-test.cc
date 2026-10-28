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

#include "kudu/util/locks.h"

#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

namespace kudu {

using HookMutex = UnlockHookMutex<simple_mutexlock>;

TEST(UnlockHookMutexTest, HookRunsBeforeEachUnlockWhileHeld) {
  HookMutex mutex;
  int calls = 0;
  bool heldDuringHook = false;
  {
    std::lock_guard l(mutex);
    mutex.setUnlockHook([&] {
      calls++;
      heldDuringHook = mutex.is_locked();
    });
  }
  EXPECT_EQ(1, calls);
  EXPECT_TRUE(heldDuringHook);
  EXPECT_FALSE(mutex.is_locked());

  mutex.lock();
  mutex.unlock();
  ASSERT_TRUE(mutex.try_lock());
  mutex.unlock();
  {
    std::unique_lock l(mutex);
  }
  EXPECT_EQ(4, calls);
}

TEST(UnlockHookMutexTest, ClearedHookDoesNotRun) {
  HookMutex mutex;
  int calls = 0;
  {
    std::lock_guard l(mutex);
    mutex.setUnlockHook([&] { calls++; });
  }
  {
    std::lock_guard l(mutex);
    mutex.setUnlockHook(nullptr);
  }
  {
    std::lock_guard l(mutex);
  }
  EXPECT_EQ(1, calls);
}

TEST(UnlockHookMutexTest, FailedTryLockRunsNoHook) {
  HookMutex mutex;
  int calls = 0;
  std::unique_lock l(mutex);
  mutex.setUnlockHook([&] { calls++; });
  std::thread t([&] { EXPECT_FALSE(mutex.try_lock()); });
  t.join();
  EXPECT_EQ(0, calls);
  l.unlock();
  EXPECT_EQ(1, calls);
}

TEST(UnlockHookMutexTest, HookRunsWhileUnderlyingMutexIsHeld) {
  HookMutex mutex;
  bool probed = false;
  bool otherThreadGotLock = true;
  {
    std::lock_guard l(mutex);
    // Probes once: if the other thread does get the mutex, its unlock runs
    // this hook too, which must not start another probe.
    mutex.setUnlockHook([&] {
      if (std::exchange(probed, true)) {
        return;
      }
      std::thread t([&] {
        otherThreadGotLock = mutex.try_lock();
        if (otherThreadGotLock) {
          mutex.unlock();
        }
      });
      t.join();
    });
  }
  EXPECT_TRUE(probed);
  EXPECT_FALSE(otherThreadGotLock);
}

// A deferred unique_lock that is released and retaken in the middle runs the
// hook at every release.
TEST(UnlockHookMutexTest, DeferredUniqueLockUnlockAndRelock) {
  HookMutex mutex;
  int calls = 0;
  {
    std::lock_guard l(mutex);
    mutex.setUnlockHook([&] { calls++; });
  }
  calls = 0;
  {
    std::unique_lock l(mutex, std::defer_lock);
    l.lock();
    l.unlock();
    EXPECT_EQ(1, calls);
    l.lock();
  }
  EXPECT_EQ(2, calls);
}

TEST(UnlockHookMutexTest, OptionalGuard) {
  HookMutex mutex;
  int calls = 0;
  {
    std::lock_guard l(mutex);
    mutex.setUnlockHook([&] { calls++; });
  }
  calls = 0;
  std::optional<std::lock_guard<HookMutex>> guard;
  guard.emplace(mutex);
  EXPECT_TRUE(mutex.is_locked());
  guard.reset();
  EXPECT_EQ(1, calls);
  EXPECT_FALSE(mutex.is_locked());
}

// A successful try_lock() marks the mutex held, as lock() does.
TEST(SimpleMutexlockTest, TryLockSetsIsLocked) {
  simple_mutexlock mutex;
  ASSERT_TRUE(mutex.try_lock());
  EXPECT_TRUE(mutex.is_locked());
  mutex.unlock();
  EXPECT_FALSE(mutex.is_locked());
}

// So installing a hook, which requires the mutex held, works after try_lock().
TEST(UnlockHookMutexTest, TryLockSetsIsLocked) {
  HookMutex mutex;
  ASSERT_TRUE(mutex.try_lock());
  EXPECT_TRUE(mutex.is_locked());
  mutex.setUnlockHook(nullptr);
  mutex.unlock();
  EXPECT_FALSE(mutex.is_locked());
}

TEST(UnlockHookMutexTest, NoHookByDefault) {
  HookMutex mutex;
  std::unique_lock l(mutex);
  EXPECT_TRUE(mutex.is_locked());
  l.unlock();
  EXPECT_FALSE(mutex.is_locked());
}

} // namespace kudu
