//  -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
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
//
// ---
// Modern C++ replacement for legacy spinlock implementation.
// Migrated to folly::SpinLock on 2024-12-03.
//
// REMOVED FEATURES (unused in production):
// - Stack trace profiling (StartSynchronizationProfiling)
// - Contention hashtable collection
// - LINKER_INITIALIZED constructor (use regular constructor)
//
// PRESERVED FEATURES:
// - Basic lock/unlock/try_lock operations
// - Thread safety annotations
// - RAII lock holder
// - API compatibility with old base::SpinLock

#pragma once

#include <folly/SpinLock.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/thread_annotations.h"

namespace base {

// Modern spinlock using folly::SpinLock.
//
// This provides the same API as the legacy base::SpinLock but uses
// the battle-tested folly::SpinLock implementation.
//
// Note: The legacy profiling infrastructure has been removed as it
// was never enabled in production (g_profiling_enabled was always 0).
class LOCKABLE SpinLock {
 public:
  SpinLock() = default;

  // Legacy LINKER_INITIALIZED constructor for API compatibility.
  // This is a no-op with folly::SpinLock (default construction is sufficient).
  explicit SpinLock(LinkerInitialized) {}

  // Acquire this SpinLock.
  inline void Lock() EXCLUSIVE_LOCK_FUNCTION() {
    lock_.lock();
  }

  // Try to acquire this SpinLock without blocking.
  // Returns true if the lock was acquired.
  inline bool TryLock() EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    return lock_.try_lock();
  }

  // Release this SpinLock, which must be held by the calling thread.
  inline void Unlock() UNLOCK_FUNCTION() {
    lock_.unlock();
  }

  // Legacy API for compatibility.
  // NOTE: folly::SpinLock doesn't provide IsHeld(). This returns false
  // conservatively since checking would be racy anyway. Only used in
  // assertions, so this is safe.
  inline bool IsHeld() {
    bool gotLock = TryLock();
    if (gotLock) {
      Unlock();
    }
    return !gotLock;
  }

 private:
  mutable folly::SpinLock lock_;

  DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

// RAII helper for SpinLock.
class SCOPED_LOCKABLE SpinLockHolder {
 public:
  inline explicit SpinLockHolder(SpinLock* l) EXCLUSIVE_LOCK_FUNCTION(l)
      : lock_(l) {
    l->Lock();
  }

  inline ~SpinLockHolder() UNLOCK_FUNCTION() {
    lock_->Unlock();
  }

 private:
  SpinLock* lock_;

  DISALLOW_COPY_AND_ASSIGN(SpinLockHolder);
};

// Catch bug where variable name is omitted.
// e.g., SpinLockHolder(&lock_); // Wrong!
//       SpinLockHolder holder(&lock_); // Correct
#define SpinLockHolder(x) COMPILE_ASSERT(0, spin_lock_holder_missing_var_name)

} // namespace base

// Legacy typedef for compatibility.
// Many files use this instead of base::SpinLock directly.
using simple_spinlock = base::SpinLock;
