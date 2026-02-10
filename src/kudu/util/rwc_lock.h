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

#pragma once

#include <folly/SharedMutex.h>

namespace kudu {

// Read-Write-Commit lock.
//
// This lock has three modes:
//   Read:
//     Multiple readers may hold the lock simultaneously.
//     Obtained via readLock()/readUnlock().
//
//   Write:
//     A single writer may hold the lock (upgrade lock).
//     Blocks other writers but allows readers to continue.
//     This is useful for preparing state changes without blocking readers.
//     Obtained via writeLock()/writeUnlock().
//
//   Commit:
//     A single committer may hold the lock (exclusive lock).
//     Blocks all readers and writers.
//     Obtained by upgrading from Write mode via upgradeToCommitLock(),
//     released via commitUnlock().
//
// Typical usage pattern:
//   rwc.writeLock();             // Acquire upgrade lock (doesn't block
//   readers)
//   ... prepare new state ...
//   rwc.upgradeToCommitLock();   // Upgrade to exclusive (waits for readers)
//   ... commit state atomically ...
//   rwc.commitUnlock();          // Release exclusive lock
//
// This implementation uses folly::SharedMutex's upgrade lock functionality
// internally, providing efficient reader-writer synchronization with an
// atomic commit phase.
class RwcLock {
 public:
  RwcLock() = default;
  ~RwcLock() = default;

  // Acquire lock in read mode. Multiple readers may hold the lock.
  void readLock() {
    lock_.lock_shared();
  }

  // Release the lock held in read mode.
  void readUnlock() {
    lock_.unlock_shared();
  }

  // Standard C++ SharedMutex interface - delegates to readLock().
  // This allows RwcLock to be used with shared_lock<RwcLock>.
  void lock_shared() {
    readLock();
  }

  // Standard C++ SharedMutex interface - delegates to readUnlock().
  void unlock_shared() {
    readUnlock();
  }

  // Acquire lock in write mode (upgrade lock).
  // Blocks other writers but allows readers. Only one writer may hold the lock.
  void writeLock() {
    lock_.lock_upgrade();
  }

  // Release the lock held in write mode.
  void writeUnlock() {
    lock_.unlock_upgrade();
  }

  // Upgrade from write mode to commit mode (exclusive lock).
  // Waits for all current readers to finish, then acquires exclusive access.
  // After this call, no readers or writers can access until commitUnlock().
  //
  // REQUIRES: Must hold the write lock (via writeLock()).
  void upgradeToCommitLock() {
    lock_.unlock_upgrade_and_lock();
  }

  // Release the lock held in commit mode.
  // REQUIRES: Must hold the commit lock (via upgradeToCommitLock()).
  void commitUnlock() {
    lock_.unlock();
  }

 private:
  mutable folly::SharedMutex lock_;

  RwcLock(const RwcLock&) = delete;
  void operator=(const RwcLock&) = delete;
};

} // namespace kudu
