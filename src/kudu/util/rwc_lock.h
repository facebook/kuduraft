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
//     Obtained via ReadLock()/ReadUnlock().
//
//   Write:
//     A single writer may hold the lock (upgrade lock).
//     Blocks other writers but allows readers to continue.
//     This is useful for preparing state changes without blocking readers.
//     Obtained via WriteLock()/WriteUnlock().
//
//   Commit:
//     A single committer may hold the lock (exclusive lock).
//     Blocks all readers and writers.
//     Obtained by upgrading from Write mode via UpgradeToCommitLock(),
//     released via CommitUnlock().
//
// Typical usage pattern:
//   rwc.WriteLock();             // Acquire upgrade lock (doesn't block
//   readers)
//   ... prepare new state ...
//   rwc.UpgradeToCommitLock();   // Upgrade to exclusive (waits for readers)
//   ... commit state atomically ...
//   rwc.CommitUnlock();          // Release exclusive lock
//
// This implementation uses folly::SharedMutex's upgrade lock functionality
// internally, providing efficient reader-writer synchronization with an
// atomic commit phase.
class RWCLock {
 public:
  RWCLock() = default;
  ~RWCLock() = default;

  // Acquire lock in read mode. Multiple readers may hold the lock.
  void ReadLock() {
    lock_.lock_shared();
  }

  // Release the lock held in read mode.
  void ReadUnlock() {
    lock_.unlock_shared();
  }

  // Standard C++ SharedMutex interface - delegates to ReadLock().
  // This allows RWCLock to be used with shared_lock<RWCLock>.
  void lock_shared() {
    ReadLock();
  }

  // Standard C++ SharedMutex interface - delegates to ReadUnlock().
  void unlock_shared() {
    ReadUnlock();
  }

  // Acquire lock in write mode (upgrade lock).
  // Blocks other writers but allows readers. Only one writer may hold the lock.
  void WriteLock() {
    lock_.lock_upgrade();
  }

  // Release the lock held in write mode.
  void WriteUnlock() {
    lock_.unlock_upgrade();
  }

  // Upgrade from write mode to commit mode (exclusive lock).
  // Waits for all current readers to finish, then acquires exclusive access.
  // After this call, no readers or writers can access until CommitUnlock().
  //
  // REQUIRES: Must hold the write lock (via WriteLock()).
  void UpgradeToCommitLock() {
    lock_.unlock_upgrade_and_lock();
  }

  // Release the lock held in commit mode.
  // REQUIRES: Must hold the commit lock (via UpgradeToCommitLock()).
  void CommitUnlock() {
    lock_.unlock();
  }

 private:
  mutable folly::SharedMutex lock_;

  RWCLock(const RWCLock&) = delete;
  void operator=(const RWCLock&) = delete;
};

} // namespace kudu
