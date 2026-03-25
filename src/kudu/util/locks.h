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
#ifndef KUDU_UTIL_LOCKS_H
#define KUDU_UTIL_LOCKS_H

#include <sched.h>

#include <algorithm> // IWYU pragma: keep
#include <atomic>
#include <mutex>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/spinlock.h"
#include "kudu/util/rw_semaphore.h"

namespace kudu {

// Wrapper around the Google SpinLock class to adapt it to the method names
// expected by Boost.
class SimpleSpinlock {
 public:
  SimpleSpinlock() {}

  void lock() {
    l_.lock();
  }

  void unlock() {
    l_.unlock();
  }

  bool try_lock() {
    return l_.tryLock();
  }

  // Return whether the lock is currently held.
  //
  // This state can change at any instant, so this is only really useful
  // for assertions where you expect to hold the lock. The success of
  // such an assertion isn't a guarantee that the current thread is the
  // holder, but the failure of such an assertion _is_ a guarantee that
  // the current thread is _not_ holding the lock!
  bool is_locked() {
    return l_.isHeld();
  }

 private:
  base::SpinLock l_;

  DISALLOW_COPY_AND_ASSIGN(SimpleSpinlock);
};

// Backward compatibility alias for code that hasn't been migrated yet
using simple_spinlock = SimpleSpinlock;

// Reader-writer lock.
// This is functionally equivalent to RwSemaphore in rw_semaphore.h, but should
// be used whenever the lock is expected to only be acquired on a single thread.
// It adds TSAN annotations which will detect misuse of the lock, but those
// annotations also assume that the same thread the takes the lock will unlock
// it.
//
// See rw_semaphore.h for documentation on the individual methods where unclear.
class rw_spinlock {
 public:
  rw_spinlock() {
    KUDU_ANNONTATE_RWLOCK_CREATE(this);
  }
  ~rw_spinlock() {
    KUDU_ANNONTATE_RWLOCK_DESTROY(this);
  }

  void lock_shared() {
    sem_.lock_shared();
    KUDU_ANNONTATE_RWLOCK_ACQUIRED(this, 0);
  }

  void unlock_shared() {
    KUDU_ANNONTATE_RWLOCK_RELEASED(this, 0);
    sem_.unlock_shared();
  }

  bool try_lock() {
    bool ret = sem_.try_lock();
    if (ret) {
      KUDU_ANNONTATE_RWLOCK_ACQUIRED(this, 1);
    }
    return ret;
  }

  void lock() {
    sem_.lock();
    KUDU_ANNONTATE_RWLOCK_ACQUIRED(this, 1);
  }

  void unlock() {
    KUDU_ANNONTATE_RWLOCK_RELEASED(this, 1);
    sem_.unlock();
  }

  bool isWriteLocked() const {
    return sem_.isWriteLocked();
  }

  bool isLocked() const {
    return sem_.isLocked();
  }

 private:
  RwSemaphore sem_;
};

// Simple implementation of the std::shared_lock API, which is not available in
// the standard library until C++14. Defers error checking to the underlying
// mutex.

template <typename Mutex>
class shared_lock {
 public:
  shared_lock() : m_(nullptr) {}

  explicit shared_lock(Mutex& m) : m_(&m) {
    m_->lock_shared();
  }

  shared_lock(Mutex& m, std::try_to_lock_t /* t */) : m_(nullptr) {
    if (m.try_lock_shared()) {
      m_ = &m;
    }
  }

  bool owns_lock() const {
    return m_;
  }

  void swap(shared_lock& other) {
    std::swap(m_, other.m_);
  }

  ~shared_lock() {
    if (m_ != nullptr) {
      m_->unlock_shared();
    }
  }

 private:
  Mutex* m_;
  DISALLOW_COPY_AND_ASSIGN(shared_lock<Mutex>);
};

class simple_mutexlock {
 public:
  simple_mutexlock() {}

  void lock() {
    m_.lock();
    is_locked_ = true;
  }

  void unlock() {
    is_locked_ = false;
    m_.unlock();
  }

  bool try_lock() {
    return m_.try_lock();
  }

  // Return whether the lock is currently held.
  //
  // This state can change at any instant, so this is only really useful
  // for assertions where you expect to hold the lock. The success of
  // such an assertion isn't a guarantee that the current thread is the
  // holder, but the failure of such an assertion _is_ a guarantee that
  // the current thread is _not_ holding the lock!
  bool is_locked() {
    return is_locked_;
  }

 private:
  std::mutex m_;

  std::atomic<bool> is_locked_{false};

  DISALLOW_COPY_AND_ASSIGN(simple_mutexlock);
};

} // namespace kudu

#endif
