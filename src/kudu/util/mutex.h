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
#ifndef KUDU_UTIL_MUTEX_H
#define KUDU_UTIL_MUTEX_H

#include <glog/logging.h>

#include "kudu/gutil/macros.h"

namespace kudu {

// A lock built around pthread_mutex_t. Does not allow recursion.
//
// The following checks will be performed in DEBUG mode:
//   acquire(), tryAcquire() - the lock isn't already held.
//   release() - the lock is already held by this thread.
//
class Mutex {
 public:
  Mutex();
  ~Mutex();

  void acquire();
  void release();
  bool tryAcquire();

  void lock() {
    acquire();
  }
  void unlock() {
    release();
  }
  bool try_lock() {
    return tryAcquire();
  }

  void assertAcquired() const {}

 private:
  friend class ConditionVariable;

  pthread_mutex_t nativeHandle_;

  DISALLOW_COPY_AND_ASSIGN(Mutex);
};

// A helper class that acquires the given Lock while the MutexLock is in scope.
class MutexLock {
 public:
  // Acquires 'lock' (must be unheld) and wraps around it.
  //
  // Sample usage:
  // {
  //   MutexLock l(lock_); // acquired
  //   ...
  // } // released
  explicit MutexLock(Mutex& lock) : lock_(&lock), owned_(true) {
    lock_->acquire();
  }

  void lock() {
    DCHECK(!owned_);
    lock_->acquire();
    owned_ = true;
  }

  void unlock() {
    DCHECK(owned_);
    lock_->assertAcquired();
    lock_->release();
    owned_ = false;
  }

  ~MutexLock() {
    if (owned_) {
      unlock();
    }
  }

  bool ownsLock() const {
    return owned_;
  }

 private:
  Mutex* lock_;
  bool owned_;
  DISALLOW_COPY_AND_ASSIGN(MutexLock);
};

} // namespace kudu
#endif /* KUDU_UTIL_MUTEX_H */
