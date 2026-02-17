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

#include <algorithm> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/rwc_lock.h"

namespace kudu {

// An object which manages its state via copy-on-write.
//
// Access to this object can be done more conveniently using the
// CowLock template class defined below.
//
// The 'State' template parameter must be swappable using std::swap.
template <class State>
class CowObject {
 public:
  CowObject() {}
  ~CowObject() {}

  // Lock an object for read.
  //
  // While locked, a mutator will be blocked when trying to commit its mutation.
  void readLock() const {
    lock_.readLock();
  }

  // Unlock an object previously locked for read, unblocking a mutator
  // actively trying to commit its mutation.
  void readUnlock() const {
    lock_.readUnlock();
  }

  // Lock the object for write (preventing concurrent mutators).
  //
  // We defer making a dirty copy of the state to mutableDirty() so that the
  // copy can be avoided if no dirty changes are actually made.
  void startMutation() {
    lock_.writeLock();
  }

  // Abort the current mutation. This drops the write lock without applying any
  // changes made to the mutable copy.
  void abortMutation() {
    dirtyState_.reset();
    lock_.writeUnlock();
  }

  // Commit the current mutation. This escalates to the "Commit" lock, which
  // blocks any concurrent readers or writers, swaps in the new version of the
  // State, and then drops the commit lock.
  void commitMutation() {
    if (!dirtyState_) {
      abortMutation();
      return;
    }
    lock_.upgradeToCommitLock();
    std::swap(state_, *dirtyState_);
    dirtyState_.reset();
    lock_.commitUnlock();
  }

  // Return the current state, not reflecting any in-progress mutations.
  State& state() {
    return state_;
  }

  const State& state() const {
    return state_;
  }

  // Returns the current dirty state (i.e reflecting in-progress mutations).
  // Should only be called by a thread who previously called startMutation().
  State* mutableDirty() {
    if (!dirtyState_) {
      dirtyState_.reset(new State(state_));
    }
    return dirtyState_.get();
  }

  const State& dirty() const {
    if (!dirtyState_) {
      return state_;
    }
    return *dirtyState_.get();
  }

 private:
  mutable RwcLock lock_;

  State state_;
  std::unique_ptr<State> dirtyState_;

  DISALLOW_COPY_AND_ASSIGN(CowObject);
};

// Lock state for the following lock-guard-like classes.
enum class LockMode {
  // The lock is held for reading.
  Read,

  // The lock is held for reading and writing.
  Write,

  // The lock is not held.
  Released
};

// Defined so LockMode is compatible with DCHECK and the like.
std::ostream& operator<<(std::ostream& o, LockMode m);

// A lock-guard-like scoped object to acquire the lock on a CowObject,
// and obtain a pointer to the correct copy to read/write.
//
// Example usage:
//
//   CowObject<Foo> my_obj;
//   {
//     CowLock<Foo> l(&my_obj, LockMode::Read);
//     l.data().get_foo();
//     ...
//   }
//   {
//     CowLock<Foo> l(&my_obj, LockMode::Write);
//     l->mutableData()->set_foo(...);
//     ...
//     l.commit();
//   }
template <class State>
class CowLock {
 public:
  // An unlocked CowLock. This is useful for default constructing a lock to be
  // moved in to.
  CowLock() : cow_(nullptr), mode_(LockMode::Released) {}

  // Lock in either read or write mode.
  CowLock(CowObject<State>* cow, LockMode mode) : cow_(cow), mode_(mode) {
    switch (mode) {
      case LockMode::Read:
        cow_->readLock();
        break;
      case LockMode::Write:
        cow_->startMutation();
        break;
      default:
        LOG(FATAL) << "Cannot lock in mode " << mode;
    }
  }

  // Lock in read mode.
  // A const object may not be locked in write mode.
  CowLock(const CowObject<State>* info, LockMode mode)
      : cow_(const_cast<CowObject<State>*>(info)), mode_(mode) {
    switch (mode) {
      case LockMode::Read:
        cow_->readLock();
        break;
      case LockMode::Write:
        LOG(FATAL) << "Cannot write-lock a const pointer";
      default:
        LOG(FATAL) << "Cannot lock in mode " << mode;
    }
  }

  // Disable copying.
  CowLock(const CowLock&) = delete;
  CowLock& operator=(const CowLock&) = delete;

  // Allow moving.
  CowLock(CowLock&& other) noexcept : cow_(other.cow_), mode_(other.mode_) {
    other.cow_ = nullptr;
    other.mode_ = LockMode::Released;
  }
  CowLock& operator=(CowLock&& other) noexcept {
    cow_ = other.cow_;
    mode_ = other.mode_;
    other.cow_ = nullptr;
    other.mode_ = LockMode::Released;
    return *this;
  }

  // Commit the underlying object.
  // Requires that the caller hold the lock in write mode.
  void commit() {
    DCHECK_EQ(LockMode::Write, mode_);
    cow_->commitMutation();
    mode_ = LockMode::Released;
  }

  void unlock() {
    switch (mode_) {
      case LockMode::Read:
        cow_->readUnlock();
        break;
      case LockMode::Write:
        cow_->abortMutation();
        break;
      default:
        DCHECK_EQ(LockMode::Released, mode_);
        break;
    }
    mode_ = LockMode::Released;
  }

  // Obtain the underlying data. In WRITE mode, this returns the
  // same data as mutableData() (not the safe unchanging copy).
  const State& data() const {
    switch (mode_) {
      case LockMode::Read:
        return cow_->state();
      case LockMode::Write:
        return cow_->dirty();
      default:
        LOG(FATAL) << "Cannot access data after committing";
    }
  }

  // Obtain the mutable data. This may only be called in WRITE mode.
  State* mutableData() {
    switch (mode_) {
      case LockMode::Read:
        LOG(FATAL) << "Cannot mutate data with READ lock";
      case LockMode::Write:
        return cow_->mutableDirty();
      default:
        LOG(FATAL) << "Cannot access data after committing";
    }
  }

  bool isWriteLocked() const {
    return mode_ == LockMode::Write;
  }

  // Drop the lock. If the lock is held in WRITE mode, and the
  // lock has not yet been released, aborts the mutation, restoring
  // the underlying object to its original data.
  ~CowLock() {
    unlock();
  }

 private:
  CowObject<State>* cow_;
  LockMode mode_;
};

// Scoped object that locks multiple CowObjects for reading or for writing.
// When locked for writing and mutations are completed, can also commit those
// mutations, which releases the lock.
//
// CowObjects are stored in an std::map, which provides two important
// properties:
// 1. addObject() can deduplicate CowObjects already inserted.
// 2. When locking for writing, the deterministic iteration order provided by
//    std::map prevents deadlocks.
//
// The use of std::map forces callers to provide a key for each CowObject. For
// a key implementation to be usable, an appropriate overload of operator<
// must be available.
//
// Unlike CowLock, does not mediate access to the CowObject data itself;
// callers should access the data out of band.
//
// Sample usage:
//
//   struct Foo {
//     string id_;
//     string data_;
//   };
//
//   vector<CowObject<Foo>> foos;
//
// 1. Locking a group of CowObjects for reading:
//
//   CowGroupLock<string, Foo> l(LockMode::Released);
//   for (const auto& f : foos) {
//     l.addObject(f.id_, f);
//   }
//   l.lock(LockMode::Read);
//   for (const auto& f : foos) {
//     cout << f.state().data_ << endl;
//   }
//   l.unlock();
//
// 2. Tracking already-write-locked CowObjects for group commit:
//
//   CowGroupLock<string, Foo> l(LockMode::Write);
//   for (const auto& f : foos) {
//     l.addObject(f.id_, f);
//     f.mutableDirty().data_ = "modified";
//   }
//   l.commit();
//
// 3. Aggregating unlocked CowObjects, locking them safely, and committing them
// together:
//
//   CowGroupLock<string, Foo> l(LockMode::Released);
//   for (const auto& f : foos) {
//     l.addObject(f.id_, f);
//   }
//   l.lock(LockMode::Write);
//   for (const auto& f : foos) {
//     f.mutableDirty().data_ = "modified";
//   }
//   l.commit();
template <class Key, class Value>
class CowGroupLock {
 public:
  explicit CowGroupLock(LockMode mode) : mode_(mode) {}

  ~CowGroupLock() {
    unlock();
  }

  void unlock() {
    switch (mode_) {
      case LockMode::Read:
        for (const auto& e : cows_) {
          e.second->readUnlock();
        }
        break;
      case LockMode::Write:
        for (const auto& e : cows_) {
          e.second->abortMutation();
        }
        break;
      default:
        DCHECK_EQ(LockMode::Released, mode_);
        break;
    }

    cows_.clear();
    mode_ = LockMode::Released;
  }

  void lock(LockMode newMode) {
    DCHECK_EQ(LockMode::Released, mode_);

    switch (newMode) {
      case LockMode::Read:
        for (const auto& e : cows_) {
          e.second->readLock();
        }
        break;
      case LockMode::Write:
        for (const auto& e : cows_) {
          e.second->startMutation();
        }
        break;
      default:
        LOG(FATAL) << "Cannot lock in mode " << newMode;
    }
    mode_ = newMode;
  }

  void commit() {
    DCHECK_EQ(LockMode::Write, mode_);
    for (const auto& e : cows_) {
      e.second->commitMutation();
    }
    cows_.clear();
    mode_ = LockMode::Released;
  }

  // Adds a new CowObject to be tracked by the lock guard. Does nothing if a
  // CowObject with the same key was already added.
  //
  // It is the responsibility of the caller to ensure:
  // 1. That 'object' remains alive until the lock is released.
  // 2. That if 'object' was already added, both objects point to the same
  //    memory address.
  // 3. That if the CowGroupLock is already locked in a particular mode,
  //    'object' is also already locked in that mode.
  void addObject(Key key, const CowObject<Value>* object) {
    auto r =
        cows_.emplace(std::move(key), const_cast<CowObject<Value>*>(object));
    DCHECK_EQ(r.first->second, object);
  }

  // Like the above, but for mutable objects.
  void addMutableObject(Key key, CowObject<Value>* object) {
    auto r = cows_.emplace(std::move(key), object);
    DCHECK_EQ(r.first->second, object);
  }

 private:
  std::map<Key, CowObject<Value>*> cows_;
  LockMode mode_;

  DISALLOW_COPY_AND_ASSIGN(CowGroupLock);
};

} // namespace kudu
