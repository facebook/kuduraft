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
#ifndef KUDU_UTIL_BLOCKING_QUEUE_H
#define KUDU_UTIL_BLOCKING_QUEUE_H

#include <unistd.h>
#include <list>
#include <memory>
#include <type_traits>
#include <vector>

#include "kudu/gutil/basictypes.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

// Return values for BlockingQueue::put()
enum QueueStatus { kQueueSuccess = 0, kQueueShutdown = 1, kQueueFull = 2 };

// Default logical length implementation: always returns 1.
struct DefaultLogicalSize {
  template <typename T>
  static size_t logicalSize(const T& /* unused */) {
    return 1;
  }
};

template <typename T, class LogicalSize = DefaultLogicalSize>
class BlockingQueue {
 public:
  // If T is a pointer, this will be the base type.  If T is not a pointer, you
  // can ignore this and the functions which make use of it.
  // Template substitution failure is not an error.
  using TVal = typename std::remove_pointer<T>::type;

  explicit BlockingQueue(size_t maxSize)
      : shutdown_(false),
        size_(0),
        maxSize_(maxSize),
        notEmpty_(&lock_),
        notFull_(&lock_) {}

  // If the queue holds a bare pointer, it must be empty on destruction, since
  // it may have ownership of the pointer.
  ~BlockingQueue() {
    DCHECK(list_.empty() || !std::is_pointer<T>::value)
        << "BlockingQueue holds bare pointers at destruction time";
  }

  BlockingQueue(const BlockingQueue&) = delete;
  BlockingQueue& operator=(const BlockingQueue&) = delete;
  BlockingQueue(BlockingQueue&&) = delete;
  BlockingQueue& operator=(BlockingQueue&&) = delete;

  // Get an element from the queue.  Returns false if we were shut down prior to
  // getting the element.
  bool blockingGet(T* out) {
    MutexLock l(lock_);
    while (true) {
      if (!list_.empty()) {
        *out = list_.front();
        list_.pop_front();
        decrementSizeUnlocked(*out);
        notFull_.signal();
        return true;
      }
      if (shutdown_) {
        return false;
      }
      notEmpty_.wait();
    }
  }

  // Get an element from the queue.  Returns false if the queue is empty and
  // we were shut down prior to getting the element.
  bool blockingGet(std::unique_ptr<TVal>* out) {
    T t = NULL;
    bool gotElement = blockingGet(&t);
    if (!gotElement) {
      return false;
    }
    out->reset(t);
    return true;
  }

  // Get all elements from the queue and append them to a vector.
  //
  // If 'deadline' passes and no elements have been returned from the
  // queue, returns Status::TimedOut(). If 'deadline' is uninitialized,
  // no deadline is used.
  //
  // If the queue has been shut down, but there are still elements waiting,
  // then it returns those elements as if the queue were not yet shut down.
  //
  // Returns:
  // - OK if successful
  // - TimedOut if the deadline passed
  // - Aborted if the queue shut down
  Status blockingDrainTo(std::vector<T>* out, MonoTime deadline = MonoTime()) {
    MutexLock l(lock_);
    while (true) {
      if (!list_.empty()) {
        out->reserve(list_.size());
        for (const T& elt : list_) {
          out->push_back(elt);
          decrementSizeUnlocked(elt);
        }
        list_.clear();
        notFull_.signal();
        return Status::OK();
      }
      if (PREDICT_FALSE(shutdown_)) {
        return Status::Aborted("");
      }
      if (!deadline.Initialized()) {
        notEmpty_.wait();
      } else if (PREDICT_FALSE(!notEmpty_.waitUntil(deadline))) {
        return Status::TimedOut("");
      }
    }
  }

  // Attempts to put the given value in the queue.
  // Returns:
  //   kQueueSuccess: if successfully inserted
  //   kQueueFull: if the queue has reached maxSize_
  //   kQueueShutdown: if someone has already called shutdown()
  QueueStatus put(const T& val) {
    MutexLock l(lock_);
    if (size_ >= maxSize_) {
      return kQueueFull;
    }
    if (shutdown_) {
      return kQueueShutdown;
    }
    list_.push_back(val);
    incrementSizeUnlocked(val);
    l.unlock();
    notEmpty_.signal();
    return kQueueSuccess;
  }

  // Returns the same as the other put() overload above.
  // If the element was inserted, the std::unique_ptr releases its contents.
  QueueStatus put(std::unique_ptr<TVal>* val) {
    QueueStatus s = put(val->get());
    if (s == kQueueSuccess) {
      ignoreResult<>(val->release());
    }
    return s;
  }

  // Gets an element for the queue; if the queue is full, blocks until
  // space becomes available. Returns false if we were shutdown prior
  // to enqueueing the element.
  bool blockingPut(const T& val) {
    MutexLock l(lock_);
    while (true) {
      if (shutdown_) {
        return false;
      }
      if (size_ < maxSize_) {
        list_.push_back(val);
        incrementSizeUnlocked(val);
        l.unlock();
        notEmpty_.signal();
        return true;
      }
      notFull_.wait();
    }
  }

  // Shut down the queue.
  // When a blocking queue is shut down, no more elements can be added to it,
  // and put() will return kQueueShutdown.
  // Existing elements will drain out of it, and then blockingGet will start
  // returning false.
  void shutdown() {
    MutexLock l(lock_);
    shutdown_ = true;
    notFull_.broadcast();
    notEmpty_.broadcast();
  }

 private:
  // Increments queue size. Must be called when 'lock_' is held.
  void incrementSizeUnlocked(const T& t) {
    size_ += LogicalSize::logicalSize(t);
  }

  // Decrements queue size. Must be called when 'lock_' is held.
  void decrementSizeUnlocked(const T& t) {
    size_ -= LogicalSize::logicalSize(t);
  }

  bool shutdown_;
  size_t size_;
  size_t maxSize_;
  mutable Mutex lock_;
  ConditionVariable notEmpty_;
  ConditionVariable notFull_;
  std::list<T> list_;
};

} // namespace kudu

#endif
