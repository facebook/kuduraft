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

#include "kudu/util/mem_tracker.h"

#include <algorithm>
#include <cstddef>
#include <deque>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>

#include <fmt/core.h>
#include "kudu/gutil/port.h"
#include "kudu/util/mutex.h"
#include "kudu/util/process_memory.h"

namespace kudu {

// NOTE: this class has been adapted from Impala, so the code style varies
// somewhat from kudu.

using std::deque;
using std::list;
using std::shared_ptr;
using std::string;
using std::vector;
using std::weak_ptr;

// The ancestor for all trackers. Every tracker is visible from the root down.
static shared_ptr<MemTracker> rootTracker;
static std::once_flag rootTrackerOnce;

void MemTracker::createRootTracker() {
  rootTracker.reset(new MemTracker(-1, "root", shared_ptr<MemTracker>()));
  rootTracker->init();
}

shared_ptr<MemTracker> MemTracker::createTracker(
    int64_t byteLimit,
    const string& id,
    shared_ptr<MemTracker> parent) {
  shared_ptr<MemTracker> realParent;
  if (parent) {
    realParent = std::move(parent);
  } else {
    realParent = getRootTracker();
  }
  shared_ptr<MemTracker> tracker(new MemTracker(byteLimit, id, realParent));
  realParent->addChildTracker(tracker);
  tracker->init();

  return tracker;
}

MemTracker::MemTracker(
    int64_t byteLimit,
    const string& id,
    shared_ptr<MemTracker> parent)
    : limit_(byteLimit),
      id_(id),
      descr_(fmt::format("memory consumption for {}", id)),
      parent_(std::move(parent)),
      consumption_(0) {
  VLOG(1) << "Creating tracker " << toString();
}

MemTracker::~MemTracker() {
  VLOG(1) << "Destroying tracker " << toString();
  if (parent_) {
    DCHECK(consumption() == 0)
        << "Memory tracker " << toString() << " has unreleased consumption "
        << consumption();
    parent_->release(consumption());

    MutexLock l(parent_->childTrackersLock_);
    if (childTrackerIt_ != parent_->childTrackers_.end()) {
      parent_->childTrackers_.erase(childTrackerIt_);
      childTrackerIt_ = parent_->childTrackers_.end();
    }
  }
}

string MemTracker::toString() const {
  string s;
  const MemTracker* tracker = this;
  while (tracker) {
    if (s != "") {
      s += "->";
    }
    s += tracker->id();
    tracker = tracker->parent_.get();
  }
  return s;
}

bool MemTracker::findTracker(
    const string& id,
    shared_ptr<MemTracker>* tracker,
    const shared_ptr<MemTracker>& parent) {
  return findTrackerInternal(id, tracker, parent ? parent : getRootTracker());
}

bool MemTracker::findTrackerInternal(
    const string& id,
    shared_ptr<MemTracker>* tracker,
    const shared_ptr<MemTracker>& parent) {
  DCHECK(parent != NULL);

  list<weak_ptr<MemTracker>> children;
  {
    MutexLock l(parent->childTrackersLock_);
    children = parent->childTrackers_;
  }

  // Search for the matching child without holding the parent's lock.
  //
  // If the lock were held while searching, it'd be possible for 'child' to be
  // the last live ref to a tracker, which would lead to a recursive
  // acquisition of the parent lock during the 'child' destructor call.
  vector<shared_ptr<MemTracker>> found;
  for (const auto& childWeak : children) {
    shared_ptr<MemTracker> child = childWeak.lock();
    if (child && child->id() == id) {
      found.emplace_back(std::move(child));
    }
  }
  if (PREDICT_TRUE(found.size() == 1)) {
    *tracker = found[0];
    return true;
  } else if (found.size() > 1) {
    LOG(DFATAL) << fmt::format(
        "Multiple memtrackers with same id ({}) found on parent {}",
        id,
        parent->toString());
    *tracker = found[0];
    return true;
  }
  return false;
}

shared_ptr<MemTracker> MemTracker::findOrCreateGlobalTracker(
    int64_t byte_limit,
    const string& id) {
  // The calls below comprise a critical section, but we can't use the root
  // tracker's childTrackersLock_ to synchronize it as the lock must be
  // released during findTrackerInternal(). Since this function creates
  // globally-visible MemTrackers which are the exception rather than the rule,
  // it's reasonable to synchronize their creation on a singleton lock.
  static Mutex findOrCreateLock;
  MutexLock l(findOrCreateLock);

  shared_ptr<MemTracker> found;
  if (findTrackerInternal(id, &found, getRootTracker())) {
    return found;
  }
  return createTracker(byte_limit, id, getRootTracker());
}

void MemTracker::listTrackers(vector<shared_ptr<MemTracker>>* trackers) {
  trackers->clear();
  deque<shared_ptr<MemTracker>> toProcess;
  toProcess.push_front(getRootTracker());
  while (!toProcess.empty()) {
    shared_ptr<MemTracker> t = toProcess.back();
    toProcess.pop_back();

    trackers->push_back(t);
    {
      MutexLock l(t->childTrackersLock_);
      for (const auto& childWeak : t->childTrackers_) {
        shared_ptr<MemTracker> child = childWeak.lock();
        if (child) {
          toProcess.emplace_back(std::move(child));
        }
      }
    }
  }
}

void MemTracker::consume(int64_t bytes) {
  if (bytes < 0) {
    release(-bytes);
    return;
  }

  if (bytes == 0) {
    return;
  }
  for (auto& tracker : allTrackers_) {
    tracker->consumption_.incrementBy(bytes);
  }
}

bool MemTracker::tryConsume(int64_t bytes) {
  if (bytes <= 0) {
    release(-bytes);
    return true;
  }

  int i = 0;
  // Walk the tracker tree top-down, consuming memory from each in turn.
  for (i = allTrackers_.size() - 1; i >= 0; --i) {
    MemTracker* tracker = allTrackers_[i];
    if (tracker->limit_ < 0) {
      tracker->consumption_.incrementBy(bytes);
    } else {
      if (!tracker->consumption_.tryIncrementBy(bytes, tracker->limit_)) {
        break;
      }
    }
  }
  // Everyone succeeded, return.
  if (i == -1) {
    return true;
  }

  // Someone failed, roll back the ones that succeeded.
  // TODO(todd): this doesn't roll it back completely since the max values for
  // the updated trackers aren't decremented. The max values are only used
  // for error reporting so this is probably okay. Rolling those back is
  // pretty hard; we'd need something like 2PC.
  for (int j = allTrackers_.size() - 1; j > i; --j) {
    allTrackers_[j]->consumption_.incrementBy(-bytes);
  }
  return false;
}

void MemTracker::release(int64_t bytes) {
  if (bytes < 0) {
    consume(-bytes);
    return;
  }

  if (bytes == 0) {
    return;
  }

  for (auto& tracker : allTrackers_) {
    tracker->consumption_.incrementBy(-bytes);
  }
  process_memory::MaybeGCAfterRelease(bytes);
}

bool MemTracker::anyLimitExceeded() {
  for (const auto& tracker : limitTrackers_) {
    if (tracker->limitExceeded()) {
      return true;
    }
  }
  return false;
}

int64_t MemTracker::spareCapacity() const {
  int64_t result = std::numeric_limits<int64_t>::max();
  for (const auto& tracker : limitTrackers_) {
    int64_t memLeft = tracker->limit() - tracker->consumption();
    result = std::min(result, memLeft);
  }
  return result;
}

void MemTracker::init() {
  // populate allTrackers_ and limitTrackers_
  MemTracker* tracker = this;
  while (tracker) {
    allTrackers_.push_back(tracker);
    if (tracker->hasLimit()) {
      limitTrackers_.push_back(tracker);
    }
    tracker = tracker->parent_.get();
  }
  DCHECK_GT(allTrackers_.size(), 0);
  DCHECK_EQ(allTrackers_[0], this);
}

void MemTracker::addChildTracker(const shared_ptr<MemTracker>& tracker) {
  MutexLock l(childTrackersLock_);
  tracker->childTrackerIt_ =
      childTrackers_.insert(childTrackers_.end(), tracker);
}

shared_ptr<MemTracker> MemTracker::getRootTracker() {
  std::call_once(rootTrackerOnce, &createRootTracker);
  return rootTracker;
}

} // namespace kudu
