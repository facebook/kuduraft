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

void MemTracker::CreateRootTracker() {
  rootTracker.reset(new MemTracker(-1, "root", shared_ptr<MemTracker>()));
  rootTracker->Init();
}

shared_ptr<MemTracker> MemTracker::CreateTracker(
    int64_t byteLimit,
    const string& id,
    shared_ptr<MemTracker> parent) {
  shared_ptr<MemTracker> realParent;
  if (parent) {
    realParent = std::move(parent);
  } else {
    realParent = GetRootTracker();
  }
  shared_ptr<MemTracker> tracker(new MemTracker(byteLimit, id, realParent));
  realParent->AddChildTracker(tracker);
  tracker->Init();

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
  VLOG(1) << "Creating tracker " << ToString();
}

MemTracker::~MemTracker() {
  VLOG(1) << "Destroying tracker " << ToString();
  if (parent_) {
    DCHECK(consumption() == 0)
        << "Memory tracker " << ToString() << " has unreleased consumption "
        << consumption();
    parent_->Release(consumption());

    MutexLock l(parent_->child_trackers_lock_);
    if (child_tracker_it_ != parent_->child_trackers_.end()) {
      parent_->child_trackers_.erase(child_tracker_it_);
      child_tracker_it_ = parent_->child_trackers_.end();
    }
  }
}

string MemTracker::ToString() const {
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

bool MemTracker::FindTracker(
    const string& id,
    shared_ptr<MemTracker>* tracker,
    const shared_ptr<MemTracker>& parent) {
  return FindTrackerInternal(id, tracker, parent ? parent : GetRootTracker());
}

bool MemTracker::FindTrackerInternal(
    const string& id,
    shared_ptr<MemTracker>* tracker,
    const shared_ptr<MemTracker>& parent) {
  DCHECK(parent != NULL);

  list<weak_ptr<MemTracker>> children;
  {
    MutexLock l(parent->child_trackers_lock_);
    children = parent->child_trackers_;
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
        parent->ToString());
    *tracker = found[0];
    return true;
  }
  return false;
}

shared_ptr<MemTracker> MemTracker::FindOrCreateGlobalTracker(
    int64_t byte_limit,
    const string& id) {
  // The calls below comprise a critical section, but we can't use the root
  // tracker's child_trackers_lock_ to synchronize it as the lock must be
  // released during FindTrackerInternal(). Since this function creates
  // globally-visible MemTrackers which are the exception rather than the rule,
  // it's reasonable to synchronize their creation on a singleton lock.
  static Mutex findOrCreateLock;
  MutexLock l(findOrCreateLock);

  shared_ptr<MemTracker> found;
  if (FindTrackerInternal(id, &found, GetRootTracker())) {
    return found;
  }
  return CreateTracker(byte_limit, id, GetRootTracker());
}

void MemTracker::ListTrackers(vector<shared_ptr<MemTracker>>* trackers) {
  trackers->clear();
  deque<shared_ptr<MemTracker>> toProcess;
  toProcess.push_front(GetRootTracker());
  while (!toProcess.empty()) {
    shared_ptr<MemTracker> t = toProcess.back();
    toProcess.pop_back();

    trackers->push_back(t);
    {
      MutexLock l(t->child_trackers_lock_);
      for (const auto& childWeak : t->child_trackers_) {
        shared_ptr<MemTracker> child = childWeak.lock();
        if (child) {
          toProcess.emplace_back(std::move(child));
        }
      }
    }
  }
}

void MemTracker::Consume(int64_t bytes) {
  if (bytes < 0) {
    Release(-bytes);
    return;
  }

  if (bytes == 0) {
    return;
  }
  for (auto& tracker : all_trackers_) {
    tracker->consumption_.IncrementBy(bytes);
  }
}

bool MemTracker::TryConsume(int64_t bytes) {
  if (bytes <= 0) {
    Release(-bytes);
    return true;
  }

  int i = 0;
  // Walk the tracker tree top-down, consuming memory from each in turn.
  for (i = all_trackers_.size() - 1; i >= 0; --i) {
    MemTracker* tracker = all_trackers_[i];
    if (tracker->limit_ < 0) {
      tracker->consumption_.IncrementBy(bytes);
    } else {
      if (!tracker->consumption_.TryIncrementBy(bytes, tracker->limit_)) {
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
  for (int j = all_trackers_.size() - 1; j > i; --j) {
    all_trackers_[j]->consumption_.IncrementBy(-bytes);
  }
  return false;
}

void MemTracker::Release(int64_t bytes) {
  if (bytes < 0) {
    Consume(-bytes);
    return;
  }

  if (bytes == 0) {
    return;
  }

  for (auto& tracker : all_trackers_) {
    tracker->consumption_.IncrementBy(-bytes);
  }
  process_memory::MaybeGCAfterRelease(bytes);
}

bool MemTracker::AnyLimitExceeded() {
  for (const auto& tracker : limit_trackers_) {
    if (tracker->LimitExceeded()) {
      return true;
    }
  }
  return false;
}

int64_t MemTracker::SpareCapacity() const {
  int64_t result = std::numeric_limits<int64_t>::max();
  for (const auto& tracker : limit_trackers_) {
    int64_t memLeft = tracker->limit() - tracker->consumption();
    result = std::min(result, memLeft);
  }
  return result;
}

void MemTracker::Init() {
  // populate all_trackers_ and limit_trackers_
  MemTracker* tracker = this;
  while (tracker) {
    all_trackers_.push_back(tracker);
    if (tracker->has_limit()) {
      limit_trackers_.push_back(tracker);
    }
    tracker = tracker->parent_.get();
  }
  DCHECK_GT(all_trackers_.size(), 0);
  DCHECK_EQ(all_trackers_[0], this);
}

void MemTracker::AddChildTracker(const shared_ptr<MemTracker>& tracker) {
  MutexLock l(child_trackers_lock_);
  tracker->child_tracker_it_ =
      child_trackers_.insert(child_trackers_.end(), tracker);
}

shared_ptr<MemTracker> MemTracker::GetRootTracker() {
  std::call_once(rootTrackerOnce, &CreateRootTracker);
  return rootTracker;
}

} // namespace kudu
