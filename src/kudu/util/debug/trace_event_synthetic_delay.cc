// Copyright 2014 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/util/debug/trace_event_synthetic_delay.h"

#include <cstring>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/singleton.h"

namespace {
const int kMaxSyntheticDelays = 32;
} // namespace

namespace kudu {
namespace debug {

TraceEventSyntheticDelayClock::TraceEventSyntheticDelayClock() {}
TraceEventSyntheticDelayClock::~TraceEventSyntheticDelayClock() {}

class TraceEventSyntheticDelayRegistry : public TraceEventSyntheticDelayClock {
 public:
  static TraceEventSyntheticDelayRegistry* getInstance();

  TraceEventSyntheticDelay* getOrCreateDelay(const char* name);
  void resetAllDelays();

  // TraceEventSyntheticDelayClock implementation.
  virtual MonoTime now() override;

 private:
  TraceEventSyntheticDelayRegistry();
  ~TraceEventSyntheticDelayRegistry() override = default;

  friend class Singleton<TraceEventSyntheticDelayRegistry>;

  Mutex lock_;
  TraceEventSyntheticDelay delays_[kMaxSyntheticDelays];
  TraceEventSyntheticDelay dummyDelay_;
  base::subtle::Atomic32 delayCount_;

  DISALLOW_COPY_AND_ASSIGN(TraceEventSyntheticDelayRegistry);
  TraceEventSyntheticDelayRegistry(TraceEventSyntheticDelayRegistry&&) = delete;
  TraceEventSyntheticDelayRegistry& operator=(
      TraceEventSyntheticDelayRegistry&&) = delete;
};

TraceEventSyntheticDelay::TraceEventSyntheticDelay()
    : mode_(kStatic), beginCount_(0), triggerCount_(0), clock_(nullptr) {}

TraceEventSyntheticDelay::~TraceEventSyntheticDelay() {}

TraceEventSyntheticDelay* TraceEventSyntheticDelay::lookup(
    const std::string& name) {
  return TraceEventSyntheticDelayRegistry::getInstance()->getOrCreateDelay(
      name.c_str());
}

void TraceEventSyntheticDelay::initialize(
    const std::string& name,
    TraceEventSyntheticDelayClock* clock) {
  name_ = name;
  clock_ = clock;
}

void TraceEventSyntheticDelay::setTargetDuration(
    const MonoDelta& targetDuration) {
  MutexLock lock(lock_);
  targetDuration_ = targetDuration;
  triggerCount_ = 0;
  beginCount_ = 0;
}

void TraceEventSyntheticDelay::setMode(Mode mode) {
  MutexLock lock(lock_);
  mode_ = mode;
}

void TraceEventSyntheticDelay::setClock(TraceEventSyntheticDelayClock* clock) {
  MutexLock lock(lock_);
  clock_ = clock;
}

void TraceEventSyntheticDelay::begin() {
  // Note that we check for a non-zero target duration without locking to keep
  // things quick for the common case when delays are disabled. Since the delay
  // calculation is done with a lock held, it will always be correct. The only
  // downside of this is that we may fail to apply some delays when the target
  // duration changes.
  KUDU_ANNONTATE_BENIGN_RACE(&targetDuration_, "Synthetic delay duration");
  if (!targetDuration_.Initialized()) {
    return;
  }

  MonoTime startTime = clock_->now();
  {
    MutexLock lock(lock_);
    if (++beginCount_ != 1) {
      return;
    }
    endTime_ = calculateEndTimeLocked(startTime);
  }
}

void TraceEventSyntheticDelay::beginParallel(MonoTime* outEndTime) {
  // See note in begin().
  KUDU_ANNONTATE_BENIGN_RACE(&targetDuration_, "Synthetic delay duration");
  if (!targetDuration_.Initialized()) {
    *outEndTime = MonoTime();
    return;
  }

  MonoTime startTime = clock_->now();
  {
    MutexLock lock(lock_);
    *outEndTime = calculateEndTimeLocked(startTime);
  }
}

void TraceEventSyntheticDelay::end() {
  // See note in begin().
  KUDU_ANNONTATE_BENIGN_RACE(&targetDuration_, "Synthetic delay duration");
  if (!targetDuration_.Initialized()) {
    return;
  }

  MonoTime endTime;
  {
    MutexLock lock(lock_);
    if (!beginCount_ || --beginCount_ != 0) {
      return;
    }
    endTime = endTime_;
  }
  if (endTime.Initialized()) {
    applyDelay(endTime);
  }
}

void TraceEventSyntheticDelay::endParallel(const MonoTime& endTime) {
  if (endTime.Initialized()) {
    applyDelay(endTime);
  }
}

MonoTime TraceEventSyntheticDelay::calculateEndTimeLocked(
    const MonoTime& startTime) {
  if (mode_ == kOneShot && triggerCount_++) {
    return MonoTime();
  } else if (mode_ == kAlternating && triggerCount_++ % 2) {
    return MonoTime();
  }
  return startTime + targetDuration_;
}

void TraceEventSyntheticDelay::applyDelay(const MonoTime& endTime) {
  TRACE_EVENT0("synthetic_delay", name_.c_str());
  while (clock_->now() < endTime) {
    // Busy loop.
  }
}

TraceEventSyntheticDelayRegistry*
TraceEventSyntheticDelayRegistry::getInstance() {
  return Singleton<TraceEventSyntheticDelayRegistry>::get();
}

TraceEventSyntheticDelayRegistry::TraceEventSyntheticDelayRegistry()
    : delayCount_(0) {}

TraceEventSyntheticDelay* TraceEventSyntheticDelayRegistry::getOrCreateDelay(
    const char* name) {
  // Try to find an existing delay first without locking to make the common case
  // fast.
  int delayCount = base::subtle::Acquire_Load(&delayCount_);
  for (int i = 0; i < delayCount; ++i) {
    if (!strcmp(name, delays_[i].name_.c_str())) {
      return &delays_[i];
    }
  }

  MutexLock lock(lock_);
  delayCount = base::subtle::Acquire_Load(&delayCount_);
  for (int i = 0; i < delayCount; ++i) {
    if (!strcmp(name, delays_[i].name_.c_str())) {
      return &delays_[i];
    }
  }

  DCHECK(delayCount < kMaxSyntheticDelays)
      << "must increase kMaxSyntheticDelays";
  if (delayCount >= kMaxSyntheticDelays) {
    return &dummyDelay_;
  }

  delays_[delayCount].initialize(std::string(name), this);
  base::subtle::Release_Store(&delayCount_, delayCount + 1);
  return &delays_[delayCount];
}

MonoTime TraceEventSyntheticDelayRegistry::now() {
  return MonoTime::Now();
}

void TraceEventSyntheticDelayRegistry::resetAllDelays() {
  MutexLock lock(lock_);
  int delayCount = base::subtle::Acquire_Load(&delayCount_);
  for (int i = 0; i < delayCount; ++i) {
    delays_[i].setTargetDuration(MonoDelta());
    delays_[i].setClock(this);
  }
}

void resetTraceEventSyntheticDelays() {
  TraceEventSyntheticDelayRegistry::getInstance()->resetAllDelays();
}

} // namespace debug
} // namespace kudu

namespace trace_event_internal {

ScopedSyntheticDelay::ScopedSyntheticDelay(
    const char* name,
    AtomicWord* implPtr)
    : delayImpl_(getOrCreateDelay(name, implPtr)) {
  delayImpl_->beginParallel(&endTime_);
}

ScopedSyntheticDelay::~ScopedSyntheticDelay() {
  delayImpl_->endParallel(endTime_);
}

kudu::debug::TraceEventSyntheticDelay* getOrCreateDelay(
    const char* name,
    AtomicWord* implPtr) {
  kudu::debug::TraceEventSyntheticDelay* delayImpl =
      reinterpret_cast<kudu::debug::TraceEventSyntheticDelay*>(
          base::subtle::Acquire_Load(implPtr));
  if (!delayImpl) {
    delayImpl = kudu::debug::TraceEventSyntheticDelayRegistry::getInstance()
                    ->getOrCreateDelay(name);
    base::subtle::Release_Store(
        implPtr, reinterpret_cast<AtomicWord>(delayImpl));
  }
  return delayImpl;
}

} // namespace trace_event_internal
