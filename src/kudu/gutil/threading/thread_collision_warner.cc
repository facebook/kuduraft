// Copyright (c) 2010 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/gutil/threading/thread_collision_warner.h"

#ifdef __linux__
#include <syscall.h>
#else
#include <sys/syscall.h>
#endif

#include <unistd.h>

#include <cstdint>
#include <ostream>

#include <glog/logging.h>

namespace base {

void DCheckAsserter::warn(int64_t previousThreadId, int64_t currentThreadId) {
  LOG(FATAL) << "Thread Collision! Previous thread id: " << previousThreadId
             << ", current thread id: " << currentThreadId;
}

#if 0
// Original source from Chromium -- we didn't import their threading library
// into Cloudera source as of yet

static subtle::Atomic32 CurrentThread() {
  const PlatformThreadId current_thread_id = PlatformThread::CurrentId();
  // We need to get the thread id into an atomic data type. This might be a
  // truncating conversion, but any loss-of-information just increases the
  // chance of a fault negative, not a false positive.
  const subtle::Atomic32 atomic_thread_id =
      static_cast<subtle::Atomic32>(current_thread_id);

  return atomic_thread_id;
}
#else

static subtle::Atomic64 currentThread() {
#if defined(__APPLE__)
  uint64_t tid;
  CHECK_EQ(0, pthread_threadid_np(NULL, &tid));
  return tid;
#elif defined(__linux__)
  return syscall(__NR_gettid);
#endif
}

#endif

void ThreadCollisionWarner::enterSelf() {
  // If the active thread is 0 then I'll write the current thread ID
  // if two or more threads arrive here only one will succeed to
  // write on validThreadId_ the current thread ID.
  subtle::Atomic64 currentThreadId = currentThread();

  int64_t previousThreadId =
      subtle::NoBarrier_CompareAndSwap(&validThreadId_, 0, currentThreadId);
  if (previousThreadId != 0 && previousThreadId != currentThreadId) {
    // gotcha! a thread is trying to use the same class and that is
    // not current thread.
    asserter_->warn(previousThreadId, currentThreadId);
  }

  subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::enter() {
  subtle::Atomic64 currentThreadId = currentThread();

  int64_t previousThreadId =
      subtle::NoBarrier_CompareAndSwap(&validThreadId_, 0, currentThreadId);
  if (previousThreadId != 0) {
    // gotcha! another thread is trying to use the same class.
    asserter_->warn(previousThreadId, currentThreadId);
  }

  subtle::NoBarrier_AtomicIncrement(&counter_, 1);
}

void ThreadCollisionWarner::leave() {
  if (subtle::Barrier_AtomicIncrement(&counter_, -1) == 0) {
    subtle::NoBarrier_Store(&validThreadId_, 0);
  }
}

} // namespace base
