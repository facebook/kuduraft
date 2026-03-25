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
//
// This class defines a singleton thread which manages a map of other thread IDs
// to watch. Before performing some operation which may stall (eg IO) or which
// we expect should be short (e.g. a callback on a critical thread that should
// not block), threads may mark themselves as "watched", with a threshold beyond
// which they would like warnings to be emitted including their stack trace at
// that time.
//
// In the background, a separate watchdog thread periodically wakes up, and if a
// thread has been marked longer than its provided threshold, it will dump the
// stack trace of that thread (both kernel-mode and user-mode stacks).
//
// This can be useful for diagnosing I/O stalls coming from the kernel, for
// example.
//
// Users will typically use the macro SCOPED_WATCH_STACK. Example usage:
//
//   // We expect the Write() to return in <100ms. If it takes longer than that
//   // we'll see warnings indicating why it is stalled.
//   {
//     SCOPED_WATCH_STACK(100);
//     file->Write(...);
//   }
//
// If the Write call takes too long, a stack trace will be logged at WARNING
// level. Note that the threshold time parameter is not a guarantee that a stall
// will be caught by the watchdog thread. The watchdog only wakes up
// periodically to look for threads that have been stalled too long. For
// example, if the threshold is 10ms and the thread blocks for only 20ms, it's
// quite likely that the watchdog will have missed the event.
//
// The SCOPED_WATCH_STACK macro is designed to have minimal overhead:
// approximately equivalent to a clock_gettime() and a single 'mfence'
// instruction. Micro-benchmarks measure the cost at about 50ns per call. Thus,
// it may safely be used in hot code paths.
//
// Scopes with SCOPED_WATCH_STACK may be nested, but only up to a hard-coded
// limited depth (currently 8).
#ifndef KUDU_UTIL_KERNEL_STACK_WATCHDOG_H
#define KUDU_UTIL_KERNEL_STACK_WATCHDOG_H

#include <ctime>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/mutex.h"
#include "kudu/util/threadlocal.h"

#define SCOPED_WATCH_STACK(threshold_ms) \
  ScopedWatchKernelStack _stack_watcher( \
      __FILE__ ":" AS_STRING(__LINE__), threshold_ms)

namespace kudu {

class Thread;

// Singleton thread which implements the watchdog.
class KernelStackWatchdog {
 public:
  static KernelStackWatchdog* getInstance() {
    return Singleton<KernelStackWatchdog>::get();
  }

  // Instead of logging through glog, log warning messages into a vector.
  //
  // If 'saveLogs' is true, will start saving to the vector, and forget any
  // previously logged messages.
  // If 'saveLogs' is false, disables this functionality.
  void saveLogsForTests(bool saveLogs);

  // Return any log messages saved since the last call to
  // saveLogsForTests(true).
  std::vector<std::string> loggedMessagesForTests() const;

 private:
  friend class Singleton<KernelStackWatchdog>;
  friend class ScopedWatchKernelStack;

  // The thread-local state which captures whether a thread should be watched by
  // the watchdog. This structure is constructed as a thread-local on first use
  // and destructed when the thread exits. Upon construction, the Tls structure
  // registers itself with the WatchDog, and on destruction, unregisters itself.
  //
  // See 'seqLock' below for details on thread-safe operation.
  struct Tls {
    Tls();
    ~Tls();

    enum Constants {
      // The maximum nesting depth of SCOPED_WATCH_STACK() macros.
      kMaxDepth = 8
    };

    // Because we support nested SCOPED_WATCH_STACK() macros, we need to capture
    // multiple active frames within the Tls.
    struct Frame {
      // The time at which this frame entered the SCOPED_WATCH_STACK section.
      // We use MicrosecondsInt64 instead of MonoTime because it inlines a bit
      // better.
      kudu::MicrosecondsInt64 startTime;
      // The threshold of time beyond which the watchdog should emit warnings.
      int thresholdMs;
      // A string explaining the state that the thread is in (typically a
      // file:line string). This is expected to be static storage and is not
      // freed.
      const char* status;
    };

    // The data within the Tls. This is a POD type so that the watchdog can
    // easily copy data out of a thread's Tls.
    struct Data {
      Frame frames[kMaxDepth];
      Atomic32 depth;

      // Counter implementing a simple "sequence lock".
      //
      // Before modifying any data inside its Tls, the watched thread increments
      // this value so it is odd. When the modifications are complete, it
      // increments it again, making it even.
      //
      // To read the Tls data from a target thread, the watchdog thread waits
      // for the value to become even, indicating that no write is in progress.
      // Then, it does a potentially racy copy of the entire 'Data' structure.
      // Then, it validates the value again. If it is has not changed, then the
      // snapshot is guaranteed to be consistent.
      //
      // We use this type of locking to ensure that the watched thread is as
      // fast as possible, allowing us to use SCOPED_WATCH_STACK even in hot
      // code paths. In particular, the watched thread is wait-free, since it
      // doesn't need to loop or retry. In addition, the memory is only written
      // by that thread, eliminating any cache-line bouncing. The watchdog
      // thread may have to loop multiple times to see a consistent snapshot,
      // but we're OK delaying the watchdog arbitrarily since it isn't on any
      // critical path.
      Atomic32 seqLock;

      // Take a consistent snapshot of this data into 'dst'. This may block if
      // the target thread is currently modifying its Tls.
      void snapshotCopy(Data* dst) const;
    };
    Data data;
  };

  KernelStackWatchdog();
  ~KernelStackWatchdog();

  // Get or create the Tls for the current thread.
  static Tls* getTls() {
    if (PREDICT_FALSE(!tls_)) {
      createAndRegisterTls();
    }
    return tls_;
  }

  // Create a new Tls for the current thread, and register it with the watchdog.
  // Installs a callback to automatically unregister the thread upon its exit.
  static void createAndRegisterTls();

  // Callback which is registered to run at thread-exit time by
  // createAndRegisterTls().
  static void threadExiting(void* tlsVoid);

  // Register a new thread's Tls with the watchdog.
  // Called by any thread the first time it enters a watched section, when its
  // Tls is constructed.
  void registerTls(Tls* tls);

  // Called when a thread is in the process of exiting, and has a registered Tls
  // object.
  void unregisterTls();

  // The actual watchdog loop that the watchdog thread runs.
  void runThread();

  DECLARE_STATIC_THREAD_LOCAL(Tls, tls_);

  using TlsMap = std::unordered_map<pid_t, Tls*>;
  TlsMap tlsByTid_;

  // If a thread exits while the watchdog is in the middle of accessing the Tls
  // objects, we can't immediately delete the Tls struct. Instead, the thread
  // enqueues it here for later deletion by the watchdog thread within
  // runThread().
  std::vector<std::unique_ptr<Tls>> pendingDelete_;

  // If non-NULL, warnings will be emitted into this vector instead of glog.
  // Used by tests.
  std::unique_ptr<std::vector<std::string>> logCollector_;

  // Lock protecting logCollector_.
  mutable SimpleSpinlock logLock_;

  // Lock protecting tlsByTid_ and pendingDelete_.
  mutable SimpleSpinlock tlsLock_;

  // Lock which prevents threads from unregistering while the watchdog
  // sends signals.
  //
  // This is used to prevent the watchdog from sending a signal to a pid just
  // after the pid has actually exited and been reused. Sending a signal to
  // a non-Kudu thread could have unintended consequences.
  //
  // When this lock is held concurrently with 'tlsLock_' or 'logLock_',
  // this lock must be acquired first.
  Mutex unregisterLock_;

  // The watchdog thread itself.
  std::shared_ptr<Thread> thread_;

  // Signal to stop the watchdog.
  CountDownLatch finish_;

  DISALLOW_COPY_AND_ASSIGN(KernelStackWatchdog);
};

// Scoped object which marks the current thread for watching.
class ScopedWatchKernelStack {
 public:
  // If the current scope is active more than 'thresholdMs' milliseconds, the
  // watchdog thread will log a warning including the message 'label'. 'label'
  // is not copied or freed.
  ScopedWatchKernelStack(const char* label, int thresholdMs) {
    if (thresholdMs <= 0) {
      return;
    }

    // Rather than just using the lazy getTls() method, we'll first try to load
    // the Tls ourselves. This is usually successful, and avoids us having to
    // inline the Tls construction path at call sites.
    KernelStackWatchdog::Tls* tls = KernelStackWatchdog::tls_;
    if (PREDICT_FALSE(tls == NULL)) {
      tls = KernelStackWatchdog::getTls();
    }
    KernelStackWatchdog::Tls::Data* tlsData = &tls->data;

    // "Acquire" the sequence lock. While the lock value is odd, readers will
    // block.
    // TODO: technically this barrier is stronger than we need: we are the only
    // writer to this data, so it's OK to allow loads from within the critical
    // section to reorder above this next line. All we need is a "StoreStore"
    // barrier (i.e. prevent any stores in the critical section from getting
    // reordered above the increment of the counter). However, atomicops.h
    // doesn't provide such a barrier as of yet, so we'll do the slightly more
    // expensive one for now.
    base::subtle::Acquire_Store(&tlsData->seqLock, tlsData->seqLock + 1);

    KernelStackWatchdog::Tls::Frame* frame = &tlsData->frames[tlsData->depth++];
    DCHECK_LE(tlsData->depth, KernelStackWatchdog::Tls::kMaxDepth);
    frame->startTime = getMonoTimeMicros();
    frame->thresholdMs = thresholdMs;
    frame->status = label;

    // "Release" the sequence lock. This resets the lock value to be even, so
    // readers will proceed.
    base::subtle::Release_Store(&tlsData->seqLock, tlsData->seqLock + 1);
  }

  ~ScopedWatchKernelStack() {
    if (!KernelStackWatchdog::tls_) {
      return;
    }

    KernelStackWatchdog::Tls::Data* tls = &KernelStackWatchdog::tls_->data;
    int d = tls->depth;
    DCHECK_GT(d, 0);

    // We don't bother with a lock/unlock, because the change we're making here
    // is atomic. If we race with the watchdog, either they'll see the old
    // depth or the new depth, but in either case the underlying data is
    // perfectly valid.
    base::subtle::NoBarrier_Store(&tls->depth, d - 1);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedWatchKernelStack);
};

} // namespace kudu
#endif /* KUDU_UTIL_KERNEL_STACK_WATCHDOG_H */
