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

#include "kudu/util/kernel_stack_watchdog.h"

#include <cstdint>
#include <cstring>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/os-util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_int32(
    hung_task_check_interval_ms,
    200,
    "Number of milliseconds in between checks for hung threads");
TAG_FLAG(hung_task_check_interval_ms, hidden);

DEFINE_int32(
    inject_latency_on_kernel_stack_lookup_ms,
    0,
    "Number of milliseconds of latency to inject when reading a thread's "
    "kernel stack");
TAG_FLAG(inject_latency_on_kernel_stack_lookup_ms, hidden);

using std::lock_guard;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

__thread KernelStackWatchdog::Tls* KernelStackWatchdog::tls_;

KernelStackWatchdog::KernelStackWatchdog()
    : logCollector_(nullptr), finish_(1) {
  // During creation of the stack watchdog thread, we need to disable using
  // the stack watchdog itself. Otherwise, the 'StartThread' function will
  // try to call back into initializing the stack watchdog, and will
  // self-deadlock.
  CHECK_OK(
      Thread::CreateWithFlags(
          "kernel-watchdog",
          "kernel-watcher",
          boost::bind(&KernelStackWatchdog::runThread, this),
          Thread::kNoStackWatchdog,
          &thread_));
}

KernelStackWatchdog::~KernelStackWatchdog() {
  finish_.CountDown();
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
}

void KernelStackWatchdog::saveLogsForTests(bool saveLogs) {
  lock_guard<simple_spinlock> l(logLock_);
  if (saveLogs) {
    logCollector_.reset(new std::vector<string>());
  } else {
    logCollector_.reset();
  }
}

std::vector<string> KernelStackWatchdog::loggedMessagesForTests() const {
  lock_guard<simple_spinlock> l(logLock_);
  CHECK(logCollector_) << "Must call saveLogsForTests(true) first";
  return *logCollector_;
}

void KernelStackWatchdog::registerTls(Tls* tls) {
  int64_t tid = Thread::CurrentThreadId();
  lock_guard<simple_spinlock> l(tlsLock_);
  auto result = tlsByTid_.emplace(tid, tls);
  CHECK(result.second) << "Thread " << tid << " already registered";
}

void KernelStackWatchdog::unregisterTls() {
  int64_t tid = Thread::CurrentThreadId();

  std::unique_ptr<Tls> tls(tls_);
  {
    std::unique_lock<Mutex> l(unregisterLock_, std::try_to_lock);
    lock_guard<simple_spinlock> l2(tlsLock_);
    CHECK(tlsByTid_.erase(tid));
    if (!l.owns_lock()) {
      // The watchdog is in the middle of running and might be accessing
      // 'tls', so just enqueue it for later deletion. Otherwise it
      // will go out of scope at the end of this function and get
      // deleted here.
      pendingDelete_.emplace_back(std::move(tls));
    }
  }
  tls_ = nullptr;
}

Status getKernelStack(pid_t p, string* ret) {
  MAYBE_INJECT_FIXED_LATENCY(FLAGS_inject_latency_on_kernel_stack_lookup_ms);
  faststring buf;
  RETURN_NOT_OK(
      ReadFileToString(Env::Default(), fmt::format("/proc/{}/stack", p), &buf));
  *ret = buf.ToString();
  return Status::OK();
}

void KernelStackWatchdog::runThread() {
  while (true) {
    MonoDelta delta =
        MonoDelta::FromMilliseconds(FLAGS_hung_task_check_interval_ms);
    if (finish_.WaitFor(delta)) {
      // Watchdog exiting.
      break;
    }

    // Don't send signals while the debugger is running, since it makes it hard
    // to use.
    if (isBeingDebugged()) {
      continue;
    }

    // Prevent threads from deleting their Tls objects between the snapshot loop
    // and the sending of signals. This makes it safe for us to access their
    // Tls.
    //
    // NOTE: it's still possible that the thread will have exited in between
    // grabbing its pointer and sending a signal, but DumpThreadStack() already
    // is safe about not sending a signal to some other non-Kudu thread.
    MutexLock l(unregisterLock_);

    // Take the snapshot of the thread information under a short lock.
    //
    // 'tlsLock_' prevents new threads from starting, so we don't want to do
    // any lengthy work (such as gathering stack traces) under this lock.
    TlsMap tlsMapCopy;
    vector<unique_ptr<Tls>> toDelete;
    {
      lock_guard<simple_spinlock> l2(tlsLock_);
      toDelete.swap(pendingDelete_);
      tlsMapCopy = tlsByTid_;
    }
    // Actually delete the no-longer-used Tls entries outside of the lock.
    toDelete.clear();

    kudu::MicrosecondsInt64 now = GetMonoTimeMicros();
    for (const auto& entry : tlsMapCopy) {
      pid_t p = entry.first;
      Tls::Data* tls = &entry.second->data_;
      Tls::Data tlsCopy;
      tls->snapshotCopy(&tlsCopy);
      for (int i = 0; i < tlsCopy.depth_; i++) {
        const Tls::Frame* frame = &tlsCopy.frames_[i];

        int pausedMs = (now - frame->startTime_) / 1000;
        if (pausedMs > frame->thresholdMs_) {
          string kernelStack;
          Status s = getKernelStack(p, &kernelStack);
          if (!s.ok()) {
            // Can't read the kernel stack of the pid, just ignore it.
            kernelStack = "(could not read kernel stack)";
          }

          string userStack = DumpThreadStack(p);

          // If the thread exited the frame we're looking at in between when we
          // started grabbing the stack and now, then our stack isn't correct.
          // We shouldn't log it.
          //
          // We just use unprotected reads here since this is a somewhat
          // best-effort check.
          if (KUDU_ANNONTATE_UNPROTECTED_READ(tls->depth_) < tlsCopy.depth_ ||
              KUDU_ANNONTATE_UNPROTECTED_READ(tls->frames_[i].startTime_) !=
                  frame->startTime_) {
            break;
          }

          lock_guard<simple_spinlock> l2(logLock_);
          LOG_STRING(WARNING, logCollector_.get())
              << "Thread " << p << " stuck at " << frame->status_ << " for "
              << pausedMs << "ms" << ":\n"
              << "Kernel stack:\n"
              << kernelStack << "\n"
              << "User stack:\n"
              << userStack;
        }
      }
    }
  }
}

void KernelStackWatchdog::threadExiting(void* /* unused */) {
  KernelStackWatchdog::getInstance()->unregisterTls();
}

void KernelStackWatchdog::createAndRegisterTls() {
  DCHECK(!tls_);
  // Disable leak check. LSAN sometimes gets false positives on thread locals.
  // See: https://github.com/google/sanitizers/issues/757
  debug::ScopedLeakCheckDisabler d;
  auto* tls = new Tls();
  KernelStackWatchdog::getInstance()->registerTls(tls);
  tls_ = tls;
  kudu::threadlocal::internal::addDestructor(&threadExiting, nullptr);
}

KernelStackWatchdog::Tls::Tls() {
  memset(&data_, 0, sizeof(data_));
}

KernelStackWatchdog::Tls::~Tls() {}

// Optimistic concurrency control approach to snapshot the value of another
// thread's Tls, even though that thread might be changing it.
//
// Called by the watchdog thread to see if a target thread is currently in the
// middle of a watched section.
void KernelStackWatchdog::Tls::Data::snapshotCopy(Data* copy) const {
  while (true) {
    Atomic32 v0 = base::subtle::Acquire_Load(&seqLock_);
    if (v0 & 1) {
      // If the value is odd, then the thread is in the middle of modifying
      // its Tls, and we have to spin.
      base::subtle::PauseCPU();
      continue;
    }
    KUDU_ANNONTATE_IGNORE_READS_BEGIN();
    memcpy(copy, this, sizeof(*copy));
    KUDU_ANNONTATE_IGNORE_READS_END();
    Atomic32 v1 = base::subtle::Release_Load(&seqLock_);

    // If the value hasn't changed since we started the copy, then
    // we know that the copy was a consistent snapshot.
    if (v1 == v0) {
      break;
    }
  }
}

} // namespace kudu
