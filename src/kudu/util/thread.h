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
// Copied from Impala and adapted to Kudu.

#ifndef KUDU_UTIL_THREAD_H
#define KUDU_UTIL_THREAD_H

#include <pthread.h>
#if defined(__linux__)
#include <syscall.h>
#else
#include <sys/syscall.h>
#endif
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/function.hpp> // IWYU pragma: keep

#include <folly/synchronization/Baton.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

class MetricEntity;
class Thread;
class WebCallbackRegistry;

// Utility to join on a thread, printing warning messages if it
// takes too long. For example:
//
//   ThreadJoiner(&my_thread, "processing thread")
//     .warn_after_ms(1000)
//     .warn_every_ms(5000)
//     .Join();
//
// TODO: would be nice to offer a way to use ptrace() or signals to
// dump the stack trace of the thread we're trying to join on if it
// gets stuck. But, after looking for 20 minutes or so, it seems
// pretty complicated to get right.
class ThreadJoiner {
 public:
  explicit ThreadJoiner(Thread* thread);

  // Start emitting warnings after this many milliseconds.
  //
  // Default: 1000 ms.
  ThreadJoiner& warn_after_ms(int ms);

  // After the warnings after started, emit another warning at the
  // given interval.
  //
  // Default: 1000 ms.
  ThreadJoiner& warn_every_ms(int ms);

  // If the thread has not stopped after this number of milliseconds, give up
  // joining on it and return Status::Aborted.
  //
  // -1 (the default) means to wait forever trying to join.
  ThreadJoiner& give_up_after_ms(int ms);

  // Join the thread, subject to the above parameters. If the thread joining
  // fails for any reason, returns RuntimeError. If it times out, returns
  // Aborted.
  Status Join();

 private:
  enum {
    kDefaultWarnAfterMs = 1000,
    kDefaultWarnEveryMs = 1000,
    kDefaultGiveUpAfterMs = -1 // forever
  };

  Thread* thread_;

  int warn_after_ms_;
  int warn_every_ms_;
  int give_up_after_ms_;

  DISALLOW_COPY_AND_ASSIGN(ThreadJoiner);
};

// Thin wrapper around pthread that can register itself with the singleton
// ThreadMgr (a private class implemented in thread.cc entirely, which tracks
// all live threads so that they may be monitored via the debug webpages). This
// class has a limited subset of boost::thread's API. Construction is almost the
// same, but clients must supply a category and a name for each thread so that
// they can be identified in the debug web UI. Otherwise, Join() is the only
// supported method from boost::thread.
//
// Each Thread object knows its operating system thread ID (TID), which can be
// used to attach debuggers to specific threads, to retrieve resource-usage
// statistics from the operating system, and to assign threads to resource
// control groups.
//
// Threads are shared objects, but in a degenerate way. They may only have
// up to two referents: the caller that created the thread (parent), and
// the thread itself (child). Moreover, the only two methods to mutate state
// (Join() and the destructor) are constrained: the child may not Join() on
// itself, and the destructor is only run when there's one referent left.
// These constraints allow us to access thread internals without any locks.
class Thread : public std::enable_shared_from_this<Thread> {
 public:
  // Flags passed to Thread::CreateWithFlags().
  enum CreateFlags {
    NO_FLAGS = 0,

    // Disable the use of KernelStackWatchdog to detect and log slow
    // thread creations. This is necessary when starting the kernel stack
    // watchdog thread itself to avoid reentrancy.
    NO_STACK_WATCHDOG = 1 << 0
  };

  // This constructor pattern mimics that in boost::thread. There is
  // one constructor for each number of arguments that the thread
  // function accepts. To extend the set of acceptable signatures, add
  // another constructor with <class F, class A1.... class An>.
  //
  // In general:
  //  - category: string identifying the thread category to which this thread
  //  belongs,
  //    used for organising threads together on the debug UI.
  //  - name: name of this thread. Will be appended with "-<thread-id>" to
  //  ensure
  //    uniqueness.
  //  - F - a method type that supports operator(), and the instance passed to
  //  the
  //    constructor is executed immediately in a separate thread.
  //  - A1...An - argument types whose instances are passed to f(...)
  //  - holder - optional shared pointer to hold a reference to the created
  //  thread.
  template <class F>
  static Status CreateWithFlags(
      const std::string& category,
      const std::string& name,
      const F& f,
      uint64_t flags,
      std::shared_ptr<Thread>* holder) {
    return StartThread(category, name, f, flags, holder);
  }
  template <class F>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      std::shared_ptr<Thread>* holder) {
    return StartThread(category, name, f, NO_FLAGS, holder);
  }

  template <class F, class A1>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      std::shared_ptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1), NO_FLAGS, holder);
  }

  template <class F, class A1, class A2>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      const A2& a2,
      std::shared_ptr<Thread>* holder) {
    return StartThread(
        category, name, boost::bind(f, a1, a2), NO_FLAGS, holder);
  }

  template <class F, class A1, class A2, class A3>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      const A2& a2,
      const A3& a3,
      std::shared_ptr<Thread>* holder) {
    return StartThread(
        category, name, boost::bind(f, a1, a2, a3), NO_FLAGS, holder);
  }

  template <class F, class A1, class A2, class A3, class A4>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      const A2& a2,
      const A3& a3,
      const A4& a4,
      std::shared_ptr<Thread>* holder) {
    return StartThread(
        category, name, boost::bind(f, a1, a2, a3, a4), NO_FLAGS, holder);
  }

  template <class F, class A1, class A2, class A3, class A4, class A5>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      const A2& a2,
      const A3& a3,
      const A4& a4,
      const A5& a5,
      std::shared_ptr<Thread>* holder) {
    return StartThread(
        category, name, boost::bind(f, a1, a2, a3, a4, a5), NO_FLAGS, holder);
  }

  template <class F, class A1, class A2, class A3, class A4, class A5, class A6>
  static Status Create(
      const std::string& category,
      const std::string& name,
      const F& f,
      const A1& a1,
      const A2& a2,
      const A3& a3,
      const A4& a4,
      const A5& a5,
      const A6& a6,
      std::shared_ptr<Thread>* holder) {
    return StartThread(
        category,
        name,
        boost::bind(f, a1, a2, a3, a4, a5, a6),
        NO_FLAGS,
        holder);
  }

  // Emulates boost::thread and detaches.
  ~Thread();

  // Blocks until this thread finishes execution. Once this method returns, the
  // thread will be unregistered with the ThreadMgr and will not appear in the
  // debug UI.
  void Join() {
    ThreadJoiner(this).Join();
  }

  // The thread ID assigned to this thread by the operating system. If the
  // thread has not yet started running, returns INVALID_TID.
  //
  // With Baton-based synchronization, StartThread() waits for the child thread
  // to initialize before returning, so tid() will always return a valid TID
  // or INVALID_TID (never an intermediate state).
  int64_t tid() const {
    return base::subtle::Acquire_Load(&tid_);
  }

  // Returns the thread's pthread ID.
  pthread_t pthread_id() const {
    return thread_;
  }

  const std::string& name() const {
    return name_;
  }
  const std::string& category() const {
    return category_;
  }

  // Return a string representation of the thread identifying information.
  std::string ToString() const;

  // The current thread of execution, or NULL if the current thread isn't a
  // kudu::Thread. This call is signal-safe.
  static Thread* current_thread() {
    return tls_;
  }

  // Returns a unique, stable identifier for this thread. Note that this is a
  // static method and thus can be used on any thread, including the main thread
  // of the process.
  //
  // In general, this should be used when a value is required that is unique to
  // a thread and must work on any thread including the main process thread.
  //
  // NOTE: this is _not_ the TID, but rather a unique value assigned by the
  // thread implementation. So, this value should not be presented to the user
  // in log messages, etc.
  static int64_t UniqueThreadId() {
#if defined(__linux__)
    // This cast is a little bit ugly, but it is significantly faster than
    // calling syscall(SYS_gettid). In particular, this speeds up some code
    // paths in the tracing implementation.
    return static_cast<int64_t>(pthread_self());
#elif defined(__APPLE__)
    uint64_t tid;
    CHECK_EQ(0, pthread_threadid_np(NULL, &tid));
    return tid;
#else
#error Unsupported platform
#endif
  }

  // Returns the system thread ID (tid on Linux) for the current thread. Note
  // that this is a static method and thus can be used from any thread,
  // including the main thread of the process. This is in contrast to
  // Thread::tid(), which only works on kudu::Threads.
  //
  // Thread::tid() will return the same value, but the value is cached in the
  // Thread object, so will be faster to call.
  //
  // Thread::UniqueThreadId() (or Thread::tid()) should be preferred for
  // performance sensistive code, however it is only guaranteed to return a
  // unique and stable thread ID, not necessarily the system thread ID.
  static int64_t CurrentThreadId() {
#if defined(__linux__)
    return syscall(SYS_gettid);
#else
    return UniqueThreadId();
#endif
  }

 private:
  friend class ThreadJoiner;

  // See 'tid_' docs.
  enum {
    INVALID_TID = -1,
  };

  // Function object that wraps the user-supplied function to run in a separate
  // thread.
  using ThreadFunctor = boost::function<void()>;

  Thread(std::string category, std::string name, ThreadFunctor functor)
      : thread_(0),
        category_(std::move(category)),
        name_(std::move(name)),
        tid_(INVALID_TID),
        functor_(std::move(functor)),
        done_(1),
        joinable_(false) {}

  // Library-specific thread ID.
  pthread_t thread_;

  // Name and category for this thread.
  const std::string category_;
  const std::string name_;

  // OS-specific thread ID. Once the constructor finishes StartThread(),
  // guaranteed to be set either to a non-negative integer, or to INVALID_TID.
  //
  // The tid_ member goes through the following states:
  // 1. INVALID_TID: the thread has not been started, or has already exited.
  // 2. <positive value>: the thread is running.
  //
  // With Baton-based synchronization, StartThread() waits for the child thread
  // to set the TID before returning, so there's no intermediate state.
  int64_t tid_;

  // User function to be executed by this thread.
  const ThreadFunctor functor_;

  // Joiners wait on this latch to be notified if the thread is done.
  //
  // Note that Joiners must additionally pthread_join(), otherwise certain
  // resources that callers expect to be destroyed (like TLS) may still be
  // alive when a Joiner finishes.
  CountDownLatch done_;

  bool joinable_;

  // Thread local pointer to the current thread of execution. Will be NULL if
  // the current thread is not a Thread.
  static __thread Thread* tls_;

  // Struct to pass both the Thread shared_ptr and synchronization Baton to
  // SuperviseThread.
  struct SuperviseArgs {
    std::shared_ptr<Thread> thread;
    folly::Baton<>* ready_baton;
  };

  // Starts the thread running SuperviseThread(), and returns once that thread
  // has initialised and its TID has been read. Waits for notification from the
  // started thread that initialisation is complete before returning. On
  // success, stores a reference to the thread in holder.
  static Status StartThread(
      const std::string& category,
      const std::string& name,
      const ThreadFunctor& functor,
      uint64_t flags,
      std::shared_ptr<Thread>* holder);

  // Wrapper for the user-supplied function. Invoked from the new thread,
  // with SuperviseArgs as its only argument. Executes functor_, but before
  // doing so registers with the global ThreadMgr and reads the thread's
  // system ID. After functor_ terminates, unregisters with the ThreadMgr.
  // Always returns NULL.
  //
  // SuperviseThread() posts to the ready_baton once it has taken ownership
  // of the Thread shared_ptr, allowing StartThread() to safely return.
  // This ensures the Thread object stays alive even if the caller drops
  // its reference immediately after StartThread() returns.
  static void* SuperviseThread(void* arg);

  // Invoked when the user-supplied function finishes or in the case of an
  // abrupt exit (i.e. pthread_exit()). Cleans up after SuperviseThread().
  static void FinishThread(void* arg);
};

// Registers /threadz with the debug webserver, and creates thread-tracking
// metrics under the given entity. If 'web' is NULL, does not register the path
// handler.
Status StartThreadInstrumentation(
    const std::shared_ptr<MetricEntity>& server_metrics,
    WebCallbackRegistry* web);

// Container class for any details we want to capture about a thread
// TODO: Add start-time.
// TODO: Track fragment ID.
// fb: Move to header file, so can be used by plugin
class ThreadDescriptor {
 public:
  ThreadDescriptor() {}
  ThreadDescriptor(std::string category, std::string name, int64_t thread_id)
      : name_(std::move(name)),
        category_(std::move(category)),
        thread_id_(thread_id) {}

  const std::string& name() const {
    return name_;
  }
  const std::string& category() const {
    return category_;
  }
  int64_t thread_id() const {
    return thread_id_;
  }
  int priority() const {
    return priority_;
  }
  void setPriority(int p) {
    priority_ = p;
  }

 private:
  // Thread name
  std::string name_;

  // Thread pool name
  std::string category_;

  // Thread's OS pid
  int64_t thread_id_;

  // Thread priority in NICE value
  int priority_;
};

// Show the status of all kudu threads, see ThreadDescriptor for what detailed
// information is included for each thread.
//
// @param threads Output parameters for all thread info.
// @return Status:OK if succeed
Status GlobalShowThreadStatus(std::vector<ThreadDescriptor>* threads);

// Change thread priority for a particular category, this not only changes the
// current threads belong to that category, but also future threads spawned in
// that category.
//
// @param category In the other words, thread pool name
// @param priority thread priority based on nice. Should be -20 to 19
// @return Status:OK if succeed
Status GlobalChangeThreadPriority(std::string category, int priority);
} // namespace kudu

#endif /* KUDU_UTIL_THREAD_H */
