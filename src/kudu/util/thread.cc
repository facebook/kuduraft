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

#include "kudu/util/thread.h"

#if defined(__linux__)
#include <sys/capability.h>
#include <sys/prctl.h>
#endif // defined(__linux__)
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/smart_ptr/shared_ptr.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <folly/synchronization/Baton.h>

#include <fmt/core.h>
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/os-util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/web_callback_registry.h"

using boost::bind;
using boost::mem_fn;
using std::endl;
using std::map;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::vector;

METRIC_DEFINE_gauge_uint64(
    server,
    threads_started,
    "Threads Started",
    kudu::MetricUnit::kThreads,
    "Total number of threads started on this server",
    kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(
    server,
    threads_running,
    "Threads Running",
    kudu::MetricUnit::kThreads,
    "Current number of running threads");

METRIC_DEFINE_gauge_uint64(
    server,
    cpu_utime,
    "User CPU Time",
    kudu::MetricUnit::kMilliseconds,
    "Total user CPU time of the process",
    kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(
    server,
    cpu_stime,
    "System CPU Time",
    kudu::MetricUnit::kMilliseconds,
    "Total system CPU time of the process",
    kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(
    server,
    voluntary_context_switches,
    "Voluntary Context Switches",
    kudu::MetricUnit::kContextSwitches,
    "Total voluntary context switches",
    kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(
    server,
    involuntary_context_switches,
    "Involuntary Context Switches",
    kudu::MetricUnit::kContextSwitches,
    "Total involuntary context switches",
    kudu::EXPOSE_AS_COUNTER);

DEFINE_int32(
    thread_inject_start_latency_ms,
    0,
    "Number of ms to sleep when starting a new thread. (For tests).");
TAG_FLAG(thread_inject_start_latency_ms, hidden);
TAG_FLAG(thread_inject_start_latency_ms, unsafe);

namespace kudu {

static uint64_t getCpuUTime() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_utime.tv_sec * 1000UL + ru.ru_utime.tv_usec / 1000UL;
}

static uint64_t getCpuSTime() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_stime.tv_sec * 1000UL + ru.ru_stime.tv_usec / 1000UL;
}

static uint64_t getVoluntaryContextSwitches() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_nvcsw;
  ;
}

static uint64_t getInVoluntaryContextSwitches() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_nivcsw;
}

class ThreadMgr;

__thread Thread* Thread::tls_ = nullptr;

// Singleton instance of ThreadMgr. Only visible in this file, used only by
// Thread. The Thread class adds a reference to threadManager while it is
// supervising a thread so that a race between the end of the process's main
// thread (and therefore the destruction of threadManager) and the end of a
// thread that tries to remove itself from the manager after the destruction can
// be avoided.
static shared_ptr<ThreadMgr> threadManager;

// Controls the single (lazy) initialization of threadManager.
static std::once_flag once;

// A singleton class that tracks all live threads, and groups them together for
// easy auditing. Used only by Thread.
class ThreadMgr {
 public:
  ThreadMgr() : threads_started_metric_(0), threads_running_metric_(0) {}

  ~ThreadMgr() {
    MutexLock l(lock_);
    thread_categories_.clear();
  }

  static void SetThreadName(const std::string& name, int64_t tid);

  Status StartInstrumentation(
      const std::shared_ptr<MetricEntity>& metrics,
      WebCallbackRegistry* web);

  // Registers a thread to the supplied category. The key is a pthread_t,
  // not the system TID, since pthread_t is less prone to being recycled.
  void AddThread(
      const pthread_t& pthread_id,
      const string& name,
      const string& category,
      int64_t tid);

  // Removes a thread from the supplied category. If the thread has
  // already been removed, this is a no-op.
  void RemoveThread(const pthread_t& pthread_id, const string& category);

  Status ShowThreadStatus(vector<ThreadDescriptor>* threads);

  Status ChangeThreadPriority(string category, int priority);

  void SetToDefaultPriority(Thread* thread);

 private:
  // Default thread priority for each category
  map<string, int> category2priority_;

  // A ThreadCategory is a set of threads that are logically related.
  // TODO: unordered_map is incompatible with pthread_t, but would be more
  // efficient here.
  using ThreadCategory = map<const pthread_t, ThreadDescriptor>;

  // All thread categorys, keyed on the category name.
  using ThreadCategoryMap = map<string, ThreadCategory>;

  // Protects thread_categories_ and thread metrics.
  Mutex lock_;

  // All thread categorys that ever contained a thread, even if empty
  ThreadCategoryMap thread_categories_;

  // Counters to track all-time total number of threads, and the
  // current number of running threads.
  uint64_t threads_started_metric_;
  uint64_t threads_running_metric_;

  // Metric callbacks.
  uint64_t ReadThreadsStarted();
  uint64_t ReadThreadsRunning();

  // Webpage callback; prints all threads by category.
  void ThreadPathHandler(
      const WebCallbackRegistry::WebRequest& req,
      WebCallbackRegistry::PrerenderedWebResponse* resp);
  void PrintThreadCategoryRows(
      const ThreadCategory& category,
      ostringstream* output);
};

void ThreadMgr::SetThreadName(const string& name, int64_t tid) {
  // On linux we can get the thread names to show up in the debugger by setting
  // the process name for the LWP.  We don't want to do this for the main
  // thread because that would rename the process, causing tools like killall
  // to stop working.
  if (tid == getpid()) {
    return;
  }

#if defined(__linux__)
  // http://0pointer.de/blog/projects/name-your-threads.html
  // Set the name for the LWP (which gets truncated to 15 characters).
  // Note that glibc also has a 'pthread_setname_np' api, but it may not be
  // available everywhere and it's only benefit over using prctl directly is
  // that it can set the name of threads other than the current thread.
  int err = prctl(PR_SET_NAME, name.c_str());
#else
  int err = pthread_setname_np(name.c_str());
#endif // defined(__linux__)
  // We expect EPERM failures in sandboxed processes, just ignore those.
  if (err < 0 && errno != EPERM) {
    PLOG(ERROR) << "SetThreadName";
  }
}

Status ThreadMgr::StartInstrumentation(
    const std::shared_ptr<MetricEntity>& metrics,
    WebCallbackRegistry* web) {
  MutexLock l(lock_);

  // Use function gauges here so that we can register a unique copy of these
  // metrics in multiple tservers, even though the ThreadMgr is itself a
  // singleton.
  metrics->NeverRetire(METRIC_threads_started.InstantiateFunctionGauge(
      metrics, Bind(&ThreadMgr::ReadThreadsStarted, Unretained(this))));
  metrics->NeverRetire(METRIC_threads_running.InstantiateFunctionGauge(
      metrics, Bind(&ThreadMgr::ReadThreadsRunning, Unretained(this))));
  metrics->NeverRetire(
      METRIC_cpu_utime.InstantiateFunctionGauge(metrics, Bind(&getCpuUTime)));
  metrics->NeverRetire(
      METRIC_cpu_stime.InstantiateFunctionGauge(metrics, Bind(&getCpuSTime)));
  metrics->NeverRetire(
      METRIC_voluntary_context_switches.InstantiateFunctionGauge(
          metrics, Bind(&getVoluntaryContextSwitches)));
  metrics->NeverRetire(
      METRIC_involuntary_context_switches.InstantiateFunctionGauge(
          metrics, Bind(&getInVoluntaryContextSwitches)));

  if (web) {
    WebCallbackRegistry::PrerenderedPathHandlerCallback thread_callback =
        bind<void>(mem_fn(&ThreadMgr::ThreadPathHandler), this, _1, _2);
    DCHECK_NOTNULL(web)->RegisterPrerenderedPathHandler(
        "/threadz",
        "Threads",
        thread_callback,
        true /* is_styled*/,
        true /* is_on_nav_bar */);
  }
  return Status::OK();
}

uint64_t ThreadMgr::ReadThreadsStarted() {
  MutexLock l(lock_);
  return threads_started_metric_;
}

uint64_t ThreadMgr::ReadThreadsRunning() {
  MutexLock l(lock_);
  return threads_running_metric_;
}

void ThreadMgr::AddThread(
    const pthread_t& pthread_id,
    const string& name,
    const string& category,
    int64_t tid) {
  // These annotations cause TSAN to ignore the synchronization on lock_
  // without causing the subsequent mutations to be treated as data races
  // in and of themselves (that's what IGNORE_READS_AND_WRITES does).
  //
  // Why do we need them here and in SuperviseThread()? TSAN operates by
  // observing synchronization events and using them to establish "happens
  // before" relationships between threads. Where these relationships are
  // not built, shared state access constitutes a data race. The
  // synchronization events here, in RemoveThread(), and in
  // SuperviseThread() may cause TSAN to establish a "happens before"
  // relationship between thread functors, ignoring potential data races.
  // The annotations prevent this from happening.
  KUDU_ANNONTATE_IGNORE_SYNC_BEGIN();
  KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN();
  {
    MutexLock l(lock_);
    thread_categories_[category][pthread_id] =
        ThreadDescriptor(category, name, tid);
    threads_running_metric_++;
    threads_started_metric_++;
  }
  KUDU_ANNONTATE_IGNORE_SYNC_END();
  KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END();
}

void ThreadMgr::RemoveThread(
    const pthread_t& pthread_id,
    const string& category) {
  KUDU_ANNONTATE_IGNORE_SYNC_BEGIN();
  KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN();
  {
    MutexLock l(lock_);
    auto category_it = thread_categories_.find(category);
    DCHECK(category_it != thread_categories_.end());
    category_it->second.erase(pthread_id);
    threads_running_metric_--;
  }
  KUDU_ANNONTATE_IGNORE_SYNC_END();
  KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END();
}

void ThreadMgr::PrintThreadCategoryRows(
    const ThreadCategory& category,
    ostringstream* output) {
  for (const ThreadCategory::value_type& thread : category) {
    ThreadStats stats;
    Status status = getThreadStats(thread.second.thread_id(), &stats);
    if (!status.ok()) {
      KLOG_EVERY_N(INFO, 100)
          << "Could not get per-thread statistics: " << status.ToString();
    }
    (*output) << "<tr><td>" << thread.second.name() << "</td><td>"
              << (static_cast<double>(stats.userNs) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.kernelNs) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.iowaitNs) / 1e9) << "</td></tr>";
  }
}

void ThreadMgr::ThreadPathHandler(
    const WebCallbackRegistry::WebRequest& req,
    WebCallbackRegistry::PrerenderedWebResponse* resp) {
  ostringstream* output = resp->output;
  MutexLock l(lock_);
  vector<const ThreadCategory*> categories_to_print;
  auto category_name = req.parsed_args.find("group");
  if (category_name != req.parsed_args.end()) {
    string group = escapeForHtmlToString(category_name->second);
    (*output) << "<h2>Thread Group: " << group << "</h2>" << endl;
    if (group != "all") {
      ThreadCategoryMap::const_iterator category =
          thread_categories_.find(group);
      if (category == thread_categories_.end()) {
        (*output) << "Thread group '" << group << "' not found" << endl;
        return;
      }
      categories_to_print.push_back(&category->second);
      (*output) << "<h3>" << category->first << " : " << category->second.size()
                << "</h3>";
    } else {
      for (const ThreadCategoryMap::value_type& category : thread_categories_) {
        categories_to_print.push_back(&category.second);
      }
      (*output) << "<h3>All Threads : </h3>";
    }

    (*output) << "<table class='table table-hover table-border'>";
    (*output)
        << "<thead><tr><th>Thread name</th><th>Cumulative User CPU(s)</th>"
        << "<th>Cumulative Kernel CPU(s)</th>"
        << "<th>Cumulative IO-wait(s)</th></tr></thead>";
    (*output) << "<tbody>\n";

    for (const ThreadCategory* category : categories_to_print) {
      PrintThreadCategoryRows(*category, output);
    }
    (*output) << "</tbody></table>";
  } else {
    (*output) << "<h2>Thread Groups</h2>";
    (*output) << "<h4>" << threads_running_metric_ << " thread(s) running";
    (*output) << "<a href='/threadz?group=all'><h3>All Threads</h3>";

    for (const ThreadCategoryMap::value_type& category : thread_categories_) {
      string category_arg;
      urlEncode(category.first, &category_arg);
      (*output) << "<a href='/threadz?group=" << category_arg << "'><h3>"
                << category.first << " : " << category.second.size()
                << "</h3></a>";
    }
  }
}

static void initThreading() {
  threadManager.reset(new ThreadMgr());
}

Status StartThreadInstrumentation(
    const std::shared_ptr<MetricEntity>& server_metrics,
    WebCallbackRegistry* web) {
  std::call_once(once, initThreading);
  return threadManager->StartInstrumentation(server_metrics, web);
}

ThreadJoiner::ThreadJoiner(Thread* thr)
    : thread_(CHECK_NOTNULL(thr)),
      warn_after_ms_(kDefaultWarnAfterMs),
      warn_every_ms_(kDefaultWarnEveryMs),
      give_up_after_ms_(kDefaultGiveUpAfterMs) {}

ThreadJoiner& ThreadJoiner::warn_after_ms(int ms) {
  warn_after_ms_ = ms;
  return *this;
}

ThreadJoiner& ThreadJoiner::warn_every_ms(int ms) {
  warn_every_ms_ = ms;
  return *this;
}

ThreadJoiner& ThreadJoiner::give_up_after_ms(int ms) {
  give_up_after_ms_ = ms;
  return *this;
}

Status ThreadJoiner::Join() {
  if (Thread::current_thread() &&
      Thread::current_thread()->tid() == thread_->tid()) {
    return Status::InvalidArgument("Can't join on own thread", thread_->name_);
  }

  // Early exit: double join is a no-op.
  if (!thread_->joinable_) {
    return Status::OK();
  }

  int waited_ms = 0;
  bool keep_trying = true;
  while (keep_trying) {
    if (waited_ms >= warn_after_ms_) {
      LOG(WARNING) << fmt::format(
          "Waited for {}ms trying to join with {} (tid {})",
          waited_ms,
          thread_->name_,
          thread_->tid_);
    }

    int remaining_before_giveup = MathLimits<int>::kMax;
    if (give_up_after_ms_ != -1) {
      remaining_before_giveup = give_up_after_ms_ - waited_ms;
    }

    int remaining_before_next_warn = warn_every_ms_;
    if (waited_ms < warn_after_ms_) {
      remaining_before_next_warn = warn_after_ms_ - waited_ms;
    }

    if (remaining_before_giveup < remaining_before_next_warn) {
      keep_trying = false;
    }

    int wait_for =
        std::min(remaining_before_giveup, remaining_before_next_warn);

    if (thread_->done_.WaitFor(MonoDelta::FromMilliseconds(wait_for))) {
      // Unconditionally join before returning, to guarantee that any TLS
      // has been destroyed (pthread_key_create() destructors only run
      // after a pthread's user method has returned).
      int ret = pthread_join(thread_->thread_, nullptr);
      CHECK_EQ(ret, 0);
      thread_->joinable_ = false;
      return Status::OK();
    }
    waited_ms += wait_for;
  }
  return Status::Aborted(
      fmt::format(
          "Timed out after {}ms joining on {}", waited_ms, thread_->name_));
}

Thread::~Thread() {
  if (joinable_) {
    int ret = pthread_detach(thread_);
    CHECK_EQ(ret, 0);
  }
}

std::string Thread::ToString() const {
  return fmt::format(
      "Thread {} (name: \"{}\", category: \"{}\")", tid(), name_, category_);
}

Status Thread::StartThread(
    const std::string& category,
    const std::string& name,
    const ThreadFunctor& functor,
    uint64_t /* flags */,
    std::shared_ptr<Thread>* holder) {
  TRACE_COUNTER_INCREMENT("threads_started", 1);
  TRACE_COUNTER_SCOPE_LATENCY_US("thread_start_us");
  std::call_once(once, initThreading);

  const string log_prefix = fmt::format("{} ({}) ", name, category);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(
      WARNING, 500 /* ms */, log_prefix, "starting thread");

  // Create the thread with shared_ptr. Since the constructor is private,
  // we use the shared_ptr constructor instead of make_shared.
  std::shared_ptr<Thread> t(new Thread(category, name, functor));

  // Optional, and only set if the thread was successfully created.
  //
  // We have to set this before we even start the thread because it's
  // allowed for the thread functor to access 'holder'.
  if (holder) {
    *holder = t;
  }

  // Create a Baton for synchronization. The child thread will post to this
  // after it has taken ownership of the Thread shared_ptr, ensuring the Thread
  // stays alive even if the caller drops its reference.
  folly::Baton<> ready_baton;

  // Stack-allocate SuperviseArgs since we wait for the child thread to copy
  // the shared_ptr before returning.
  SuperviseArgs args{t, &ready_baton};

  if (PREDICT_FALSE(FLAGS_thread_inject_start_latency_ms > 0)) {
    LOG(INFO) << "Injecting " << FLAGS_thread_inject_start_latency_ms
              << "ms sleep on thread start";
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_thread_inject_start_latency_ms));
  }

  {
    SCOPED_LOG_SLOW_EXECUTION_PREFIX(
        WARNING, 500 /* ms */, log_prefix, "creating pthread");
    // SCOPED_WATCH_STACK((flags & NO_STACK_WATCHDOG) ? 0 : 250);
    int ret =
        pthread_create(&t->thread_, nullptr, &Thread::SuperviseThread, &args);
    if (ret) {
      return Status::RuntimeError(
          "Could not create thread", strerror(ret), ret);
    }
  }

  // The thread has been created and is now joinable.
  //
  // Why set this in the parent and not the child? Because only the parent
  // (or someone communicating with the parent) can join, so joinable must
  // be set before the parent returns.
  t->joinable_ = true;

  // Wait for the child thread to take ownership of the shared_ptr and
  // initialize. This ensures the Thread object stays alive even if the caller
  // drops its reference.
  {
    SCOPED_LOG_SLOW_EXECUTION_PREFIX(
        WARNING, 500 /* ms */, log_prefix, "waiting for thread to initialize");
    ready_baton.wait();
  }

  VLOG(2) << "Started thread " << t->tid() << " - " << category << ":" << name;
  return Status::OK();
}

void* Thread::SuperviseThread(void* arg) {
  // Get the SuperviseArgs from the parent's stack.
  // We'll copy what we need before posting to the Baton.
  SuperviseArgs* args = static_cast<SuperviseArgs*>(arg);

  // Take a copy of the shared_ptr to keep the Thread alive.
  std::shared_ptr<Thread> t_owner = args->thread;
  Thread* t = t_owner.get();

  // Take a pointer to the baton.
  folly::Baton<>* ready_baton = args->ready_baton;

  int64_t system_tid = Thread::CurrentThreadId();
  PCHECK(system_tid != -1);

  // Take an additional reference to the thread manager, which we'll need below.
  KUDU_ANNONTATE_IGNORE_SYNC_BEGIN();
  shared_ptr<ThreadMgr> threadMgrRef = threadManager;
  KUDU_ANNONTATE_IGNORE_SYNC_END();

  // Set up the TLS.
  //
  // We store a bare pointer in the TLS, since its lifecycle is poorly defined.
  // The shared_ptr t_owner keeps the Thread alive.
  Thread::tls_ = t;

  // Publish our tid to 'tid_', which allows tid() to return the correct value.
  // IMPORTANT: This MUST be done before posting to the baton, otherwise the
  // parent could wake from baton.wait() and call tid() before it's been
  // initialized.
  Release_Store(&t->tid_, system_tid);

  // Signal the parent thread that we've successfully taken ownership of the
  // Thread shared_ptr and initialized. It's now safe for the parent to return
  // and for the args struct on the parent's stack to go out of scope.
  ready_baton->post();

  string name = fmt::format("{}-{}", t->name(), system_tid);
  threadManager->SetThreadName(name, t->tid_);
  threadManager->AddThread(pthread_self(), name, t->category(), t->tid_);
  threadManager->SetToDefaultPriority(t);

  // FinishThread() is guaranteed to run (even if functor_ throws an
  // exception) because pthread_cleanup_push() creates a scoped object
  // whose destructor invokes the provided callback.
  pthread_cleanup_push(&Thread::FinishThread, t);
  t->functor_();
  pthread_cleanup_pop(true);

  return nullptr;
}

void Thread::FinishThread(void* arg) {
  Thread* t = static_cast<Thread*>(arg);

  // We're here either because of the explicit pthread_cleanup_pop() in
  // SuperviseThread() or through pthread_exit(). In either case,
  // threadManager is guaranteed to be live because threadMgrRef in
  // SuperviseThread() is still live.
  threadManager->RemoveThread(pthread_self(), t->category());

  // Signal any Joiner that we're done.
  t->done_.CountDown();

  VLOG(2) << "Ended thread " << t->tid_ << " - " << t->category() << ":"
          << t->name();

  // Note: With std::shared_ptr, we don't need to manually Release().
  // The t_owner shared_ptr in SuperviseThread() will be destroyed when
  // that function exits, automatically decrementing the reference count.
  // NOTE: after this function returns, 'this' may be destroyed if the
  // t_owner shared_ptr in SuperviseThread was the last reference.
  // so 'this' could be destructed at this point. Do not add any code
  // following here!
}

static bool setCapabilityFlag(cap_value_t capability, cap_flag_value_t flag) {
  cap_value_t capList[] = {capability};
  cap_t caps = cap_get_proc();
  bool ret = true;

  if (!caps || cap_set_flag(caps, CAP_EFFECTIVE, 1, capList, flag) ||
      cap_set_proc(caps)) {
    LOG(ERROR) << "Can not set capability flag";
    ret = false;
  }

  if (caps) {
    cap_free(caps);
  }

  return ret;
}

static bool acquireCapability(cap_value_t capability) {
  return setCapabilityFlag(capability, CAP_SET);
}

static bool dropCapability(cap_value_t capability) {
  return setCapabilityFlag(capability, CAP_CLEAR);
}

// CAP_SYS_NICE capability is acquired before changing the thread priority,
// and dropped after the action is done.
// This is the same flow like we did in mysqld
static int setSystemThreadPriority(pid_t tid, int pri) {
  acquireCapability(CAP_SYS_NICE);
  int ret = setpriority(PRIO_PROCESS, tid, pri) != 0;
  dropCapability(CAP_SYS_NICE);

  return ret;
}

static int getSystemThreadPriority(pid_t tid) {
  return getpriority(PRIO_PROCESS, tid);
}

Status ThreadMgr::ShowThreadStatus(vector<ThreadDescriptor>* threads) {
  MutexLock l(lock_);
  for (auto const& name2category : thread_categories_) {
    ThreadCategory category = name2category.second;
    for (auto thread_info : category) {
      int pri = getSystemThreadPriority(thread_info.second.thread_id());
      thread_info.second.setPriority(pri);
      threads->push_back(thread_info.second);
    }
  }
  return Status::OK();
}

Status ThreadMgr::ChangeThreadPriority(string category, int priority) {
  MutexLock l(lock_);
  // Change the default for particular pool
  category2priority_[category] = priority;

  // Change current thread priority
  if (thread_categories_.count(category)) {
    for (auto const& thread_info : thread_categories_[category]) {
      uint64_t thread_id = thread_info.second.thread_id();
      int ret = setSystemThreadPriority(thread_id, priority);
      if (ret != 0) {
        return Status::RuntimeError(
            "Can not change thread priority", strerror(ret), ret);
      }
    }
  }
  return Status::OK();
}

void ThreadMgr::SetToDefaultPriority(Thread* thread) {
  MutexLock l(lock_);
  if (category2priority_.count(thread->category())) {
    setSystemThreadPriority(
        thread->tid(), category2priority_[thread->category()]);
  }
}

Status GlobalShowThreadStatus(vector<ThreadDescriptor>* threads) {
  return threadManager->ShowThreadStatus(threads);
}

Status GlobalChangeThreadPriority(string category, int priority) {
  return threadManager->ChangeThreadPriority(category, priority);
}

} // namespace kudu
