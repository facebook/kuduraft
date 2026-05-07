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
#ifndef KUDU_UTIL_STOPWATCH_H
#define KUDU_UTIL_STOPWATCH_H

#include <glog/logging.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <string>
#if defined(__APPLE__)
#include <mach/clock.h> // @manual
#include <mach/mach.h> // @manual
#include <mach/thread_info.h> // @manual
#endif // defined(__APPLE__)

#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h"

#include <fmt/core.h>

namespace kudu {

// Macro for logging timing of a block. Usage:
//   LOG_TIMING_PREFIX_IF(INFO, FLAGS_should_record_time, "Tablet X: ", "doing
//   some task") {
//     ... some task which takes some time
//   }
// If FLAGS_should_record_time is true, yields a log like:
// I1102 14:35:51.726186 23082 file.cc:167] Tablet X: Time spent doing some
// task:
//   real 3.729s user 3.570s sys 0.150s
// The task will always execute regardless of whether the timing information is
// printed.
#define LOG_TIMING_PREFIX_IF(severity, condition, prefix, description) \
  for (kudu::sw_internal::LogTiming _l(                                \
           __FILE__,                                                   \
           __LINE__,                                                   \
           google::severity,                                           \
           prefix,                                                     \
           description,                                                \
           -1,                                                         \
           (condition));                                               \
       !_l.hasRun();                                                   \
       _l.markHasRun())

// Conditionally log, no prefix.
#define LOG_TIMING_IF(severity, condition, description) \
  LOG_TIMING_PREFIX_IF(severity, (condition), "", (description))

// Always log, no prefix.
#define LOG_TIMING(severity, description) \
  LOG_TIMING_IF(severity, true, (description))

// Macro to log the time spent in the rest of the block.
#define SCOPED_LOG_TIMING(severity, description)             \
  kudu::sw_internal::LogTiming VARNAME_LINENUM(_log_timing)( \
      __FILE__, __LINE__, google::severity, "", description, -1, true);

// Scoped version of LOG_SLOW_EXECUTION().
#define SCOPED_LOG_SLOW_EXECUTION(severity, max_expected_millis, description) \
  kudu::sw_internal::LogTiming VARNAME_LINENUM(_log_timing)(                  \
      __FILE__,                                                               \
      __LINE__,                                                               \
      google::severity,                                                       \
      "",                                                                     \
      description,                                                            \
      max_expected_millis,                                                    \
      true)

// Scoped version of LOG_SLOW_EXECUTION() but with a prefix.
#define SCOPED_LOG_SLOW_EXECUTION_PREFIX(                    \
    severity, max_expected_millis, prefix, description)      \
  kudu::sw_internal::LogTiming VARNAME_LINENUM(_log_timing)( \
      __FILE__,                                              \
      __LINE__,                                              \
      google::severity,                                      \
      prefix,                                                \
      description,                                           \
      max_expected_millis,                                   \
      true)

// Macro for logging timing of a block. Usage:
//   LOG_SLOW_EXECUTION(INFO, 5, "doing some task") {
//     ... some task which takes some time
//   }
// when slower than 5 milliseconds, yields a log like:
// I1102 14:35:51.726186 23082 file.cc:167] Time spent doing some task:
//   real 3.729s user 3.570s sys 0.150s
#define LOG_SLOW_EXECUTION(severity, max_expected_millis, description) \
  for (kudu::sw_internal::LogTiming _l(                                \
           __FILE__,                                                   \
           __LINE__,                                                   \
           google::severity,                                           \
           "",                                                         \
           description,                                                \
           max_expected_millis,                                        \
           true);                                                      \
       !_l.hasRun();                                                   \
       _l.markHasRun())

// Workaround for the clang analyzer being confused by the above loop-based
// macros. The analyzer thinks the macros might loop more than once, and thus
// generates false positives. So, for its purposes, just make them empty.
#if defined(CLANG_TIDY) || defined(__clang_analyzer__)

#undef LOG_TIMING_PREFIX_IF
#define LOG_TIMING_PREFIX_IF(severity, condition, prefix, description)

#undef LOG_SLOW_EXECUTION
#define LOG_SLOW_EXECUTION(severity, max_expected_millis, description)
#endif

#define NANOS_PER_SECOND 1000000000.0
#define NANOS_PER_MILLISECOND 1000000.0

using NanosecondType = int64_t;

// Structure which contains an elapsed amount of wall/user/sys time.
struct CpuTimes {
  NanosecondType wall;
  NanosecondType user;
  NanosecondType system;
  int64_t contextSwitches;

  void clear() {
    wall = user = system = contextSwitches = 0LL;
  }

  // Return a string formatted similar to the output of the "time" shell
  // command.
  std::string toString() const {
    return fmt::format(
        "real {:.3f}s\tuser {:.3f}s\tsys {:.3f}s",
        wallSeconds(),
        userCpuSeconds(),
        systemCpuSeconds());
  }

  double wallMillis() const {
    return static_cast<double>(wall) / NANOS_PER_MILLISECOND;
  }

  double wallSeconds() const {
    return static_cast<double>(wall) / NANOS_PER_SECOND;
  }

  double userCpuSeconds() const {
    return static_cast<double>(user) / NANOS_PER_SECOND;
  }

  double systemCpuSeconds() const {
    return static_cast<double>(system) / NANOS_PER_SECOND;
  }
};

// A Stopwatch is a convenient way of timing a given operation.
//
// Wall clock time is based on a monotonic timer, so can be reliably used for
// determining durations.
// CPU time is based on either current thread's usage or the usage of the whole
// process, depending on the value of 'Mode' passed to the constructor.
//
// The implementation relies on several syscalls, so should not be used for
// hot paths, but is useful for timing anything on the granularity of seconds
// or more.
//
// NOTE: the user time reported by this class is based on Linux scheduler ticks
// and thus has low precision. Use GetThreadCpuTimeMicros() from walltime.h if
// more accurate per-thread CPU usage timing is required.
class Stopwatch {
 public:
  enum Mode {
    // Collect usage only about the calling thread.
    // This may not be supported on older versions of Linux.
    kThisThread,
    // Collect usage of all threads.
    kAllThreads
  };

  // Construct a new stopwatch. The stopwatch is initially stopped.
  explicit Stopwatch(Mode mode = kThisThread) : mode_(mode), stopped_(true) {
    times_.clear();
  }

  // Start counting. If the stopwatch is already counting, then resets the
  // start point at the current time.
  void start() {
    stopped_ = false;
    getTimes(&times_);
  }

  // Stop counting. If the stopwatch is already stopped, has no effect.
  void stop() {
    if (stopped_) {
      return;
    }
    stopped_ = true;

    CpuTimes current;
    getTimes(&current);
    times_.wall = current.wall - times_.wall;
    times_.user = current.user - times_.user;
    times_.system = current.system - times_.system;
    times_.contextSwitches = current.contextSwitches - times_.contextSwitches;
  }

  // Return the elapsed amount of time. If the stopwatch is running, then
  // returns the amount of time since it was started. If it is stopped, returns
  // the amount of time between the most recent start/stop pair. If the
  // stopwatch has never been started, the elapsed time is considered to be
  // zero.
  CpuTimes elapsed() const {
    if (stopped_) {
      return times_;
    }

    CpuTimes current;
    getTimes(&current);
    current.wall -= times_.wall;
    current.user -= times_.user;
    current.system -= times_.system;
    current.contextSwitches -= times_.contextSwitches;
    return current;
  }

 private:
  void getTimes(CpuTimes* times) const {
    struct rusage usage;
    struct timespec wall;

#if defined(__APPLE__)
    if (mode_ == kThisThread) {
      // Adapted from https://codereview.chromium.org/16818003
      thread_basic_info_data_t tInfo;
      mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
      CHECK_EQ(
          KERN_SUCCESS,
          thread_info(
              mach_thread_self(),
              THREAD_BASIC_INFO,
              (thread_info_t)&tInfo,
              &count));
      usage.ru_utime.tv_sec = tInfo.user_time.seconds;
      usage.ru_utime.tv_usec = tInfo.user_time.microseconds;
      usage.ru_stime.tv_sec = tInfo.system_time.seconds;
      usage.ru_stime.tv_usec = tInfo.system_time.microseconds;
      usage.ru_nivcsw = tInfo.suspend_count;
      usage.ru_nvcsw = 0;
    } else {
      CHECK_EQ(0, getrusage(RUSAGE_SELF, &usage));
    }

    mach_timespec_t ts;
    walltime_internal::getCurrentTime(&ts);
    wall.tv_sec = ts.tv_sec;
    wall.tv_nsec = ts.tv_nsec;
#else
    CHECK_EQ(
        0,
        getrusage(
            (mode_ == kThisThread) ? RUSAGE_THREAD : RUSAGE_SELF, &usage));
    CHECK_EQ(0, clock_gettime(CLOCK_MONOTONIC, &wall));
#endif // defined(__APPLE__)
    times->wall = wall.tv_sec * 1000000000L + wall.tv_nsec;
    times->user =
        usage.ru_utime.tv_sec * 1000000000L + usage.ru_utime.tv_usec * 1000L;
    times->system =
        usage.ru_stime.tv_sec * 1000000000L + usage.ru_stime.tv_usec * 1000L;
    times->contextSwitches = usage.ru_nvcsw + usage.ru_nivcsw;
  }

  const Mode mode_;
  bool stopped_;
  CpuTimes times_;
};

namespace sw_internal {

// Internal class used by the LOG_TIMING macro.
class LogTiming {
 public:
  LogTiming(
      const char* file,
      int line,
      google::LogSeverity severity,
      std::string prefix,
      std::string description,
      int64_t maxExpectedMillis,
      bool shouldPrint)
      : file_(file),
        line_(line),
        severity_(severity),
        prefix_(std::move(prefix)),
        description_(std::move(description)),
        maxExpectedMillis_(maxExpectedMillis),
        shouldPrint_(shouldPrint),
        hasRun_(false) {
    stopwatch_.start();
  }

  ~LogTiming() {
    if (shouldPrint_) {
      print(maxExpectedMillis_);
    }
  }

  // Allows this object to be used as the loop variable in for-loop macros.
  // Call hasRun() in the conditional check in the for-loop.
  bool hasRun() {
    return hasRun_;
  }

  // Allows this object to be used as the loop variable in for-loop macros.
  // Call markHasRun() in the "increment" section of the for-loop.
  void markHasRun() {
    hasRun_ = true;
  }

 private:
  Stopwatch stopwatch_;
  const char* file_;
  const int line_;
  const google::LogSeverity severity_;
  const std::string prefix_;
  const std::string description_;
  const int64_t maxExpectedMillis_;
  const bool shouldPrint_;
  bool hasRun_;

  // Print if the number of expected millis exceeds the max.
  // Passing a negative number implies "always print".
  void print(int64_t maxExpectedMillis) {
    stopwatch_.stop();
    CpuTimes times = stopwatch_.elapsed();
    // TODO(todd): for some reason, times.wallMillis() sometimes ends up
    // negative on rare occasion, for unclear reasons, so we have to check
    // maxExpectedMillis < 0 to be sure we always print when requested.
    if (maxExpectedMillis < 0 || times.wallMillis() > maxExpectedMillis) {
      google::LogMessage(file_, line_, severity_).stream()
          << prefix_ << "Time spent " << description_ << ": "
          << times.toString();
    }
  }
};

} // namespace sw_internal
} // namespace kudu

#endif
