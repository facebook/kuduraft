// Copyright 2012 Google Inc. All Rights Reserved.
//
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

#pragma once

#include <sys/time.h>

#include <ctime>
#include <mutex>
#include <string>

#if defined(__APPLE__)
#include <mach/clock.h> // @manual
#include <mach/mach.h> // @manual
#include <mach/mach_time.h> // @manual

#include <glog/logging.h> // @manual

#endif // #if defined(__APPLE__)

#include <cstdint>

namespace kudu {

using WallTime = double;

// Append result to a supplied string.
// If an error occurs during conversion 'dst' is not modified.
void stringAppendStrftime(
    std::string* dst,
    const char* format,
    time_t when,
    bool local);

// Return the local time as a string suitable for user display.
std::string localTimeAsString();

// Similar to the WallTime_Parse, but it takes a boolean flag local as
// argument specifying if the time_spec is in local time or UTC
// time. If local is set to true, the same exact result as
// WallTime_Parse is returned.
bool wallTimeParseTimezone(
    const char* time_spec,
    const char* format,
    const struct tm* default_time,
    bool local,
    WallTime* result);

// Return current time in seconds as a WallTime.
WallTime wallTimeNow();

using MicrosecondsInt64 = int64_t;

namespace walltime_internal {

#if defined(__APPLE__)

extern std::once_flag timebase_info_once;
extern mach_timebase_info_data_t timebase_info;
extern void initializeTimebaseInfo();

inline void getCurrentTime(mach_timespec_t* ts) {
  clock_serv_t cclock;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  CHECK_EQ(KERN_SUCCESS, clock_get_time(cclock, ts));
  mach_port_deallocate(mach_task_self(), cclock);
}

inline MicrosecondsInt64 getCurrentTimeMicros() {
  mach_timespec_t ts;
  getCurrentTime(&ts);
  // 'tv_sec' is just 4 bytes on macOS, need to be careful not
  // to convert to nanos until we've moved to a larger int.
  MicrosecondsInt64 micros_from_secs = ts.tv_sec;
  micros_from_secs *= 1000 * 1000;
  micros_from_secs += ts.tv_nsec / 1000;
  return micros_from_secs;
}

inline int64_t getMonoTimeNanos() {
  // See Apple Technical Q&A QA1398 for further detail on mono time in OS X.
  std::call_once(timebase_info_once, initializeTimebaseInfo);

  uint64_t time = mach_absolute_time();

  // mach_absolute_time returns ticks, which need to be scaled by the timebase
  // info to get nanoseconds.
  return time * timebase_info.numer / timebase_info.denom;
}

inline MicrosecondsInt64 getMonoTimeMicros() {
  return getMonoTimeNanos() / 1000;
}

inline MicrosecondsInt64 getThreadCpuTimeMicros() {
  // See https://www.gnu.org/software/hurd/gnumach-doc/Thread-Information.html
  // and Chromium base/time/time_mac.cc.
  task_t thread = mach_thread_self();
  if (thread == MACH_PORT_NULL) {
    LOG(WARNING) << "Failed to get mach_thread_self()";
    return 0;
  }

  mach_msg_type_number_t thread_info_count = THREAD_BASIC_INFO_COUNT;
  thread_basic_info_data_t thread_info_data;

  kern_return_t result = thread_info(
      thread,
      THREAD_BASIC_INFO,
      reinterpret_cast<thread_info_t>(&thread_info_data),
      &thread_info_count);

  if (result != KERN_SUCCESS) {
    LOG(WARNING) << "Failed to get thread_info()";
    return 0;
  }

  return thread_info_data.user_time.seconds * 1000000 +
      thread_info_data.user_time.microseconds;
}

#else

inline MicrosecondsInt64 getClockTimeMicros(clockid_t clock) {
  timespec ts;
  clock_gettime(clock, &ts);
  // 'tv_sec' is usually 8 bytes, but the spec says it only
  // needs to be 'a signed int'. Moved to a 64 bit var before
  // converting to micros to be safe.
  MicrosecondsInt64 micros_from_secs = ts.tv_sec;
  micros_from_secs *= 1000 * 1000;
  micros_from_secs += ts.tv_nsec / 1000;
  return micros_from_secs;
}

#endif // defined(__APPLE__)

} // namespace walltime_internal

// Returns the time since the Epoch measured in microseconds.
inline MicrosecondsInt64 getCurrentTimeMicros() {
#if defined(__APPLE__)
  return walltime_internal::getCurrentTimeMicros();
#else
  return walltime_internal::getClockTimeMicros(CLOCK_REALTIME);
#endif // defined(__APPLE__)
}

// Returns the time since some arbitrary reference point, measured in
// microseconds. Guaranteed to be monotonic (and therefore useful for measuring
// intervals)
inline MicrosecondsInt64 getMonoTimeMicros() {
#if defined(__APPLE__)
  return walltime_internal::getMonoTimeMicros();
#else
  return walltime_internal::getClockTimeMicros(CLOCK_MONOTONIC);
#endif // defined(__APPLE__)
}

// Returns the time spent in user CPU on the current thread, measured in
// microseconds.
inline MicrosecondsInt64 getThreadCpuTimeMicros() {
#if defined(__APPLE__)
  return walltime_internal::getThreadCpuTimeMicros();
#else
  return walltime_internal::getClockTimeMicros(CLOCK_THREAD_CPUTIME_ID);
#endif // defined(__APPLE__)
}

// A CycleClock yields the value of a cycle counter that increments at a rate
// that is approximately constant.
class CycleClock {
 public:
  // Return the value of the counter.
  static inline int64_t Now();

 private:
  CycleClock();
};
} // namespace kudu

// inline method bodies
#include "kudu/gutil/cycleclock-inl.h" // IWYU pragma: export
