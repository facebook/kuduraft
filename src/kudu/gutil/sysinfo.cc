// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2006, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <fcntl.h> // for open()
#include <unistd.h> // for read()

#include "kudu/gutil/sysinfo.h"

#include <cerrno> // for errno
#include <cstdlib> // for getenv()
#include <cstring> // for memmove(), memchr(), etc.
#include <ctime>
#include <ostream>

#include <cstdint>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h" // for RunningOnValgrind
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/walltime.h"

// This isn't in the 'base' namespace in tcmallc. But, tcmalloc
// exports these functions, so we need to namespace them to avoid
// the conflict.
namespace base {

// ----------------------------------------------------------------------
// cyclesPerSecond()
// numCpus()
//    It's important this not call malloc! -- they may be called at
//    global-construct time, before we've set up all our proper malloc
//    hooks and such.
// ----------------------------------------------------------------------

static double cpuinfoCyclesPerSecond = 1.0; // 0.0 might be dangerous
static int cpuinfoNumCpus = 1; // Conservative guess
static int cpuinfoMaxCpuIndex = -1;

void sleepForNanoseconds(int64_t nanoseconds) {
  // Sleep for nanosecond duration
  struct timespec sleep_time;
  sleep_time.tv_sec = nanoseconds / 1000 / 1000 / 1000;
  sleep_time.tv_nsec = (nanoseconds % (1000 * 1000 * 1000));
  while (nanosleep(&sleep_time, &sleep_time) != 0 && errno == EINTR) {
    ; // Ignore signals and wait for the full interval to elapse.
  }
}

void sleepForMilliseconds(int64_t milliseconds) {
  sleepForNanoseconds(milliseconds * 1000 * 1000);
}

// Helper function estimates cycles/sec by observing cycles elapsed during
// sleep(). Using small sleep time decreases accuracy significantly.
static int64_t estimateCyclesPerSecond(const int estimate_time_ms) {
  CHECK(estimate_time_ms > 0);
  if (estimate_time_ms <= 0) {
    return 1;
  }
  double multiplier =
      1000.0 / static_cast<double>(estimate_time_ms); // scale by this much

  const int64_t start_ticks = kudu::CycleClock::Now();
  sleepForMilliseconds(estimate_time_ms);
  const int64_t guess =
      int64_t(multiplier * (kudu::CycleClock::Now() - start_ticks));
  return guess;
}

// Slurp a file with a single read() call into 'buf'. This is only safe to use
// on small files in places like /proc where we are guaranteed not to get a
// partial read. Any remaining bytes in the buffer are zeroed.
//
// 'buflen' must be more than large enough to hold the whole file, or else this
// will issue a FATAL error.
static bool slurpSmallTextFile(const char* file, char* buf, int buflen) {
  bool ret = false;
  int fd;
  RETRY_ON_EINTR(fd, open(file, O_RDONLY));
  if (fd == -1) {
    return ret;
  }

  memset(buf, '\0', buflen);
  int n;
  RETRY_ON_EINTR(n, read(fd, buf, buflen - 1));
  CHECK_NE(n, buflen - 1) << "buffer of len " << buflen
                          << " not large enough to store " << "contents of "
                          << file;
  if (n > 0) {
    ret = true;
  }

  int close_ret;
  RETRY_ON_EINTR(close_ret, close(fd));
  if (PREDICT_FALSE(close_ret != 0)) {
    PLOG(WARNING) << "Failed to close fd " << fd;
  }

  return ret;
}

// Helper function for reading an int from a file. Returns true if successful
// and the memory location pointed to by value is set to the value read.
static bool readIntFromFile(const char* file, int* value) {
  char line[1024];
  if (!slurpSmallTextFile(file, line, arraysize(line))) {
    return false;
  }
  char* err;
  const int temp_value = strtol(line, &err, 10);
  if (line[0] != '\0' && (*err == '\n' || *err == '\0')) {
    *value = temp_value;
    return true;
  }
  return false;
}

static int readMaxCpuIndex() {
  char buf[1024];
  CHECK(slurpSmallTextFile(
      "/sys/devices/system/cpu/present", buf, arraysize(buf)));

  // On a single-core machine, 'buf' will contain the string '0' with a newline.
  if (strcmp(buf, "0\n") == 0) {
    return 0;
  }

  // On multi-core, it will have a CPU range like '0-7'.
  CHECK_EQ(0, memcmp(buf, "0-", 2)) << "bad list of possible CPUs: " << buf;

  char* max_cpu_str = &buf[2];
  char* err;
  int val = strtol(max_cpu_str, &err, 10);
  CHECK(*err == '\n' || *err == '\0')
      << "unable to parse max CPU index from: " << buf;
  return val;
}

int parseMaxCpuIndex(const char* str) {
  DCHECK(str != nullptr);
  const char* pos = str;
  // Initialize max_idx to invalid so we can just return if we find zero ranges.
  int max_idx = -1;

  while (true) {
    const char* range_start = pos;
    const char* dash = nullptr;
    // Scan forward until we find the separator indicating end of range, which
    // is always a newline or comma if the input is valid.

    for (; *pos != ',' && *pos != '\n'; pos++) {
      // Check for early end of string - bail here to avoid advancing past end.
      if (*pos == '\0') {
        return -1;
      }

      if (*pos == '-') {
        // Multiple dashes in range is invalid.
        if (dash != nullptr) {
          return -1;
        }

        dash = pos;
      } else if (!isdigit(*pos)) {
        return -1;
      }
    }

    // At this point we found a range [range_start, pos) comprised of digits and
    // an optional dash.

    const char* num_start = dash == nullptr ? range_start : dash + 1;
    // Check for ranges with missing numbers, e.g. "", "3-", "-3".
    if (num_start == pos || dash == range_start) {
      return -1;
    }
    // The numbers are comprised only of digits, so it can only fail if it is
    // out of range of int (the return type of this function).

    unsigned long start_idx = strtoul(range_start, nullptr, 10);
    if (start_idx > std::numeric_limits<int>::max()) {
      return -1;
    }

    unsigned long end_idx = strtoul(num_start, nullptr, 10);
    if (end_idx > std::numeric_limits<int>::max() || start_idx > end_idx) {
      return -1;
    }
    // Keep track of the max index we've seen so far.
    max_idx = std::max(static_cast<int>(end_idx), max_idx);
    // End of line, expect no more input.
    if (*pos == '\n') {
      break;
    }

    ++pos;
  }
  // String must have a single newline at the very end.
  if (*pos != '\n' || *(pos + 1) != '\0') {
    return -1;
  }

  return max_idx;
}

// WARNING: logging calls back to initializeSystemInfo() so it must
// not invoke any logging code.  Also, initializeSystemInfo() can be
// called before main() -- in fact it *must* be since already_called
// isn't protected -- before malloc hooks are properly set up, so
// we make an effort not to call any routines which might allocate
// memory.

static void initializeSystemInfo() {
  static bool already_called = false; // safe if we run before threads
  if (already_called) {
    return;
  }
  already_called = true;

  bool saw_mhz = false;

  if (RunningOnValgrind()) {
    // Valgrind may slow the progress of time artificially (--scale-time=N
    // option). We thus can't rely on CPU Mhz info stored in /sys or /proc
    // files. Thus, actually measure the cps.
    cpuinfoCyclesPerSecond = estimateCyclesPerSecond(100);
    saw_mhz = true;
  }

  char line[1024];
  char* err;
  int freq;

  // If the kernel is exporting the tsc frequency use that. There are issues
  // where cpuinfo_max_freq cannot be relied on because the BIOS may be
  // exporintg an invalid p-state (on x86) or p-states may be used to put the
  // processor in a new mode (turbo mode). Essentially, those frequencies
  // cannot always be relied upon. The same reasons apply to /proc/cpuinfo as
  // well.
  if (!saw_mhz &&
      readIntFromFile("/sys/devices/system/cpu/cpu0/tsc_freq_khz", &freq)) {
    // The value is in kHz (as the file name suggests).  For example, on a
    // 2GHz warpstation, the file contains the value "2000000".
    cpuinfoCyclesPerSecond = freq * 1000.0;
    saw_mhz = true;
  }

  // If CPU scaling is in effect, we want to use the *maximum* frequency,
  // not whatever CPU speed some random processor happens to be using now.
  if (!saw_mhz &&
      readIntFromFile(
          "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq", &freq)) {
    // The value is in kHz.  For example, on a 2GHz machine, the file
    // contains the value "2000000".
    cpuinfoCyclesPerSecond = freq * 1000.0;
    saw_mhz = true;
  }

  // Read /proc/cpuinfo for other values, and if there is no cpuinfo_max_freq.
  const char* pname = "/proc/cpuinfo";
  int fd;
  RETRY_ON_EINTR(fd, open(pname, O_RDONLY));
  if (fd == -1) {
    PLOG(FATAL)
        << "Unable to read CPU info from /proc. procfs must be mounted.";
  }

  double bogo_clock = 1.0;
  bool saw_bogo = false;
  int num_cpus = 0;
  line[0] = line[1] = '\0';
  int chars_read = 0;
  do { // we'll exit when the last read didn't read anything
    // Move the next line to the beginning of the buffer
    const int oldlinelen = strlen(line);
    if (sizeof(line) == oldlinelen + 1) { // oldlinelen took up entire line
      line[0] = '\0';
    } else { // still other lines left to save
      memmove(line, line + oldlinelen + 1, sizeof(line) - (oldlinelen + 1));
    }
    // Terminate the new line, reading more if we can't find the newline
    char* newline = strchr(line, '\n');
    if (newline == nullptr) {
      const int linelen = strlen(line);
      const int bytes_to_read = sizeof(line) - 1 - linelen;
      CHECK(bytes_to_read > 0); // because the memmove recovered >=1 bytes
      RETRY_ON_EINTR(chars_read, read(fd, line + linelen, bytes_to_read));
      line[linelen + chars_read] = '\0';
      newline = strchr(line, '\n');
    }
    if (newline != nullptr) {
      *newline = '\0';
    }

#if defined(__powerpc__) || defined(__ppc__)
    // PowerPC cpus report the frequency in "clock" line
    if (strncasecmp(line, "clock", sizeof("clock") - 1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        // PowerPC frequencies are only reported as MHz (check 'show_cpuinfo'
        // function at arch/powerpc/kernel/setup-common.c)
        char* endp = strstr(line, "MHz");
        if (endp) {
          *endp = 0;
          cpuinfoCyclesPerSecond = strtod(freqstr + 1, &err) * 1000000.0;
          if (freqstr[1] != '\0' && *err == '\0' && cpuinfoCyclesPerSecond > 0)
            saw_mhz = true;
        }
      }
#else
    // When parsing the "cpu MHz" and "bogomips" (fallback) entries, we only
    // accept postive values. Some environments (virtual machines) report zero,
    // which would cause infinite looping in WallTime_Init.
    if (!saw_mhz && strncasecmp(line, "cpu MHz", sizeof("cpu MHz") - 1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        cpuinfoCyclesPerSecond = strtod(freqstr + 1, &err) * 1000000.0;
        if (freqstr[1] != '\0' && *err == '\0' && cpuinfoCyclesPerSecond > 0) {
          saw_mhz = true;
        }
      }
    } else if (strncasecmp(line, "bogomips", sizeof("bogomips") - 1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        bogo_clock = strtod(freqstr + 1, &err) * 1000000.0;
        if (freqstr[1] != '\0' && *err == '\0' && bogo_clock > 0) {
          saw_bogo = true;
        }
      }
#endif
    } else if (strncasecmp(line, "processor", sizeof("processor") - 1) == 0) {
      num_cpus++; // count up every time we see an "processor :" entry
    }
  } while (chars_read > 0);
  int ret;
  RETRY_ON_EINTR(ret, close(fd));
  if (PREDICT_FALSE(ret != 0)) {
    PLOG(WARNING) << "Failed to close fd " << fd;
  }

  if (!saw_mhz) {
    if (saw_bogo) {
      // If we didn't find anything better, we'll use bogomips, but
      // we're not happy about it.
      cpuinfoCyclesPerSecond = bogo_clock;
    } else {
      // If we don't even have bogomips, we'll use the slow estimation.
      cpuinfoCyclesPerSecond = estimateCyclesPerSecond(1000);
    }
  }
  if (cpuinfoCyclesPerSecond == 0.0) {
    cpuinfoCyclesPerSecond = 1.0; // maybe unnecessary, but safe
  }
  if (num_cpus > 0) {
    cpuinfoNumCpus = num_cpus;
  }
  cpuinfoMaxCpuIndex = readMaxCpuIndex();

  // On platforms where we can't determine the max CPU index, just use the
  // number of CPUs. This might break if CPUs are taken offline, but
  // better than a wild guess.
  if (cpuinfoMaxCpuIndex < 0) {
    cpuinfoMaxCpuIndex = cpuinfoNumCpus - 1;
  }
}

double cyclesPerSecond(void) {
  initializeSystemInfo();
  return cpuinfoCyclesPerSecond;
}

int numCpus(void) {
  initializeSystemInfo();
  return cpuinfoNumCpus;
}

int maxCpuIndex(void) {
  initializeSystemInfo();
  return cpuinfoMaxCpuIndex;
}

} // namespace base
