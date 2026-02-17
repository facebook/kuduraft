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

#include "kudu/util/pstack_watcher.h"

#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/thread.h"

namespace kudu {

using std::string;
using std::vector;
using strings::SkipEmpty;
using strings::SkipWhitespace;
using strings::Split;

PstackWatcher::PstackWatcher(MonoDelta timeout)
    : timeout_(timeout), running_(true), cond_(&lock_) {
  CHECK_OK(
      Thread::Create(
          "pstack_watcher",
          "pstack_watcher",
          boost::bind(&PstackWatcher::run, this),
          &thread_));
}

PstackWatcher::~PstackWatcher() {
  shutdown();
}

void PstackWatcher::shutdown() {
  {
    MutexLock guard(lock_);
    running_ = false;
    cond_.broadcast();
  }
  if (thread_) {
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
    thread_.reset();
  }
}

bool PstackWatcher::isRunning() const {
  MutexLock guard(lock_);
  return running_;
}

void PstackWatcher::wait() const {
  MutexLock lock(lock_);
  while (running_) {
    cond_.wait();
  }
}

void PstackWatcher::run() {
  MutexLock guard(lock_);
  if (!running_) {
    return;
  }
  cond_.waitFor(timeout_);
  if (!running_) {
    return;
  }

  WARN_NOT_OK(dumpStacks(kDumpFull), "Unable to print pstack from watcher");
  running_ = false;
  cond_.broadcast();
}

Status PstackWatcher::hasProgram(const char* progname) {
  Subprocess proc({"which", progname});
  proc.DisableStderr();
  proc.DisableStdout();
  RETURN_NOT_OK_PREPEND(
      proc.Start(),
      fmt::format("HasProgram({}): error running 'which'", progname));
  RETURN_NOT_OK(proc.Wait());
  int exitStatus;
  string exitInfo;
  RETURN_NOT_OK(proc.GetExitStatus(&exitStatus, &exitInfo));
  if (exitStatus == 0) {
    return Status::OK();
  }
  return Status::NotFound(fmt::format("can't find {}: {}", progname, exitInfo));
}

Status PstackWatcher::hasGoodGdb() {
  // Check for the existence of gdb.
  RETURN_NOT_OK(hasProgram("gdb"));

  // gdb exists, run it and parse the output of --version. For example:
  //
  // GNU gdb (GDB) Red Hat Enterprise Linux (7.2-75.el6)
  // ...
  //
  // Or:
  //
  // GNU gdb (Ubuntu 7.11.1-0ubuntu1~16.5) 7.11.1
  // ...
  string stdout;
  RETURN_NOT_OK(Subprocess::Call({"gdb", "--version"}, "", &stdout));
  vector<string> lines = Split(stdout, "\n", SkipEmpty());
  if (lines.empty()) {
    return Status::Incomplete("gdb version not found");
  }
  vector<string> words = Split(lines[0], " ", SkipWhitespace());
  if (words.empty()) {
    return Status::Incomplete("could not parse gdb version");
  }
  string version = words[words.size() - 1];
  version = StripPrefixString(version, "(");
  version = StripSuffixString(version, ")");

  // The variable pretty print routine in older versions of gdb is buggy in
  // that it reads the values of all local variables, including uninitialized
  // ones. For some variable types with an embedded length (such as std::string
  // or std::vector), this can lead to all sorts of incorrect memory accesses,
  // causing deadlocks or seemingly infinite loops within gdb.
  //
  // It's not clear exactly when this behavior was fixed, so we whitelist the
  // oldest known good version: the one found in Ubuntu 14.04.
  //
  // See the following gdb bug reports for more information:
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=11868
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=12127
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=16196
  // - https://sourceware.org/bugzilla/show_bug.cgi?id=16286
  autodigit_less lt;
  if (lt(version, "7.7")) {
    return Status::NotSupported("gdb version too old", version);
  }

  return Status::OK();
}

Status PstackWatcher::dumpStacks(int flags) {
  return dumpPidStacks(getpid(), flags);
}

Status PstackWatcher::dumpPidStacks(pid_t pid, int flags) {
  // Prefer GDB if available; it gives us line numbers and thread names.
  Status s = hasGoodGdb();
  if (s.ok()) {
    return runGdbStackDump(pid, flags);
  }
  WARN_NOT_OK(s, "gdb not available");

  // Otherwise, try to use pstack or gstack.
  for (const auto& p : {"pstack", "gstack"}) {
    s = hasProgram(p);
    if (s.ok()) {
      return runPstack(p, pid);
    }
    WARN_NOT_OK(s, fmt::format("{} not available", p));
  }

  return Status::ServiceUnavailable(
      "Neither gdb, pstack, nor gstack appear to be installed.");
}

Status PstackWatcher::runGdbStackDump(pid_t pid, int flags) {
  // Command: gdb -quiet -batch -nx -ex cmd1 -ex cmd2 /proc/$PID/exe $PID
  vector<string> argv;
  argv.emplace_back("gdb");
  // Don't print introductory version/copyright messages.
  argv.emplace_back("-quiet");
  // Exit after processing all of the commands below.
  argv.emplace_back("-batch");
  // Don't run commands from .gdbinit
  argv.emplace_back("-nx");
  argv.emplace_back("-ex");
  argv.emplace_back("set print pretty on");
  argv.emplace_back("-ex");
  argv.emplace_back("info threads");
  argv.emplace_back("-ex");
  argv.emplace_back("thread apply all bt");
  if (flags & kDumpFull) {
    argv.emplace_back("-ex");
    argv.emplace_back("thread apply all bt full");
  }
  string executable;
  Env* env = Env::Default();
  RETURN_NOT_OK(env->GetExecutablePath(&executable));
  argv.push_back(executable);
  argv.push_back(fmt::format("{}", pid));
  return runStackDump(argv);
}

Status PstackWatcher::runPstack(const std::string& progname, pid_t pid) {
  string pidString(fmt::format("{}", pid));
  vector<string> argv;
  argv.push_back(progname);
  argv.push_back(pidString);
  return runStackDump(argv);
}

Status PstackWatcher::runStackDump(const vector<string>& argv) {
  printf("************************ BEGIN STACKS **************************\n");
  if (fflush(stdout) == EOF) {
    return Status::IOError(
        "Unable to flush stdout", ErrnoToString(errno), errno);
  }
  Subprocess pstackProc(argv);
  RETURN_NOT_OK_PREPEND(pstackProc.Start(), "RunStackDump proc.Start() failed");
  int ret;
  RETRY_ON_EINTR(ret, ::close(pstackProc.ReleaseChildStdinFd()));
  if (ret == -1) {
    return Status::IOError(
        "Unable to close child stdin", ErrnoToString(errno), errno);
  }
  RETURN_NOT_OK_PREPEND(pstackProc.Wait(), "RunStackDump proc.Wait() failed");
  int exitCode;
  string exitInfo;
  RETURN_NOT_OK_PREPEND(
      pstackProc.GetExitStatus(&exitCode, &exitInfo),
      "RunStackDump proc.GetExitStatus() failed");
  if (exitCode != 0) {
    return Status::RuntimeError("RunStackDump proc.Wait() error", exitInfo);
  }
  printf("************************* END STACKS ***************************\n");
  if (fflush(stdout) == EOF) {
    return Status::IOError(
        "Unable to flush stdout", ErrnoToString(errno), errno);
  }

  return Status::OK();
}

} // namespace kudu
