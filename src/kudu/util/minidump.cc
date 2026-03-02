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

#include "kudu/util/minidump.h"

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::string;

static constexpr bool kMinidumpPlatformSupported = false;

DECLARE_string(log_dir);

DEFINE_bool(
    enable_minidumps,
    kMinidumpPlatformSupported,
    "Whether to enable minidump generation upon process crash or SIGUSR1. "
    "Currently only supported on Linux systems.");
TAG_FLAG(enable_minidumps, advanced);
TAG_FLAG(enable_minidumps, evolving);
static bool ValidateMinidumpEnabled(const char* /*flagname*/, bool value) {
  if (value && !kMinidumpPlatformSupported) {
    return false; // NOLINT(*)
  }
  return true;
}
DEFINE_validator(enable_minidumps, &ValidateMinidumpEnabled);

DEFINE_string(
    minidump_path,
    "minidumps",
    "Directory to write minidump files to. This "
    "can be either an absolute path or a path relative to --log_dir. Each daemon will "
    "create an additional sub-directory to prevent naming conflicts and to make it "
    "easier to identify a crashing daemon. Minidump files contain crash-related "
    "information in a compressed format. Minidumps will be written when a daemon exits "
    "unexpectedly, for example on an unhandled exception or signal, or when a "
    "SIGUSR1 signal is sent to the process. Cannot be set to an empty value.");
TAG_FLAG(minidump_path, evolving);
// The minidump path cannot be empty.
static bool ValidateMinidumpPath(
    const char* /*flagname*/,
    const string& value) {
  return !value.empty();
}
DEFINE_validator(minidump_path, &ValidateMinidumpPath);

DEFINE_int32(
    max_minidumps,
    9,
    "Maximum number of minidump files to keep per daemon. "
    "Older files are removed first. Set to 0 to keep all minidump files.");
TAG_FLAG(max_minidumps, evolving);

namespace google_breakpad {
// Define this as an empty class to avoid an undefined symbol error on Mac.
class ExceptionHandler {
 public:
  ExceptionHandler() {}
  ~ExceptionHandler() {}
};
} // namespace google_breakpad

namespace kudu {

static sigset_t GetSigset(int signo) {
  sigset_t signals;
  CHECK_EQ(0, sigemptyset(&signals));
  CHECK_EQ(0, sigaddset(&signals, signo));
  return signals;
}

// At the time of writing, we don't support breakpad on Mac so we just stub out
// all the methods defined in the header file.

Status MinidumpExceptionHandler::InitMinidumpExceptionHandler() {
  return Status::OK();
}

// No-op on non-Linux platforms.
Status MinidumpExceptionHandler::RegisterMinidumpExceptionHandler() {
  return Status::OK();
}

void MinidumpExceptionHandler::UnregisterMinidumpExceptionHandler() {}

bool MinidumpExceptionHandler::WriteMinidump() {
  return true;
}

Status MinidumpExceptionHandler::StartUserSignalHandlerThread() {
  return Status::OK();
}

void MinidumpExceptionHandler::StopUserSignalHandlerThread() {}

void MinidumpExceptionHandler::RunUserSignalHandlerThread() {}

std::atomic<int> MinidumpExceptionHandler::current_num_instances_;

MinidumpExceptionHandler::MinidumpExceptionHandler() {
  CHECK_OK(RegisterMinidumpExceptionHandler());
}

MinidumpExceptionHandler::~MinidumpExceptionHandler() {
  UnregisterMinidumpExceptionHandler();
}

Status MinidumpExceptionHandler::DeleteExcessMinidumpFiles(Env* env) {
  // Do not delete minidump files if minidumps are disabled.
  if (!FLAGS_enable_minidumps) {
    return Status::OK();
  }

  int32_t max_minidumps = FLAGS_max_minidumps;
  // Disable rotation if set to 0 or less.
  if (max_minidumps <= 0) {
    return Status::OK();
  }

  // Minidump filenames are created by breakpad in the following format, for
  // example: 7b57915b-ee6a-dbc5-21e59491-5c60a2cf.dmp.
  string pattern = JoinPathSegments(minidump_dir(), "*.dmp");

  // Use mtime to determine which minidumps to delete. While this could
  // potentially be ambiguous if many minidumps were created in quick
  // succession, users can always increase 'FLAGS_max_minidumps' if desired
  // in order to work around the problem.
  return env_util::deleteExcessFilesByPattern(env, pattern, max_minidumps);
}

string MinidumpExceptionHandler::minidump_dir() const {
  return minidump_dir_;
}

Status BlockSigUSR1() {
  sigset_t signals = GetSigset(SIGUSR1);
  int ret = pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  if (ret == 0) {
    return Status::OK();
  }
  return Status::InvalidArgument("pthread_sigmask", errnoToString(ret), ret);
}

} // namespace kudu
