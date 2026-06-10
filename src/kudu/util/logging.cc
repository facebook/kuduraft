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
#include "kudu/util/logging.h"

#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <utility>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include <folly/synchronization/CallOnce.h>
#include "kudu/gutil/callback.h" // IWYU pragma: keep
#include "kudu/gutil/port.h"
#include "kudu/gutil/spinlock.h"

#include "kudu/util/async_logger.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging_callback.h"
#include "kudu/util/minidump.h"
#include "kudu/util/signal.h"
#include "kudu/util/status.h"

DEFINE_string(
    log_filename,
    "",
    "Prefix of log filename - "
    "full path is <log_dir>/<log_filename>.[INFO|WARN|ERROR|FATAL]");
TAG_FLAG(log_filename, stable);

DEFINE_bool(
    log_async,
    true,
    "Enable asynchronous writing to log files. This improves "
    "latency and stability.");
TAG_FLAG(log_async, hidden);

DEFINE_int32(
    log_async_buffer_bytes_per_level,
    2 * 1024 * 1024,
    "The number of bytes of buffer space used by each log "
    "level. Only relevant when --log_async is enabled.");
TAG_FLAG(log_async_buffer_bytes_per_level, hidden);

DEFINE_int32(
    max_log_files,
    10,
    "Maximum number of log files to retain per severity level. The most recent "
    "log files are retained. If set to 0, all log files are retained.");
TAG_FLAG(max_log_files, runtime);
TAG_FLAG(max_log_files, experimental);

#define PROJ_NAME "kudu"

bool loggingInitialized = false;

using namespace std; // NOLINT(*)
using namespace boost::uuids; // NOLINT(*)

using base::SpinLock;
using base::SpinLockHolder;

namespace kudu {

__thread bool tlsRedactUserData = true;
kudu::RedactContext gShouldRedact;
const char* const kRedactionMessage = "<redacted>";

namespace {

class SimpleSink : public google::LogSink {
 public:
  explicit SimpleSink(LoggingCallback cb) : cb_(std::move(cb)) {}

  virtual ~SimpleSink() override {}

  virtual void send(
      google::LogSeverity severity,
      const char* fullFilename,
      const char* /* base_filename */,
      int line,
      const struct ::tm* tm_time,
      const char* message,
      size_t messageLen) override {
    LogSeverity kuduSeverity;
    switch (severity) {
      case google::INFO:
        kuduSeverity = kSeverityInfo;
        break;
      case google::WARNING:
        kuduSeverity = kSeverityWarning;
        break;
      case google::ERROR:
        kuduSeverity = kSeverityError;
        break;
      case google::FATAL:
        kuduSeverity = kSeverityFatal;
        break;
      default:
        LOG(FATAL) << "Unknown glog severity: " << severity;
    }
    cb_.Run(kuduSeverity, fullFilename, line, tm_time, message, messageLen);
  }

 private:
  LoggingCallback cb_;
};

SpinLock loggingMutex(base::kLinkerInitialized);

// There can only be a single instance of a SimpleSink.
//
// Protected by 'loggingMutex'.
SimpleSink* registeredSink = nullptr;

// Records the logging severity after the first call to
// initGoogleLoggingSafe{Basic}. Calls to unregisterLoggingCallback()
// will restore stderr logging back to this severity level.
//
// Protected by 'loggingMutex'.
int initialStderrSeverity;

void unregisterLoggingCallbackUnlocked() {
  CHECK(loggingMutex.isHeld());
  CHECK(registeredSink);

  // Restore logging to stderr, then remove our sink. This ordering ensures
  // that no log messages are missed.
  google::SetStderrLogging(initialStderrSeverity);
  google::RemoveLogSink(registeredSink);
  delete registeredSink;
  registeredSink = nullptr;
}

void flushCoverageOnExit() {
  // Coverage flushing is not re-entrant, but this might be called from a
  // crash signal context, so avoid re-entrancy.
  static __thread bool inCall = false;
  if (inCall) {
    return;
  }
  inCall = true;

  // The failure writer will be called multiple times per exit.
  // We only need to flush coverage once. We use a 'once' here so that,
  // if another thread is already flushing, we'll block and wait for them
  // to finish before allowing this thread to call abort().
  static std::once_flag once;
  std::call_once(once, [] {
    static const char msg[] = "Flushing coverage data before crash...\n";
    write(STDERR_FILENO, msg, arraysize(msg));
    tryFlushCoverage();
  });
  inCall = false;
}

// On SEGVs, etc, glog will call this function to write the error to stderr.
// This implementation is copied from glog with the exception that we also flush
// coverage the first time it's called.
//
// NOTE: this is only used in coverage builds!
void failureWriterWithCoverage(const char* data, int size) {
  flushCoverageOnExit();

  // Original implementation from glog:
  if (write(STDERR_FILENO, data, size) < 0) {
    // Ignore errors.
  }
}

// GLog "failure function". This is called in the case of LOG(FATAL) to
// ensure that we flush coverage even on crashes.
//
// NOTE: this is only used in coverage builds!
[[noreturn]] void flushCoverageAndAbort() {
  flushCoverageOnExit();
  abort();
}
} // anonymous namespace

void enableAsyncLogging() {
  static folly::once_flag once;
  folly::call_once(once, [] {
    debug::ScopedLeakCheckDisabler leaky;

    // Enable Async for every level except for FATAL. Fatal should be
    // synchronous to ensure that we get the fatal log message written before
    // exiting.
    for (auto level : {google::INFO, google::WARNING, google::ERROR}) {
      auto* orig = google::base::GetLogger(level);
      auto* async =
          new AsyncLogger(orig, FLAGS_log_async_buffer_bytes_per_level);
      async->start();
      google::base::SetLogger(level, async);
    }

    LOG(INFO) << "Async logging enabled with buffer size "
              << FLAGS_log_async_buffer_bytes_per_level << " bytes per level";
  });
}

void initGoogleLoggingSafe(const char* arg) {
  SpinLockHolder l(loggingMutex);
  if (loggingInitialized) {
    return;
  }

  google::InstallFailureSignalHandler();

  if (!FLAGS_log_filename.empty()) {
    for (int severity = google::INFO; severity <= google::FATAL; ++severity) {
      google::SetLogSymlink(severity, FLAGS_log_filename.c_str());
    }
  }

  // This forces our logging to use /tmp rather than looking for a
  // temporary directory if none is specified. This is done so that we
  // can reliably construct the log file name without duplicating the
  // complex logic that glog uses to guess at a temporary dir.
  if (FLAGS_log_dir.empty()) {
    FLAGS_log_dir = "/tmp";
  }

  if (!FLAGS_logtostderr) {
    // Verify that a log file can be created in log_dir by creating a tmp file.
    ostringstream ss;
    random_generator uuidGenerator;
    ss << FLAGS_log_dir << "/" << PROJ_NAME "_test_log." << uuidGenerator();
    const string fileName = ss.str();
    ofstream testFile(fileName.c_str());
    if (!testFile.is_open()) {
      ostringstream errorMsg;
      errorMsg << "Could not open file in log_dir " << FLAGS_log_dir;
      perror(errorMsg.str().c_str());
      // Unlock the mutex before exiting the program to avoid mutex d'tor
      // assert.
      loggingMutex.unlock();
      exit(1);
    }
    remove(fileName.c_str());
  }

  google::InitGoogleLogging(arg);

  // In coverage builds, we should flush coverage before exiting on crash.
  // This way, fault injection tests still capture coverage of the daemon
  // that "crashed".
  if (isCoverageBuild()) {
    // We have to use both the "failure writer" and the "FailureFunction".
    // This allows us to handle both LOG(FATAL) and unintended crashes like
    // SEGVs.
    google::InstallFailureWriter(failureWriterWithCoverage);
    google::InstallFailureFunction(flushCoverageAndAbort);
  }

  // Needs to be done after InitGoogleLogging
  if (FLAGS_log_filename.empty()) {
    CHECK_STRNE(gflags::ProgramInvocationShortName(), "UNKNOWN")
        << ": must initialize gflags before glog";
    FLAGS_log_filename = gflags::ProgramInvocationShortName();
  }

  // File logging: on.
  // Stderr logging threshold: FLAGS_stderrthreshold.
  // Sink logging: off.
  initialStderrSeverity = FLAGS_stderrthreshold;

  // Ignore SIGPIPE early in the startup process so that threads writing to TLS
  // sockets do not crash when writing to a closed socket. See KUDU-1910.
  ignoreSigPipe();

  // For minidump support. Must be called before logging threads started.
  CHECK_OK(blockSigUsr1());

  if (FLAGS_log_async) {
    enableAsyncLogging();
  }

  loggingInitialized = true;
}

void initGoogleLoggingSafeBasic(const char* arg) {
  SpinLockHolder l(loggingMutex);
  if (loggingInitialized) {
    return;
  }

  google::InitGoogleLogging(arg);

  // This also disables file-based logging.
  google::LogToStderr();

  // File logging: off.
  // Stderr logging threshold: INFO.
  // Sink logging: off.
  initialStderrSeverity = google::INFO;
  loggingInitialized = true;
}

void registerLoggingCallback(const LoggingCallback& cb) {
  SpinLockHolder l(loggingMutex);
  CHECK(loggingInitialized);

  if (registeredSink) {
    LOG(WARNING) << "Cannot register logging callback: one already registered";
    return;
  }

  // AddLogSink() claims to take ownership of the sink, but it doesn't
  // really; it actually expects it to remain valid until
  // google::ShutdownGoogleLogging() is called.
  registeredSink = new SimpleSink(cb);
  google::AddLogSink(registeredSink);

  // Even when stderr logging is ostensibly off, it's still emitting
  // ERROR-level stuff. This is the default.
  google::SetStderrLogging(google::ERROR);

  // File logging: yes, if initGoogleLoggingSafe() was called earlier.
  // Stderr logging threshold: ERROR.
  // Sink logging: on.
}

void unregisterLoggingCallback() {
  SpinLockHolder l(loggingMutex);
  CHECK(loggingInitialized);

  if (!registeredSink) {
    LOG(WARNING) << "Cannot unregister logging callback: none registered";
    return;
  }

  unregisterLoggingCallbackUnlocked();
  // File logging: yes, if initGoogleLoggingSafe() was called earlier.
  // Stderr logging threshold: initialStderrSeverity.
  // Sink logging: off.
}

void getFullLogFilename(google::LogSeverity severity, string* filename) {
  ostringstream ss;
  ss << FLAGS_log_dir << "/" << FLAGS_log_filename << "."
     << google::GetLogSeverityName(severity);
  *filename = ss.str();
}

std::string formatTimestampForLog(kudu::MicrosecondsInt64 microsSinceEpoch) {
  time_t secsSinceEpoch = microsSinceEpoch / 1000000;
  int usecs = microsSinceEpoch % 1000000;
  struct tm tm_time;
  localtime_r(&secsSinceEpoch, &tm_time);

  return fmt::format(
      "{:02d}{:02d} {:02d}:{:02d}:{:02d}.{:06d}",
      1 + tm_time.tm_mon,
      tm_time.tm_mday,
      tm_time.tm_hour,
      tm_time.tm_min,
      tm_time.tm_sec,
      usecs);
}

void shutdownLoggingSafe() {
  SpinLockHolder l(loggingMutex);
  if (!loggingInitialized) {
    return;
  }

  if (registeredSink) {
    unregisterLoggingCallbackUnlocked();
  }

  google::ShutdownGoogleLogging();

  loggingInitialized = false;
}

Status deleteExcessLogFiles(Env* env) {
  int32_t maxLogFiles = FLAGS_max_log_files;
  // Ignore bad input or disable log rotation.
  if (maxLogFiles <= 0) {
    return Status::OK();
  }

  for (int severity = 0; severity < google::NUM_SEVERITIES; ++severity) {
    // Build glob pattern for input
    // e.g. /var/log/kudu/kudu-master.*.INFO.*
    string pattern = fmt::format(
        "{}/{}.*.{}.*",
        FLAGS_log_dir,
        FLAGS_log_filename,
        google::GetLogSeverityName(severity));

    // Keep the 'maxLogFiles' most recent log files, as compared by
    // modification time. Glog files contain a second-granularity timestamp in
    // the name, so this could potentially use the filename sort order as
    // guaranteed by glob, however this code has been adapted from Impala which
    // uses mtime to determine which files to delete, and there haven't been any
    // issues in production settings.
    RETURN_NOT_OK(
        env_util::deleteExcessFilesByPattern(env, pattern, maxLogFiles));
  }
  return Status::OK();
}

// Support for the special kThrottleMsg token in a log message stream.
ostream& operator<<(ostream& os, const PrivateThrottleMsg& /*unused*/) {
  using google::LogMessage;
#ifdef DISABLE_RTTI
  LogMessage::LogStream* log = static_cast<LogMessage::LogStream*>(&os);
#else
  LogMessage::LogStream* log = dynamic_cast<LogMessage::LogStream*>(&os);
#endif
  CHECK(log && log == log->self())
      << "You must not use COUNTER with non-glog ostream";
  int ctr = log->ctr();
  if (ctr > 0) {
    os << " [suppressed " << ctr << " similar messages]";
  }
  return os;
}

} // namespace kudu
