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
#ifndef KUDU_UTIL_LOGGING_H
#define KUDU_UTIL_LOGGING_H

#include <iosfwd>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/logging_callback.h"
#include "kudu/util/status.h"

////////////////////////////////////////////////////////////////////////////////
// Redaction support
////////////////////////////////////////////////////////////////////////////////

// Disable redaction of user data while evaluating the expression 'expr'.
// This may be used inline as an expression, such as:
//
//   LOG(INFO) << KUDU_DISABLE_REDACTION(schema.DebugRow(my_row));
//
// or with a block:
//
//  KUDU_DISABLE_REDACTION({
//    LOG(INFO) << schema.DebugRow(my_row);
//  });
//
// Redaction should be disabled in the following cases:
//
// 1) Outputting strings to a "secure" endpoint (for example an authenticated
// and authorized
//    web UI)
//
// 2) Using methods like schema.DebugRow(...) when the parameter is not in fact
// a user-provided
//    row, but instead some piece of metadata such as a partition boundary.
#define KUDU_DISABLE_REDACTION(expr) \
  ([&]() {                           \
    kudu::ScopedDisableRedaction s;  \
    return (expr);                   \
  })()

// Evaluates to 'true' if the caller should redact any user data in the current
// scope. Most callers should instead use KUDU_REDACT(...) defined below, but
// this can be useful to short-circuit expensive logic.
#define KUDU_SHOULD_REDACT()                              \
  ((kudu::g_should_redact == kudu::RedactContext::ALL ||  \
    kudu::g_should_redact == kudu::RedactContext::LOG) && \
   kudu::tls_redact_user_data)

// Either evaluate and return 'expr', or return the string "<redacted>",
// depending on whether redaction is enabled in the current scope.
#define KUDU_REDACT(expr) (KUDU_SHOULD_REDACT() ? kRedactionMessage : (expr))

// Like the above, but with the additional condition that redaction will only
// be performed if 'cond' must be true.
#define KUDU_MAYBE_REDACT_IF(cond, expr) \
  ((KUDU_SHOULD_REDACT() && (cond)) ? kudu::kRedactionMessage : (expr))

////////////////////////////////////////
// Redaction implementation details follow.
////////////////////////////////////////

namespace kudu {

// Flag which allows redaction to be enabled or disabled for a thread context.
// Defaults to enabling redaction, since it's the safer default with respect to
// leaking user data, and it's easier to identify when data is over-redacted
// than vice-versa.
extern __thread bool tls_redact_user_data;

// Redacted log messages are replaced with this constant.
extern const char* const kRedactionMessage;

enum class RedactContext { ALL, LOG, NONE };

// Flag to indicate which redaction context is enabled.
extern kudu::RedactContext g_should_redact;

class ScopedDisableRedaction {
 public:
  ScopedDisableRedaction() : oldVal_(tls_redact_user_data) {
    tls_redact_user_data = false;
  }

  ~ScopedDisableRedaction() {
    tls_redact_user_data = oldVal_;
  }

 private:
  bool oldVal_;
};

} // namespace kudu

////////////////////////////////////////////////////////////////////////////////
// Throttled logging support
////////////////////////////////////////////////////////////////////////////////

// Logs a message throttled to appear at most once every 'n_secs' seconds to
// the given severity.
//
// The log message may include the special token 'THROTTLE_MSG' which expands
// to either an empty string or '[suppressed <n> similar messages]'.
//
// Example usage:
//   KLOG_EVERY_N_SECS(WARNING, 1) << "server is low on memory" << THROTTLE_MSG;
//
//
// Advanced per-instance throttling
// -----------------------------------
// For cases where the throttling should be scoped to a given class instance,
// you may define a logging::LogThrottler object and pass it to the
// KLOG_EVERY_N_SECS_THROTTLER(...) macro. In addition, you must pass a "tag".
// Only log messages with equal tags (by pointer equality) will be throttled.
// For example:
//
//    struct MyThing {
//      string name;
//      LogThrottler throttler;
//    };
//
//    if (...) {
//      LOG_EVERY_N_SECS_THROTTLER(INFO, 1, my_thing->throttler, "coffee") <<
//        my_thing->name << " needs coffee!";
//    } else {
//      LOG_EVERY_N_SECS_THROTTLER(INFO, 1, my_thing->throttler, "wine") <<
//        my_thing->name << " needs wine!";
//    }
//
// In this example, the "coffee"-related message will be collapsed into other
// such messages within the prior one second; however, if the state alternates
// between the "coffee" message and the "wine" message, then each such
// alternation will yield a message.

#define KLOG_EVERY_N_SECS_THROTTLER(severity, n_secs, throttler, tag)       \
  int VARNAME_LINENUM(num_suppressed) = 0;                                  \
  if ((throttler).shouldLog(n_secs, tag, &VARNAME_LINENUM(num_suppressed))) \
  google::LogMessage(                                                       \
      __FILE__,                                                             \
      __LINE__,                                                             \
      google::GLOG_##severity,                                              \
      VARNAME_LINENUM(num_suppressed),                                      \
      &google::LogMessage::SendToLog)                                       \
      .stream()

#define KLOG_EVERY_N_SECS(severity, n_secs)   \
  static logging::LogThrottler LOG_THROTTLER; \
  KLOG_EVERY_N_SECS_THROTTLER(severity, n_secs, LOG_THROTTLER, "no-tag")

namespace kudu {
enum PRIVATE_ThrottleMsg { THROTTLE_MSG };
} // namespace kudu

////////////////////////////////////////////////////////////////////////////////
// Versions of glog macros for "LOG_EVERY" and "LOG_FIRST" that annotate the
// benign races on their internal static variables.
////////////////////////////////////////////////////////////////////////////////

// The "base" macros.
#define KUDU_SOME_KIND_OF_LOG_EVERY_N(severity, n, what_to_do)   \
  static int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;     \
  KUDU_ANNONTATE_BENIGN_RACE(                                    \
      &LOG_OCCURRENCES, "Logging every N is approximate");       \
  KUDU_ANNONTATE_BENIGN_RACE(                                    \
      &LOG_OCCURRENCES_MOD_N, "Logging every N is approximate"); \
  ++LOG_OCCURRENCES;                                             \
  if (++LOG_OCCURRENCES_MOD_N > (n))                             \
    LOG_OCCURRENCES_MOD_N -= (n);                                \
  if (LOG_OCCURRENCES_MOD_N == 1)                                \
  google::LogMessage(                                            \
      __FILE__,                                                  \
      __LINE__,                                                  \
      google::GLOG_##severity,                                   \
      LOG_OCCURRENCES,                                           \
      &what_to_do) /*NOLINT(bugprone-macro-parentheses)*/        \
      .stream()

#define KUDU_SOME_KIND_OF_LOG_FIRST_N(severity, n, what_to_do) \
  static uint64_t LOG_OCCURRENCES = 0;                         \
  KUDU_ANNONTATE_BENIGN_RACE(                                  \
      &LOG_OCCURRENCES, "Logging the first N is approximate"); \
  if (LOG_OCCURRENCES++ < (n))                                 \
  google::LogMessage(                                          \
      __FILE__,                                                \
      __LINE__,                                                \
      google::GLOG_##severity,                                 \
      LOG_OCCURRENCES,                                         \
      &what_to_do) /*NOLINT(bugprone-macro-parentheses)*/      \
      .stream()

// The direct user-facing macros.
#define KLOG_EVERY_N(severity, n)                       \
  GOOGLE_GLOG_COMPILE_ASSERT(                           \
      google::GLOG_##severity < google::NUM_SEVERITIES, \
      INVALID_REQUESTED_LOG_SEVERITY);                  \
  KUDU_SOME_KIND_OF_LOG_EVERY_N(severity, (n), google::LogMessage::SendToLog)

#define KLOG_FIRST_N(severity, n) \
  KUDU_SOME_KIND_OF_LOG_FIRST_N(severity, (n), google::LogMessage::SendToLog)

namespace kudu {

class Env;

// glog doesn't allow multiple invocations of InitGoogleLogging. This method
// conditionally calls InitGoogleLogging only if it hasn't been called before.
//
// It also takes care of installing the google failure signal handler and
// setting the signal handler for SIGPIPE to SIG_IGN.
void initGoogleLoggingSafe(const char* arg);

// Like initGoogleLoggingSafe() but stripped down: no signal handlers are
// installed, regular logging is disabled, and log events of any severity
// will be written to stderr.
//
// These properties make it attractive for us in libraries.
void initGoogleLoggingSafeBasic(const char* arg);

// Demotes stderr logging to ERROR or higher and registers 'cb' as the
// recipient for all log events.
//
// Subsequent calls to registerLoggingCallback no-op (until the callback
// is unregistered with unregisterLoggingCallback()).
void registerLoggingCallback(const LoggingCallback& cb);

// Unregisters a callback previously registered with
// registerLoggingCallback() and promotes stderr logging back to all
// severities.
//
// If no callback is registered, this is a no-op.
void unregisterLoggingCallback();

// Returns the full pathname of the symlink to the most recent log
// file corresponding to this severity
void getFullLogFilename(google::LogSeverity severity, std::string* filename);

// Format a timestamp in the same format as used by GLog.
std::string formatTimestampForLog(kudu::MicrosecondsInt64 microsSinceEpoch);

// Enable asynchronous logging for glog.
// Wraps the glog Logger for INFO, WARNING, and ERROR with an AsyncLogger
// that buffers messages and writes them in a background thread.
// FATAL messages are always logged synchronously.
// Uses FLAGS_log_async_buffer_bytes_per_level to set the buffer size.
// Safe to call multiple times — subsequent calls are no-ops.
void enableAsyncLogging();

// Shuts down the google logging library. Call before exit to ensure that log
// files are flushed.
void shutdownLoggingSafe();

// Deletes excess rotated log files.
//
// Keeps at most 'FLAG_max_log_files' of the most recent log files at every
// severity level, using the file's modified time to determine recency.
Status deleteExcessLogFiles(Env* env);

namespace logging {

// A LogThrottler instance tracks the throttling state for a particular
// log message.
//
// This is used internally by KLOG_EVERY_N_SECS, but can also be used
// explicitly in conjunction with KLOG_EVERY_N_SECS_THROTTLER. See the
// macro descriptions above for details.
class LogThrottler {
 public:
  LogThrottler() : numSuppressed_(0), lastTs_(0), lastTag_(nullptr) {
    KUDU_ANNONTATE_BENIGN_RACE_SIZED(
        this, sizeof(*this), "OK to be sloppy with log throttling");
  }

  bool shouldLog(int n_secs, const char* tag, int* num_suppressed) {
    kudu::MicrosecondsInt64 ts = getMonoTimeMicros();

    // When we switch tags, we should not show the "suppressed" messages,
    // because in fact it's a different message that we skipped. So, reset it to
    // zero, and always log the new message.
    if (tag != lastTag_) {
      *num_suppressed = numSuppressed_ = 0;
      lastTag_ = tag;
      lastTs_ = ts;
      return true;
    }

    if (ts - lastTs_ < n_secs * 1000000) {
      *num_suppressed =
          base::subtle::NoBarrier_AtomicIncrement(&numSuppressed_, 1);
      return false;
    }
    lastTs_ = ts;
    *num_suppressed =
        base::subtle::NoBarrier_AtomicExchange(&numSuppressed_, 0);
    return true;
  }

 private:
  Atomic32 numSuppressed_;
  kudu::MicrosecondsInt64 lastTs_;
  const char* lastTag_;
};
} // namespace logging

std::ostream& operator<<(std::ostream& os, const PRIVATE_ThrottleMsg&);

// Convenience macros to prefix log messages with some prefix, these are the
// unlocked versions and should not obtain a lock (if one is required to obtain
// the prefix). There must be a logPrefixUnlocked()/logPrefix() method
// available in the current scope in order to use these macros.
#define LOG_WITH_PREFIX_UNLOCKED(severity) LOG(severity) << logPrefixUnlocked()
#define VLOG_WITH_PREFIX_UNLOCKED(verboselevel) \
  LOG_IF(INFO, VLOG_IS_ON(verboselevel)) << logPrefixUnlocked()
#define LOG_WITH_PREFIX_UNLOCKED_EVERY_N(severity, n) \
  LOG_EVERY_N(severity, n) << logPrefixUnlocked()

// Same as the above, but obtain the lock.
#define LOG_WITH_PREFIX(severity) LOG(severity) << logPrefix()
#define VLOG_WITH_PREFIX(verboselevel) \
  LOG_IF(INFO, VLOG_IS_ON(verboselevel)) << logPrefix()

} // namespace kudu

#endif // KUDU_UTIL_LOGGING_H
