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

#include "kudu/util/rolling_log.h"

#include <unistd.h>

#include <ctime>
#include <iomanip>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <zlib.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/user.h"

using std::ostringstream;
using std::setw;
using std::string;
using std::unique_ptr;

static const int kDefaultRollThresholdBytes = 64 * 1024 * 1024; // 64MB

DECLARE_int32(max_log_files);

namespace kudu {

RollingLog::RollingLog(Env* env, string logDir, string logName)
    : env_(env),
      logDir_(std::move(logDir)),
      logName_(std::move(logName)),
      rollThresholdBytes_(kDefaultRollThresholdBytes),
      maxNumSegments_(FLAGS_max_log_files),
      compressAfterClose_(true) {}

RollingLog::~RollingLog() {
  WARN_NOT_OK(close(), "Unable to close RollingLog");
}

void RollingLog::setRollThresholdBytes(int64_t size) {
  CHECK_GT(size, 0);
  rollThresholdBytes_ = size;
}

void RollingLog::setMaxNumSegments(int numSegments) {
  CHECK_GT(numSegments, 0);
  maxNumSegments_ = numSegments;
}

void RollingLog::setCompressionEnabled(bool compress) {
  compressAfterClose_ = compress;
}

namespace {

string hostnameOrUnknown() {
  string hostname;
  Status s = GetHostname(&hostname);
  if (!s.ok()) {
    return "unknown_host";
  }
  return hostname;
}

string usernameOrUnknown() {
  string userName;
  Status s = getLoggedInUser(&userName);
  if (!s.ok()) {
    return "unknown_user";
  }
  return userName;
}

string formattedTimestamp() {
  // Implementation cribbed from glog/logging.cc
  time_t time = static_cast<time_t>(WallTime_Now());
  struct ::tm tmTime;
  localtime_r(&time, &tmTime);

  ostringstream str;
  str.fill('0');
  str << 1900 + tmTime.tm_year << setw(2) << 1 + tmTime.tm_mon << setw(2)
      << tmTime.tm_mday << '-' << setw(2) << tmTime.tm_hour << setw(2)
      << tmTime.tm_min << setw(2) << tmTime.tm_sec;
  return str.str();
}

} // anonymous namespace

string RollingLog::getLogFileName(int sequence) const {
  return fmt::format(
      "{}.{}.{}.{}.{}.{}.{}",
      gflags::ProgramInvocationShortName(),
      hostnameOrUnknown(),
      usernameOrUnknown(),
      logName_,
      formattedTimestamp(),
      sequence,
      getpid());
}

string RollingLog::getLogFilePattern() const {
  return fmt::format(
      "{}.{}.{}.{}.{}.{}.{}",
      gflags::ProgramInvocationShortName(),
      hostnameOrUnknown(),
      usernameOrUnknown(),
      logName_,
      /* any timestamp */ '*',
      /* any sequence number */ '*',
      /* any pid */ '*');
}

Status RollingLog::open() {
  CHECK(!file_);

  for (int sequence = 0;; sequence++) {
    string path = JoinPathSegments(logDir_, getLogFileName(sequence));
    // Don't reuse an existing path if there is already a log
    // or a compressed log with the same name.
    if (env_->FileExists(path) || env_->FileExists(path + ".gz")) {
      continue;
    }

    WritableFileOptions opts;
    // Logs aren't worth the performance cost of durability.
    opts.sync_on_close = false;
    opts.mode = Env::CREATE_NON_EXISTING;

    RETURN_NOT_OK(env_->NewWritableFile(opts, path, &file_));

    VLOG(1) << "Rolled " << logName_ << " log to new file: " << path;
    break;
  }
  return Status::OK();
}

Status RollingLog::close() {
  if (!file_) {
    return Status::OK();
  }
  string path = file_->filename();
  RETURN_NOT_OK_PREPEND(
      file_->Close(), fmt::format("Unable to close {}", path));
  file_.reset();
  if (compressAfterClose_) {
    WARN_NOT_OK(compressFile(path), "Unable to compress old log file");
  }
  auto glob = JoinPathSegments(logDir_, getLogFilePattern());
  WARN_NOT_OK(
      env_util::deleteExcessFilesByPattern(env_, glob, maxNumSegments_),
      fmt::format("failed to delete old {} log files", logName_));
  return Status::OK();
}

Status RollingLog::append(StringPiece s) {
  if (!file_) {
    RETURN_NOT_OK_PREPEND(open(), "Unable to open log");
  }

  RETURN_NOT_OK(file_->Append(s));
  if (file_->Size() > rollThresholdBytes_) {
    RETURN_NOT_OK_PREPEND(close(), "Unable to close prev log");
    rollCount_++;
    RETURN_NOT_OK_PREPEND(open(), "Unable to open new log");
  }
  return Status::OK();
}

namespace {

Status gzClose(gzFile f) {
  int err = gzclose(f);
  switch (err) {
    case Z_OK:
      return Status::OK();
    case Z_STREAM_ERROR:
      return Status::InvalidArgument("Stream not valid");
    case Z_ERRNO:
      return Status::IOError("IO Error closing stream");
    case Z_MEM_ERROR:
      return Status::RuntimeError("Out of memory");
    case Z_BUF_ERROR:
      return Status::IOError("read ended in the middle of a stream");
    default:
      return Status::IOError("Unknown zlib error", SimpleItoa(err));
  }
}

class ScopedGzipCloser {
 public:
  explicit ScopedGzipCloser(gzFile f) : file_(f) {}

  ~ScopedGzipCloser() {
    if (file_) {
      WARN_NOT_OK(gzClose(file_), "Unable to close gzip stream");
    }
  }

  void cancel() {
    file_ = nullptr;
  }

 private:
  gzFile file_;
};
} // anonymous namespace

// We implement compressFile() manually using zlib APIs rather than forking
// out to '/bin/gzip' since fork() can be expensive on processes that use a
// large amount of memory. During the time of the fork, other threads could end
// up blocked. Implementing it using the zlib stream APIs isn't too much code
// and is less likely to be problematic.
Status RollingLog::compressFile(const std::string& path) const {
  unique_ptr<SequentialFile> inFile;
  RETURN_NOT_OK_PREPEND(
      env_->NewSequentialFile(path, &inFile),
      "Unable to open input file to compress");

  string gzPath = path + ".gz";
  gzFile gzf = gzopen(gzPath.c_str(), "w");
  if (!gzf) {
    return Status::IOError("Unable to open gzip stream");
  }

  ScopedGzipCloser closer(gzf);

  // Loop reading data from the input file and writing to the gzip stream.
  uint8_t buf[32 * 1024];
  while (true) {
    Slice result(buf, arraysize(buf));
    RETURN_NOT_OK_PREPEND(
        inFile->Read(&result), "Unable to read from gzip input");
    if (result.size() == 0) {
      break;
    }
    int n = gzwrite(gzf, result.data(), result.size());
    if (n == 0) {
      int errnum;
      return Status::IOError(
          "Unable to write to gzip output", gzerror(gzf, &errnum));
    }
  }
  closer.cancel();
  RETURN_NOT_OK_PREPEND(gzClose(gzf), "Unable to close gzip output");

  WARN_NOT_OK(
      env_->DeleteFile(path),
      "Unable to delete gzip input file after compression");
  return Status::OK();
}

} // namespace kudu
