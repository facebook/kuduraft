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

#include "kudu/util/env_util.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/bind.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_int64(
    disk_reserved_bytes_free_for_testing,
    -1,
    "For testing only! Set to number of bytes free on each filesystem. "
    "Set to -1 to disable this test-specific override");
TAG_FLAG(disk_reserved_bytes_free_for_testing, runtime);
TAG_FLAG(disk_reserved_bytes_free_for_testing, unsafe);

// We define some flags for testing purposes: Two prefixes and their associated
// "bytes free" overrides.
DEFINE_string(
    disk_reserved_override_prefix_1_path_for_testing,
    "",
    "For testing only! Specifies a prefix to override the visible 'bytes free' on. "
    "Use --disk_reserved_override_prefix_1_bytes_free_for_testing to set the number of "
    "bytes free for this path prefix. Set to empty string to disable.");
DEFINE_int64(
    disk_reserved_override_prefix_1_bytes_free_for_testing,
    -1,
    "For testing only! Set number of bytes free on the path prefix specified by "
    "--disk_reserved_override_prefix_1_path_for_testing. Set to -1 to disable.");
DEFINE_string(
    disk_reserved_override_prefix_2_path_for_testing,
    "",
    "For testing only! Specifies a prefix to override the visible 'bytes free' on. "
    "Use --disk_reserved_override_prefix_2_bytes_free_for_testing to set the number of "
    "bytes free for this path prefix. Set to empty string to disable.");
DEFINE_int64(
    disk_reserved_override_prefix_2_bytes_free_for_testing,
    -1,
    "For testing only! Set number of bytes free on the path prefix specified by "
    "--disk_reserved_override_prefix_2_path_for_testing. Set to -1 to disable.");
TAG_FLAG(disk_reserved_override_prefix_1_path_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_2_path_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_1_bytes_free_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_2_bytes_free_for_testing, unsafe);
TAG_FLAG(disk_reserved_override_prefix_1_bytes_free_for_testing, runtime);
TAG_FLAG(disk_reserved_override_prefix_2_bytes_free_for_testing, runtime);

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace env_util {

Status
openFileForWrite(Env* env, const string& path, shared_ptr<WritableFile>* file) {
  return openFileForWrite(WritableFileOptions(), env, path, file);
}

Status openFileForWrite(
    const WritableFileOptions& opts,
    Env* env,
    const string& path,
    shared_ptr<WritableFile>* file) {
  unique_ptr<WritableFile> w;
  RETURN_NOT_OK(env->NewWritableFile(opts, path, &w));
  file->reset(w.release());
  return Status::OK();
}

Status openFileForRandom(
    Env* env,
    const string& path,
    shared_ptr<RandomAccessFile>* file) {
  unique_ptr<RandomAccessFile> r;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

Status openFileForSequential(
    Env* env,
    const string& path,
    shared_ptr<SequentialFile>* file) {
  unique_ptr<SequentialFile> r;
  RETURN_NOT_OK(env->NewSequentialFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

// If any of the override gflags specifies an override for the given path, then
// override the free bytes to match what is specified in the flag. See the
// definitions of these test-only flags for more information.
static void overrideBytesFreeWithTestingFlags(
    const string& path,
    int64_t* bytesFree) {
  const string* prefixes[] = {
      &FLAGS_disk_reserved_override_prefix_1_path_for_testing,
      &FLAGS_disk_reserved_override_prefix_2_path_for_testing};
  const int64_t* overrides[] = {
      &FLAGS_disk_reserved_override_prefix_1_bytes_free_for_testing,
      &FLAGS_disk_reserved_override_prefix_2_bytes_free_for_testing};
  for (int i = 0; i < arraysize(prefixes); i++) {
    if (*overrides[i] != -1 && !prefixes[i]->empty() &&
        hasPrefixString(path, *prefixes[i])) {
      *bytesFree = *overrides[i];
      return;
    }
  }
}

Status verifySufficientDiskSpace(
    Env* env,
    const std::string& path,
    int64_t requestedBytes,
    int64_t reservedBytes) {
  const int64_t kOnePercentReservation = -1;
  DCHECK_GE(requestedBytes, 0);

  SpaceInfo spaceInfo;
  RETURN_NOT_OK(env->GetSpaceInfo(path, &spaceInfo));
  int64_t availableBytes = spaceInfo.free_bytes;

  // Allow overriding these values by tests.
  if (PREDICT_FALSE(FLAGS_disk_reserved_bytes_free_for_testing > -1)) {
    availableBytes = FLAGS_disk_reserved_bytes_free_for_testing;
  }
  if (PREDICT_FALSE(
          FLAGS_disk_reserved_override_prefix_1_bytes_free_for_testing != -1 ||
          FLAGS_disk_reserved_override_prefix_2_bytes_free_for_testing != -1)) {
    overrideBytesFreeWithTestingFlags(path, &availableBytes);
  }

  // If they requested a one percent reservation, calculate what that is in
  // bytes.
  if (reservedBytes == kOnePercentReservation) {
    reservedBytes = spaceInfo.capacity_bytes / 100;
  }

  if (availableBytes - requestedBytes < reservedBytes) {
    return Status::IOError(
        fmt::format(
            "Insufficient disk space to allocate {} bytes under path {} "
            "({} bytes available vs {} bytes reserved)",
            requestedBytes,
            path,
            availableBytes,
            reservedBytes),
        "",
        ENOSPC);
  }
  return Status::OK();
}

Status createDirIfMissing(Env* env, const string& path, bool* created) {
  Status s = env->CreateDir(path);
  if (created != nullptr) {
    *created = s.ok();
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

Status createDirsRecursively(Env* env, const string& path) {
  vector<string> segments = SplitPath(path);
  string partialPath;
  for (const string& segment : segments) {
    partialPath =
        partialPath.empty() ? segment : JoinPathSegments(partialPath, segment);
    bool isDir;
    Status s = env->IsDirectory(partialPath, &isDir);
    if (s.ok()) {
      // We didn't get a NotFound error, so something is there.
      if (isDir) {
        continue; // It's a normal directory.
      }
      // Maybe a file or a symlink. Let's try to follow the symlink.
      string realPartialPath;
      RETURN_NOT_OK(env->Canonicalize(partialPath, &realPartialPath));
      s = env->IsDirectory(realPartialPath, &isDir);
      if (s.ok() && isDir) {
        continue; // It's a symlink to a directory.
      }
    }
    RETURN_NOT_OK_PREPEND(
        env->CreateDir(partialPath), "Unable to create directory");
  }
  return Status::OK();
}

Status copyFile(
    Env* env,
    const string& sourcePath,
    const string& destPath,
    WritableFileOptions opts) {
  unique_ptr<SequentialFile> source;
  RETURN_NOT_OK(env->NewSequentialFile(sourcePath, &source));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(sourcePath, &size));

  unique_ptr<WritableFile> dest;
  RETURN_NOT_OK(env->NewWritableFile(opts, destPath, &dest));
  RETURN_NOT_OK(dest->PreAllocate(size));

  const int32_t kBufferSize = 1024 * 1024;
  unique_ptr<uint8_t[]> scratch(new uint8_t[kBufferSize]);

  uint64_t bytesRead = 0;
  while (bytesRead < size) {
    uint64_t maxBytesToRead = std::min<uint64_t>(size - bytesRead, kBufferSize);
    Slice data(scratch.get(), maxBytesToRead);
    RETURN_NOT_OK(source->Read(&data));
    RETURN_NOT_OK(dest->Append(data));
    bytesRead += data.size();
  }
  return Status::OK();
}

Status
deleteExcessFilesByPattern(Env* env, const string& pattern, int maxMatches) {
  // Negative numbers don't make sense for our interface.
  DCHECK_GE(maxMatches, 0);

  vector<string> matchingFiles;
  RETURN_NOT_OK(env->Glob(pattern, &matchingFiles));

  if (matchingFiles.size() <= maxMatches) {
    return Status::OK();
  }

  vector<pair<time_t, string>> matchingFileMtimes;
  for (string& matchingFilePath : matchingFiles) {
    int64_t mtime;
    RETURN_NOT_OK(env->GetFileModifiedTime(matchingFilePath, &mtime));
    matchingFileMtimes.emplace_back(mtime, std::move(matchingFilePath));
  }

  // Use mtime to determine which matching files to delete. This could
  // potentially be ambiguous, depending on the resolution of last-modified
  // timestamp in the filesystem, but that is part of the contract.
  std::sort(matchingFileMtimes.begin(), matchingFileMtimes.end());
  matchingFileMtimes.resize(matchingFileMtimes.size() - maxMatches);

  for (const auto& matchingFile : matchingFileMtimes) {
    RETURN_NOT_OK(env->DeleteFile(matchingFile.second));
  }

  return Status::OK();
}

// Callback for deleteTmpFilesRecursively().
//
// Tests 'basename' for the Kudu-specific tmp file infix, and if found,
// deletes the file.
static Status deleteTmpFilesRecursivelyCb(
    Env* env,
    Env::FileType fileType,
    const string& dirname,
    const string& basename) {
  if (fileType != Env::FILE_TYPE) {
    // Skip directories.
    return Status::OK();
  }

  if (basename.find(kTmpInfix) != string::npos) {
    string filename = JoinPathSegments(dirname, basename);
    WARN_NOT_OK(
        env->DeleteFile(filename),
        fmt::format("Failed to remove temporary file {}", filename));
  }
  return Status::OK();
}

Status deleteTmpFilesRecursively(Env* env, const string& path) {
  return env->Walk(
      path, Env::PRE_ORDER, Bind(&deleteTmpFilesRecursivelyCb, env));
}

Status isDirectoryEmpty(Env* env, const string& path, bool* isEmpty) {
  vector<string> children;
  RETURN_NOT_OK(env->GetChildren(path, &children));
  for (const auto& c : children) {
    if (c == "." || c == "..") {
      continue;
    }
    *isEmpty = false;
    return Status::OK();
  }
  *isEmpty = true;
  return Status::OK();
}

Status syncAllParentDirs(
    Env* env,
    const vector<string>& dirs,
    const vector<string>& files) {
  // An unordered_set is used to deduplicate the set of directories.
  unordered_set<string> toSync;
  for (const auto& d : dirs) {
    toSync.insert(DirName(d));
  }
  for (const auto& f : files) {
    toSync.insert(DirName(f));
  }
  for (const auto& d : toSync) {
    RETURN_NOT_OK_PREPEND(
        env->SyncDir(d), fmt::format("unable to synchronize directory {}", d));
  }
  return Status::OK();
}

Status listFilesInDir(Env* env, const string& path, vector<string>* entries) {
  RETURN_NOT_OK(env->GetChildren(path, entries));
  auto iter = entries->begin();
  while (iter != entries->end()) {
    if (*iter == "." || *iter == ".." ||
        iter->find(kTmpInfix) != string::npos) {
      iter = entries->erase(iter);
      continue;
    }
    ++iter;
  }
  return Status::OK();
}

} // namespace env_util
} // namespace kudu
