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

#include "kudu/fs/fs_manager.h"

#include <ctime>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

DEFINE_bool(
    enable_data_block_fsync,
    true,
    "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
    "Disabling this flag may cause data loss in the event of a system crash.");
TAG_FLAG(enable_data_block_fsync, unsafe);

DEFINE_string(
    fs_wal_dir,
    "",
    "Directory with write-ahead logs. If this is not specified, the "
    "program will not start. May be the same as fs_data_dirs");
TAG_FLAG(fs_wal_dir, stable);
DEFINE_string(
    fs_metadata_dir,
    "",
    "Directory with metadata. If this is not specified, for "
    "compatibility with Kudu 1.6 and below, Kudu will check the "
    "first entry of fs_data_dirs for metadata and use it as the "
    "metadata directory if any exists. If none exists, fs_wal_dir "
    "will be used as the metadata directory.");
TAG_FLAG(fs_metadata_dir, stable);

using kudu::fs::ConsistencyCheckBehavior;
using kudu::fs::FsReport;
using kudu::pb_util::SecureDebugString;
using std::ostream;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace kudu {

// ==========================================================================
//  FS Paths
// ==========================================================================
const char* FsManager::kWalDirName = "wals";
const char* FsManager::kWalFileNamePrefix = "wal";
const char* FsManager::kWalsRecoveryDirSuffix = ".recovery";
const char* FsManager::kTabletMetadataDirName = "tablet-meta";
const char* FsManager::kDataDirName = "data";
const char* FsManager::kCorruptedSuffix = ".corrupted";
const char* FsManager::kInstanceMetadataFileName = "instance";
const char* FsManager::kConsensusMetadataDirName = "consensus-meta";

namespace {
vector<string> getRootNames(const CanonicalizedRootsList& rootList) {
  vector<string> roots;
  std::transform(
      rootList.begin(),
      rootList.end(),
      std::back_inserter(roots),
      [&](const CanonicalizedRootAndStatus& r) { return r.path; });
  return roots;
}
} // namespace

FsManagerOpts::FsManagerOpts()
    : walRoot(FLAGS_fs_wal_dir),
      metadataRoot(FLAGS_fs_metadata_dir),
      readOnly(false),
      consistencyCheck(ConsistencyCheckBehavior::EnforceConsistency) {}

FsManagerOpts::FsManagerOpts(const string& root)
    : walRoot(root),
      readOnly(false),
      consistencyCheck(ConsistencyCheckBehavior::EnforceConsistency) {}

FsManager::FsManager(Env* env, const string& rootPath)
    : env_(DCHECK_NOTNULL(env)),
      opts_(FsManagerOpts(rootPath)),
      initted_(false) {}

FsManager::FsManager(Env* env, FsManagerOpts opts)
    : env_(DCHECK_NOTNULL(env)), opts_(std::move(opts)), initted_(false) {
  DCHECK(
      opts_.consistencyCheck != ConsistencyCheckBehavior::UpdateOnDisk ||
      !opts_.readOnly);
}

FsManager::~FsManager() {}

Status FsManager::init() {
  if (initted_) {
    return Status::OK();
  }

  // The wal root must be set.
  if (opts_.walRoot.empty()) {
    return Status::IOError(
        "Write-ahead log directory (fs_wal_dir) not provided");
  }

  // Deduplicate all of the roots.
  unordered_set<string> allRoots = {opts_.walRoot};

  // If the metadata root not set, Kudu will either use the wal root or the
  // first data root, in which case we needn't canonicalize additional roots.
  if (!opts_.metadataRoot.empty()) {
    allRoots.insert(opts_.metadataRoot);
  }

  // Build a map of original root --> canonicalized root, sanitizing each
  // root as we go and storing the canonicalization status.
  using RootMap = unordered_map<string, CanonicalizedRootAndStatus>;
  RootMap canonicalizedRoots;
  for (const string& root : allRoots) {
    if (root.empty()) {
      return Status::IOError("Empty string provided for path");
    }
    if (root[0] != '/') {
      return Status::IOError(fmt::format("Relative path {} provided", root));
    }
    string rootCopy = root;
    StripWhiteSpace(&rootCopy);
    if (root != rootCopy) {
      return Status::IOError(
          fmt::format("Path {} contains illegal whitespace", root));
    }

    // Strip the basename when canonicalizing, as it may not exist. The
    // dirname, however, must exist.
    string canonicalized;
    Status s = env_->Canonicalize(dirName(root), &canonicalized);
    if (PREDICT_FALSE(!s.ok())) {
      if (s.IsNotFound() || s.isDiskFailure()) {
        // If the directory fails to canonicalize due to disk failure, store
        // the non-canonicalized form and the returned error.
        canonicalized = dirName(root);
      } else {
        return s.cloneAndPrepend(
            fmt::format("Failed to canonicalize {}", root));
      }
    }
    canonicalized = JoinPathSegments(canonicalized, baseName(root));
    auto [it, inserted] = canonicalizedRoots.emplace(
        root, CanonicalizedRootAndStatus{canonicalized, s});
    CHECK(inserted) << "Duplicate root: " << root;
  }

  // All done, use the map to set the canonicalized state.

  auto itWal = canonicalizedRoots.find(opts_.walRoot);
  CHECK(itWal != canonicalizedRoots.end())
      << "Map key not found: " << opts_.walRoot;
  canonicalizedWalFsRoot_ = itWal->second;
  unordered_set<string> uniqueRoots;
  LOG(INFO) << "Data directories (fs_data_dirs) not provided";
  LOG(INFO) << "Using write-ahead log directory (fs_wal_dir) as data directory";
  canonicalizedDataFsRoots_.emplace_back(canonicalizedWalFsRoot_);
  if (uniqueRoots.insert(canonicalizedWalFsRoot_.path).second) {
    canonicalizedAllFsRoots_.emplace_back(canonicalizedWalFsRoot_);
  }

  // Decide on a metadata root to use.
  if (opts_.metadataRoot.empty()) {
    // Check the first data root for metadata.
    const string metaDirInDataRoot = JoinPathSegments(
        canonicalizedDataFsRoots_[0].path, kTabletMetadataDirName);
    // If there is already metadata in the first data root, use it. Otherwise,
    // use the WAL root.
    LOG(INFO) << "Metadata directory not provided";
    if (env_->FileExists(metaDirInDataRoot)) {
      canonicalizedMetadataFsRoot_ = canonicalizedDataFsRoots_[0];
      LOG(INFO) << "Using existing metadata directory in first data directory";
    } else {
      canonicalizedMetadataFsRoot_ = canonicalizedWalFsRoot_;
      LOG(INFO)
          << "Using write-ahead log directory (fs_wal_dir) as metadata directory";
    }
  } else {
    // Keep track of the explicitly-defined metadata root.
    auto itMeta = canonicalizedRoots.find(opts_.metadataRoot);
    CHECK(itMeta != canonicalizedRoots.end())
        << "Map key not found: " << opts_.metadataRoot;
    canonicalizedMetadataFsRoot_ = itMeta->second;
    if (insertIfNotPresent(&uniqueRoots, canonicalizedMetadataFsRoot_.path)) {
      canonicalizedAllFsRoots_.emplace_back(canonicalizedMetadataFsRoot_);
    }
  }

  // The server cannot start if the WAL root or metadata root failed to
  // canonicalize.
  const string& walRoot = canonicalizedWalFsRoot_.path;
  RETURN_NOT_OK_PREPEND(
      canonicalizedWalFsRoot_.status,
      fmt::format(
          "Write-ahead log directory {} failed to canonicalize", walRoot));
  const string& metaRoot = canonicalizedMetadataFsRoot_.path;
  RETURN_NOT_OK_PREPEND(
      canonicalizedMetadataFsRoot_.status,
      fmt::format("Metadata directory {} failed to canonicalize", metaRoot));

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "WAL root: " << canonicalizedWalFsRoot_.path;
    VLOG(1) << "Metadata root: " << canonicalizedMetadataFsRoot_.path;
    VLOG(1) << "Data roots: "
            << JoinStrings(getRootNames(canonicalizedDataFsRoots_), ",");
    VLOG(1) << "All roots: "
            << JoinStrings(getRootNames(canonicalizedAllFsRoots_), ",");
  }

  initted_ = true;
  return Status::OK();
}

Status FsManager::Open(FsReport* report) {
  RETURN_NOT_OK(init());

  // Load and verify the instance metadata files.
  //
  // Done first to minimize side effects in the case that the configured roots
  // are not yet initialized on disk.
  CanonicalizedRootsList missingRoots;
  for (auto& root : canonicalizedAllFsRoots_) {
    if (!root.status.ok()) {
      continue;
    }
    unique_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    Status s = pb_util::ReadPBContainerFromPath(
        env_, getInstanceMetadataPath(root.path), pb.get());
    if (PREDICT_FALSE(!s.ok())) {
      if (s.IsNotFound()) {
        missingRoots.emplace_back(root);
        continue;
      }
      if (s.isDiskFailure()) {
        root.status = s.cloneAndPrepend("Failed to open instance file");
        continue;
      }
      return s;
    }

    if (!metadata_) {
      metadata_.reset(pb.release());
    } else if (pb->uuid() != metadata_->uuid()) {
      return Status::Corruption(
          fmt::format(
              "Mismatched UUIDs across filesystem roots: {} vs. {}; configuring "
              "multiple Kudu processes with the same directory is not supported",
              metadata_->uuid(),
              pb->uuid()));
    }
  }

  if (!metadata_) {
    return Status::NotFound("could not find a healthy instance file");
  }

  // Ensure all of the ancillary directories exist.
  vector<string> ancillaryDirs = {
      GetWalsRootDir(), GetTabletMetadataDir(), GetConsensusMetadataDir()};
  for (const auto& d : ancillaryDirs) {
    bool isDir;
    RETURN_NOT_OK_PREPEND(
        env_->IsDirectory(d, &isDir),
        fmt::format("could not verify required directory {}", d));
    if (!isDir) {
      return Status::Corruption(
          fmt::format(
              "Required directory {} exists but is not a directory", d));
    }
  }

  // In the event of failure, delete everything we created.
  vector<string> createdDirs;
  vector<string> createdFiles;
  auto deleter = folly::makeGuard([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : createdFiles) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = createdDirs.rbegin(); it != createdDirs.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // Create any missing roots, if desired.
  if (opts_.consistencyCheck == ConsistencyCheckBehavior::UpdateOnDisk) {
    RETURN_NOT_OK_PREPEND(
        createFileSystemRoots(
            missingRoots, *metadata_, &createdDirs, &createdFiles),
        "unable to create missing filesystem roots");
  }

  // Only clean temporary files after the data dir manager successfully opened.
  // This ensures that we were able to obtain the exclusive directory locks
  // on the data directories before we start deleting files.
  if (!opts_.readOnly) {
    cleanTmpFiles();
    checkAndFixPermissions();
    createDataDirLayoutForBackwardCompat();
  }

  // Report wal and metadata directories.
  if (report) {
    report->walDir = canonicalizedWalFsRoot_.path;
    report->metadataDir = canonicalizedMetadataFsRoot_.path;
  }

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(
        env_util::syncAllParentDirs(env_, createdDirs, createdDirs),
        "could not sync newly created fs roots");
  }

  LOG(INFO) << "Opened local filesystem: "
            << JoinStrings(getRootNames(canonicalizedAllFsRoots_), ",")
            << std::endl
            << SecureDebugString(*metadata_);

  if (!createdDirs.empty()) {
    LOG(INFO) << "New directories created while opening local filesystem: "
              << JoinStrings(createdDirs, ", ");
  }
  if (!createdFiles.empty()) {
    LOG(INFO) << "New files created while opening local filesystem: "
              << JoinStrings(createdFiles, ", ");
  }

  // Success: do not delete any missing roots created.
  deleter.dismiss();
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout(std::optional<string> uuid) {
  CHECK(!opts_.readOnly);

  RETURN_NOT_OK(init());

  // In the event of failure, delete everything we created.
  vector<string> createdDirs;
  vector<string> createdFiles;
  auto deleter = folly::makeGuard([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : createdFiles) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = createdDirs.rbegin(); it != createdDirs.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // Create the filesystem roots.
  //
  // Files/directories created will NOT be synchronized to disk.
  InstanceMetadataPB metadata;
  RETURN_NOT_OK_PREPEND(
      createInstanceMetadata(std::move(uuid), &metadata),
      "unable to create instance metadata");
  RETURN_NOT_OK_PREPEND(
      FsManager::createFileSystemRoots(
          canonicalizedAllFsRoots_, metadata, &createdDirs, &createdFiles),
      "unable to create file system roots");

  // Create ancillary directories.
  vector<string> ancillaryDirs = {
      GetWalsRootDir(), GetTabletMetadataDir(), GetConsensusMetadataDir()};
  for (const string& dir : ancillaryDirs) {
    bool created;
    RETURN_NOT_OK_PREPEND(
        env_util::createDirIfMissing(env_, dir, &created),
        fmt::format("Unable to create directory {}", dir));
    if (created) {
      createdDirs.emplace_back(dir);
    }
  }

  // Create backward-compat data dir layout so that rollback to older code
  // (which expects DataDirManager artifacts) does not crash.
  createDataDirLayoutForBackwardCompat();

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(
        env_util::syncAllParentDirs(env_, createdDirs, createdFiles),
        "could not sync newly created fs roots");
  }

  // Success: don't delete any files.
  deleter.dismiss();
  return Status::OK();
}

Status FsManager::createFileSystemRoots(
    const CanonicalizedRootsList& canonicalizedRoots,
    const InstanceMetadataPB& metadata,
    vector<string>* createdDirs,
    vector<string>* createdFiles) {
  CHECK(!opts_.readOnly);

  // It's OK if a root already exists as long as there's nothing in it.
  vector<string> nonEmptyRoots;
  for (const auto& root : canonicalizedRoots) {
    if (!root.status.ok()) {
      return Status::IOError(
          "cannot create FS layout; at least one directory "
          "failed to canonicalize",
          root.path);
    }
    if (!env_->FileExists(root.path)) {
      // We'll create the directory below.
      continue;
    }
    bool isEmpty;
    RETURN_NOT_OK_PREPEND(
        env_util::isDirectoryEmpty(env_, root.path, &isEmpty),
        "unable to check if FSManager root is empty");
    if (!isEmpty) {
      nonEmptyRoots.emplace_back(root.path);
    }
  }

  if (!nonEmptyRoots.empty()) {
    return Status::AlreadyPresent(
        fmt::format(
            "FSManager roots already exist: {}",
            JoinStrings(nonEmptyRoots, ",")));
  }

  // All roots are either empty or non-existent. Create missing roots and all
  // subdirectories.
  for (const auto& root : canonicalizedRoots) {
    if (!root.status.ok()) {
      continue;
    }
    string rootName = root.path;
    bool created;
    RETURN_NOT_OK_PREPEND(
        env_util::createDirIfMissing(env_, rootName, &created),
        "unable to create FSManager root");
    if (created) {
      createdDirs->emplace_back(rootName);
    }
    RETURN_NOT_OK_PREPEND(
        writeInstanceMetadata(metadata, rootName),
        "unable to write instance metadata");
    createdFiles->emplace_back(getInstanceMetadataPath(rootName));
  }
  return Status::OK();
}

Status FsManager::createInstanceMetadata(
    std::optional<string> uuid,
    InstanceMetadataPB* metadata) {
  if (uuid) {
    string canonicalizedUuid;
    RETURN_NOT_OK(oidGenerator_.canonicalize(*uuid, &canonicalizedUuid));
    metadata->set_uuid(canonicalizedUuid);
  } else {
    metadata->set_uuid(oidGenerator_.next());
  }

  string timeStr;
  stringAppendStrftime(&timeStr, "%Y-%m-%d %H:%M:%S", time(nullptr), false);
  string hostname;
  if (!getHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  metadata->set_format_stamp(
      fmt::format("Formatted at {} on {}", timeStr, hostname));
  return Status::OK();
}

Status FsManager::writeInstanceMetadata(
    const InstanceMetadataPB& metadata,
    const string& root) {
  const string path = getInstanceMetadataPath(root);

  // The instance metadata is written effectively once per TS, so the
  // durability cost is negligible.
  RETURN_NOT_OK(
      pb_util::WritePBContainerToPath(
          env_, path, metadata, pb_util::kNoOverwrite, pb_util::kSync));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << SecureDebugString(metadata);
  return Status::OK();
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

string FsManager::GetTabletMetadataDir() const {
  DCHECK(initted_);
  return JoinPathSegments(
      canonicalizedMetadataFsRoot_.path, kTabletMetadataDirName);
}

string FsManager::GetTabletMetadataPath(const string& tabletId) const {
  return JoinPathSegments(GetTabletMetadataDir(), tabletId);
}

bool FsManager::isValidTabletId(const string& fname) {
  // Prevent warning logs for hidden files or ./..
  if (hasPrefixString(fname, ".")) {
    VLOG(1) << "Ignoring hidden file in tablet metadata dir: " << fname;
    return false;
  }

  string canonicalizedUuid;
  Status s = oidGenerator_.canonicalize(fname, &canonicalizedUuid);

  if (!s.ok()) {
    LOG(WARNING) << "Ignoring file in tablet metadata dir: " << fname << ": "
                 << s.message().toString();
    return false;
  }

  if (fname != canonicalizedUuid) {
    LOG(WARNING) << "Ignoring file in tablet metadata dir: " << fname << ": "
                 << fmt::format(
                        "canonicalized uuid {} does not match file name",
                        canonicalizedUuid);
    return false;
  }

  return true;
}

Status FsManager::listTabletIds(vector<string>* tabletIds) {
  string dir = GetTabletMetadataDir();
  vector<string> children;
  RETURN_NOT_OK_PREPEND(
      listDir(dir, &children),
      fmt::format("Couldn't list tablets in metadata directory {}", dir));

  vector<string> tablets;
  for (const string& child : children) {
    if (!isValidTabletId(child)) {
      continue;
    }
    tabletIds->push_back(child);
  }
  return Status::OK();
}

string FsManager::getInstanceMetadataPath(const string& root) const {
  return JoinPathSegments(root, kInstanceMetadataFileName);
}

string FsManager::getTabletWalRecoveryDir(const string& tabletId) const {
  string path = JoinPathSegments(GetWalsRootDir(), tabletId);
  strAppend(&path, kWalsRecoveryDirSuffix);
  return path;
}

string FsManager::getWalSegmentFileName(
    const string& tabletId,
    uint64_t sequenceNumber) const {
  return JoinPathSegments(
      getTabletWalDir(tabletId),
      fmt::format(
          "{}-{}", kWalFileNamePrefix, fmt::format("{:09d}", sequenceNumber)));
}

void FsManager::cleanTmpFiles() {
  DCHECK(!opts_.readOnly);
  // Temporary files in the Block Manager directories are cleaned during
  // Block Manager startup.
  for (const auto& s :
       {GetWalsRootDir(), GetTabletMetadataDir(), GetConsensusMetadataDir()}) {
    WARN_NOT_OK(
        env_util::deleteTmpFilesRecursively(env_, s),
        fmt::format("Error deleting tmp files in {}", s));
  }
}

void FsManager::checkAndFixPermissions() {
  for (const auto& root : canonicalizedAllFsRoots_) {
    if (!root.status.ok()) {
      continue;
    }
    WARN_NOT_OK(
        env_->ensureFileModeAdheresToUmask(root.path),
        fmt::format(
            "could not check and fix permissions for path: {}", root.path));
  }
}

// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::dumpFileSystemTree(ostream& out) {
  DCHECK(initted_);

  for (const auto& root : canonicalizedAllFsRoots_) {
    if (!root.status.ok()) {
      continue;
    }
    out << "File-System Root: " << root.path << std::endl;

    vector<string> objects;
    Status s = env_->GetChildren(root.path, &objects);
    if (!s.ok()) {
      LOG(ERROR) << "Unable to list the fs-tree: " << s.ToString();
      return;
    }

    dumpFileSystemTree(out, "|-", root.path, objects);
  }
}

void FsManager::dumpFileSystemTree(
    ostream& out,
    const string& prefix,
    const string& path,
    const vector<string>& objects) {
  for (const string& name : objects) {
    if (name == "." || name == "..") {
      continue;
    }

    vector<string> subObjects;
    string subPath = JoinPathSegments(path, name);
    Status s = env_->GetChildren(subPath, &subObjects);
    if (s.ok()) {
      out << prefix << name << "/" << std::endl;
      dumpFileSystemTree(out, prefix + "---", subPath, subObjects);
    } else {
      out << prefix << name << std::endl;
    }
  }
}

std::ostream& operator<<(std::ostream& o, const BlockId& blockId) {
  return o << blockId.toString();
}

void FsManager::createDataDirLayoutForBackwardCompat() {
  static const char* kBlockManagerInstanceFileName = "block_manager_instance";

  for (const auto& root : canonicalizedDataFsRoots_) {
    if (!root.status.ok()) {
      continue;
    }

    // Create <root>/data/ directory.
    const string dataDir = JoinPathSegments(root.path, kDataDirName);
    bool created;
    Status s = env_util::createDirIfMissing(env_, dataDir, &created);
    if (!s.ok()) {
      WARN_NOT_OK(
          s,
          fmt::format("Could not create backward-compat data dir {}", dataDir));
      continue;
    }

    // Write <root>/data/block_manager_instance if it doesn't already exist.
    const string instancePath =
        JoinPathSegments(dataDir, kBlockManagerInstanceFileName);
    if (env_->FileExists(instancePath)) {
      continue;
    }

    PathInstanceMetadataPB pb;
    const string uuid = oidGenerator_.next();
    pb.mutable_path_set()->set_uuid(uuid);
    pb.mutable_path_set()->add_all_uuids(uuid);
    pb.set_block_manager_type("log");

    uint64_t blockSize;
    s = env_->GetBlockSize(dataDir, &blockSize);
    if (!s.ok()) {
      WARN_NOT_OK(
          s,
          fmt::format(
              "Could not get block size for backward-compat data dir {}",
              dataDir));
      continue;
    }
    pb.set_filesystem_block_size_bytes(blockSize);

    s = pb_util::WritePBContainerToPath(
        env_, instancePath, pb, pb_util::kNoOverwrite, pb_util::kSync);
    WARN_NOT_OK(
        s,
        fmt::format(
            "Could not write backward-compat block_manager_instance at {}",
            instancePath));
  }
}

} // namespace kudu
