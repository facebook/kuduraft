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

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <optional>

#include "kudu/gutil/macros.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {

class BlockId;
class InstanceMetadataPB;
class MemTracker;

struct CanonicalizedRootAndStatus {
  std::string path;
  Status status;
};
using CanonicalizedRootsList = std::vector<CanonicalizedRootAndStatus>;

namespace fs {

// Defines the behavior of the consistency checks performed when the directory
// manager is opened.
enum class ConsistencyCheckBehavior {
  // If the data directories don't match the on-disk path sets, fail.
  ENFORCE_CONSISTENCY,

  // If the data directories don't match the on-disk path sets, update the
  // on-disk data to match. The directory manager must not be read-only.
  UPDATE_ON_DISK,

  // If the data directories don't match the on-disk path sets, continue
  // without updating the on-disk data.
  IGNORE_INCONSISTENCY
};

struct FsReport;

} // namespace fs

namespace itest {
class MiniClusterFsInspector;
} // namespace itest

namespace tserver {
class MiniTabletServerTest_TestFsLayoutEndToEnd_Test;
} // namespace tserver

struct FsManagerOpts {
  // Creates a new FsManagerOpts with default values.
  FsManagerOpts();

  // Creates a new FsManagerOpts with default values except 'wal_root' and
  // 'data_roots', which are both initialized to 'root'.
  //
  // Should only be used in unit tests.
  explicit FsManagerOpts(const std::string& root);

  // The entity under which all metrics should be grouped. If null, metrics
  // will not be produced.
  //
  // Defaults to null.
  std::shared_ptr<MetricEntity> metric_entity;

  // The memory tracker under which all new memory trackers will be parented.
  // If null, new memory trackers will be parented to the root tracker.
  //
  // Defaults to null.
  std::shared_ptr<MemTracker> parent_mem_tracker;

  // The directory root where WALs will be stored. Cannot be empty.
  std::string wal_root;

  // The directory root where metadata will be stored. If empty, Kudu will use
  // the WAL root, or the first configured data root if metadata already exists
  // in it from a previous deployment (the only option in Kudu 1.6 and below
  // was to use the first data root).
  std::string metadata_root;

  // Whether or not read-write operations should be allowed.
  //
  // Defaults to false.
  bool read_only;

  // The behavior to use when comparing 'data_roots' to the on-disk path sets.
  //
  // Defaults to ENFORCE_CONSISTENCY.
  fs::ConsistencyCheckBehavior consistency_check;
};

// FsManager provides helpers to read data and metadata files,
// and it's responsible for abstracting the file-system layout.
//
// The user should not be aware of where files are placed,
// but instead should interact with the storage in terms of "open the block xyz"
// or "write a new schema metadata file for table kwz".
//
// The current layout is:
//    <kudu.root.dir>/data/
//    <kudu.root.dir>/data/<prefix-0>/<prefix-2>/<prefix-4>/<name>
class FsManager {
 public:
  static const char* kWalFileNamePrefix;
  static const char* kWalsRecoveryDirSuffix;

  // Only for unit tests.
  FsManager(Env* env, const std::string& rootPath);

  FsManager(Env* env, FsManagerOpts opts);
  ~FsManager();

  // Initialize and load the basic filesystem metadata, checking it for
  // inconsistencies. If found, and if the FsManager was not constructed in
  // read-only mode, an attempt will be made to repair them.
  //
  // If 'report' is not null, it will be populated with the results of the
  // check (and repair, if applicable); otherwise, the results of the check
  // will be logged and the presence of fatal inconsistencies will manifest as
  // a returned error.
  //
  // If the filesystem has not been initialized, returns NotFound. In that
  // case, CreateInitialFileSystemLayout() may be used to initialize the
  // on-disk and in-memory structures.
  Status Open(fs::FsReport* report = nullptr);

  // Create the initial filesystem layout. If 'uuid' is provided, uses it as
  // uuid of the filesystem. Otherwise generates one at random.
  //
  // Returns an error if the file system is already initialized.
  Status CreateInitialFileSystemLayout(std::optional<std::string> uuid = {});

  void DumpFileSystemTree(std::ostream& out);

  // Return the UUID persisted in the local filesystem. If Open()
  // has not been called, this will crash.
  const std::string& uuid() const;

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  std::vector<std::string> GetDataRootDirs() const;

  std::string GetWalsRootDir() const {
    DCHECK(initted_);
    return JoinPathSegments(canonicalizedWalFsRoot_.path, kWalDirName);
  }

  std::string GetTabletWalDir(const std::string& tabletId) const {
    return JoinPathSegments(GetWalsRootDir(), tabletId);
  }

  std::string GetTabletWalRecoveryDir(const std::string& tabletId) const;

  std::string GetWalSegmentFileName(
      const std::string& tabletId,
      uint64_t sequenceNumber) const;

  // Return the directory where tablet superblocks should be stored.
  std::string GetTabletMetadataDir() const;

  // Return the path for a specific tablet's superblock.
  std::string GetTabletMetadataPath(const std::string& tabletId) const;

  // List the tablet IDs in the metadata directory.
  Status ListTabletIds(std::vector<std::string>* tabletIds);

  // Return the path where InstanceMetadataPB is stored.
  std::string GetInstanceMetadataPath(const std::string& root) const;

  // Return the directory where the consensus metadata is stored.
  std::string GetConsensusMetadataDir() const {
    DCHECK(initted_);
    return JoinPathSegments(
        canonicalizedMetadataFsRoot_.path, kConsensusMetadataDirName);
  }

  // Return the path where ConsensusMetadataPB is stored.
  std::string GetConsensusMetadataPath(const std::string& tabletId) const {
    return JoinPathSegments(GetConsensusMetadataDir(), tabletId);
  }

  // Return the path where ProxyTopologyPB is stored.
  std::string GetProxyMetadataPath(const std::string& tabletId) const {
    return JoinPathSegments(GetConsensusMetadataDir(), tabletId + ".proxy");
  }

  // Return the path where PersistentVarsPB is stored.
  std::string GetPersistentVarsPath(const std::string& tabletId) const {
    return JoinPathSegments(
        GetConsensusMetadataDir(), tabletId + ".persistent_vars");
  }

  Env* env() {
    return env_;
  }

  bool read_only() const {
    return opts_.read_only;
  }

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  bool Exists(const std::string& path) const {
    return env_->FileExists(path);
  }

  Status ListDir(const std::string& path, std::vector<std::string>* objects)
      const {
    return env_->GetChildren(path, objects);
  }

 private:
  FRIEND_TEST(FsManagerTestBase, TestDuplicatePaths);
  FRIEND_TEST(FsManagerTestBase, TestMetadataDirInWALRoot);
  FRIEND_TEST(FsManagerTestBase, TestMetadataDirInDataRoot);
  FRIEND_TEST(FsManagerTestBase, TestIsolatedMetadataDir);
  FRIEND_TEST(tserver::MiniTabletServerTest, TestFsLayoutEndToEnd);
  friend class itest::MiniClusterFsInspector; // for access to directory names

  // Initializes, sanitizes, and canonicalizes the filesystem roots.
  // Determines the correct filesystem root for tablet-specific metadata.
  Status init();

  // Creates filesystem roots from 'canonicalized_roots', writing new on-disk
  // instances using 'metadata'.
  //
  //
  // All created directories and files will be appended to 'created_dirs' and
  // 'created_files' respectively. It is the responsibility of the caller to
  // synchronize the directories containing these newly created file objects.
  Status createFileSystemRoots(
      const CanonicalizedRootsList& canonicalizedRoots,
      const InstanceMetadataPB& metadata,
      std::vector<std::string>* createdDirs,
      std::vector<std::string>* createdFiles);

  // Create a new InstanceMetadataPB.
  Status createInstanceMetadata(
      std::optional<std::string> uuid,
      InstanceMetadataPB* metadata);

  // Save a InstanceMetadataPB to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status writeInstanceMetadata(
      const InstanceMetadataPB& metadata,
      const std::string& root);

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  void DumpFileSystemTree(
      std::ostream& out,
      const std::string& prefix,
      const std::string& path,
      const std::vector<std::string>& objects);

  // Deletes leftover temporary files in all "special" top-level directories
  // (e.g. WAL root directory).
  //
  // Logs warnings in case of errors.
  void cleanTmpFiles();

  // Checks that the permissions of the root data directories conform to the
  // configured umask, and tightens them as necessary if they do not.
  void checkAndFixPermissions();

  // Returns true if 'fname' is a valid tablet ID.
  bool isValidTabletId(const std::string& fname);

  // Creates the data dir layout (<root>/data/ and
  // <root>/data/block_manager_instance) for backward compatibility with
  // older code that expects DataDirManager artifacts to exist on disk.
  // Failures are logged but not fatal.
  void createDataDirLayoutForBackwardCompat();

  static const char* kDataDirName;
  static const char* kTabletMetadataDirName;
  static const char* kWalDirName;
  static const char* kCorruptedSuffix;
  static const char* kInstanceMetadataFileName;
  static const char* kInstanceMetadataMagicNumber;
  static const char* kTabletSuperBlockMagicNumber;
  static const char* kConsensusMetadataDirName;

  // The environment to be used for all filesystem operations.
  Env* env_;

  // The options that the FsManager was created with.
  const FsManagerOpts opts_;

  // Canonicalized forms of the root directories. Constructed during init()
  // with ordering maintained.
  //
  // - The first data root is used as the metadata root.
  // - Common roots in the collections have been deduplicated.
  CanonicalizedRootAndStatus canonicalizedWalFsRoot_;
  CanonicalizedRootAndStatus canonicalizedMetadataFsRoot_;
  CanonicalizedRootsList canonicalizedDataFsRoots_;
  CanonicalizedRootsList canonicalizedAllFsRoots_;

  std::unique_ptr<InstanceMetadataPB> metadata_;

  ObjectIdGenerator oidGenerator_;

  bool initted_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu
