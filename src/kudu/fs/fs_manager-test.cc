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

#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flags.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::fs::ConsistencyCheckBehavior;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

DECLARE_bool(crash_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);
DECLARE_string(env_inject_lock_failure_globs);
DECLARE_string(umask);

namespace kudu {

class FsManagerTestBase : public KuduTest {
 public:
  FsManagerTestBase() : fsRoot_(GetTestPath("fs_root")) {}

  void SetUp() override {
    KuduTest::SetUp();

    // Initialize File-System Layout
    reinitFsManager();
    ASSERT_OK(fsManager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fsManager_->Open());
  }

  void reinitFsManager() {
    reinitFsManagerWithPaths(fsRoot_, {fsRoot_});
  }

  void reinitFsManagerWithPaths(string walPath, vector<string> dataPaths) {
    FsManagerOpts opts;
    opts.wal_root = std::move(walPath);
    opts.data_roots = std::move(dataPaths);
    reinitFsManagerWithOpts(std::move(opts));
  }

  void reinitFsManagerWithOpts(FsManagerOpts opts) {
    fsManager_.reset(new FsManager(env_, std::move(opts)));
  }

  void testReadWriteDataFile(const Slice& data) {
    uint8_t buffer[64];
    DCHECK_LT(data.size(), sizeof(buffer));

    // Test Write
    unique_ptr<fs::WritableBlock> writer;
    ASSERT_OK(fsManager()->CreateNewBlock({}, &writer));
    ASSERT_OK(writer->Append(data));
    ASSERT_OK(writer->Close());

    // Test Read
    Slice result(buffer, data.size());
    unique_ptr<fs::ReadableBlock> reader;
    ASSERT_OK(fsManager()->OpenBlock(writer->id(), &reader));
    ASSERT_OK(reader->Read(0, result));
    ASSERT_EQ(0, result.compare(data));
  }

  FsManager* fsManager() const {
    return fsManager_.get();
  }

 protected:
  const string fsRoot_;

 private:
  unique_ptr<FsManager> fsManager_;
};

TEST_F(FsManagerTestBase, TestBaseOperations) {
  fsManager()->DumpFileSystemTree(std::cout);

  testReadWriteDataFile(Slice("test0"));
  testReadWriteDataFile(Slice("test1"));

  fsManager()->DumpFileSystemTree(std::cout);
}

TEST_F(FsManagerTestBase, TestIllegalPaths) {
  vector<string> illegal = {"", "asdf", "/foo\n\t"};
  for (const string& path : illegal) {
    reinitFsManagerWithPaths(path, {path});
    ASSERT_TRUE(fsManager()->CreateInitialFileSystemLayout().IsIOError());
  }
}

TEST_F(FsManagerTestBase, TestMultiplePaths) {
  string walPath = GetTestPath("a");
  vector<string> dataPaths = {
      GetTestPath("a"), GetTestPath("b"), GetTestPath("c")};
  reinitFsManagerWithPaths(walPath, dataPaths);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fsManager()->Open());
}

TEST_F(FsManagerTestBase, TestMatchingPathsWithMismatchedSlashes) {
  string walPath = GetTestPath("foo");
  vector<string> dataPaths = {walPath + "/"};
  reinitFsManagerWithPaths(walPath, dataPaths);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
}

TEST_F(FsManagerTestBase, TestDuplicatePaths) {
  string path = GetTestPath("foo");
  reinitFsManagerWithPaths(path, {path, path, path});
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_EQ(
      vector<string>({JoinPathSegments(path, fsManager()->kDataDirName)}),
      fsManager()->GetDataRootDirs());
}

TEST_F(FsManagerTestBase, TestListTablets) {
  vector<string> tabletIds;
  ASSERT_OK(fsManager()->ListTabletIds(&tabletIds));
  ASSERT_EQ(0, tabletIds.size());

  string path = fsManager()->GetTabletMetadataDir();
  unique_ptr<WritableFile> writer;
  ASSERT_OK(
      env_->NewWritableFile(JoinPathSegments(path, "foo.kudutmp"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "foo.kudutmp.abc123"), &writer));
  ASSERT_OK(env_->NewWritableFile(JoinPathSegments(path, "foo.bak"), &writer));
  ASSERT_OK(
      env_->NewWritableFile(JoinPathSegments(path, "foo.bak.abc123"), &writer));
  ASSERT_OK(env_->NewWritableFile(JoinPathSegments(path, ".hidden"), &writer));
  // An uncanonicalized id.
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"), &writer));
  // 1 valid tablet id.
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "922ff7ed14c14dbca4ee16331dfda42a"), &writer));

  ASSERT_OK(fsManager()->ListTabletIds(&tabletIds));
  ASSERT_EQ(1, tabletIds.size()) << tabletIds;
}

TEST_F(FsManagerTestBase, TestCannotUseNonEmptyFsRoot) {
  string path = GetTestPath("new_fs_root");
  ASSERT_OK(env_->CreateDir(path));
  {
    unique_ptr<WritableFile> writer;
    ASSERT_OK(
        env_->NewWritableFile(JoinPathSegments(path, "some_file"), &writer));
  }

  // Try to create the FS layout. It should fail.
  reinitFsManagerWithPaths(path, {path});
  ASSERT_TRUE(fsManager()->CreateInitialFileSystemLayout().IsAlreadyPresent());
}

TEST_F(FsManagerTestBase, TestEmptyWALPath) {
  reinitFsManagerWithPaths("", {});
  Status s = fsManager()->CreateInitialFileSystemLayout();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "directory (fs_wal_dir) not provided");
}

TEST_F(FsManagerTestBase, TestOnlyWALPath) {
  string path = GetTestPath("new_fs_root");
  ASSERT_OK(env_->CreateDir(path));

  reinitFsManagerWithPaths(path, {});
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_TRUE(hasPrefixString(fsManager()->GetWalsRootDir(), path));
  ASSERT_TRUE(hasPrefixString(fsManager()->GetConsensusMetadataDir(), path));
  ASSERT_TRUE(hasPrefixString(fsManager()->GetTabletMetadataDir(), path));
  vector<string> dataDirs = fsManager()->GetDataRootDirs();
  ASSERT_EQ(1, dataDirs.size());
  ASSERT_TRUE(hasPrefixString(dataDirs[0], path));
}

TEST_F(FsManagerTestBase, TestFormatWithSpecificUUID) {
  string path = GetTestPath("new_fs_root");
  reinitFsManagerWithPaths(path, {});

  // Use an invalid uuid at first.
  string uuid = "not_a_valid_uuid";
  Status s = fsManager()->CreateInitialFileSystemLayout(uuid);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), fmt::format("invalid uuid {}", uuid));

  // Now use a valid one.
  ObjectIdGenerator oidGenerator;
  uuid = oidGenerator.next();
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout(uuid));
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(uuid, fsManager()->uuid());
}

TEST_F(FsManagerTestBase, TestMetadataDirInWALRoot) {
  // By default, the FsManager should put metadata in the wal root.
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = {GetTestPath("data")};
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fsManager()->Open());
  ASSERT_STR_CONTAINS(
      fsManager()->GetTabletMetadataDir(),
      JoinPathSegments("wal", FsManager::kTabletMetadataDirName));

  // Reinitializing the FS layout with any other configured metadata root
  // should fail, as a non-empty metadata root will be used verbatim.
  opts.metadata_root = GetTestPath("asdf");
  reinitFsManagerWithOpts(opts);
  Status s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // The above comment also applies to the default value before Kudu 1.6: the
  // first configured data directory. Let's check that too.
  opts.metadata_root = opts.data_roots[0];
  reinitFsManagerWithOpts(opts);
  s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // We should be able to verify that the metadata is in the WAL root.
  opts.metadata_root = opts.wal_root;
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
}

TEST_F(FsManagerTestBase, TestMetadataDirInDataRoot) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = {GetTestPath("data1")};

  // Creating a brand new FS layout configured with metadata in the first data
  // directory emulates the default behavior in Kudu 1.6 and below.
  opts.metadata_root = opts.data_roots[0];
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fsManager()->Open());
  const string& metaRootSuffix =
      JoinPathSegments("data1", FsManager::kTabletMetadataDirName);
  ASSERT_STR_CONTAINS(fsManager()->GetTabletMetadataDir(), metaRootSuffix);

  // Opening the FsManager with an empty fs_metadata_dir flag should account
  // for the old default and use the first data directory for metadata.
  opts.metadata_root.clear();
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_STR_CONTAINS(fsManager()->GetTabletMetadataDir(), metaRootSuffix);
}

TEST_F(FsManagerTestBase, TestIsolatedMetadataDir) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = {GetTestPath("data")};

  // Creating a brand new FS layout configured to a directory outside the WAL
  // or data directories is supported.
  opts.metadata_root = GetTestPath("asdf");
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fsManager()->Open());
  ASSERT_STR_CONTAINS(
      fsManager()->GetTabletMetadataDir(),
      JoinPathSegments("asdf", FsManager::kTabletMetadataDirName));
  ASSERT_NE(
      dirName(fsManager()->GetTabletMetadataDir()),
      dirName(fsManager()->GetWalsRootDir()));
  ASSERT_NE(
      dirName(fsManager()->GetTabletMetadataDir()),
      dirName(fsManager()->GetDataRootDirs()[0]));

  // If the user henceforth forgets to specify the metadata root, the FsManager
  // will fail to open.
  opts.metadata_root.clear();
  reinitFsManagerWithOpts(opts);
  Status s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

Status countTmpFiles(
    Env* env,
    const string& path,
    const vector<string>& children,
    unordered_set<string>* checkedDirs,
    int* count) {
  int n = 0;
  vector<string> subObjects;
  for (const string& name : children) {
    if (name == "." || name == "..")
      continue;

    string subPath;
    RETURN_NOT_OK(env->Canonicalize(JoinPathSegments(path, name), &subPath));
    bool isDirectory;
    RETURN_NOT_OK(env->IsDirectory(subPath, &isDirectory));
    if (isDirectory) {
      if (checkedDirs->find(subPath) == checkedDirs->end()) {
        checkedDirs->insert(subPath);
        RETURN_NOT_OK(env->GetChildren(subPath, &subObjects));
        int subdirCount = 0;
        RETURN_NOT_OK(
            countTmpFiles(env, subPath, subObjects, checkedDirs, &subdirCount));
        n += subdirCount;
      }
    } else if (name.find(kTmpInfix) != string::npos) {
      n++;
    }
  }
  *count = n;
  return Status::OK();
}

Status countTmpFiles(Env* env, const vector<string>& roots, int* count) {
  unordered_set<string> checkedDirs;
  int n = 0;
  for (const string& root : roots) {
    vector<string> children;
    RETURN_NOT_OK(env->GetChildren(root, &children));
    int dirCount;
    RETURN_NOT_OK(countTmpFiles(env, root, children, &checkedDirs, &dirCount));
    n += dirCount;
  }
  *count = n;
  return Status::OK();
}

TEST_F(FsManagerTestBase, TestCreateWithFailedDirs) {
  string walPath = GetTestPath("wals");
  // Create some top-level paths to place roots in.
  vector<string> dataPaths = {
      GetTestPath("data1"), GetTestPath("data2"), GetTestPath("data3")};
  for (const string& path : dataPaths) {
    env_->CreateDir(path);
  }
  // Initialize the FS layout with roots in subdirectories of dataPaths. When
  // we canonicalize paths, we canonicalize the dirname of each path (e.g.
  // data1) to ensure it exists. With this, we can inject failures in
  // canonicalization by failing the dirname.
  vector<string> dataRoots = JoinPathSegmentsV(dataPaths, "root");

  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;

  // Fail a directory, avoiding the metadata directory.
  FLAGS_env_inject_eio_globs = dataPaths[1];
  reinitFsManagerWithPaths(walPath, dataRoots);
  Status s = fsManager()->CreateInitialFileSystemLayout();
  ASSERT_STR_MATCHES(
      s.ToString(),
      "cannot create FS layout; at least one directory "
      "failed to canonicalize");
}

TEST_F(FsManagerTestBase, TestOpenWithNoBlockManagerInstances) {
  // Open a healthy FS layout, sharing the WAL directory with a data directory.
  const string walPath = GetTestPath("wals");
  FsManagerOpts opts;
  opts.wal_root = walPath;
  reinitFsManagerWithOpts(std::move(opts));
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fsManager()->Open());

  // Now try moving the data directory out of WAL directory.
  // Even if we're not enforcing consistency, we must be able to find an
  // existing block manager instance to open the FsManager successfully.
  for (auto checkBehavior :
       {ConsistencyCheckBehavior::IGNORE_INCONSISTENCY,
        ConsistencyCheckBehavior::UPDATE_ON_DISK}) {
    FsManagerOpts newOpts;
    newOpts.wal_root = walPath;
    newOpts.data_roots = {GetTestPath("data")};
    newOpts.consistency_check = checkBehavior;
    reinitFsManagerWithOpts(newOpts);
    Status s = fsManager()->Open();
    ASSERT_STR_CONTAINS(s.ToString(), "no healthy data directories found");
    ASSERT_TRUE(s.IsNotFound());

    // Once we supply the WAL directory as a data directory, we can open
    // successfully.
    newOpts.data_roots.emplace_back(walPath);
    reinitFsManagerWithOpts(std::move(newOpts));
    ASSERT_OK(fsManager()->Open());
  }
}

// Test the behavior when we fail to open a data directory for some reason (its
// mountpoint failed, it's missing, etc). Kudu should allow this and open up
// with failed data directories listed.
TEST_F(FsManagerTestBase, TestOpenWithUnhealthyDataDir) {
  // Successfully create a multi-directory FS layout.
  const string newRoot = GetTestPath("new_root");
  FsManagerOpts opts;
  opts.wal_root = fsRoot_;
  opts.data_roots = {fsRoot_, newRoot};
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  string newRootUuid;
  ASSERT_TRUE(fsManager()->dd_manager()->FindUuidByRoot(newRoot, &newRootUuid));

  // Fail the new directory. Kudu should have no problem starting up with this
  // and should list one as failed.
  FLAGS_env_inject_eio_globs = JoinPathSegments(newRoot, "**");
  FLAGS_env_inject_eio = 1.0;
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(1, fsManager()->dd_manager()->GetFailedDataDirs().size());

  // Now remove the new directory on disk. Similarly, Kudu should have no
  // problem starting up and it should list one failed data directory.
  FLAGS_env_inject_eio = 0;
  ASSERT_OK(env_->DeleteRecursively(newRoot));
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(1, fsManager()->dd_manager()->GetFailedDataDirs().size());

  // Now let's simulate the operator replacing the drive. The update tool will
  // be run and the new directory, even at the same mountpoint, will be
  // assigned a new UUID.
  //
  // At this point, our remaining healthy instance file should know about two
  // data directories. Kudu should detect one missing and create a new one.
  // Let's update and ensure we get a new UUID.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(0, fsManager()->dd_manager()->GetFailedDataDirs().size());
  string newRootUuidPostUpdate;
  ASSERT_TRUE(
      fsManager()->dd_manager()->FindUuidByRoot(
          newRoot, &newRootUuidPostUpdate));
  ASSERT_NE(newRootUuid, newRootUuidPostUpdate);

  // Now let's try failing all the directories. Kudu should yield an error,
  // complaining it couldn't find any healthy data directories.
  FLAGS_env_inject_eio_globs =
      JoinStrings(JoinPathSegmentsV(opts.data_roots, "**"), ",");
  FLAGS_env_inject_eio = 1.0;
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  reinitFsManagerWithOpts(opts);
  Status s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy instance file");

  // Upon returning from FsManager::Open() with a NotFound error, Kudu will
  // attempt to create a new FS layout. With bad mountpoints, this should fail.
  s = fsManager()->CreateInitialFileSystemLayout();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "cannot create FS layout");

  // The above behavior should be seen if the data directories are missing...
  FLAGS_env_inject_eio = 0;
  for (const auto& root : opts.data_roots) {
    ASSERT_OK(env_->DeleteRecursively(root));
  }
  reinitFsManagerWithOpts(opts);
  s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy instance file");

  // ...except we should be able to successfully create a new FS layout.
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  ASSERT_EQ(0, fsManager()->dd_manager()->GetFailedDataDirs().size());
}

// When we canonicalize a directory, we actually canonicalize the directory's
// parent directory; as such, canonicalization can fail if the parent directory
// can't be read (e.g. due to a disk error or because it's flat out missing).
// In such cases, we should still be able to open the FS layout.
TEST_F(FsManagerTestBase, TestOpenWithCanonicalizationFailure) {
  // Create some parent directories and subdirectories.
  const string dir1 = GetTestPath("test1");
  const string dir2 = GetTestPath("test2");
  ASSERT_OK(env_->CreateDir(dir1));
  ASSERT_OK(env_->CreateDir(dir2));
  const string subdir1 = GetTestPath("test1/subdir");
  const string subdir2 = GetTestPath("test2/subdir");
  FsManagerOpts opts;
  opts.wal_root = subdir1;
  opts.data_roots = {subdir1, subdir2};
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());

  // Fail the canonicalization by injecting errors to a parent directory.
  reinitFsManagerWithOpts(opts);
  FLAGS_env_inject_eio_globs = JoinPathSegments(dir2, "**");
  FLAGS_env_inject_eio = 1.0;
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(1, fsManager()->dd_manager()->GetFailedDataDirs().size());
  FLAGS_env_inject_eio = 0;

  // Now fail the canonicalization by deleting a parent directory. This
  // simulates the mountpoint disappearing.
  ASSERT_OK(env_->DeleteRecursively(dir2));
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(1, fsManager()->dd_manager()->GetFailedDataDirs().size());

  // In both of the above failures, the appropriate steps would be to run the
  // update tool after ensuring the bad mountpoint is replaced with a healthy
  // one. Until that happens, we won't be able to update the data dirs.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  reinitFsManagerWithOpts(opts);
  Status s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not add new data directories");

  // Let's try that again, but with the appropriate mountpoint/directory.
  ASSERT_OK(env_->CreateDir(dir2));
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->Open());
  ASSERT_EQ(0, fsManager()->dd_manager()->GetFailedDataDirs().size());
}

TEST_F(FsManagerTestBase, TestTmpFilesCleanup) {
  string walPath = GetTestPath("wals");
  vector<string> dataPaths = {
      GetTestPath("data1"), GetTestPath("data2"), GetTestPath("data3")};
  reinitFsManagerWithPaths(walPath, dataPaths);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());

  // Create a few tmp files here
  shared_ptr<WritableFile> tmpWriter;

  string tmpPath =
      JoinPathSegments(fsManager()->GetWalsRootDir(), "wal.kudutmp.file");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  tmpPath =
      JoinPathSegments(fsManager()->GetDataRootDirs()[0], "data1.kudutmp.file");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  tmpPath = JoinPathSegments(
      fsManager()->GetConsensusMetadataDir(), "12345.kudutmp.asdfg");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  tmpPath = JoinPathSegments(
      fsManager()->GetTabletMetadataDir(), "12345.kudutmp.asdfg");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  // Not a misprint here: checking for just ".kudutmp" as well
  tmpPath =
      JoinPathSegments(fsManager()->GetDataRootDirs()[1], "data2.kudutmp");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  // Try with nested directory
  string nestedDirPath =
      JoinPathSegments(fsManager()->GetDataRootDirs()[2], "data4");
  ASSERT_OK(env_util::createDirIfMissing(fsManager()->env(), nestedDirPath));
  tmpPath = JoinPathSegments(nestedDirPath, "data4.kudutmp.file");
  ASSERT_OK(
      env_util::openFileForWrite(fsManager()->env(), tmpPath, &tmpWriter));

  // Add a loop using symlink
  string data3Link = JoinPathSegments(nestedDirPath, "data3-link");
  int symlinkError =
      symlink(fsManager()->GetDataRootDirs()[2].c_str(), data3Link.c_str());
  ASSERT_EQ(0, symlinkError);

  vector<string> lookupDirs = fsManager()->GetDataRootDirs();
  lookupDirs.emplace_back(fsManager()->GetWalsRootDir());
  lookupDirs.emplace_back(fsManager()->GetConsensusMetadataDir());
  lookupDirs.emplace_back(fsManager()->GetTabletMetadataDir());

  int nTmpFiles = 0;
  ASSERT_OK(countTmpFiles(fsManager()->env(), lookupDirs, &nTmpFiles));
  ASSERT_EQ(6, nTmpFiles);

  // The FsManager should not delete any tmp files if it fails to acquire
  // a lock on the data dir.
  string bmInstance = JoinPathSegments(
      fsManager()->GetDataRootDirs()[1], "block_manager_instance");
  {
    gflags::FlagSaver saver;
    FLAGS_env_inject_lock_failure_globs = bmInstance;
    reinitFsManagerWithPaths(walPath, dataPaths);
    Status s = fsManager()->Open();
    ASSERT_STR_MATCHES(s.ToString(), "Could not lock.*");
    ASSERT_OK(countTmpFiles(fsManager()->env(), lookupDirs, &nTmpFiles));
    ASSERT_EQ(6, nTmpFiles);
  }

  // Now start up without the injected lock failure, and ensure that tmp files
  // are deleted.
  reinitFsManagerWithPaths(walPath, dataPaths);
  ASSERT_OK(fsManager()->Open());

  nTmpFiles = 0;
  ASSERT_OK(countTmpFiles(fsManager()->env(), lookupDirs, &nTmpFiles));
  ASSERT_EQ(0, nTmpFiles);
}

namespace {

string filePermsAsString(const string& path) {
  struct stat s;
  CHECK_ERR(stat(path.c_str(), &s));
  return fmt::format("{:03o}", s.st_mode & ACCESSPERMS);
}

} // anonymous namespace

TEST_F(FsManagerTestBase, TestUmask) {
  // With the default umask, we should create files with permissions 600
  // and directories with permissions 700.
  ASSERT_EQ(077, gParsedUmask) << "unexpected default value";
  string root = GetTestPath("fs_root");
  EXPECT_EQ("700", filePermsAsString(root));
  EXPECT_EQ("700", filePermsAsString(fsManager()->GetConsensusMetadataDir()));
  EXPECT_EQ(
      "600", filePermsAsString(fsManager()->GetInstanceMetadataPath(root)));

  // With umask 007, we should create files with permissions 660
  // and directories with 770.
  FLAGS_umask = "007";
  handleCommonFlags();
  ASSERT_EQ(007, gParsedUmask);
  root = GetTestPath("new_root");
  reinitFsManagerWithPaths(root, {root});
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  EXPECT_EQ("770", filePermsAsString(root));
  EXPECT_EQ("770", filePermsAsString(fsManager()->GetConsensusMetadataDir()));
  EXPECT_EQ(
      "660", filePermsAsString(fsManager()->GetInstanceMetadataPath(root)));

  // If we change the umask back to being restrictive and re-open the
  // filesystem, the permissions on the root dir should be fixed accordingly.
  FLAGS_umask = "077";
  handleCommonFlags();
  ASSERT_EQ(077, gParsedUmask);
  reinitFsManagerWithPaths(root, {root});
  ASSERT_OK(fsManager()->Open());
  EXPECT_EQ("700", filePermsAsString(root));
}

TEST_F(FsManagerTestBase, TestOpenFailsWhenMissingImportantDir) {
  const string kWalRoot = fsManager()->GetWalsRootDir();

  ASSERT_OK(env_->DeleteDir(kWalRoot));
  reinitFsManager();
  Status s = fsManager()->Open();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "could not verify required directory");

  unique_ptr<WritableFile> f;
  ASSERT_OK(env_->NewWritableFile(kWalRoot, &f));
  s = fsManager()->Open();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "exists but is not a directory");
}

TEST_F(FsManagerTestBase, TestAncillaryDirsReported) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = {GetTestPath("data")};
  opts.metadata_root = GetTestPath("metadata");
  reinitFsManagerWithOpts(opts);
  ASSERT_OK(fsManager()->CreateInitialFileSystemLayout());
  fs::FsReport report;
  ASSERT_OK(fsManager()->Open(&report));
  string reportStr = report.toString();
  ASSERT_STR_CONTAINS(reportStr, "wal directory: " + opts.wal_root);
  ASSERT_STR_CONTAINS(reportStr, "metadata directory: " + opts.metadata_root);
}

} // namespace kudu
