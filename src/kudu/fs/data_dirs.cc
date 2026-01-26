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

#include "kudu/fs/data_dirs.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/fs/block_manager.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/thread_pool_builder.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(
    fs_target_data_dirs_per_tablet,
    3,
    "Indicates the target number of data dirs to spread each "
    "tablet's data across. If greater than the number of data dirs "
    "available, data will be striped across those available. A "
    "value of 0 indicates striping should occur across all healthy "
    "data dirs. Using fewer data dirs per tablet means a single "
    "drive failure will be less likely to affect a given tablet.");
DEFINE_validator(
    fs_target_data_dirs_per_tablet,
    [](const char* /*n*/, int32_t v) { return v >= 0; });
TAG_FLAG(fs_target_data_dirs_per_tablet, advanced);
TAG_FLAG(fs_target_data_dirs_per_tablet, evolving);

DEFINE_int64(
    fs_data_dirs_reserved_bytes,
    -1,
    "Number of bytes to reserve on each data directory filesystem for "
    "non-Kudu usage. The default, which is represented by -1, is that "
    "1% of the disk space on each disk will be reserved. Any other "
    "value specified represents the number of bytes reserved and must "
    "be greater than or equal to 0. Explicit percentages to reserve "
    "are not currently supported");
DEFINE_validator(fs_data_dirs_reserved_bytes, [](const char* /*n*/, int64_t v) {
  return v >= -1;
});
TAG_FLAG(fs_data_dirs_reserved_bytes, runtime);
TAG_FLAG(fs_data_dirs_reserved_bytes, evolving);

DEFINE_int32(
    fs_data_dirs_full_disk_cache_seconds,
    30,
    "Number of seconds we cache the full-disk status in the block manager. "
    "During this time, writes to the corresponding root path will not be attempted.");
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, advanced);
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, evolving);

DEFINE_bool(
    fs_lock_data_dirs,
    true,
    "Lock the data directories to prevent concurrent usage. "
    "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(fs_lock_data_dirs, unsafe);
TAG_FLAG(fs_lock_data_dirs, evolving);

METRIC_DEFINE_gauge_uint64(
    server,
    data_dirs_failed,
    "Data Directories Failed",
    kudu::MetricUnit::kDataDirectories,
    "Number of data directories whose disks are currently "
    "in a failed state");
METRIC_DEFINE_gauge_uint64(
    server,
    data_dirs_full,
    "Data Directories Full",
    kudu::MetricUnit::kDataDirectories,
    "Number of data directories whose disks are currently full");

DECLARE_bool(enable_data_block_fsync);

namespace kudu::fs {

using internal::DataDirGroup;
using std::default_random_engine;
using std::iota;
using std::pair;
using std::set;
using std::shuffle;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
// strings::SubstituteAndAppend removed - migrated to fmt

namespace {

// Wrapper for env_util::DeleteTmpFilesRecursively that is suitable for parallel
// execution on a data directory's thread pool (which requires the return value
// be void).
void DeleteTmpFilesRecursively(Env* env, const string& path) {
  WARN_NOT_OK(
      env_util::DeleteTmpFilesRecursively(env, path),
      "Error while deleting temp files");
}

} // anonymous namespace

////////////////////////////////////////////////////////////
// DataDirMetrics
////////////////////////////////////////////////////////////

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
DataDirMetrics::DataDirMetrics(const std::shared_ptr<MetricEntity>& entity)
    : GINIT(data_dirs_failed), GINIT(data_dirs_full) {}
#undef GINIT

////////////////////////////////////////////////////////////
// DataDir
////////////////////////////////////////////////////////////

DataDir::DataDir(
    Env* env,
    DataDirMetrics* metrics,
    DataDirFsType fs_type,
    string dir,
    unique_ptr<PathInstanceMetadataFile> metadata_file,
    unique_ptr<ThreadPool> pool)
    : env_(env),
      metrics_(metrics),
      fs_type_(fs_type),
      dir_(std::move(dir)),
      metadata_file_(std::move(metadata_file)),
      pool_(std::move(pool)),
      is_shutdown_(false),
      is_full_(false) {}

DataDir::~DataDir() {
  Shutdown();
}

void DataDir::Shutdown() {
  if (is_shutdown_) {
    return;
  }

  WaitOnClosures();
  pool_->Shutdown();
  is_shutdown_ = true;
}

void DataDir::ExecClosure(const Closure& task) {
  Status s = pool_->SubmitClosure(task);
  if (!s.ok()) {
    WARN_NOT_OK(
        s, "Could not submit task to thread pool, running it synchronously");
    task.Run();
  }
}

void DataDir::WaitOnClosures() {
  pool_->Wait();
}

Status DataDir::RefreshIsFull(RefreshMode mode) {
  switch (mode) {
    case RefreshMode::EXPIRED_ONLY: {
      std::lock_guard<simple_spinlock> l(lock_);
      DCHECK(last_check_is_full_.Initialized());
      MonoTime expiry = last_check_is_full_ +
          MonoDelta::FromSeconds(FLAGS_fs_data_dirs_full_disk_cache_seconds);
      if (!is_full_ || MonoTime::Now() < expiry) {
        break;
      }
      FALLTHROUGH_INTENDED; // Root was previously full, check again.
    }
    case RefreshMode::ALWAYS: {
      Status s = env_util::VerifySufficientDiskSpace(
          env_, dir_, 0, FLAGS_fs_data_dirs_reserved_bytes);
      bool is_full_new;
      if (PREDICT_FALSE(s.IsIOError() && s.posix_code() == ENOSPC)) {
        LOG(WARNING) << fmt::format(
            "Insufficient disk space under path {}: creation of new data "
            "blocks under this path can be retried after {} seconds: {}",
            dir_,
            FLAGS_fs_data_dirs_full_disk_cache_seconds,
            s.ToString());
        s = Status::OK();
        is_full_new = true;
      } else {
        is_full_new = false;
      }
      RETURN_NOT_OK_PREPEND(
          s,
          "Could not refresh fullness"); // Catch other types of IOErrors, etc.
      {
        std::lock_guard<simple_spinlock> l(lock_);
        if (metrics_ && is_full_ != is_full_new) {
          metrics_->data_dirs_full->IncrementBy(is_full_new ? 1 : -1);
        }
        is_full_ = is_full_new;
        last_check_is_full_ = MonoTime::Now();
      }
      break;
    }
    default:
      LOG(FATAL) << "Unknown check mode";
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////
// DataDirGroup
////////////////////////////////////////////////////////////

DataDirGroup::DataDirGroup() = default;

DataDirGroup::DataDirGroup(vector<int> uuid_indices)
    : uuid_indices_(std::move(uuid_indices)) {}

Status DataDirGroup::LoadFromPB(
    const UuidIndexByUuidMap& uuid_idx_by_uuid,
    const DataDirGroupPB& pb) {
  vector<int> uuid_indices;
  for (const auto& uuid : pb.uuids()) {
    auto it = uuid_idx_by_uuid.find(uuid);
    if (it == uuid_idx_by_uuid.end()) {
      return Status::NotFound(
          fmt::format("could not find data dir with uuid {}", uuid));
    }
    uuid_indices.emplace_back(it->second);
  }

  uuid_indices_ = std::move(uuid_indices);
  return Status::OK();
}

Status DataDirGroup::CopyToPB(
    const UuidByUuidIndexMap& uuid_by_uuid_idx,
    DataDirGroupPB* pb) const {
  DCHECK(pb);
  DataDirGroupPB group;
  for (auto uuid_idx : uuid_indices_) {
    auto it = uuid_by_uuid_idx.find(uuid_idx);
    if (it == uuid_by_uuid_idx.end()) {
      return Status::NotFound(
          fmt::format("could not find data dir with uuid index {}", uuid_idx));
    }
    *group.mutable_uuids()->Add() = it->second;
  }

  *pb = std::move(group);
  return Status::OK();
}

////////////////////////////////////////////////////////////
// DataDirManagerOptions
////////////////////////////////////////////////////////////

DataDirManagerOptions::DataDirManagerOptions()
    : read_only(false),
      consistency_check(ConsistencyCheckBehavior::ENFORCE_CONSISTENCY) {}

////////////////////////////////////////////////////////////
// DataDirManager
////////////////////////////////////////////////////////////

vector<string> DataDirManager::GetRootNames(
    const CanonicalizedRootsList& root_list) {
  vector<string> roots;
  std::transform(
      root_list.begin(),
      root_list.end(),
      std::back_inserter(roots),
      [&](const CanonicalizedRootAndStatus& r) { return r.path; });
  return roots;
}

DataDirManager::DataDirManager(
    Env* env,
    DataDirManagerOptions opts,
    CanonicalizedRootsList canonicalized_data_roots)
    : env_(env),
      opts_(std::move(opts)),
      canonicalized_data_fs_roots_(std::move(canonicalized_data_roots)),
      rng_(GetRandomSeed32()) {
  DCHECK_GT(canonicalized_data_fs_roots_.size(), 0);
  DCHECK(
      opts_.consistency_check != ConsistencyCheckBehavior::UPDATE_ON_DISK ||
      !opts_.read_only);

  if (opts_.metric_entity) {
    metrics_.reset(new DataDirMetrics(opts_.metric_entity));
  }
}

DataDirManager::~DataDirManager() {
  Shutdown();
}

void DataDirManager::WaitOnClosures() {
  for (const auto& dd : data_dirs_) {
    dd->WaitOnClosures();
  }
}

void DataDirManager::Shutdown() {
  // We may be waiting here for a while on outstanding closures.
  LOG_SLOW_EXECUTION(
      INFO,
      1000,
      fmt::format(
          "waiting on {} block manager thread pools", data_dirs_.size())) {
    for (const auto& dd : data_dirs_) {
      dd->Shutdown();
    }
  }
}

Status DataDirManager::OpenExistingForTests(
    Env* env,
    const vector<string>& data_fs_roots,
    DataDirManagerOptions opts,
    unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : data_fs_roots) {
    roots.push_back({r, Status::OK()});
  }
  return DataDirManager::OpenExisting(
      env, std::move(roots), std::move(opts), dd_manager);
}

Status DataDirManager::OpenExisting(
    Env* env,
    CanonicalizedRootsList data_fs_roots,
    DataDirManagerOptions opts,
    unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, std::move(opts), std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

Status DataDirManager::CreateNewForTests(
    Env* env,
    const vector<string>& data_fs_roots,
    DataDirManagerOptions opts,
    unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : data_fs_roots) {
    roots.push_back({r, Status::OK()});
  }
  return DataDirManager::CreateNew(
      env, std::move(roots), std::move(opts), dd_manager);
}

Status DataDirManager::CreateNew(
    Env* env,
    CanonicalizedRootsList data_fs_roots,
    DataDirManagerOptions opts,
    unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, std::move(opts), std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Create());
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

Status DataDirManager::Create() {
  CHECK(!opts_.read_only);

  // Generate a new UUID for each data directory.
  ObjectIdGenerator gen;
  vector<string> all_uuids;
  vector<pair<string, string>> root_uuid_pairs_to_create;
  for (const auto& r : canonicalized_data_fs_roots_) {
    RETURN_NOT_OK_PREPEND(
        r.status, "Could not create directory manager with disks failed");
    string uuid = gen.next();
    all_uuids.emplace_back(uuid);
    root_uuid_pairs_to_create.emplace_back(r.path, std::move(uuid));
  }
  RETURN_NOT_OK_PREPEND(
      CreateNewDataDirectoriesAndUpdateInstances(
          std::move(root_uuid_pairs_to_create), {}, std::move(all_uuids)),
      "could not create new data directories");
  return Status::OK();
}

Status DataDirManager::CreateNewDataDirectoriesAndUpdateInstances(
    const vector<pair<string, string>>& root_uuid_pairs_to_create,
    const vector<unique_ptr<PathInstanceMetadataFile>>& instances_to_update,
    const vector<string>& all_uuids) {
  CHECK(!opts_.read_only);

  vector<string> created_dirs;
  vector<string> created_files;
  auto deleter = folly::makeGuard([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : created_files) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // Ensure the data dirs exist and create the instance files.
  for (const auto& p : root_uuid_pairs_to_create) {
    string data_dir = JoinPathSegments(p.first, kDataDirName);
    bool created;
    RETURN_NOT_OK_PREPEND(
        env_util::CreateDirIfMissing(env_, data_dir, &created),
        fmt::format("Could not create directory {}", data_dir));
    if (created) {
      created_dirs.emplace_back(data_dir);
    }

    string instance_filename =
        JoinPathSegments(data_dir, kInstanceMetadataFileName);
    PathInstanceMetadataFile metadata(env_, instance_filename);
    RETURN_NOT_OK_PREPEND(
        metadata.Create(p.second, all_uuids), instance_filename);
    created_files.emplace_back(instance_filename);
  }

  // Update existing instances, if any.
  RETURN_NOT_OK_PREPEND(
      UpdateInstances(instances_to_update, all_uuids),
      "could not update existing data directories");

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    WARN_NOT_OK(
        env_util::SyncAllParentDirs(env_, created_dirs, created_files),
        "could not sync newly created data directories");
  }

  // Success: don't delete any files.
  deleter.dismiss();
  return Status::OK();
}

Status DataDirManager::UpdateInstances(
    const vector<unique_ptr<PathInstanceMetadataFile>>& instances_to_update,
    const vector<string>& new_all_uuids) {
  // Prepare a scoped cleanup for managing instance metadata copies.
  unordered_map<string, string> copies_to_restore;
  unordered_set<string> copies_to_delete;
  auto copy_cleanup = folly::makeGuard([&]() {
    for (const auto& f : copies_to_delete) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    for (const auto& f : copies_to_restore) {
      WARN_NOT_OK(
          env_->RenameFile(f.first, f.second),
          fmt::format("Could not restore file {} from {}", f.second, f.first));
    }
  });

  // Make a copy of every existing instance metadata file.
  //
  // This is done before performing any updates, so that if there's a failure
  // while copying, there's no metadata to restore.
  WritableFileOptions opts;
  opts.sync_on_close = true;
  for (const auto& instance : instances_to_update) {
    if (!instance->healthy()) {
      continue;
    }
    const string& instance_filename = instance->path();
    string copy_filename = instance_filename + kTmpInfix;
    RETURN_NOT_OK_PREPEND(
        env_util::CopyFile(env_, instance_filename, copy_filename, opts),
        "unable to backup existing data directory instance metadata");
    auto [it, inserted] = copies_to_delete.insert(copy_filename);
    CHECK(inserted) << "Key already exists: " << copy_filename;
  }

  // Update existing instance metadata files with the new value of all_uuids.
  for (const auto& instance : instances_to_update) {
    if (!instance->healthy()) {
      continue;
    }
    const string& instance_filename = instance->path();
    string copy_filename = instance_filename + kTmpInfix;

    // We've made enough progress on this instance that we should restore its
    // copy on failure, even if the update below fails. That's because it's a
    // multi-step process and it's possible for it to return failure despite
    // the update taking place (e.g. synchronization failure).
    CHECK_EQ(1, copies_to_delete.erase(copy_filename));
    auto [it, inserted] =
        copies_to_restore.emplace(copy_filename, instance_filename);
    CHECK(inserted) << "Key already exists: " << copy_filename;

    // Perform the update.
    PathInstanceMetadataPB new_pb = *instance->metadata();
    new_pb.mutable_path_set()->mutable_all_uuids()->Clear();
    for (const auto& uuid : new_all_uuids) {
      new_pb.mutable_path_set()->add_all_uuids(uuid);
    }
    RETURN_NOT_OK_PREPEND(
        pb_util::WritePBContainerToPath(
            env_,
            instance_filename,
            new_pb,
            pb_util::OVERWRITE,
            FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC),
        "unable to overwrite existing data directory instance metadata");
  }

  // Success; instance metadata copies will be deleted by 'copy_cleanup'.
  for (const auto& [key, value] : copies_to_restore) {
    copies_to_delete.insert(key);
  }
  copies_to_restore.clear();
  return Status::OK();
}

Status DataDirManager::LoadInstances(
    vector<unique_ptr<PathInstanceMetadataFile>>* loaded_instances) {
  LockMode lock_mode;
  if (!FLAGS_fs_lock_data_dirs) {
    lock_mode = LockMode::NONE;
  } else if (opts_.read_only) {
    lock_mode = LockMode::OPTIONAL;
  } else {
    lock_mode = LockMode::MANDATORY;
  }
  vector<string> missing_roots_tmp;
  vector<unique_ptr<PathInstanceMetadataFile>> loaded_instances_tmp;
  for (int i = 0; i < canonicalized_data_fs_roots_.size(); i++) {
    const auto& root = canonicalized_data_fs_roots_[i];
    string data_dir = JoinPathSegments(root.path, kDataDirName);
    string instance_filename =
        JoinPathSegments(data_dir, kInstanceMetadataFileName);

    unique_ptr<PathInstanceMetadataFile> instance(
        new PathInstanceMetadataFile(env_, instance_filename));
    if (PREDICT_FALSE(!root.status.ok())) {
      instance->SetInstanceFailed(root.status);
    } else {
      // This may return OK and mark 'instance' as unhealthy if the file could
      // not be loaded (e.g. not found, disk errors).
      RETURN_NOT_OK_PREPEND(
          instance->LoadFromDisk(),
          fmt::format("could not load {}", instance_filename));
    }

    // Try locking the instance.
    if (instance->healthy() && lock_mode != LockMode::NONE) {
      // This may return OK and mark 'instance' as unhealthy if the file could
      // not be locked due to non-locking issues (e.g. disk errors).
      Status s = instance->Lock();
      if (!s.ok()) {
        if (lock_mode == LockMode::OPTIONAL) {
          LOG(WARNING) << s.ToString();
          LOG(WARNING) << "Proceeding without lock";
        } else {
          DCHECK(LockMode::MANDATORY == lock_mode);
          return s;
        }
      }
    }

    loaded_instances_tmp.emplace_back(std::move(instance));
  }

  int num_healthy_instances = 0;
  for (const auto& instance : loaded_instances_tmp) {
    if (instance->healthy()) {
      num_healthy_instances++;
    }
  }
  if (num_healthy_instances == 0) {
    return Status::NotFound(
        "could not open directory manager; no healthy "
        "data directories found");
  }
  loaded_instances->swap(loaded_instances_tmp);
  return Status::OK();
}

Status DataDirManager::Open() {
  constexpr int kMaxDataDirs = (1 << 16) - 1;

  // Find and load existing data directory instances.
  vector<unique_ptr<PathInstanceMetadataFile>> loaded_instances;
  RETURN_NOT_OK(LoadInstances(&loaded_instances));

  // Add new or remove existing data directories, if desired.
  if (opts_.consistency_check == ConsistencyCheckBehavior::UPDATE_ON_DISK) {
    return Status::InvalidArgument(
        "file block manager may not add or remove data directories");
  }

  // Check the integrity of all loaded instances.
  if (opts_.consistency_check !=
      ConsistencyCheckBehavior::IGNORE_INCONSISTENCY) {
    RETURN_NOT_OK_PREPEND(
        PathInstanceMetadataFile::CheckIntegrity(loaded_instances),
        fmt::format(
            "could not verify integrity of files: {}",
            JoinStrings(GetDataDirs(), ",")));
  }

  // All instances are present and accounted for. Time to create the in-memory
  // data directory structures.
  int i = 0;
  vector<unique_ptr<DataDir>> dds;
  for (auto& instance : loaded_instances) {
    const string data_dir = instance->dir();

    // Create a per-dir thread pool.
    unique_ptr<ThreadPool> pool;
    RETURN_NOT_OK(ThreadPoolBuilder(fmt::format("data dir {}", i))
                      .set_max_threads(1)
                      .set_trace_metric_prefix("data dirs")
                      .Build(&pool));

    // Figure out what filesystem the data directory is on.
    DataDirFsType fs_type = DataDirFsType::OTHER;
    if (instance->healthy()) {
      bool result;
      RETURN_NOT_OK(env_->IsOnExtFilesystem(data_dir, &result));
      if (result) {
        fs_type = DataDirFsType::EXT;
      } else {
        RETURN_NOT_OK(env_->IsOnXfsFilesystem(data_dir, &result));
        if (result) {
          fs_type = DataDirFsType::XFS;
        }
      }
    }

    unique_ptr<DataDir> dd(new DataDir(
        env_,
        metrics_.get(),
        fs_type,
        data_dir,
        std::move(instance),
        unique_ptr<ThreadPool>(pool.release())));
    dds.emplace_back(std::move(dd));
    i++;
  }

  // Use the per-dir thread pools to delete temporary files in parallel.
  for (const auto& dd : dds) {
    if (dd->instance()->healthy()) {
      dd->ExecClosure(Bind(&DeleteTmpFilesRecursively, env_, dd->dir()));
    }
  }
  for (const auto& dd : dds) {
    dd->WaitOnClosures();
  }

  // Build in-memory maps of on-disk state.
  UuidByRootMap uuid_by_root;
  UuidByUuidIndexMap uuid_by_idx;
  UuidIndexByUuidMap idx_by_uuid;
  UuidIndexMap dd_by_uuid_idx;
  ReverseUuidIndexMap uuid_idx_by_dd;
  TabletsByUuidIndexMap tablets_by_uuid_idx_map;
  FailedDataDirSet failed_data_dirs;

  const auto insert_to_maps = [&](int idx, const string& uuid, DataDir* dd) {
    auto [it1, inserted1] = uuid_by_root.emplace(DirName(dd->dir()), uuid);
    CHECK(inserted1) << "Key already exists: " << DirName(dd->dir());
    auto [it2, inserted2] = uuid_by_idx.emplace(idx, uuid);
    CHECK(inserted2) << "Key already exists: " << idx;
    auto [it3, inserted3] = idx_by_uuid.emplace(uuid, idx);
    CHECK(inserted3) << "Key already exists: " << uuid;
    auto [it4, inserted4] = dd_by_uuid_idx.emplace(idx, dd);
    CHECK(inserted4) << "Key already exists: " << idx;
    auto [it5, inserted5] = uuid_idx_by_dd.emplace(dd, idx);
    CHECK(inserted5) << "Key already exists: " << static_cast<void*>(dd);
    auto [it6, inserted6] = tablets_by_uuid_idx_map.emplace(idx, set<string>{});
    CHECK(inserted6) << "Key already exists: " << idx;
  };

  if (opts_.consistency_check !=
      ConsistencyCheckBehavior::IGNORE_INCONSISTENCY) {
    // If we're not in IGNORE_INCONSISTENCY mode, we're guaranteed that the
    // healthy instances match from the above integrity check, so we can assign
    // each healthy directory a UUID in accordance with its instance file.
    //
    // A directory may not have been assigned a UUID because its instance file
    // could not be read, in which case, we track it and assign a UUID to it
    // later if we can.
    vector<DataDir*> unassigned_dirs;
    int first_healthy = -1;
    for (int dir = 0; dir < dds.size(); dir++) {
      const auto& dd = dds[dir];
      if (PREDICT_FALSE(!dd->instance()->healthy())) {
        // Keep track of failed directories so we can assign them UUIDs later.
        unassigned_dirs.push_back(dd.get());
        continue;
      }
      if (first_healthy == -1) {
        first_healthy = dir;
      }
      const PathSetPB& path_set = dd->instance()->metadata()->path_set();
      int idx = -1;
      for (int i_2 = 0; i_2 < path_set.all_uuids_size(); i_2++) {
        if (path_set.uuid() == path_set.all_uuids(i_2)) {
          idx = i_2;
          break;
        }
      }
      if (idx == -1) {
        return Status::IOError(
            fmt::format(
                "corrupt path set for data directory {}: uuid {} not found in path set",
                dd->dir(),
                path_set.uuid()));
      }
      if (idx > kMaxDataDirs) {
        return Status::NotSupported(
            fmt::format(
                "block manager supports a maximum of {} paths", kMaxDataDirs));
      }
      insert_to_maps(idx, path_set.uuid(), dd.get());
    }
    CHECK_NE(first_healthy, -1); // Guaranteed by LoadInstances().

    // If the uuid index was not assigned, assign it to a failed directory. Use
    // the path set from the first healthy instance.
    PathSetPB path_set = dds[first_healthy]->instance()->metadata()->path_set();
    int failed_dir_idx = 0;
    for (int uuid_idx = 0; uuid_idx < path_set.all_uuids_size(); uuid_idx++) {
      if (!uuid_by_idx.contains(uuid_idx)) {
        const string& unassigned_uuid = path_set.all_uuids(uuid_idx);
        insert_to_maps(
            uuid_idx, unassigned_uuid, unassigned_dirs[failed_dir_idx]);

        // Record the directory as failed.
        if (metrics_) {
          metrics_->data_dirs_failed->IncrementBy(1);
        }
        auto [it, inserted] = failed_data_dirs.insert(uuid_idx);
        CHECK(inserted) << "Key already exists: " << uuid_idx;
        failed_dir_idx++;
      }
    }
    CHECK_EQ(unassigned_dirs.size(), failed_dir_idx);
  } else {
    // If we are in IGNORE_INCONSISTENCY mode, all bets are off. The most we
    // can do is make a best effort assignment of data dirs to UUIDs based on
    // the ones that are healthy, and for the sake of completeness, assign
    // artificial UUIDs to the unhealthy ones.
    for (int dir = 0; dir < dds.size(); dir++) {
      DataDir* dd = dds[dir].get();
      if (dd->instance()->healthy()) {
        insert_to_maps(dir, dd->instance()->metadata()->path_set().uuid(), dd);
      } else {
        insert_to_maps(dir, fmt::format("<unknown uuid {}>", dir), dd);
        auto [it, inserted] = failed_data_dirs.insert(dir);
        CHECK(inserted) << "Key already exists: " << dir;
      }
    }
  }

  data_dirs_.swap(dds);
  uuid_by_idx_.swap(uuid_by_idx);
  idx_by_uuid_.swap(idx_by_uuid);
  data_dir_by_uuid_idx_.swap(dd_by_uuid_idx);
  uuid_idx_by_data_dir_.swap(uuid_idx_by_dd);
  tablets_by_uuid_idx_map_.swap(tablets_by_uuid_idx_map);
  failed_data_dirs_.swap(failed_data_dirs);
  uuid_by_root_.swap(uuid_by_root);

  // From this point onwards, the above in-memory maps must be consistent with
  // the main path set.

  // Initialize the 'fullness' status of the data directories.
  for (const auto& dd : data_dirs_) {
    int uuid_idx;
    CHECK(FindUuidIndexByDataDir(dd.get(), &uuid_idx));
    if (failed_data_dirs_.contains(uuid_idx)) {
      continue;
    }
    Status refresh_status = dd->RefreshIsFull(DataDir::RefreshMode::ALWAYS);
    if (PREDICT_FALSE(!refresh_status.ok())) {
      if (refresh_status.IsDiskFailure()) {
        RETURN_NOT_OK(MarkDataDirFailed(uuid_idx, refresh_status.ToString()));
        continue;
      }
      return refresh_status;
    }
  }

  return Status::OK();
}

Status DataDirManager::LoadDataDirGroupFromPB(
    const std::string& tablet_id,
    const DataDirGroupPB& pb) {
  std::lock_guard<folly::SharedMutex> lock(dir_group_lock_);
  DataDirGroup group_from_pb;
  RETURN_NOT_OK_PREPEND(
      group_from_pb.LoadFromPB(idx_by_uuid_, pb),
      fmt::format("could not load data dir group for tablet {}", tablet_id));
  auto [it, inserted] = group_by_tablet_map_.emplace(tablet_id, group_from_pb);
  if (!inserted) {
    return Status::AlreadyPresent(
        fmt::format(
            "tried to load directory group for tablet {} but one is already registered",
            tablet_id));
  }
  for (int uuid_idx : group_from_pb.uuid_indices()) {
    auto uuid_it = tablets_by_uuid_idx_map_.find(uuid_idx);
    CHECK(uuid_it != tablets_by_uuid_idx_map_.end())
        << "Map key not found: " << uuid_idx;
    auto insert_result = uuid_it->second.insert(tablet_id);
    CHECK(insert_result.second) << "Key already exists: " << tablet_id;
  }
  return Status::OK();
}

Status DataDirManager::CreateDataDirGroup(
    const string& tablet_id,
    DirDistributionMode mode) {
  std::lock_guard<folly::SharedMutex> write_lock(dir_group_lock_);
  if (group_by_tablet_map_.contains(tablet_id)) {
    return Status::AlreadyPresent(
        "Tried to create directory group for tablet but one is already "
        "registered",
        tablet_id);
  }
  // Adjust the disk group size to fit within the total number of data dirs.
  int group_target_size;
  if (FLAGS_fs_target_data_dirs_per_tablet == 0) {
    group_target_size = data_dirs_.size();
  } else {
    group_target_size = std::min(
        FLAGS_fs_target_data_dirs_per_tablet,
        static_cast<int>(data_dirs_.size()));
  }
  vector<int> group_indices;
  if (mode == DirDistributionMode::ACROSS_ALL_DIRS) {
    // If using all dirs, add all regardless of directory state.
    for (const auto& [key, value] : data_dir_by_uuid_idx_) {
      group_indices.push_back(key);
    }
  } else {
    // Randomly select directories, giving preference to those with fewer
    // tablets.
    if (PREDICT_FALSE(!failed_data_dirs_.empty())) {
      group_target_size = std::min(
          group_target_size,
          static_cast<int>(data_dirs_.size()) -
              static_cast<int>(failed_data_dirs_.size()));

      // A size of 0 would indicate no healthy disks, which should crash the
      // server.
      DCHECK_GE(group_target_size, 0);
      if (group_target_size == 0) {
        return Status::IOError(
            "No healthy data directories available", "", ENODEV);
      }
    }
    GetDirsForGroupUnlocked(group_target_size, &group_indices);
    if (PREDICT_FALSE(group_indices.empty())) {
      return Status::IOError(
          "All healthy data directories are full", "", ENOSPC);
    }
    if (PREDICT_FALSE(
            group_indices.size() < FLAGS_fs_target_data_dirs_per_tablet)) {
      string msg = fmt::format(
          "Could only allocate {} dirs of requested {} for tablet "
          "{}. {} dirs total",
          group_indices.size(),
          FLAGS_fs_target_data_dirs_per_tablet,
          tablet_id,
          data_dirs_.size());
      if (metrics_) {
        msg += fmt::format(
            ", {} dirs full, {} dirs failed",
            metrics_->data_dirs_full->value(),
            metrics_->data_dirs_failed->value());
      }
      LOG(INFO) << msg;
    }
  }
  auto [it, inserted] =
      group_by_tablet_map_.emplace(tablet_id, DataDirGroup(group_indices));
  CHECK(inserted) << "Key already exists: " << tablet_id;
  for (int uuid_idx : group_indices) {
    auto uuid_it = tablets_by_uuid_idx_map_.find(uuid_idx);
    CHECK(uuid_it != tablets_by_uuid_idx_map_.end())
        << "Map key not found: " << uuid_idx;
    auto insert_result = uuid_it->second.insert(tablet_id);
    CHECK(insert_result.second) << "Key already exists: " << tablet_id;
  }
  return Status::OK();
}

Status DataDirManager::GetNextDataDir(
    const CreateBlockOptions& opts,
    DataDir** dir) {
  std::shared_lock<folly::SharedMutex> lock(dir_group_lock_);
  const vector<int>* group_uuid_indices;
  vector<int> valid_uuid_indices;
  if (PREDICT_TRUE(!opts.tablet_id.empty())) {
    // Get the data dir group for the tablet.
    auto it = group_by_tablet_map_.find(opts.tablet_id);
    if (it == group_by_tablet_map_.end()) {
      return Status::NotFound(
          "Tried to get directory but no directory group "
          "registered for tablet",
          opts.tablet_id);
    }
    if (PREDICT_TRUE(failed_data_dirs_.empty())) {
      group_uuid_indices = &it->second.uuid_indices();
    } else {
      RemoveUnhealthyDataDirsUnlocked(
          it->second.uuid_indices(), &valid_uuid_indices);
      group_uuid_indices = &valid_uuid_indices;
      if (valid_uuid_indices.empty()) {
        return Status::IOError(
            "No healthy directories exist in tablet's "
            "directory group",
            opts.tablet_id,
            ENODEV);
      }
    }
  } else {
    // This should only be reached by some tests; in cases where there is no
    // natural tablet_id, select a data dir from any of the directories.
    CHECK(IsGTest());
    for (const auto& [key, value] : data_dir_by_uuid_idx_) {
      valid_uuid_indices.push_back(key);
    }
    group_uuid_indices = &valid_uuid_indices;
  }
  vector<int> random_indices(group_uuid_indices->size());
  iota(random_indices.begin(), random_indices.end(), 0);
  shuffle(
      random_indices.begin(),
      random_indices.end(),
      default_random_engine(rng_.Next()));

  // Randomly select a member of the group that is not full.
  for (int i : random_indices) {
    int uuid_idx = (*group_uuid_indices)[i];
    auto it = data_dir_by_uuid_idx_.find(uuid_idx);
    CHECK(it != data_dir_by_uuid_idx_.end())
        << "Map key not found: " << uuid_idx;
    DataDir* candidate = it->second;
    Status s = candidate->RefreshIsFull(DataDir::RefreshMode::EXPIRED_ONLY);
    WARN_NOT_OK(
        s, fmt::format("failed to refresh fullness of {}", candidate->dir()));
    if (s.ok() && !candidate->is_full()) {
      *dir = candidate;
      return Status::OK();
    }
  }
  string tablet_id_str;
  if (PREDICT_TRUE(!opts.tablet_id.empty())) {
    tablet_id_str = fmt::format("{}'s ", opts.tablet_id);
  }
  string dirs_state_str = fmt::format("{} failed", failed_data_dirs_.size());
  if (metrics_) {
    dirs_state_str = fmt::format(
        "{} full, {}", metrics_->data_dirs_full->value(), dirs_state_str);
  }
  return Status::IOError(
      fmt::format(
          "No directories available to add to {}directory group ({} "
          "dirs total, {}).",
          tablet_id_str,
          data_dirs_.size(),
          dirs_state_str),
      "",
      ENOSPC);
}

void DataDirManager::DeleteDataDirGroup(const std::string& tablet_id) {
  std::lock_guard<folly::SharedMutex> lock(dir_group_lock_);
  auto it = group_by_tablet_map_.find(tablet_id);
  if (it == group_by_tablet_map_.end()) {
    return;
  }
  DataDirGroup* group = &it->second;
  // Remove the tablet_id from every data dir in its group.
  for (int uuid_idx : group->uuid_indices()) {
    auto it2 = tablets_by_uuid_idx_map_.find(uuid_idx);
    CHECK(it2 != tablets_by_uuid_idx_map_.end())
        << "Map key not found: " << uuid_idx;
    it2->second.erase(tablet_id);
  }
  group_by_tablet_map_.erase(tablet_id);
}

Status DataDirManager::GetDataDirGroupPB(
    const string& tablet_id,
    DataDirGroupPB* pb) const {
  std::shared_lock<folly::SharedMutex> lock(dir_group_lock_);
  auto it = group_by_tablet_map_.find(tablet_id);
  if (it == group_by_tablet_map_.end()) {
    return Status::NotFound(
        fmt::format("could not find data dir group for tablet {}", tablet_id));
  }
  const DataDirGroup* group = &it->second;
  RETURN_NOT_OK(group->CopyToPB(uuid_by_idx_, pb));
  return Status::OK();
}

void DataDirManager::GetDirsForGroupUnlocked(
    int target_size,
    vector<int>* group_indices) {
  // Note: folly::SharedMutex doesn't have is_locked() method
  vector<int> candidate_indices;
  for (auto& e : data_dir_by_uuid_idx_) {
    if (failed_data_dirs_.contains(e.first)) {
      continue;
    }
    Status s = e.second->RefreshIsFull(DataDir::RefreshMode::ALWAYS);
    WARN_NOT_OK(
        s, fmt::format("failed to refresh fullness of {}", e.second->dir()));
    if (s.ok() && !e.second->is_full()) {
      // TODO(awong): If a disk is unhealthy at the time of group creation, the
      // resulting group may be below targeted size. Add functionality to
      // resize groups. See KUDU-2040 for more details.
      candidate_indices.push_back(e.first);
    }
  }
  while (group_indices->size() < target_size && !candidate_indices.empty()) {
    shuffle(
        candidate_indices.begin(),
        candidate_indices.end(),
        default_random_engine(rng_.Next()));
    if (candidate_indices.size() == 1) {
      group_indices->push_back(candidate_indices[0]);
      candidate_indices.erase(candidate_indices.begin());
    } else {
      auto it0 = tablets_by_uuid_idx_map_.find(candidate_indices[0]);
      CHECK(it0 != tablets_by_uuid_idx_map_.end())
          << "Map key not found: " << candidate_indices[0];
      auto it1 = tablets_by_uuid_idx_map_.find(candidate_indices[1]);
      CHECK(it1 != tablets_by_uuid_idx_map_.end())
          << "Map key not found: " << candidate_indices[1];
      if (it0->second.size() < it1->second.size()) {
        group_indices->push_back(candidate_indices[0]);
        candidate_indices.erase(candidate_indices.begin());
      } else {
        group_indices->push_back(candidate_indices[1]);
        candidate_indices.erase(candidate_indices.begin() + 1);
      }
    }
  }
}

DataDir* DataDirManager::FindDataDirByUuidIndex(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  auto it = data_dir_by_uuid_idx_.find(uuid_idx);
  return (it != data_dir_by_uuid_idx_.end()) ? it->second : nullptr;
}

bool DataDirManager::FindUuidIndexByDataDir(DataDir* dir, int* uuid_idx) const {
  auto it = uuid_idx_by_data_dir_.find(dir);
  if (it != uuid_idx_by_data_dir_.end()) {
    *uuid_idx = it->second;
    return true;
  }
  return false;
}

bool DataDirManager::FindUuidIndexByRoot(const string& root, int* uuid_idx)
    const {
  string uuid;
  if (FindUuidByRoot(root, &uuid)) {
    return FindUuidIndexByUuid(uuid, uuid_idx);
  }
  return false;
}

bool DataDirManager::FindUuidIndexByUuid(const string& uuid, int* uuid_idx)
    const {
  auto it = idx_by_uuid_.find(uuid);
  if (it != idx_by_uuid_.end()) {
    *uuid_idx = it->second;
    return true;
  }
  return false;
}

bool DataDirManager::FindUuidByRoot(const string& root, string* uuid) const {
  auto it = uuid_by_root_.find(root);
  if (it != uuid_by_root_.end()) {
    *uuid = it->second;
    return true;
  }
  return false;
}

set<string> DataDirManager::FindTabletsByDataDirUuidIdx(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  std::shared_lock<folly::SharedMutex> lock(dir_group_lock_);
  auto it = tablets_by_uuid_idx_map_.find(uuid_idx);
  if (it != tablets_by_uuid_idx_map_.end()) {
    return it->second;
  }
  return {};
}

void DataDirManager::MarkDataDirFailedByUuid(const string& uuid) {
  int uuid_idx;
  CHECK(FindUuidIndexByUuid(uuid, &uuid_idx));
  WARN_NOT_OK(MarkDataDirFailed(uuid_idx), "Failed to handle disk failure");
}

Status DataDirManager::MarkDataDirFailed(
    int uuid_idx,
    const string& error_message) {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  std::lock_guard<folly::SharedMutex> lock(dir_group_lock_);
  DataDir* dd = FindDataDirByUuidIndex(uuid_idx);
  DCHECK(dd);
  if (failed_data_dirs_.insert(uuid_idx).second) {
    if (failed_data_dirs_.size() == data_dirs_.size()) {
      // TODO(awong): pass 'error_message' as a Status instead of an string so
      // we can avoid returning this artificial status.
      return Status::IOError(
          fmt::format("All data dirs have failed: {}", error_message));
    }
    if (metrics_) {
      metrics_->data_dirs_failed->IncrementBy(1);
    }
    string error_prefix;
    if (!error_message.empty()) {
      error_prefix = fmt::format("{}: ", error_message);
    }
    LOG(ERROR) << error_prefix
               << fmt::format("Directory {} marked as failed", dd->dir());
  }
  return Status::OK();
}

bool DataDirManager::IsDataDirFailed(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  std::shared_lock<folly::SharedMutex> lock(dir_group_lock_);
  return failed_data_dirs_.contains(uuid_idx);
}

bool DataDirManager::IsTabletInFailedDir(const string& tablet_id) const {
  const set<int> failed_dirs = GetFailedDataDirs();
  for (int failed_dir : failed_dirs) {
    const auto& tablets = FindTabletsByDataDirUuidIdx(failed_dir);
    if (tablets.contains(tablet_id)) {
      return true;
    }
  }
  return false;
}

void DataDirManager::RemoveUnhealthyDataDirsUnlocked(
    const vector<int>& uuid_indices,
    vector<int>* healthy_indices) const {
  if (PREDICT_TRUE(failed_data_dirs_.empty())) {
    return;
  }
  healthy_indices->clear();
  for (int uuid_idx : uuid_indices) {
    DCHECK_LT(uuid_idx, data_dirs_.size());
    if (!failed_data_dirs_.contains(uuid_idx)) {
      healthy_indices->emplace_back(uuid_idx);
    }
  }
}

vector<string> DataDirManager::GetDataRoots() const {
  return GetRootNames(canonicalized_data_fs_roots_);
}

vector<string> DataDirManager::GetDataDirs() const {
  return JoinPathSegmentsV(GetDataRoots(), kDataDirName);
}

} // namespace kudu::fs
