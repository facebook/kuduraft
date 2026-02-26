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

#include "kudu/util/file_cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/macros.h"
#include "kudu/util/array_view.h"
#include "kudu/util/cache.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h" // IWYU pragma: keep
#include "kudu/util/monotime.h"
#include "kudu/util/once.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_int32(
    file_cache_expiry_period_ms,
    60 * 1000,
    "Period of time (in ms) between removing expired file cache descriptors");
TAG_FLAG(file_cache_expiry_period_ms, advanced);

using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace kudu {

namespace {

template <class FileType>
FileType* cacheValueToFileType(Slice s) {
  return reinterpret_cast<FileType*>(
      *reinterpret_cast<void**>(s.mutableData()));
}

template <class FileType>
class EvictionCallback : public Cache::EvictionCallback {
 public:
  EvictionCallback() {}

  void evictedEntry(Slice key, Slice value) override {
    VLOG(2) << "Evicted fd belonging to " << key.ToString();
    delete cacheValueToFileType<FileType>(value);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(EvictionCallback);
};

} // anonymous namespace

namespace internal {

template <class FileType>
class ScopedOpenedDescriptor;

// Encapsulates common descriptor fields and methods.
template <class FileType>
class BaseDescriptor {
 public:
  BaseDescriptor(FileCache<FileType>* fileCache, string filename)
      : fileCache_(fileCache), fileName_(std::move(filename)) {}

  ~BaseDescriptor() {
    VLOG(2) << "Out of scope descriptor with file name: " << filename();

    // The (now expired) weak_ptr remains in 'descriptors_', to be removed by
    // the next call to runDescriptorExpiry(). Removing it here would risk a
    // deadlock on recursive acquisition of 'lock_'.

    if (deleted()) {
      cache()->Erase(filename());

      VLOG(1) << "Deleting file: " << filename();
      WARN_NOT_OK(env()->DeleteFile(filename()), "");
    }
  }

  // Insert a pointer to an open file object into the file cache with the
  // filename as the cache key.
  //
  // Returns a handle to the inserted entry. The handle always contains an open
  // file.
  ScopedOpenedDescriptor<FileType> insertIntoCache(void* filePtr) const {
    // The allocated charge is always one byte. This is incorrect with respect
    // to memory tracking, but it's necessary if the cache capacity is to be
    // equivalent to the max number of fds.
    Cache::PendingHandle* pending =
        CHECK_NOTNULL(cache()->Allocate(filename(), sizeof(filePtr), 1));
    memcpy(cache()->MutableValue(pending), &filePtr, sizeof(filePtr));
    return ScopedOpenedDescriptor<FileType>(
        this,
        Cache::UniqueHandle(
            cache()->Insert(pending, fileCache_->evictionCb_.get()),
            Cache::HandleDeleter(cache())));
  }

  // Retrieves a pointer to an open file object from the file cache with the
  // filename as the cache key.
  //
  // Returns a handle to the looked up entry. The handle may or may not contain
  // an open file, depending on whether the cache hit or missed.
  ScopedOpenedDescriptor<FileType> lookupFromCache() const {
    return ScopedOpenedDescriptor<FileType>(
        this,
        Cache::UniqueHandle(
            cache()->Lookup(filename(), Cache::kExpectInCache),
            Cache::HandleDeleter(cache())));
  }

  // Mark this descriptor as to-be-deleted later.
  void markDeleted() {
    DCHECK(!deleted());
    while (true) {
      auto v = flags_.load();
      if (flags_.compare_exchange_weak(v, v | kFileDeleted)) {
        return;
      }
    }
  }

  // Mark this descriptor as invalidated. No further access is allowed
  // to this file.
  void markInvalidated() {
    DCHECK(!invalidated());
    while (true) {
      auto v = flags_.load();
      if (flags_.compare_exchange_weak(v, v | kInvalidated)) {
        return;
      }
    }
  }

  Cache* cache() const {
    return fileCache_->cache_.get();
  }

  Env* env() const {
    return fileCache_->env_;
  }

  const string& filename() const {
    return fileName_;
  }

  bool deleted() const {
    return flags_.load() & kFileDeleted;
  }
  bool invalidated() const {
    return flags_.load() & kInvalidated;
  }

 private:
  FileCache<FileType>* fileCache_;
  const string fileName_;
  enum Flags { kFileDeleted = 1 << 0, kInvalidated = 1 << 1 };
  std::atomic<uint8_t> flags_{0};

  DISALLOW_COPY_AND_ASSIGN(BaseDescriptor);
};

// A "smart" retrieved LRU cache handle.
//
// The cache handle is released when this object goes out of scope, possibly
// closing the opened file if it is no longer in the cache.
template <class FileType>
class ScopedOpenedDescriptor {
 public:
  // A not-yet-but-soon-to-be opened descriptor.
  explicit ScopedOpenedDescriptor(const BaseDescriptor<FileType>* desc)
      : desc_(desc), handle_(nullptr, Cache::HandleDeleter(desc_->cache())) {}

  // An opened descriptor. Its handle may or may not contain an open file.
  ScopedOpenedDescriptor(
      const BaseDescriptor<FileType>* desc,
      Cache::UniqueHandle handle)
      : desc_(desc), handle_(std::move(handle)) {}

  bool opened() const {
    return handle_.get();
  }

  FileType* file() const {
    DCHECK(opened());
    return cacheValueToFileType<FileType>(desc_->cache()->Value(handle_.get()));
  }

 private:
  const BaseDescriptor<FileType>* desc_;
  Cache::UniqueHandle handle_;
};

// Reference to an on-disk file that may or may not be opened (and thus
// cached) in the file cache.
//
// This empty template is just a specification; actual descriptor classes must
// be fully specialized.
template <class FileType>
class Descriptor : public FileType {};

// A descriptor adhering to the RWFile interface (i.e. when opened, provides
// a read-write interface to the underlying file).
template <>
class Descriptor<RWFile> : public RWFile {
 public:
  Descriptor(FileCache<RWFile>* fileCache, const string& filename)
      : base_(fileCache, filename) {}

  ~Descriptor() = default;

  Status Read(uint64_t offset, Slice result) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Read(offset, result);
  }

  Status ReadV(uint64_t offset, ArrayView<Slice> results) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->ReadV(offset, results);
  }

  Status Write(uint64_t offset, const Slice& data) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Write(offset, data);
  }

  Status WriteV(uint64_t offset, ArrayView<const Slice> data) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->WriteV(offset, data);
  }

  Status PreAllocate(uint64_t offset, size_t length, PreAllocateMode mode)
      override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->PreAllocate(offset, length, mode);
  }

  Status Truncate(uint64_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Truncate(length);
  }

  Status PunchHole(uint64_t offset, size_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->PunchHole(offset, length);
  }

  Status Flush(FlushMode mode, uint64_t offset, size_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Flush(mode, offset, length);
  }

  Status Sync() override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Sync();
  }

  Status Close() override {
    // Intentional no-op; actual closing is deferred to LRU cache eviction.
    return Status::OK();
  }

  Status Size(uint64_t* size) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Size(size);
  }

  Status GetExtentMap(ExtentMap* out) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->GetExtentMap(out);
  }

  const string& filename() const override {
    return base_.filename();
  }

 private:
  friend class FileCache<RWFile>;

  Status Init() {
    return once_.init([this] { return initOnce(); });
  }

  Status initOnce() {
    return reopenFileIfNecessary(nullptr);
  }

  Status reopenFileIfNecessary(ScopedOpenedDescriptor<RWFile>* out) const {
    ScopedOpenedDescriptor<RWFile> found(base_.lookupFromCache());
    CHECK(!base_.invalidated());
    if (found.opened()) {
      // The file is already open in the cache, return it.
      if (out) {
        *out = std::move(found);
      }
      return Status::OK();
    }

    // The file was evicted, reopen it.
    RWFileOptions opts;
    opts.mode = Env::OPEN_EXISTING;
    unique_ptr<RWFile> f;
    RETURN_NOT_OK(base_.env()->NewRWFile(opts, base_.filename(), &f));

    // The cache will take ownership of the newly opened file.
    ScopedOpenedDescriptor<RWFile> opened(base_.insertIntoCache(f.release()));
    if (out) {
      *out = std::move(opened);
    }
    return Status::OK();
  }

  BaseDescriptor<RWFile> base_;
  KuduOnceLambda once_;

  DISALLOW_COPY_AND_ASSIGN(Descriptor);
};

// A descriptor adhering to the RandomAccessFile interface (i.e. when opened,
// provides a read-only interface to the underlying file).
template <>
class Descriptor<RandomAccessFile> : public RandomAccessFile {
 public:
  Descriptor(FileCache<RandomAccessFile>* fileCache, const string& filename)
      : base_(fileCache, filename) {}

  ~Descriptor() = default;

  Status Read(uint64_t offset, Slice result) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Read(offset, result);
  }

  Status ReadV(uint64_t offset, ArrayView<Slice> results) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->ReadV(offset, results);
  }

  Status Size(uint64_t* size) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(reopenFileIfNecessary(&opened));
    return opened.file()->Size(size);
  }

  const string& filename() const override {
    return base_.filename();
  }

  size_t memory_footprint() const override {
    // Normally we would use kuduMallocUsableSize(this). However, that's
    // not safe because 'this' was allocated via std::make_shared(), which
    // means it isn't necessarily the base of the memory allocation; it may be
    // preceded by the shared_ptr control block.
    //
    // It doesn't appear possible to get the base of the allocation via any
    // shared_ptr APIs, so we'll use sizeof(*this) + 16 instead. The 16 bytes
    // represent the shared_ptr control block. Overall the object size is still
    // undercounted as it doesn't account for any internal heap fragmentation,
    // but at least it's safe.
    //
    // Some anecdotal memory measurements taken inside gdb:
    // - glibc 2.23 malloc_usable_size() on make_shared<FileType>: 88 bytes.
    // - tcmalloc malloc_usable_size() on make_shared<FileType>: 96 bytes.
    // - sizeof(std::_Sp_counted_base<>) with libstdc++ 5.4: 16 bytes.
    // - sizeof(std::__1::__shared_ptr_emplace<>) with libc++ 3.9: 16 bytes.
    // - sizeof(*this): 72 bytes.
    return sizeof(*this) + 16 + // shared_ptr control block
        once_.memoryFootprintExcludingThis() + base_.filename().capacity();
  }

 private:
  friend class FileCache<RandomAccessFile>;

  Status Init() {
    return once_.init([this] { return initOnce(); });
  }

  Status initOnce() {
    return reopenFileIfNecessary(nullptr);
  }

  Status reopenFileIfNecessary(
      ScopedOpenedDescriptor<RandomAccessFile>* out) const {
    ScopedOpenedDescriptor<RandomAccessFile> found(base_.lookupFromCache());
    CHECK(!base_.invalidated());
    if (found.opened()) {
      // The file is already open in the cache, return it.
      if (out) {
        *out = std::move(found);
      }
      return Status::OK();
    }

    // The file was evicted, reopen it.
    unique_ptr<RandomAccessFile> f;
    RETURN_NOT_OK(base_.env()->NewRandomAccessFile(base_.filename(), &f));

    // The cache will take ownership of the newly opened file.
    ScopedOpenedDescriptor<RandomAccessFile> opened(
        base_.insertIntoCache(f.release()));
    if (out) {
      *out = std::move(opened);
    }
    return Status::OK();
  }

  BaseDescriptor<RandomAccessFile> base_;
  KuduOnceLambda once_;

  DISALLOW_COPY_AND_ASSIGN(Descriptor);
};

} // namespace internal

template <class FileType>
FileCache<FileType>::FileCache(
    const string& cacheName,
    Env* env,
    int maxOpenFiles,
    const std::shared_ptr<MetricEntity>& entity)
    : env_(env),
      cacheName_(cacheName),
      evictionCb_(new EvictionCallback<FileType>()),
      cache_(newLruCache(kDramCache, maxOpenFiles, cacheName)),
      running_(1) {
  if (entity) {
    cache_->SetMetrics(entity);
  }
  LOG(INFO) << fmt::format(
      "Constructed file cache {} with capacity {}", cacheName, maxOpenFiles);
}

template <class FileType>
FileCache<FileType>::~FileCache() {
  running_.CountDown();
  if (descriptorExpiryThread_) {
    descriptorExpiryThread_->Join();
  }
}

template <class FileType>
Status FileCache<FileType>::init() {
  return Thread::Create(
      "cache",
      fmt::format("{}-evict", cacheName_),
      &FileCache::runDescriptorExpiry,
      this,
      &descriptorExpiryThread_);
}

template <class FileType>
Status FileCache<FileType>::openExistingFile(
    const string& fileName,
    shared_ptr<FileType>* file) {
  shared_ptr<internal::Descriptor<FileType>> desc;
  {
    // Find an existing descriptor, or create one if none exists.
    std::lock_guard<simple_spinlock> l(lock_);
    RETURN_NOT_OK(findDescriptorUnlocked(fileName, &desc));
    if (desc) {
      VLOG(2) << "Found existing descriptor: " << desc->filename();
    } else {
      desc = std::make_shared<internal::Descriptor<FileType>>(this, fileName);
      auto result = descriptors_.emplace(fileName, desc);
      CHECK(result.second) << "Descriptor already exists for: " << fileName;
      VLOG(2) << "Created new descriptor: " << desc->filename();
    }
  }

  // Check that the underlying file can be opened (no-op for found
  // descriptors). Done outside the lock.
  RETURN_NOT_OK(desc->Init());
  *file = std::move(desc);
  return Status::OK();
}

template <class FileType>
Status FileCache<FileType>::deleteFile(const string& fileName) {
  {
    std::lock_guard<simple_spinlock> l(lock_);
    shared_ptr<internal::Descriptor<FileType>> desc;
    RETURN_NOT_OK(findDescriptorUnlocked(fileName, &desc));

    if (desc) {
      VLOG(2) << "Marking file for deletion: " << fileName;
      desc->base_.markDeleted();
      return Status::OK();
    }
  }

  // There is no outstanding descriptor. Delete the file now.
  //
  // Make sure it's been fully evicted from the cache (perhaps it was opened
  // previously?) so that the filesystem can reclaim the file data instantly.
  cache_->Erase(fileName);
  return env_->DeleteFile(fileName);
}

template <class FileType>
void FileCache<FileType>::invalidate(const string& fileName) {
  // Ensure that there is an invalidated descriptor in the map for this
  // filename.
  //
  // This ensures that any concurrent openExistingFile() during this method wil
  // see the invalidation and issue a CHECK failure.
  shared_ptr<internal::Descriptor<FileType>> desc;
  {
    // Find an existing descriptor, or create one if none exists.
    std::lock_guard<simple_spinlock> l(lock_);
    auto it = descriptors_.find(fileName);
    if (it != descriptors_.end()) {
      desc = it->second.lock();
    }
    if (!desc) {
      desc = std::make_shared<internal::Descriptor<FileType>>(this, fileName);
      descriptors_.emplace(fileName, desc);
    }

    desc->base_.markInvalidated();
  }
  // Remove it from the cache so that if the same path is opened again, we
  // will re-open a new FD rather than retrieving one that might have been
  // cached prior to invalidation.
  cache_->Erase(fileName);

  // Remove the invalidated descriptor from the map. We are guaranteed it
  // is still there because we've held a strong reference to it for
  // the duration of this method, and no other methods erase strong
  // references from the map.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(1, descriptors_.erase(fileName));
  }
}

template <class FileType>
int FileCache<FileType>::numDescriptorsForTests() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return descriptors_.size();
}

template <class FileType>
string FileCache<FileType>::toDebugString() const {
  std::lock_guard<simple_spinlock> l(lock_);
  string ret;
  for (const auto& e : descriptors_) {
    bool strong = false;
    bool deleted = false;
    bool opened = false;
    shared_ptr<internal::Descriptor<FileType>> desc = e.second.lock();
    if (desc) {
      strong = true;
      if (desc->base_.deleted()) {
        deleted = true;
      }
      internal::ScopedOpenedDescriptor<FileType> o(
          desc->base_.lookupFromCache());
      if (o.opened()) {
        opened = true;
      }
    }
    if (strong) {
      ret += fmt::format(
          "{} (S{}{})\n", e.first, deleted ? "D" : "", opened ? "O" : "");
    } else {
      ret += fmt::format("{}\n", e.first);
    }
  }
  return ret;
}

template <class FileType>
Status FileCache<FileType>::findDescriptorUnlocked(
    const string& fileName,
    shared_ptr<internal::Descriptor<FileType>>* file) {
  DCHECK(lock_.is_locked());

  auto it = descriptors_.find(fileName);
  if (it != descriptors_.end()) {
    // Found the descriptor. Has it expired?
    shared_ptr<internal::Descriptor<FileType>> desc = it->second.lock();
    if (desc) {
      CHECK(!desc->base_.invalidated());
      if (desc->base_.deleted()) {
        return Status::NotFound("File already marked for deletion", fileName);
      }

      // Descriptor is still valid, return it.
      if (file) {
        *file = desc;
      }
      return Status::OK();
    }
    // Descriptor has expired; erase it and pretend we found nothing.
    descriptors_.erase(it);
  }
  return Status::OK();
}

template <class FileType>
void FileCache<FileType>::runDescriptorExpiry() {
  while (!running_.WaitFor(
      MonoDelta::FromMilliseconds(FLAGS_file_cache_expiry_period_ms))) {
    std::lock_guard<simple_spinlock> l(lock_);
    for (auto it = descriptors_.begin(); it != descriptors_.end();) {
      if (it->second.expired()) {
        it = descriptors_.erase(it);
      } else {
        it++;
      }
    }
  }
}

// Explicit specialization for callers outside this compilation unit.
template class FileCache<RWFile>;
template class FileCache<RandomAccessFile>;

} // namespace kudu
