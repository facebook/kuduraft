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

// The implementation of the Log Index.
//
// The log index is implemented by a set of on-disk files, each containing a
// fixed number (kEntriesPerIndexChunk) of fixed size entries. Each index chunk
// is numbered such that, for a given log index, we can determine which chunk
// contains its index entry by a simple division operation. Because the entries
// are fixed size, we can compute the index offset by a modulo.
//
// When the log is GCed, we remove any index chunks which are no longer needed,
// and unmap them.

#include "kudu/consensus/log_index.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <fmt/core.h>
#include <folly/Conv.h>
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"

using std::string;
using std::vector;

DEFINE_bool(
    uncache_unmapped_index,
    false,
    "Whether explicitly uncache log cache index file after the file is closed");
METRIC_DEFINE_counter(
    server,
    log_index_chunk_mmap_for_read,
    "Number of mmap calls for reading an index chunk",
    kudu::MetricUnit::kUnits,
    "Number of times an index chunk had to be mmapeed "
    "before a read operation.");

namespace kudu::log {

// The actual physical entry in the file.
// This mirrors LogIndexEntry but uses simple primitives only so we can
// read/write it via mmap.
// See LogIndexEntry for docs.
struct PhysicalEntry {
  int64_t term;
  uint64_t segmentSequenceNumber;
  uint64_t offsetInSegment;
} PACKED;

////////////////////////////////////////////////////////////
// LogIndex::IndexChunk implementation
////////////////////////////////////////////////////////////

// A single chunk of the index, representing a fixed number of entries.
// This class maintains the open file descriptor and mapped memory.
class LogIndex::IndexChunk {
 public:
  // Construct an index chunk.
  // 'path' is the full path for the underlying file
  // 'size' is the configured size for this index chunk/file
  explicit IndexChunk(string path, int64_t size);
  ~IndexChunk();

  // Open the chunk file
  Status open();

  // Memory map the chunk file
  // This is not thread safe with getEntry() and setEntry(). The caller should
  // synchronize correctly
  Status mmapFile();

  // Unmap the chunk file from memory
  // This is not thread safe with getEntry() and setEntry(). The caller should
  // synchronize correctly
  void munmapFile();

  // Get an entry from the memory mapped cunk file for a given index
  void getEntry(int entryIndex, PhysicalEntry* ret);

  // Set an entry in the memory mapped chunk file for a given index
  void setEntry(int entryIndex, const PhysicalEntry& entry);

  // Is this chunk file memory mapped?
  bool isMmapped() const;

 private:
  const string path_; // path of the underlying chunk file
  int fd_; // file descriptor
  uint8_t* mapping_; // mmapped memory location of the chunk
  int64_t size_; // configured size for the chunk file
};

namespace {
Status checkError(int rc, const char* operation) {
  if (PREDICT_FALSE(rc < 0)) {
    int err = errno;
    return Status::IOError(operation, ErrnoToString(err), err);
  }
  return Status::OK();
}
} // anonymous namespace

LogIndex::IndexChunk::IndexChunk(std::string path, int64_t size)
    : path_(std::move(path)), fd_(-1), mapping_(nullptr), size_(size) {}

LogIndex::IndexChunk::~IndexChunk() {
  munmapFile();

  if (fd_ >= 0) {
    int ret;
    RETRY_ON_EINTR(ret, close(fd_));
    if (PREDICT_FALSE(ret != 0)) {
      PLOG(WARNING) << "Failed to close fd " << fd_;
    }
  }
}

Status LogIndex::IndexChunk::open() {
  RETRY_ON_EINTR(
      fd_, ::open(path_.c_str(), O_CLOEXEC | O_CREAT | O_RDWR, 0666));
  RETURN_NOT_OK(checkError(fd_, "open"));

  int err;
  RETRY_ON_EINTR(err, ftruncate(fd_, size_));
  RETURN_NOT_OK(checkError(fd_, "truncate"));

  return Status::OK();
}

Status LogIndex::IndexChunk::mmapFile() {
  if (fd_ == -1) {
    return Status::IOError("Chunk should be opened before mmapping");
  }

  if (mapping_) {
    // Already mmaped, return
    return Status::OK();
  }

  mapping_ = static_cast<uint8_t*>(
      mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
  if (mapping_ == nullptr) {
    int err = errno;
    return Status::IOError("Unable to mmap()", ErrnoToString(err), err);
  }

  return Status::OK();
}

void LogIndex::IndexChunk::munmapFile() {
  if (mapping_ != nullptr) {
    munmap(mapping_, size_);
    mapping_ = nullptr;
    if (fd_ > 0 && FLAGS_uncache_unmapped_index) {
      VLOG(5) << "Going to uncache " << path_;
      posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
    }
  }
}

void LogIndex::IndexChunk::getEntry(int entryIndex, PhysicalEntry* ret) {
  DCHECK_GE(fd_, 0) << "Must open() first";
  memcpy(
      ret,
      mapping_ + sizeof(PhysicalEntry) * entryIndex,
      sizeof(PhysicalEntry));
}

void LogIndex::IndexChunk::setEntry(
    int entryIndex,
    const PhysicalEntry& entry) {
  DCHECK_GE(fd_, 0) << "Must open() first";
  memcpy(
      mapping_ + sizeof(PhysicalEntry) * entryIndex,
      &entry,
      sizeof(PhysicalEntry));
}

bool LogIndex::IndexChunk::isMmapped() const {
  return (mapping_ != nullptr);
}

////////////////////////////////////////////////////////////
// LogIndex
////////////////////////////////////////////////////////////

LogIndex::LogIndex(std::string baseDir)
    : baseDir_(std::move(baseDir)), mmapForReads_(nullptr) {}

LogIndex::~LogIndex() = default;

string LogIndex::getChunkPath(int64_t chunkIdx) {
  return fmt::format("{}/index.{:09d}", baseDir_, chunkIdx);
}

Status LogIndex::openAllChunksOnStartup(
    Env* env,
    const std::shared_ptr<MetricEntity>& metricEntity) {
  DCHECK(env);
  std::vector<std::string> children;
  RETURN_NOT_OK(env->GetChildren(baseDir_, &children));

  // Initialize metric counter
  mmapForReads_ =
      metricEntity->FindOrCreateCounter(&METRIC_log_index_chunk_mmap_for_read);

  for (const auto& fname : children) {
    if (fname.find("index.") != 0) {
      continue;
    }

    vector<string> v = strings::Split(fname, ".");
    if (v.size() != 2) {
      LOG(INFO)
          << "Improperly named file in wal directory skipped on recovery: "
          << fname;
      continue;
    }

    auto chunkIdxResult = folly::tryTo<int64_t>(v[1]);
    if (!chunkIdxResult.hasValue()) {
      LOG(INFO)
          << "Improperly named file in wal directory skipped on recovery: "
          << fname;
      continue;
    }
    int64_t chunkIdx = chunkIdxResult.value();

    VLOG(1) << "Opening index file on startup: " << fname << " for chunk idx "
            << chunkIdx;

    std::shared_ptr<IndexChunk> chunk;
    RETURN_NOT_OK(openAndInsertChunk(chunkIdx, &chunk, /*shouldMmap=*/false));
  }

  // mmap 'numChunksToMmap_' chunks. Note that the latest chunks are mmapped
  // (chunks having the highest chunkIdx)
  int64_t mmappedChunks = 0;
  for (auto rit = openChunks_.rbegin(); rit != openChunks_.rend(); ++rit) {
    if (mmappedChunks == numChunksToMmap_) {
      break;
    }

    RETURN_NOT_OK(rit->second->mmapFile());
    mmappedChunks++;
  }

  return Status::OK();
}

void LogIndex::setNumMmapChunks(int64_t numChunks) {
  if (numChunks <= 0) {
    return;
  }

  std::lock_guard<simple_spinlock> l(openChunksLock_);
  numChunksToMmap_ = numChunks;

  // If necessary, unmap additional chunks
  if (openChunks_.size() <= numChunksToMmap_) {
    return;
  }

  // get the number of chunks that are currently mmapped
  int64_t numChunksMmapped = 0;
  for (auto it = openChunks_.begin(); it != openChunks_.end(); ++it) {
    if (it->second->isMmapped()) {
      numChunksMmapped++;
    }
  }

  // unmap additional chunks (starting with the oldest chunks)
  for (auto it = openChunks_.begin();
       it != openChunks_.end() && numChunksMmapped > numChunksToMmap_;
       ++it) {
    if (it->second->isMmapped()) {
      it->second->munmapFile();
      numChunksMmapped--;
    }
  }
}

void LogIndex::setNumEntriesPerChunkForTest(int64_t entries) {
  // WARNING: This should be called only for tests. Changing numer of entries
  // per chunk is dangerous and could render the chunk files unreadable
  entriesPerIndexChunk_ = entries;
}

Status LogIndex::mmapChunk(std::shared_ptr<IndexChunk>* chunk) {
  if (openChunks_.size() < numChunksToMmap_) {
    RETURN_NOT_OK((*chunk)->mmapFile());
    return Status::OK();
  }

  // The victim that needs to be unmapped is the oldest chunk (i.e the chunk
  // with the oldest chunkIdx). See documentation in log_index.h for more
  // details.
  //
  // Note that we have to reverse iterate through the openChunks_ while the
  // caller is holding onto the openChunksLock_. With 'openChunks_' map
  // having only a few hundred entries, this should be acceptable (to keep
  // things simple)
  int64_t numChunksMmapped = 0;
  auto rit = openChunks_.rbegin();
  for (; rit != openChunks_.rend(); ++rit) {
    if (rit->second->isMmapped()) {
      numChunksMmapped++;
    }

    if (numChunksMmapped == numChunksToMmap_) {
      break;
    }
  }

  // If there are 'numChunksToMmap_' chunks already mmapped, then unmap the
  // 'victim' chunk
  if (numChunksMmapped == numChunksToMmap_ && rit != openChunks_.rend()) {
    rit->second->munmapFile();
  }

  // Now mmap the provided 'chunk'
  RETURN_NOT_OK((*chunk)->mmapFile());

  return Status::OK();
}

Status LogIndex::openChunk(
    int64_t chunkIdx,
    std::shared_ptr<IndexChunk>* chunk) {
  string path = getChunkPath(chunkIdx);
  int64_t size = entriesPerIndexChunk_ * sizeof(PhysicalEntry);

  std::shared_ptr<IndexChunk> newChunk(new IndexChunk(path, size));
  RETURN_NOT_OK(newChunk->open());

  chunk->swap(newChunk);
  return Status::OK();
}

Status LogIndex::openAndInsertChunk(
    int64_t chunkIdx,
    std::shared_ptr<IndexChunk>* chunk,
    bool shouldMmap) {
  RETURN_NOT_OK_PREPEND(
      openChunk(chunkIdx, chunk), "Couldn't open index chunk");
  std::lock_guard<simple_spinlock> l(openChunksLock_);
  if (PREDICT_FALSE(openChunks_.contains(chunkIdx))) {
    // Someone else opened the chunk in the meantime.
    // We'll just return that one.
    auto it = openChunks_.find(chunkIdx);
    CHECK(it != openChunks_.end()) << "Map key not found: " << chunkIdx;
    *chunk = it->second;
    return Status::OK();
  }

  auto [it, inserted] = openChunks_.insert({chunkIdx, *chunk});
  CHECK(inserted) << "Chunk already exists: " << chunkIdx;

  if (shouldMmap) {
    RETURN_NOT_OK(mmapChunk(chunk));
  }

  return Status::OK();
}

Status LogIndex::getChunkForIndex(
    int64_t logIndex,
    bool create,
    std::shared_ptr<IndexChunk>* chunk) {
  CHECK_GT(logIndex, 0);
  int64_t chunkIdx = logIndex / entriesPerIndexChunk_;

  {
    std::lock_guard<simple_spinlock> l(openChunksLock_);
    auto it = openChunks_.find(chunkIdx);
    if (it != openChunks_.end()) {
      *chunk = it->second;
      return Status::OK();
    }
  }

  if (!create) {
    return Status::NotFound("chunk not found");
  }

  return openAndInsertChunk(chunkIdx, chunk, /*shouldMmap=*/true);
}

Status LogIndex::addEntry(const LogIndexEntry& entry) {
  std::shared_ptr<IndexChunk> chunk;
  RETURN_NOT_OK(getChunkForIndex(
      entry.opId.index(), true /* create if not found */, &chunk));

  int indexInChunk = entry.opId.index() % entriesPerIndexChunk_;
  DCHECK_LT(indexInChunk, entriesPerIndexChunk_);

  PhysicalEntry phys;
  phys.term = entry.opId.term();
  phys.segmentSequenceNumber = entry.segmentSequenceNumber;
  phys.offsetInSegment = entry.offsetInSegment;

  {
    // Grab the 'openChunksLock_' to ensure that the chunk does not get
    // unmapped
    std::lock_guard<simple_spinlock> l(openChunksLock_);
    if (PREDICT_FALSE(!chunk->isMmapped())) {
      RETURN_NOT_OK(mmapChunk(&chunk));
    }
    chunk->setEntry(indexInChunk, phys);
    VLOG(3) << "Added log index entry " << entry.toString();
  }

  return Status::OK();
}

Status LogIndex::getEntry(int64_t index, LogIndexEntry* entry) {
  std::shared_ptr<IndexChunk> chunk;
  RETURN_NOT_OK(getChunkForIndex(index, false /* do not create */, &chunk));
  int indexInChunk = index % entriesPerIndexChunk_;
  DCHECK_LT(indexInChunk, entriesPerIndexChunk_);
  PhysicalEntry phys;

  {
    // Grab the 'openChunksLock_' to ensure that the chunk does not get
    // unmapped
    std::lock_guard<simple_spinlock> l(openChunksLock_);
    if (PREDICT_FALSE(!chunk->isMmapped())) {
      RETURN_NOT_OK(mmapChunk(&chunk));

      if (mmapForReads_) {
        mmapForReads_->Increment();
      }
    }

    chunk->getEntry(indexInChunk, &phys);
  }

  // We never write any real entries to offset 0, because there's a header
  // in each log segment. So, this indicates an entry that was never written.
  if (phys.offsetInSegment == 0) {
    return Status::NotFound("entry not found");
  }

  entry->opId = consensus::MakeOpId(phys.term, index);
  entry->segmentSequenceNumber = phys.segmentSequenceNumber;
  entry->offsetInSegment = phys.offsetInSegment;

  return Status::OK();
}

void LogIndex::gc(int64_t minIndexToRetain) {
  int minChunkToRetain = minIndexToRetain / entriesPerIndexChunk_;

  // Enumerate which chunks to delete.
  vector<int64_t> chunksToDelete;
  {
    std::lock_guard<simple_spinlock> l(openChunksLock_);
    for (auto it = openChunks_.begin();
         it != openChunks_.lower_bound(minChunkToRetain);
         ++it) {
      chunksToDelete.push_back(it->first);
    }
  }

  // Outside of the lock, try to delete them (avoid holding the lock during IO).
  for (int64_t chunkIdx : chunksToDelete) {
    string path = getChunkPath(chunkIdx);
    int rc = unlink(path.c_str());
    if (rc != 0) {
      PLOG(WARNING) << "Unable to delete index chunk " << path;
      continue;
    }
    VLOG(2) << "Deleted log index segment " << path;
    {
      std::lock_guard<simple_spinlock> l(openChunksLock_);
      openChunks_.erase(chunkIdx);
    }
  }
}

string LogIndexEntry::toString() const {
  return fmt::format(
      "opId={}.{} segmentSequenceNumber={} offset={}",
      opId.term(),
      opId.index(),
      segmentSequenceNumber,
      offsetInSegment);
}

bool LogIndexEntry::operator==(const LogIndexEntry& other) const {
  return other.opId.term() == opId.term() &&
      other.opId.index() == opId.index() &&
      other.segmentSequenceNumber == segmentSequenceNumber &&
      other.offsetInSegment == offsetInSegment;
}

bool LogIndexEntry::operator!=(const LogIndexEntry& other) const {
  return !operator==(other);
}

} // namespace kudu::log
