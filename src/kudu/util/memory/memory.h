// Copyright 2010 Google Inc.  All Rights Reserved
//
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
//
//
// Classes for memory management, used by materializations
// (arenas, segments, and STL collections parametrized via arena allocators)
// so that memory usage can be controlled at the application level.
//
// Materializations can be parametrized by specifying an instance of a
// BufferAllocator. The allocator implements
// memory management policy (e.g. setting allocation limits). Allocators may
// be shared between multiple materializations; e.g. you can designate a
// single allocator per a single user request, thus setting bounds on memory
// usage on a per-request basis.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/memory/overwrite.h"

namespace kudu {

class BufferAllocator;
class MemTracker;

// Wrapper for a block of data allocated by a BufferAllocator. Owns the block.
// (To release the block, destroy the buffer - it will then return it via the
// same allocator that has been used to create it).
class Buffer {
 public:
  ~Buffer();

  void* data() const {
    return data_;
  } // The data buffer.
  size_t size() const {
    return size_;
  } // In bytes.

 private:
  friend class BufferAllocator;

  Buffer(void* data, size_t size, BufferAllocator* allocator)
      : data_(CHECK_NOTNULL(data)), size_(size), allocator_(allocator) {
#ifndef NDEBUG
    overwriteWithPattern(
        reinterpret_cast<char*>(data_),
        size_,
        "NEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEW"
        "NEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEW"
        "NEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEW");
#endif
  }

  // Called by a successful realloc.
  void update(void* newData, size_t newSize) {
#ifndef NDEBUG
    if (newSize > size_) {
      overwriteWithPattern(
          reinterpret_cast<char*>(newData) + size_, newSize - size_, "NEW");
    }
#endif
    data_ = newData;
    size_ = newSize;
  }

  void* data_;
  size_t size_;
  BufferAllocator* const allocator_;
  Buffer(Buffer&&) = delete;
  Buffer& operator=(Buffer&&) = delete;
  DISALLOW_COPY_AND_ASSIGN(Buffer);
};

// Allocators allow applications to control memory usage. They are
// used by materializations to allocate blocks of memory arenas.
// BufferAllocator is an abstract class that defines a common contract of
// all implementations of allocators. Specific allocators provide specific
// features, e.g. enforced resource limits, thread safety, etc.
class BufferAllocator {
 public:
  virtual ~BufferAllocator() = default;

  // Called by the user when a new block of memory is needed. The 'requested'
  // parameter specifies how much memory (in bytes) the user would like to get.
  // The 'minimal' parameter specifies how much he is willing to settle for.
  // The allocator returns a buffer sized in the range [minimal, requested],
  // or NULL if the request can't be satisfied. When the buffer is destroyed,
  // its destructor calls the freeInternal() method on its allocator.
  // CAVEAT: The allocator must outlive all buffers returned by it.
  //
  // Corner cases:
  // 1. If requested == 0, the allocator will always return a non-NULL Buffer
  //    with a non-NULL data pointer and zero capacity.
  // 2. If minimal == 0, the allocator will always return a non-NULL Buffer
  //    with a non-NULL data pointer, possibly with zero capacity.
  Buffer* bestEffortAllocate(size_t requested, size_t minimal) {
    DCHECK_LE(minimal, requested);
    Buffer* result = allocateInternal(requested, minimal, this);
    logAllocation(requested, minimal, result);
    return result;
  }

  // Called by the user when a new block of memory is needed. Equivalent to
  // bestEffortAllocate(requested, requested).
  Buffer* allocate(size_t requested) {
    return bestEffortAllocate(requested, requested);
  }

  // Called by the user when a previously allocated block needs to be resized.
  // Mimics semantics of <cstdlib> realloc. The 'requested' and 'minimal'
  // represent the desired final buffer size, with semantics as in the allocate.
  // If the 'buffer' parameter is NULL, the call is equivalent to
  // allocate(requested, minimal). Otherwise, a reallocation of the buffer's
  // data is attempted. On success, the original 'buffer' parameter is returned,
  // but the buffer itself might have updated size and data. On failure,
  // returns NULL, and leaves the input buffer unmodified.
  // Reallocation might happen in-place, preserving the original data
  // pointer, but it is not guaranteed - e.g. this function might degenerate to
  // Allocate-Copy-Free. Either way, the content of the data buffer, up to the
  // minimum of the new and old size, is preserved.
  //
  // Corner cases:
  // 1. If requested == 0, the allocator will always return a non-NULL Buffer
  //    with a non-NULL data pointer and zero capacity.
  // 2. If minimal == 0, the allocator will always return a non-NULL Buffer
  //    with a non-NULL data pointer, possibly with zero capacity.
  Buffer*
  bestEffortReallocate(size_t requested, size_t minimal, Buffer* buffer) {
    DCHECK_LE(minimal, requested);
    Buffer* result;
    if (buffer == nullptr) {
      result = allocateInternal(requested, minimal, this);
      logAllocation(requested, minimal, result);
      return result;
    } else {
      result = reallocateInternal(requested, minimal, buffer, this) ? buffer
                                                                    : nullptr;
      logAllocation(requested, minimal, buffer);
      return result;
    }
  }

  // Called by the user when a previously allocated block needs to be resized.
  // Equivalent to bestEffortReallocate(requested, requested, buffer).
  Buffer* reallocate(size_t requested, Buffer* buffer) {
    return bestEffortReallocate(requested, requested, buffer);
  }

  // Returns the amount of memory (in bytes) still available for this allocator.
  // For unbounded allocators (like raw HeapBufferAllocator) this is the highest
  // size_t value possible.
  // TODO(user): consider making pure virtual.
  virtual size_t available() const {
    return std::numeric_limits<size_t>::max();
  }

 protected:
  friend class Buffer;

  BufferAllocator() {}

  // Expose the constructor to subclasses of BufferAllocator.
  Buffer* createBuffer(void* data, size_t size, BufferAllocator* allocator) {
    return new Buffer(data, size, allocator);
  }

  // Expose Buffer::update to subclasses of BufferAllocator.
  void updateBuffer(void* newData, size_t newSize, Buffer* buffer) {
    buffer->update(newData, newSize);
  }

  // Called by chained buffer allocators.
  Buffer* delegateAllocate(
      BufferAllocator* delegate,
      size_t requested,
      size_t minimal,
      BufferAllocator* originator) {
    return delegate->allocateInternal(requested, minimal, originator);
  }

  // Called by chained buffer allocators.
  bool delegateReallocate(
      BufferAllocator* delegate,
      size_t requested,
      size_t minimal,
      Buffer* buffer,
      BufferAllocator* originator) {
    return delegate->reallocateInternal(requested, minimal, buffer, originator);
  }

  // Called by chained buffer allocators.
  void delegateFree(BufferAllocator* delegate, Buffer* buffer) {
    delegate->freeInternal(buffer);
  }

 private:
  // Implemented by concrete subclasses.
  virtual Buffer* allocateInternal(
      size_t requested,
      size_t minimal,
      BufferAllocator* originator) = 0;

  // Implemented by concrete subclasses. Returns false on failure.
  virtual bool reallocateInternal(
      size_t requested,
      size_t minimal,
      Buffer* buffer,
      BufferAllocator* originator) = 0;

  // Implemented by concrete subclasses.
  virtual void freeInternal(Buffer* buffer) = 0;

  // Logs a warning message if the allocation failed or if it returned less than
  // the required number of bytes.
  void logAllocation(size_t required, size_t minimal, Buffer* buffer);

  BufferAllocator(BufferAllocator&&) = delete;
  BufferAllocator& operator=(BufferAllocator&&) = delete;
  DISALLOW_COPY_AND_ASSIGN(BufferAllocator);
};

// Allocates buffers on the heap, with no memory limits. Uses standard C
// allocation functions (malloc, realloc, free).
class HeapBufferAllocator : public BufferAllocator {
 public:
  ~HeapBufferAllocator() override = default;

  // Returns a singleton instance of the heap allocator.
  static HeapBufferAllocator* get() {
    return Singleton<HeapBufferAllocator>::get();
  }

  virtual size_t available() const override {
    return std::numeric_limits<size_t>::max();
  }

 private:
  // Allocates memory that is aligned to 16 way.
  // Use if you want to boost SIMD operations on the memory area.
  const bool alignedMode_;

  friend class Singleton<HeapBufferAllocator>;

  // Always allocates 'requested'-sized buffer, or returns NULL on OOM.
  virtual Buffer* allocateInternal(
      size_t requested,
      size_t minimal,
      BufferAllocator* originator) override;

  virtual bool reallocateInternal(
      size_t requested,
      size_t minimal,
      Buffer* buffer,
      BufferAllocator* originator) override;

  void* Malloc(size_t size);
  void* Realloc(void* previousData, size_t previousSize, size_t newSize);

  virtual void freeInternal(Buffer* buffer) override;

  HeapBufferAllocator();
  explicit HeapBufferAllocator(bool alignedMode) : alignedMode_(alignedMode) {}

  HeapBufferAllocator(HeapBufferAllocator&&) = delete;
  HeapBufferAllocator& operator=(HeapBufferAllocator&&) = delete;
  DISALLOW_COPY_AND_ASSIGN(HeapBufferAllocator);
};

// BufferAllocator which uses MemTracker to keep track of and optionally
// (if a limit is set on the MemTracker) regulate memory consumption.
class MemoryTrackingBufferAllocator : public BufferAllocator {
 public:
  // Does not take ownership of the delegate. The delegate must remain
  // valid for the lifetime of this allocator. Increments reference
  // count for 'memTracker'.
  // If 'memTracker' has a limit and 'enforceLimit' is true, then
  // the classes calling this buffer allocator (whether directly, or
  // through an Arena) must be able to handle the case when allocation
  // fails. If 'enforceLimit' is false (this is the default), then
  // allocation will always succeed.
  MemoryTrackingBufferAllocator(
      BufferAllocator* const delegate,
      std::shared_ptr<MemTracker> memTracker,
      bool enforceLimit = false)
      : delegate_(delegate),
        memTracker_(std::move(memTracker)),
        enforceLimit_(enforceLimit) {}

  ~MemoryTrackingBufferAllocator() override = default;

  // If enforce limit is false, this always returns maximum possible value
  // for int64_t (std::numeric_limits<int64_t>::max()). Otherwise, this
  // is equivalent to calling memTracker_->spareCapacity();
  virtual size_t available() const override;

 private:
  // If enforceLimit_ is true, this is equivalent to calling
  // memTracker_->tryConsume(bytes). If enforceLimit_ is false and
  // memTracker_->tryConsume(bytes) is false, we call
  // memTracker_->consume(bytes) and always return true.
  bool tryConsume(int64_t bytes);

  virtual Buffer* allocateInternal(
      size_t requested,
      size_t minimal,
      BufferAllocator* originator) override;

  virtual bool reallocateInternal(
      size_t requested,
      size_t minimal,
      Buffer* buffer,
      BufferAllocator* originator) override;

  virtual void freeInternal(Buffer* buffer) override;

  BufferAllocator* delegate_;
  std::shared_ptr<MemTracker> memTracker_;
  bool enforceLimit_;
  MemoryTrackingBufferAllocator(MemoryTrackingBufferAllocator&&) = delete;
  MemoryTrackingBufferAllocator& operator=(MemoryTrackingBufferAllocator&&) =
      delete;
};

} // namespace kudu
