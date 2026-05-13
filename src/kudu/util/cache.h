// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// This is taken from LevelDB and evolved to fit the kudu codebase.
//
// TODO: this is pretty lock-heavy. Would be good to sub out something
// a little more concurrent.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"

namespace kudu {

class Cache;
class MetricEntity;

enum CacheType { kDramCache, kNvmCache };

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
Cache* newLruCache(CacheType type, size_t capacity, const std::string& id);

class Cache {
 public:
  // Callback interface which is called when an entry is evicted from the
  // cache.
  class EvictionCallback {
   public:
    virtual void evictedEntry(Slice key, Slice value) = 0;
    virtual ~EvictionCallback() {}
  };

  Cache() {}

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // Custom handle "deleter", primarily intended for use with std::unique_ptr.
  //
  // Sample usage:
  //
  //   Cache* cache = newLruCache(...);
  //   ...
  //   {
  //     unique_ptr<Cache::Handle, Cache::HandleDeleter> h(
  //       cache->lookup(...), Cache::HandleDeleter(cache));
  //     ...
  //   } // 'h' is automatically released here
  //
  // Or:
  //
  //   Cache* cache = newLruCache(...);
  //   ...
  //   {
  //     Cache::UniqueHandle h(cache->lookup(...), Cache::HandleDeleter(cache));
  //     ...
  //   } // 'h' is automatically released here
  //
  class HandleDeleter {
   public:
    explicit HandleDeleter(Cache* c) : c_(c) {}

    void operator()(Cache::Handle* h) const {
      c_->release(h);
    }

   private:
    Cache* c_;
  };
  using UniqueHandle = std::unique_ptr<Handle, HandleDeleter>;

  // Passing kExpectInCache will increment the hit/miss metrics that track the
  // number of times blocks were requested that the users were hoping to get the
  // block from the cache, along with with the basic metrics. Passing
  // kNoExpectInCache will only increment the basic metrics. This helps in
  // determining if we are effectively caching the blocks that matter the most.
  enum CacheBehavior { kExpectInCache, kNoExpectInCache };

  // If the cache has no mapping for "key", returns NULL.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* lookup(const Slice& key, CacheBehavior caching) = 0;

  // Release a mapping returned by a previous lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual Slice value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void erase(const Slice& key) = 0;

  // Pass a metric entity in order to start recoding metrics.
  virtual void setMetrics(
      const std::shared_ptr<MetricEntity>& metricEntity) = 0;

  // ------------------------------------------------------------
  // Insertion path
  // ------------------------------------------------------------
  //
  // Because some cache implementations (eg NVM) manage their own memory, and
  // because we'd like to read blocks directly into cache-managed memory rather
  // than causing an extra memcpy, the insertion of a new element into the cache
  // requires two phases. First, a PendingHandle is allocated with space for the
  // value, and then it is later inserted.
  //
  // For example:
  //
  //   PendingHandle* ph = cache_->allocate("my entry", value_size, charge);
  //   if (!ReadDataFromDisk(cache_->mutableValue(ph)).ok()) {
  //     cache_->free(ph);
  //     ... error handling ...
  //     return;
  //   }
  //   Handle* h = cache_->insert(ph, my_eviction_callback);
  //   ...
  //   cache_->release(h);

  // Opaque handle to an entry which is being prepared to be added to
  // the cache.
  struct PendingHandle {};

  // Indicates that the charge of an item in the cache should be calculated
  // based on its memory consumption.
  static constexpr int kAutomaticCharge = -1;

  // Allocate space for a new entry to be inserted into the cache.
  //
  // The provided 'key' is copied into the resulting handle object.
  // The allocated handle has enough space such that the value can
  // be written into cache_->mutableValue(handle).
  //
  // If 'charge' is not 'kAutomaticCharge', then the cache capacity will be
  // charged the explicit amount. This is useful when caching items that are
  // small but need to maintain a bounded count (eg file descriptors) rather
  // than caring about their actual memory usage.
  //
  // Note that this does not mutate the cache itself: lookups will
  // not be able to find the provided key until it is inserted.
  //
  // It is possible that this will return NULL if the cache is above its
  // capacity and eviction fails to free up enough space for the requested
  // allocation.
  //
  // NOTE: the returned memory is not automatically freed by the cache: the
  // caller must either free it using free(), or insert it using insert().
  virtual PendingHandle* allocate(Slice key, int valLen, int charge) = 0;

  virtual uint8_t* mutableValue(PendingHandle* handle) = 0;

  // Commit a prepared entry into the cache.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->release(handle) when the returned mapping is no
  // longer needed. This method always succeeds and returns a non-null
  // entry, since the space was reserved above.
  //
  // The 'pending' entry passed here should have been allocated using
  // Cache::allocate() above.
  //
  // If 'evictionCallback' is non-NULL, then it will be called when the
  // entry is later evicted or when the cache shuts down.
  virtual Handle* insert(
      PendingHandle* pending,
      EvictionCallback* evictionCallback) = 0;

  // Free 'ptr', which must have been previously allocated using 'allocate'.
  virtual void free(PendingHandle* ptr) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Cache);
};

} // namespace kudu
