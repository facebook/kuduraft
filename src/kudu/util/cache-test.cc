// Some portions Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/cache.h"
#include "kudu/util/coding.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

#if defined(__linux__)
DECLARE_string(nvm_cache_path);
#endif // defined(__linux__)

DECLARE_double(cache_memtracker_approximation_ratio);

namespace kudu {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string encodeInt(int k) {
  faststring result;
  putFixed32(&result, k);
  return result.toString();
}
static int decodeInt(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}

class CacheTest : public KuduTest,
                  public ::testing::WithParamInterface<CacheType>,
                  public Cache::EvictionCallback {
 public:
  // Implementation of the EvictionCallback interface
  void evictedEntry(Slice key, Slice val) override {
    evictedKeys.push_back(decodeInt(key));
    evictedValues.push_back(decodeInt(val));
  }
  std::vector<int> evictedKeys;
  std::vector<int> evictedValues;
  std::shared_ptr<MemTracker> memTracker;
  std::unique_ptr<Cache> cache;
  MetricRegistry metricRegistry;

  static const int kCacheSize = 14 * 1024 * 1024;

  virtual void SetUp() override {
#if defined(HAVE_LIB_VMEM)
    if (gflags::GetCommandLineFlagInfoOrDie("nvm_cache_path").is_default) {
      FLAGS_nvm_cache_path = GetTestPath("nvm-cache");
      ASSERT_OK(Env::Default()->CreateDir(FLAGS_nvm_cache_path));
    }
#endif // defined(HAVE_LIB_VMEM)

    // Disable approximate tracking of cache memory since we make specific
    // assertions on the MemTracker in this test.
    FLAGS_cache_memtracker_approximation_ratio = 0;

    cache.reset(newLruCache(GetParam(), kCacheSize, "cache_test"));

    MemTracker::findTracker("cache_test-sharded_lru_cache", &memTracker);
    // Since nvm cache does not have memtracker due to the use of
    // tcmalloc for this we only check for it in the DRAM case.
    if (GetParam() == kDramCache) {
      ASSERT_TRUE(memTracker.get());
    }

    std::shared_ptr<MetricEntity> entity =
        METRIC_ENTITY_server.instantiate(&metricRegistry, "test");
    cache->setMetrics(entity);
  }

  int Lookup(int key) {
    Cache::Handle* handle =
        cache->lookup(encodeInt(key), Cache::kExpectInCache);
    const int r = (handle == nullptr) ? -1 : decodeInt(cache->value(handle));
    if (handle != nullptr) {
      cache->release(handle);
    }
    return r;
  }

  void Insert(int key, int value, int charge = 1) {
    std::string keyStr = encodeInt(key);
    std::string valStr = encodeInt(value);
    Cache::PendingHandle* handle =
        CHECK_NOTNULL(cache->allocate(keyStr, valStr.size(), charge));
    memcpy(cache->mutableValue(handle), valStr.data(), valStr.size());

    cache->release(cache->insert(handle, this));
  }

  void Erase(int key) {
    cache->erase(encodeInt(key));
  }
};

#if defined(__linux__)
INSTANTIATE_TEST_CASE_P(
    CacheTypes,
    CacheTest,
    // FIXME(mpercy): NVM cache is not supported and likely broken.
    ::testing::Values(kDramCache /*, kNvmCache*/));
#else
INSTANTIATE_TEST_CASE_P(CacheTypes, CacheTest, ::testing::Values(kDramCache));
#endif // defined(__linux__)

TEST_P(CacheTest, TrackMemory) {
  if (memTracker) {
    Insert(100, 100, 1);
    ASSERT_EQ(1, memTracker->consumption());
    Erase(100);
    ASSERT_EQ(0, memTracker->consumption());
    ASSERT_EQ(1, memTracker->peakConsumption());
  }
}

TEST_P(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  ASSERT_EQ(1, evictedKeys.size());
  ASSERT_EQ(100, evictedKeys[0]);
  ASSERT_EQ(101, evictedValues[0]);
}

TEST_P(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0, evictedKeys.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evictedKeys.size());
  ASSERT_EQ(100, evictedKeys[0]);
  ASSERT_EQ(101, evictedValues[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evictedKeys.size());
}

TEST_P(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Cache::Handle* h1 = cache->lookup(encodeInt(100), Cache::kExpectInCache);
  ASSERT_EQ(101, decodeInt(cache->value(h1)));

  Insert(100, 102);
  Cache::Handle* h2 = cache->lookup(encodeInt(100), Cache::kExpectInCache);
  ASSERT_EQ(102, decodeInt(cache->value(h2)));
  ASSERT_EQ(0, evictedKeys.size());

  cache->release(h1);
  ASSERT_EQ(1, evictedKeys.size());
  ASSERT_EQ(100, evictedKeys[0]);
  ASSERT_EQ(101, evictedValues[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, evictedKeys.size());

  cache->release(h2);
  ASSERT_EQ(2, evictedKeys.size());
  ASSERT_EQ(100, evictedKeys[1]);
  ASSERT_EQ(102, evictedValues[1]);
}

TEST_P(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);

  const int kNumElems = 1000;
  const int kSizePerElem = kCacheSize / kNumElems;

  // Loop adding and looking up new entries, but repeatedly accessing key 101.
  // This frequently-used entry should not be evicted.
  for (int i = 0; i < kNumElems + 1000; i++) {
    Insert(1000 + i, 2000 + i, kSizePerElem);
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  // Since '200' wasn't accessed in the loop above, it should have
  // been evicted.
  ASSERT_EQ(-1, Lookup(200));
}

TEST_P(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = kCacheSize / 1000;
  const int kHeavy = kCacheSize / 100;
  int added = 0;
  int index = 0;
  while (added < 2 * kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cachedWeight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cachedWeight += weight;
      ASSERT_EQ(1000 + i, r);
    }
  }
  ASSERT_LE(cachedWeight, kCacheSize + kCacheSize / 10);
}

} // namespace kudu
