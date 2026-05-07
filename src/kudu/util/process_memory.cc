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

#include <cstddef>
#include <mutex>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>
#ifdef TCMALLOC_ENABLED
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#endif

#include <fmt/core.h>
#include "kudu/gutil/macros.h"
#include "kudu/gutil/walltime.h" // IWYU pragma: keep
#include "kudu/util/debug/trace_event.h" // IWYU pragma: keep
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mem_tracker.h" // IWYU pragma: keep
#include "kudu/util/process_memory.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

DEFINE_int64(
    memory_limit_hard_bytes,
    0,
    "Maximum amount of memory this daemon should use, in bytes. "
    "A value of 0 autosizes based on the total system memory. "
    "A value of -1 disables all memory limiting.");
TAG_FLAG(memory_limit_hard_bytes, stable);

DEFINE_int32(
    memory_pressure_percentage,
    60,
    "Percentage of the hard memory limit that this daemon may "
    "consume before flushing of in-memory data becomes prioritized.");
TAG_FLAG(memory_pressure_percentage, advanced);

DEFINE_int32(
    memory_limit_soft_percentage,
    80,
    "Percentage of the hard memory limit that this daemon may "
    "consume before memory throttling of writes begins. The greater "
    "the excess, the higher the chance of throttling. In general, a "
    "lower soft limit leads to smoother write latencies but "
    "decreased throughput, and vice versa for a higher soft limit.");
TAG_FLAG(memory_limit_soft_percentage, advanced);

DEFINE_int32(
    memory_limit_warn_threshold_percentage,
    98,
    "Percentage of the hard memory limit that this daemon may "
    "consume before WARNING level messages are periodically logged.");
TAG_FLAG(memory_limit_warn_threshold_percentage, advanced);

#ifdef TCMALLOC_ENABLED
DEFINE_int32(
    tcmalloc_max_free_bytes_percentage,
    10,
    "Maximum percentage of the RSS that tcmalloc is allowed to use for "
    "reserved but unallocated memory.");
TAG_FLAG(tcmalloc_max_free_bytes_percentage, advanced);
#endif

namespace kudu {
namespace process_memory {

namespace {
int64_t gHardLimit;
int64_t gSoftLimit;
int64_t gPressureThreshold;

ThreadSafeRandom* gRand = nullptr;

#ifdef TCMALLOC_ENABLED
// Total amount of memory released since the last GC. If this
// is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
Atomic64 gReleasedMemorySinceGc;

// Size, in bytes, that is considered a large value for Release() (or Consume()
// with a negative value). If tcmalloc is used, this can trigger it to GC. A
// higher value will make us call into tcmalloc less often (and therefore more
// efficient). A lower value will mean our memory overhead is lower.
// TODO(todd): this is a stopgap.
const int64_t kGcReleaseSize = 128 * 1024L * 1024L;

#endif // TCMALLOC_ENABLED

} // anonymous namespace

// Flag validation
// ------------------------------------------------------------
// Validate that various flags are percentages.
static bool validatePercentage(const char* flagName, int value) {
  if (value >= 0 && value <= 100) {
    return true;
  }
  LOG(ERROR) << fmt::format(
      "{} must be a percentage, value {} is invalid", flagName, value);
  return false;
}

static bool dummy[] = {
    gflags::RegisterFlagValidator(
        &FLAGS_memory_limit_soft_percentage,
        &validatePercentage),
    gflags::RegisterFlagValidator(
        &FLAGS_memory_limit_warn_threshold_percentage,
        &validatePercentage)
#ifdef TCMALLOC_ENABLED
        ,
    gflags::RegisterFlagValidator(
        &FLAGS_tcmalloc_max_free_bytes_percentage,
        &validatePercentage)
#endif
};

// Wrappers around tcmalloc functionality
// ------------------------------------------------------------
#ifdef TCMALLOC_ENABLED
static int64_t getTcmallocProperty(const char* prop) {
  size_t value;
  if (!MallocExtension::instance()->GetNumericProperty(prop, &value)) {
    LOG(DFATAL) << "Failed to get tcmalloc property " << prop;
  }
  return value;
}

int64_t getTcmallocCurrentAllocatedBytes() {
  return getTcmallocProperty("generic.current_allocated_bytes");
}

void gcTcmalloc() {
  TRACE_EVENT0("process", "gcTcmalloc");

  // Number of bytes in the 'NORMAL' free list (i.e reserved by tcmalloc but
  // not in use).
  int64_t bytesOverhead = getTcmallocProperty("tcmalloc.pageheap_free_bytes");
  // Bytes allocated by the application.
  int64_t bytesUsed = getTcmallocCurrentAllocatedBytes();

  int64_t maxOverhead =
      bytesUsed * FLAGS_tcmalloc_max_free_bytes_percentage / 100.0;
  if (bytesOverhead > maxOverhead) {
    int64_t extra = bytesOverhead - maxOverhead;
    while (extra > 0) {
      // Release 1MB at a time, so that tcmalloc releases its page heap lock
      // allowing other threads to make progress. This still disrupts the
      // current thread, but is better than disrupting all.
      MallocExtension::instance()->ReleaseToSystem(1024 * 1024);
      extra -= 1024 * 1024;
    }
  }
}
#endif // TCMALLOC_ENABLED

// Consumption and soft memory limit behavior
// ------------------------------------------------------------
namespace {
void doInitLimits() {
  int64_t limit = FLAGS_memory_limit_hard_bytes;
  if (limit == 0) {
    // If no limit is provided, we'll use 80% of system RAM.
    int64_t totalRam;
    CHECK_OK(Env::Default()->GetTotalRAMBytes(&totalRam));
    limit = totalRam * 4;
    limit /= 5;
  }
  gHardLimit = limit;
  gSoftLimit = FLAGS_memory_limit_soft_percentage * gHardLimit / 100;
  gPressureThreshold = FLAGS_memory_pressure_percentage * gHardLimit / 100;

  gRand = new ThreadSafeRandom(1);
}

void initLimits() {
  static std::once_flag once;
  std::call_once(once, doInitLimits);
}

} // anonymous namespace

int64_t currentConsumption() {
#ifdef TCMALLOC_ENABLED
  const int64_t kReadIntervalMicros = 50000;
  static Atomic64 lastReadTime = 0;
  static simple_spinlock readLock;
  static Atomic64 consumption = 0;
  uint64_t time = getMonoTimeMicros();
  if (time > lastReadTime + kReadIntervalMicros && readLock.try_lock()) {
    base::subtle::NoBarrier_Store(
        &consumption, getTcmallocCurrentAllocatedBytes());
    // Re-fetch the time after getting the consumption. This way, in case
    // fetching consumption is extremely slow for some reason (eg due to lots of
    // contention in tcmalloc) we at least ensure that we wait at least another
    // full interval before fetching the information again.
    time = getMonoTimeMicros();
    base::subtle::NoBarrier_Store(&lastReadTime, time);
    readLock.unlock();
  }

  return base::subtle::NoBarrier_Load(&consumption);
#else
  // Without tcmalloc, we have no reliable way of determining our own heap
  // size (e.g. mallinfo doesn't work in ASAN builds). So, we'll fall back
  // to just looking at the sum of our tracked memory.
  return MemTracker::getRootTracker()->consumption();
#endif
}

int64_t hardLimit() {
  initLimits();
  return gHardLimit;
}

int64_t softLimit() {
  initLimits();
  return gSoftLimit;
}

int64_t memoryPressureThreshold() {
  initLimits();
  return gPressureThreshold;
}

bool underMemoryPressure(double* currentCapacityPct) {
  initLimits();
  int64_t consumption = currentConsumption();
  if (consumption < gPressureThreshold) {
    return false;
  }
  if (currentCapacityPct) {
    *currentCapacityPct = static_cast<double>(consumption) / gHardLimit * 100;
  }
  return true;
}

bool softLimitExceeded(double* currentCapacityPct) {
  initLimits();
  int64_t consumption = currentConsumption();
  // Did we exceed the actual limit?
  if (consumption > gHardLimit) {
    if (currentCapacityPct) {
      *currentCapacityPct = static_cast<double>(consumption) / gHardLimit * 100;
    }
    return true;
  }

  // No soft limit defined.
  if (gHardLimit == gSoftLimit) {
    return false;
  }

  // Are we under the soft limit threshold?
  if (consumption < gSoftLimit) {
    return false;
  }

  // We're over the threshold; were we randomly chosen to be over the soft
  // limit?
  if (consumption + gRand->uniform64(gHardLimit - gSoftLimit) > gHardLimit) {
    if (currentCapacityPct) {
      *currentCapacityPct = static_cast<double>(consumption) / gHardLimit * 100;
    }
    return true;
  }
  return false;
}

void maybeGcAfterRelease(int64_t releasedBytes) {
#ifdef TCMALLOC_ENABLED
  int64_t nowReleased = base::subtle::NoBarrier_AtomicIncrement(
      &gReleasedMemorySinceGc, -releasedBytes);
  if (PREDICT_FALSE(nowReleased > kGcReleaseSize)) {
    base::subtle::NoBarrier_Store(&gReleasedMemorySinceGc, 0);
    gcTcmalloc();
  }
#endif
}

} // namespace process_memory
} // namespace kudu
