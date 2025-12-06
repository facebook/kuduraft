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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.
//
// Benchmark to compare performance of the new folly::SpinLock-based
// implementation versus the legacy custom spinlock implementation.

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include <folly/Benchmark.h>
#include <folly/SpinLock.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/spinlock.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"

DEFINE_int32(work_cycles, 100, "Number of work cycles in critical section");

namespace base {
namespace internal {
namespace kudu {

// Forward declarations for the legacy spinlock internal functions
void SpinLockDelay(volatile Atomic32* w, int32 value, int loop);
void SpinLockWake(volatile Atomic32* w, bool all);

} // namespace kudu
} // namespace internal
} // namespace base

namespace kudu {

using std::atomic;
using std::thread;
using std::vector;

//==============================================================================
// LEGACY SPINLOCK IMPLEMENTATION (for benchmarking only)
//==============================================================================

namespace legacy {

// Legacy SpinLock implementation - copied from the original spinlock.cc
// This is used ONLY for performance comparison in benchmarks.
class LegacySpinLock {
 public:
  LegacySpinLock() : lockword_(kSpinLockFree) {}

  explicit LegacySpinLock(base::LinkerInitialized /*x*/)
      : lockword_(kSpinLockFree) {}

  inline void Lock() {
    if (base::subtle::Acquire_CompareAndSwap(
            &lockword_, kSpinLockFree, kSpinLockHeld) != kSpinLockFree) {
      SlowLock();
    }
  }

  inline bool TryLock() {
    bool res =
        (base::subtle::Acquire_CompareAndSwap(
             &lockword_, kSpinLockFree, kSpinLockHeld) == kSpinLockFree);
    return res;
  }

  inline void Unlock() {
    uint64 wait_cycles = static_cast<uint64>(
        base::subtle::Release_AtomicExchange(&lockword_, kSpinLockFree));
    if (wait_cycles != kSpinLockHeld) {
      SlowUnlock(wait_cycles);
    }
  }

 private:
  enum { kSpinLockFree = 0 };
  enum { kSpinLockHeld = 1 };
  enum { kSpinLockSleeper = 2 };
  enum { PROFILE_TIMESTAMP_SHIFT = 7 };

  volatile Atomic32 lockword_;

  void SlowLock() {
    int64 wait_start_time = kudu::CycleClock::Now();
    Atomic32 wait_cycles;
    Atomic32 lock_value = SpinLoop(wait_start_time, &wait_cycles);

    int lock_wait_call_count = 0;
    while (lock_value != kSpinLockFree) {
      if (lock_value == kSpinLockHeld) {
        lock_value = base::subtle::Acquire_CompareAndSwap(
            &lockword_, kSpinLockHeld, kSpinLockSleeper);
        if (lock_value == kSpinLockHeld) {
          lock_value = kSpinLockSleeper;
        } else if (lock_value == kSpinLockFree) {
          lock_value = base::subtle::Acquire_CompareAndSwap(
              &lockword_, kSpinLockFree, wait_cycles);
          continue;
        }
      }

      base::internal::kudu::SpinLockDelay(
          &lockword_, lock_value, ++lock_wait_call_count);
      lock_value = SpinLoop(wait_start_time, &wait_cycles);
    }
  }

  void SlowUnlock(uint64 wait_cycles) {
    base::internal::kudu::SpinLockWake(&lockword_, false);
  }

  Atomic32 SpinLoop(int64 initial_wait_timestamp, Atomic32* wait_cycles) {
    static int adaptive_spin_count = (base::NumCPUs() > 1) ? 1000 : 0;
    int c = adaptive_spin_count;
    while (base::subtle::NoBarrier_Load(&lockword_) != kSpinLockFree &&
           --c > 0) {
      base::subtle::PauseCPU();
    }
    Atomic32 spin_loop_wait_cycles =
        CalculateWaitCycles(initial_wait_timestamp);
    Atomic32 lock_value = base::subtle::Acquire_CompareAndSwap(
        &lockword_, kSpinLockFree, spin_loop_wait_cycles);
    *wait_cycles = spin_loop_wait_cycles;
    return lock_value;
  }

  inline int32 CalculateWaitCycles(int64 wait_start_time) {
    int32 wait_cycles =
        ((kudu::CycleClock::Now() - wait_start_time) >>
         PROFILE_TIMESTAMP_SHIFT);
    wait_cycles |= kSpinLockSleeper;
    return wait_cycles;
  }

  DISALLOW_COPY_AND_ASSIGN(LegacySpinLock);
};

} // namespace legacy

//==============================================================================
// BENCHMARK UTILITIES
//==============================================================================

static void burnCycles(size_t n) {
  for (size_t i = 0; i < n; ++i) {
    folly::doNotOptimizeAway(i);
  }
}

//==============================================================================
// UNCONTENDED BENCHMARKS (single thread)
//==============================================================================

template <typename Lock>
void runUncontended(size_t iters) {
  Lock lock;
  for (size_t i = 0; i < iters; ++i) {
    lock.Lock();
    folly::doNotOptimizeAway(i);
    lock.Unlock();
  }
}

BENCHMARK(LegacySpinLock_Uncontended) {
  runUncontended<legacy::LegacySpinLock>(1000000);
}

BENCHMARK_RELATIVE(FollySpinLock_Uncontended) {
  runUncontended<base::SpinLock>(1000000);
}

BENCHMARK_DRAW_LINE();

//==============================================================================
// HIGH CONTENTION BENCHMARKS (multiple threads, single lock)
//==============================================================================

template <typename Lock>
void runHighContention(size_t numOps, size_t numThreads) {
  folly::BenchmarkSuspender braces;

  Lock lock;
  vector<thread> threads;
  threads.reserve(numThreads);

  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, numOps]() {
      for (size_t op = 0; op < numOps; ++op) {
        lock.Lock();
        burnCycles(FLAGS_work_cycles);
        lock.Unlock();
      }
    });
  }

  braces.dismiss();

  for (auto& t : threads) {
    t.join();
  }
}

void runHighContentionLegacy(size_t numOps, size_t numThreads) {
  runHighContention<legacy::LegacySpinLock>(numOps, numThreads);
}
void runHighContentionFolly(size_t numOps, size_t numThreads) {
  runHighContention<base::SpinLock>(numOps, numThreads);
}

BENCHMARK_NAMED_PARAM(runHighContentionLegacy, Legacy_2threads, 2)
BENCHMARK_RELATIVE_NAMED_PARAM(runHighContentionFolly, Folly_2threads, 2)
BENCHMARK_NAMED_PARAM(runHighContentionLegacy, Legacy_4threads, 4)
BENCHMARK_RELATIVE_NAMED_PARAM(runHighContentionFolly, Folly_4threads, 4)
BENCHMARK_NAMED_PARAM(runHighContentionLegacy, Legacy_8threads, 8)
BENCHMARK_RELATIVE_NAMED_PARAM(runHighContentionFolly, Folly_8threads, 8)

BENCHMARK_DRAW_LINE();

//==============================================================================
// LOW CONTENTION BENCHMARKS (per-thread locks)
//==============================================================================

template <typename Lock>
void runLowContention(size_t numOps, size_t numThreads) {
  folly::BenchmarkSuspender braces;

  vector<thread> threads;
  threads.reserve(numThreads);

  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([numOps]() {
      Lock lock;
      for (size_t op = 0; op < numOps; ++op) {
        lock.Lock();
        burnCycles(FLAGS_work_cycles);
        lock.Unlock();
      }
    });
  }

  braces.dismiss();

  for (auto& t : threads) {
    t.join();
  }
}

void runLowContentionLegacy(size_t numOps, size_t numThreads) {
  runLowContention<legacy::LegacySpinLock>(numOps, numThreads);
}
void runLowContentionFolly(size_t numOps, size_t numThreads) {
  runLowContention<base::SpinLock>(numOps, numThreads);
}

BENCHMARK_NAMED_PARAM(runLowContentionLegacy, Legacy_2threads, 2)
BENCHMARK_RELATIVE_NAMED_PARAM(runLowContentionFolly, Folly_2threads, 2)
BENCHMARK_NAMED_PARAM(runLowContentionLegacy, Legacy_4threads, 4)
BENCHMARK_RELATIVE_NAMED_PARAM(runLowContentionFolly, Folly_4threads, 4)
BENCHMARK_NAMED_PARAM(runLowContentionLegacy, Legacy_8threads, 8)
BENCHMARK_RELATIVE_NAMED_PARAM(runLowContentionFolly, Folly_8threads, 8)

} // namespace kudu

/**
============================================================================
fbcode/kudu/gutil/spinlock-bench.cc     relative  time/iter   iters/s
============================================================================
LegacySpinLock_Uncontended                                 13.53ms     73.94
FollySpinLock_Uncontended                       200.10%     6.76ms    147.94
----------------------------------------------------------------------------
runHighContentionLegacy(Legacy_2threads)                   49.68ns    20.13M
runHighContentionFolly(Folly_2threads)          96.201%    51.65ns    19.36M
runHighContentionLegacy(Legacy_4threads)                   80.76ns    12.38M
runHighContentionFolly(Folly_4threads)          82.558%    97.82ns    10.22M
runHighContentionLegacy(Legacy_8threads)                  123.56ns     8.09M
runHighContentionFolly(Folly_8threads)          63.164%   195.62ns     5.11M
----------------------------------------------------------------------------
runLowContentionLegacy(Legacy_2threads)                    28.39ns    35.22M
runLowContentionFolly(Folly_2threads)           99.113%    28.65ns    34.91M
runLowContentionLegacy(Legacy_4threads)                    29.54ns    33.86M
runLowContentionFolly(Folly_4threads)           102.61%    28.79ns    34.74M
runLowContentionLegacy(Legacy_8threads)                    36.23ns    27.60M
runLowContentionFolly(Folly_8threads)           102.86%    35.22ns    28.39M
*/
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
