// Copyright 2003 Google Inc.
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
// All Rights Reserved.
//
// Implementation of atomic operations for x86.  This file should not
// be included directly.  Clients should instead include
// "base/atomicops.h".
// IWYU pragma: private, include "kudu/gutil/atomicops.h"
#pragma once

#include <cstdint>
#include <ostream>

#include <glog/logging.h>

// NOTE(user): x86 does not need to define AtomicWordCastType, because it
// already matches Atomic32 or Atomic64, depending on the platform.

// This struct is not part of the public API of this module; clients may not
// use it.
// Features of this x86.
struct AtomicOpsX86CpuFeatureStruct {
  bool hasSse2; // Processor has SSE2.
  bool hasCmpxchg16b; // Processor supports cmpxchg16b instruction.
};
extern struct AtomicOpsX86CpuFeatureStruct atomicOpsInternalX86CpuFeatures;

#define ATOMICOPS_COMPILER_BARRIER() __asm__ __volatile__("" : : : "memory")

using Atomic32 = int32_t;
using Atomic64 = int64_t;

namespace base {
namespace subtle {

using Atomic32 = int32_t;
using Atomic64 = int64_t;

// These atomic primitives don't work atomically, and can cause really nasty
// hard-to-track-down bugs, if the pointer isn't naturally aligned. Check
// alignment in debug mode.
template <class T>
inline void checkNaturalAlignment(const T* ptr) {
  DCHECK_EQ(0, reinterpret_cast<const uintptr_t>(ptr) & (sizeof(T) - 1))
      << "unaligned pointer not allowed for atomics";
}

// 32-bit low-level operations on any platform.

inline Atomic32 noBarrierCompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  checkNaturalAlignment(ptr);
  Atomic32 prev;
  __asm__ __volatile__("lock; cmpxchgl %1,%2"
                       : "=a"(prev)
                       : "q"(newValue), "m"(*ptr), "0"(oldValue)
                       : "memory");
  return prev;
}

inline Atomic32 noBarrierAtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  checkNaturalAlignment(ptr);
  __asm__ __volatile__("xchgl %1,%0" // The lock prefix is implicit for xchg.
                       : "=r"(newValue)
                       : "m"(*ptr), "0"(newValue)
                       : "memory");
  return newValue; // Now it's the previous value.
}

inline Atomic32 acquireAtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  checkNaturalAlignment(ptr);
  Atomic32 oldVal = noBarrierAtomicExchange(ptr, newValue);
  return oldVal;
}

inline Atomic32 releaseAtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  return noBarrierAtomicExchange(ptr, newValue);
}

inline Atomic32 noBarrierAtomicIncrement(
    volatile Atomic32* ptr,
    Atomic32 increment) {
  checkNaturalAlignment(ptr);
  Atomic32 temp = increment;
  __asm__ __volatile__("lock; xaddl %0,%1"
                       : "+r"(temp), "+m"(*ptr)
                       :
                       : "memory");
  // temp now holds the old value of *ptr
  return temp + increment;
}

inline Atomic32 barrierAtomicIncrement(
    volatile Atomic32* ptr,
    Atomic32 increment) {
  checkNaturalAlignment(ptr);
  Atomic32 temp = increment;
  __asm__ __volatile__("lock; xaddl %0,%1"
                       : "+r"(temp), "+m"(*ptr)
                       :
                       : "memory");
  // temp now holds the old value of *ptr
  return temp + increment;
}

inline Atomic32 Acquire_CompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  Atomic32 x = noBarrierCompareAndSwap(ptr, oldValue, newValue);
  return x;
}

inline Atomic32 Release_CompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  return noBarrierCompareAndSwap(ptr, oldValue, newValue);
}

inline void noBarrierStore(volatile Atomic32* ptr, Atomic32 value) {
  checkNaturalAlignment(ptr);
  *ptr = value;
}

// Issue the x86 "pause" instruction, which tells the CPU that we
// are in a spinlock wait loop and should allow other hyperthreads
// to run, not speculate memory access, etc.
inline void pauseCpu() {
  __asm__ __volatile__("pause" : : : "memory");
}

#if defined(__x86_64__)

// 64-bit implementations of memory barrier can be simpler, because it
// "mfence" is guaranteed to exist.
inline void MemoryBarrier() {
  __asm__ __volatile__("mfence" : : : "memory");
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  checkNaturalAlignment(ptr);
  *ptr = value;
  MemoryBarrier();
}

#else

inline void MemoryBarrier() {
  if (atomicOpsInternalX86CpuFeatures.hasSse2) {
    __asm__ __volatile__("mfence" : : : "memory");
  } else { // mfence is faster but not present on PIII
    Atomic32 x = 0;
    acquireAtomicExchange(&x, 0);
  }
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  if (atomicOpsInternalX86CpuFeatures.hasSse2) {
    checkNaturalAlignment(ptr);
    *ptr = value;
    __asm__ __volatile__("mfence" : : : "memory");
  } else {
    acquireAtomicExchange(ptr, value);
  }
}
#endif

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
  checkNaturalAlignment(ptr);
  ATOMICOPS_COMPILER_BARRIER();
  *ptr = value; // An x86 store acts as a release barrier.
  // See comments in Atomic64 version of Release_Store(), below.
}

inline Atomic32 noBarrierLoad(volatile const Atomic32* ptr) {
  checkNaturalAlignment(ptr);
  return *ptr;
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
  checkNaturalAlignment(ptr);
  Atomic32 value = *ptr; // An x86 load acts as a acquire barrier.
  // See comments in Atomic64 version of Release_Store(), below.
  ATOMICOPS_COMPILER_BARRIER();
  return value;
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
  checkNaturalAlignment(ptr);
  MemoryBarrier();
  return *ptr;
}

#if defined(__x86_64__)

// 64-bit low-level operations on 64-bit platform.

inline Atomic64 noBarrierCompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  Atomic64 prev;
  checkNaturalAlignment(ptr);
  __asm__ __volatile__("lock; cmpxchgq %1,%2"
                       : "=a"(prev)
                       : "q"(newValue), "m"(*ptr), "0"(oldValue)
                       : "memory");
  return prev;
}

inline Atomic64 noBarrierAtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  checkNaturalAlignment(ptr);
  __asm__ __volatile__("xchgq %1,%0" // The lock prefix is implicit for xchg.
                       : "=r"(newValue)
                       : "m"(*ptr), "0"(newValue)
                       : "memory");
  return newValue; // Now it's the previous value.
}

inline Atomic64 acquireAtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  Atomic64 oldVal = noBarrierAtomicExchange(ptr, newValue);
  return oldVal;
}

inline Atomic64 releaseAtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  return noBarrierAtomicExchange(ptr, newValue);
}

inline Atomic64 noBarrierAtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  Atomic64 temp = increment;
  checkNaturalAlignment(ptr);
  __asm__ __volatile__("lock; xaddq %0,%1"
                       : "+r"(temp), "+m"(*ptr)
                       :
                       : "memory");
  // temp now contains the previous value of *ptr
  return temp + increment;
}

inline Atomic64 barrierAtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  Atomic64 temp = increment;
  checkNaturalAlignment(ptr);
  __asm__ __volatile__("lock; xaddq %0,%1"
                       : "+r"(temp), "+m"(*ptr)
                       :
                       : "memory");
  // temp now contains the previous value of *ptr
  return temp + increment;
}

inline void noBarrierStore(volatile Atomic64* ptr, Atomic64 value) {
  checkNaturalAlignment(ptr);
  *ptr = value;
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  checkNaturalAlignment(ptr);
  *ptr = value;
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  ATOMICOPS_COMPILER_BARRIER();
  checkNaturalAlignment(ptr);
  *ptr = value; // An x86 store acts as a release barrier
                // for current AMD/Intel chips as of Jan 2008.
                // See also Acquire_Load(), below.

  // When new chips come out, check:
  //  IA-32 Intel Architecture Software Developer's Manual, Volume 3:
  //  System Programming Guide, Chatper 7: Multiple-processor management,
  //  Section 7.2, Memory Ordering.
  // Last seen at:
  //   http://developer.intel.com/design/pentium4/manuals/index_new.htm
  //
  // x86 stores/loads fail to act as barriers for a few instructions (clflush
  // maskmovdqu maskmovq movntdq movnti movntpd movntps movntq) but these are
  // not generated by the compiler, and are rare.  Users of these instructions
  // need to know about cache behaviour in any case since all of these involve
  // either flushing cache lines or non-temporal cache hints.
}

inline Atomic64 noBarrierLoad(volatile const Atomic64* ptr) {
  checkNaturalAlignment(ptr);
  return *ptr;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  checkNaturalAlignment(ptr);
  Atomic64 value = *ptr; // An x86 load acts as a acquire barrier,
                         // for current AMD/Intel chips as of Jan 2008.
                         // See also Release_Store(), above.
  ATOMICOPS_COMPILER_BARRIER();
  return value;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  checkNaturalAlignment(ptr);
  MemoryBarrier();
  return *ptr;
}

#else // defined(__x86_64__)

// 64-bit low-level operations on 32-bit platform.

#if !((__GNUC__ > 4) || (__GNUC__ == 4 && __GNUC_MINOR__ >= 1))
// For compilers older than gcc 4.1, we use inline asm.
//
// Potential pitfalls:
//
// 1. %ebx points to Global offset table (GOT) with -fPIC.
//    We need to preserve this register.
// 2. When explicit registers are used in inline asm, the
//    compiler may not be aware of it and might try to reuse
//    the same register for another argument which has constraints
//    that allow it ("r" for example).

inline Atomic64 __sync_val_compare_and_swap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  checkNaturalAlignment(ptr);
  Atomic64 prev;
  __asm__ __volatile__(
      "push %%ebx\n\t"
      "movl (%3), %%ebx\n\t" // Move 64-bit newValue into
      "movl 4(%3), %%ecx\n\t" // ecx:ebx
      "lock; cmpxchg8b (%1)\n\t" // If edx:eax (oldValue) same
      "pop %%ebx\n\t"
      : "=A"(prev) // as contents of ptr:
      : "D"(ptr), //   ecx:ebx => ptr
        "0"(oldValue), // else:
        "S"(&newValue) //   old *ptr => edx:eax
      : "memory", "%ecx");
  return prev;
}
#endif // Compiler < gcc-4.1

inline Atomic64 noBarrierCompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldVal,
    Atomic64 newVal) {
  checkNaturalAlignment(ptr);
  return __sync_val_compare_and_swap(ptr, oldVal, newVal);
}

inline Atomic64 noBarrierAtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newVal) {
  Atomic64 oldVal;
  checkNaturalAlignment(ptr);

  do {
    oldVal = *ptr;
  } while (__sync_val_compare_and_swap(ptr, oldVal, newVal) != oldVal);

  return oldVal;
}

inline Atomic64 acquireAtomicExchange(volatile Atomic64* ptr, Atomic64 newVal) {
  checkNaturalAlignment(ptr);
  Atomic64 oldVal = noBarrierAtomicExchange(ptr, newVal);
  return oldVal;
}

inline Atomic64 releaseAtomicExchange(volatile Atomic64* ptr, Atomic64 newVal) {
  return noBarrierAtomicExchange(ptr, newVal);
}

inline Atomic64 noBarrierAtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  checkNaturalAlignment(ptr);
  Atomic64 oldVal, newVal;

  do {
    oldVal = *ptr;
    newVal = oldVal + increment;
  } while (__sync_val_compare_and_swap(ptr, oldVal, newVal) != oldVal);

  return oldVal + increment;
}

inline Atomic64 barrierAtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  checkNaturalAlignment(ptr);
  Atomic64 newVal = noBarrierAtomicIncrement(ptr, increment);
  return newVal;
}

inline void noBarrierStore(volatile Atomic64* ptr, Atomic64 value) {
  checkNaturalAlignment(ptr);
  __asm__ __volatile__(
      "movq %1, %%mm0\n\t" // Use mmx reg for 64-bit atomic
      "movq %%mm0, %0\n\t" // moves (ptr could be read-only)
      "emms\n\t" // Empty mmx state/Reset FP regs
      : "=m"(*ptr)
      : "m"(value)
      : // mark the FP stack and mmx registers as clobbered
      "st",
      "st(1)",
      "st(2)",
      "st(3)",
      "st(4)",
      "st(5)",
      "st(6)",
      "st(7)",
      "mm0",
      "mm1",
      "mm2",
      "mm3",
      "mm4",
      "mm5",
      "mm6",
      "mm7");
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  noBarrierStore(ptr, value);
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  ATOMICOPS_COMPILER_BARRIER();
  noBarrierStore(ptr, value);
}

inline Atomic64 noBarrierLoad(volatile const Atomic64* ptr) {
  checkNaturalAlignment(ptr);
  Atomic64 value;
  __asm__ __volatile__(
      "movq %1, %%mm0\n\t" // Use mmx reg for 64-bit atomic
      "movq %%mm0, %0\n\t" // moves (ptr could be read-only)
      "emms\n\t" // Empty mmx state/Reset FP regs
      : "=m"(value)
      : "m"(*ptr)
      : // mark the FP stack and mmx registers as clobbered
      "st",
      "st(1)",
      "st(2)",
      "st(3)",
      "st(4)",
      "st(5)",
      "st(6)",
      "st(7)",
      "mm0",
      "mm1",
      "mm2",
      "mm3",
      "mm4",
      "mm5",
      "mm6",
      "mm7");
  return value;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  checkNaturalAlignment(ptr);
  Atomic64 value = noBarrierLoad(ptr);
  ATOMICOPS_COMPILER_BARRIER();
  return value;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  MemoryBarrier();
  return noBarrierLoad(ptr);
}

#endif // defined(__x86_64__)

inline Atomic64 Acquire_CompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  Atomic64 x = noBarrierCompareAndSwap(ptr, oldValue, newValue);
  return x;
}

inline Atomic64 Release_CompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  return noBarrierCompareAndSwap(ptr, oldValue, newValue);
}

} // namespace subtle
} // namespace base

#undef ATOMICOPS_COMPILER_BARRIER
