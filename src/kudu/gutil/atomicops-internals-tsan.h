// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This file is an internal atomic implementation for compiler-based
// ThreadSanitizer. Use base/atomicops.h instead.
// IWYU pragma: private, include "kudu/gutil/atomicops.h"

#pragma once

#include <sanitizer/tsan_interface_atomic.h>

using Atomic32 = int32_t;
using Atomic64 = int64_t;

namespace base {
namespace subtle {

using Atomic32 = int32_t;
using Atomic64 = int64_t;

inline Atomic32 NoBarrier_CompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  Atomic32 cmp = oldValue;
  __tsan_atomic32_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_relaxed,
      __tsan_memory_order_relaxed);
  return cmp;
}

inline Atomic32 NoBarrier_AtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  return __tsan_atomic32_exchange(ptr, newValue, __tsan_memory_order_relaxed);
}

inline Atomic32 Acquire_AtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  return __tsan_atomic32_exchange(ptr, newValue, __tsan_memory_order_acquire);
}

inline Atomic32 Release_AtomicExchange(
    volatile Atomic32* ptr,
    Atomic32 newValue) {
  return __tsan_atomic32_exchange(ptr, newValue, __tsan_memory_order_release);
}

inline Atomic32 NoBarrier_AtomicIncrement(
    volatile Atomic32* ptr,
    Atomic32 increment) {
  return increment +
      __tsan_atomic32_fetch_add(ptr, increment, __tsan_memory_order_relaxed);
}

inline Atomic32 Barrier_AtomicIncrement(
    volatile Atomic32* ptr,
    Atomic32 increment) {
  return increment +
      __tsan_atomic32_fetch_add(ptr, increment, __tsan_memory_order_acq_rel);
}

inline Atomic32 Acquire_CompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  Atomic32 cmp = oldValue;
  __tsan_atomic32_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_acquire,
      __tsan_memory_order_acquire);
  return cmp;
}

inline Atomic32 Release_CompareAndSwap(
    volatile Atomic32* ptr,
    Atomic32 oldValue,
    Atomic32 newValue) {
  Atomic32 cmp = oldValue;
  __tsan_atomic32_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_release,
      __tsan_memory_order_relaxed);
  return cmp;
}

inline void NoBarrier_Store(volatile Atomic32* ptr, Atomic32 value) {
  __tsan_atomic32_store(ptr, value, __tsan_memory_order_relaxed);
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  __tsan_atomic32_store(ptr, value, __tsan_memory_order_relaxed);
  __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);
}

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
  __tsan_atomic32_store(ptr, value, __tsan_memory_order_release);
}

inline Atomic32 NoBarrier_Load(volatile const Atomic32* ptr) {
  return __tsan_atomic32_load(ptr, __tsan_memory_order_relaxed);
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
  return __tsan_atomic32_load(ptr, __tsan_memory_order_acquire);
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
  __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);
  return __tsan_atomic32_load(ptr, __tsan_memory_order_relaxed);
}

inline Atomic64 NoBarrier_CompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  Atomic64 cmp = oldValue;
  __tsan_atomic64_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_relaxed,
      __tsan_memory_order_relaxed);
  return cmp;
}

inline Atomic64 NoBarrier_AtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  return __tsan_atomic64_exchange(ptr, newValue, __tsan_memory_order_relaxed);
}

inline Atomic64 Acquire_AtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  return __tsan_atomic64_exchange(ptr, newValue, __tsan_memory_order_acquire);
}

inline Atomic64 Release_AtomicExchange(
    volatile Atomic64* ptr,
    Atomic64 newValue) {
  return __tsan_atomic64_exchange(ptr, newValue, __tsan_memory_order_release);
}

inline Atomic64 NoBarrier_AtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  return increment +
      __tsan_atomic64_fetch_add(ptr, increment, __tsan_memory_order_relaxed);
}

inline Atomic64 Barrier_AtomicIncrement(
    volatile Atomic64* ptr,
    Atomic64 increment) {
  return increment +
      __tsan_atomic64_fetch_add(ptr, increment, __tsan_memory_order_acq_rel);
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  __tsan_atomic64_store(ptr, value, __tsan_memory_order_relaxed);
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  __tsan_atomic64_store(ptr, value, __tsan_memory_order_relaxed);
  __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  __tsan_atomic64_store(ptr, value, __tsan_memory_order_release);
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  return __tsan_atomic64_load(ptr, __tsan_memory_order_relaxed);
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  return __tsan_atomic64_load(ptr, __tsan_memory_order_acquire);
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);
  return __tsan_atomic64_load(ptr, __tsan_memory_order_relaxed);
}

inline Atomic64 Acquire_CompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  Atomic64 cmp = oldValue;
  __tsan_atomic64_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_acquire,
      __tsan_memory_order_acquire);
  return cmp;
}

inline Atomic64 Release_CompareAndSwap(
    volatile Atomic64* ptr,
    Atomic64 oldValue,
    Atomic64 newValue) {
  Atomic64 cmp = oldValue;
  __tsan_atomic64_compare_exchange_strong(
      ptr,
      &cmp,
      newValue,
      __tsan_memory_order_release,
      __tsan_memory_order_relaxed);
  return cmp;
}

inline void MemoryBarrier() {
  __tsan_atomic_thread_fence(__tsan_memory_order_seq_cst);
}

inline void PauseCPU() {}

} // namespace subtle
} // namespace base
