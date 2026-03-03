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

#ifndef KUDU_UTIL_ATOMIC_H
#define KUDU_UTIL_ATOMIC_H

#include <algorithm> // IWYU pragma: keep
#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"

namespace kudu {

// See top-level comments in kudu/gutil/atomicops.h for further
// explanations of these levels.
enum MemoryOrder {
  // Relaxed memory ordering, doesn't use any barriers.
  kMemOrderNoBarrier = 0,

  // Ensures that no later memory access by the same thread can be
  // reordered ahead of the operation.
  kMemOrderAcquire = 1,

  // Ensures that no previous memory access by the same thread can be
  // reordered after the operation.
  kMemOrderRelease = 2,

  // Ensures that neither previous NOR later memory access by the same
  // thread can be reordered after the operation.
  kMemOrderBarrier = 3,
};

// Atomic integer class inspired by Impala's AtomicInt and
// std::atomic<> in C++11.
//
// NOTE: All of public operations use an implicit memory order of
// kMemOrderNoBarrier unless otherwise specified.
//
// Unlike std::atomic<>, overflowing an unsigned AtomicInt via Increment or
// IncrementBy is undefined behavior (it is also undefined for signed types,
// as always).
//
// See also: kudu/gutil/atomicops.h
template <typename T>
class AtomicInt {
 public:
  // Initialize the underlying value to 'initialValue'. The
  // initialization performs a Store with 'kMemOrderNoBarrier'.
  explicit AtomicInt(T initialValue);

  // Returns the underlying value.
  //
  // Does not support 'kMemOrderBarrier'.
  T load(MemoryOrder memOrder = kMemOrderNoBarrier) const;

  // Sets the underlying value to 'newValue'.
  //
  // Does not support 'kMemOrderBarrier'.
  void store(T newValue, MemoryOrder memOrder = kMemOrderNoBarrier);

  // Iff the underlying value is equal to 'expectedVal', sets the
  // underlying value to 'newValue' and returns true; returns false
  // otherwise.
  //
  // Does not support 'kMemOrderBarrier'.
  bool compareAndSet(
      T expectedVal,
      T newValue,
      MemoryOrder memOrder = kMemOrderNoBarrier);

  // Iff the underlying value is equal to 'expectedVal', sets the
  // underlying value to 'newValue' and returns
  // 'expectedVal'. Otherwise, returns the current underlying
  // value.
  //
  // Does not support 'kMemOrderBarrier'.
  T compareAndSwap(
      T expectedVal,
      T newValue,
      MemoryOrder memOrder = kMemOrderNoBarrier);

  // Sets the underlying value to 'newValue' iff 'newValue' is
  // greater than the current underlying value.
  //
  // Does not support 'kMemOrderBarrier'.
  void storeMax(T newValue, MemoryOrder memOrder = kMemOrderNoBarrier);

  // Sets the underlying value to 'newValue' iff 'newValue' is less
  // than the current underlying value.
  //
  // Does not support 'kMemOrderBarrier'.
  void storeMin(T newValue, MemoryOrder memOrder = kMemOrderNoBarrier);

  // Increments the underlying value by 1 and returns the new
  // underlying value.
  //
  // Does not support 'kMemOrderAcquire' or 'kMemOrderRelease'.
  T increment(MemoryOrder memOrder = kMemOrderNoBarrier);

  // Increments the underlying value by 'delta' and returns the new
  // underlying value.

  // Does not support 'kKemOrderAcquire' or 'kMemOrderRelease'.
  T incrementBy(T delta, MemoryOrder memOrder = kMemOrderNoBarrier);

  // Sets the underlying value to 'newValue' and returns the previous
  // underlying value.
  //
  // Does not support 'kMemOrderBarrier'.
  T exchange(T newValue, MemoryOrder memOrder = kMemOrderNoBarrier);

  ~AtomicInt() = default;
  AtomicInt(const AtomicInt&) = delete;
  AtomicInt& operator=(const AtomicInt&) = delete;
  AtomicInt(AtomicInt&&) = delete;
  AtomicInt& operator=(AtomicInt&&) = delete;

 private:
  // If a method 'caller' doesn't support memory order described as
  // 'requested', exit by doing perform LOG(FATAL) logging the method
  // called, the requested memory order, and the supported memory
  // orders.
  static void fatalMemOrderNotSupported(
      const char* caller,
      const char* requested = "kMemOrderBarrier",
      const char* supported =
          "kMemNorderNoBarrier, kMemOrderAcquire, kMemOrderRelease");

  // The gutil/atomicops.h functions only operate on signed types.
  // So, even if the user specializes on an unsigned type, we use a
  // signed type internally.
  using SignedT = typename std::make_signed<T>::type;
  SignedT value_;
};

// Adapts AtomicInt to handle boolean values.
//
// NOTE: All of public operations use an implicit memory order of
// kMemOrderNoBarrier unless otherwise specified.
//
// See AtomicInt above for documentation on individual methods.
class AtomicBool {
 public:
  explicit AtomicBool(bool value);

  bool load(MemoryOrder m = kMemOrderNoBarrier) const {
    return underlying_.load(m);
  }
  void store(bool n, MemoryOrder m = kMemOrderNoBarrier) {
    underlying_.store(static_cast<int32_t>(n), m);
  }
  bool compareAndSet(bool e, bool n, MemoryOrder m = kMemOrderNoBarrier) {
    return underlying_.compareAndSet(
        static_cast<int32_t>(e), static_cast<int32_t>(n), m);
  }
  bool compareAndSwap(bool e, bool n, MemoryOrder m = kMemOrderNoBarrier) {
    return underlying_.compareAndSwap(
        static_cast<int32_t>(e), static_cast<int32_t>(n), m);
  }
  bool exchange(bool n, MemoryOrder m = kMemOrderNoBarrier) {
    return underlying_.exchange(static_cast<int32_t>(n), m);
  }

  ~AtomicBool() = default;
  AtomicBool(const AtomicBool&) = delete;
  AtomicBool& operator=(const AtomicBool&) = delete;
  AtomicBool(AtomicBool&&) = delete;
  AtomicBool& operator=(AtomicBool&&) = delete;

 private:
  AtomicInt<int32_t> underlying_;
};

template <typename T>
inline T AtomicInt<T>::load(MemoryOrder memOrder) const {
  switch (memOrder) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_Load(&value_);
    }
    case kMemOrderBarrier: {
      fatalMemOrderNotSupported("load");
      break;
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_Load(&value_);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_Load(&value_);
    }
  }
  abort(); // Unnecessary, but avoids gcc complaining.
}

template <typename T>
inline void AtomicInt<T>::store(T newValue, MemoryOrder memOrder) {
  switch (memOrder) {
    case kMemOrderNoBarrier: {
      base::subtle::NoBarrier_Store(&value_, newValue);
      break;
    }
    case kMemOrderBarrier: {
      fatalMemOrderNotSupported("store");
      break;
    }
    case kMemOrderAcquire: {
      base::subtle::Acquire_Store(&value_, newValue);
      break;
    }
    case kMemOrderRelease: {
      base::subtle::Release_Store(&value_, newValue);
      break;
    }
  }
}

template <typename T>
inline bool
AtomicInt<T>::compareAndSet(T expectedVal, T newVal, MemoryOrder memOrder) {
  return compareAndSwap(expectedVal, newVal, memOrder) == expectedVal;
}

template <typename T>
inline T
AtomicInt<T>::compareAndSwap(T expectedVal, T newVal, MemoryOrder memOrder) {
  switch (memOrder) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_CompareAndSwap(
          &value_, expectedVal, newVal);
    }
    case kMemOrderBarrier: {
      fatalMemOrderNotSupported("compareAndSwap/compareAndSet");
      break;
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_CompareAndSwap(&value_, expectedVal, newVal);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_CompareAndSwap(&value_, expectedVal, newVal);
    }
  }
  abort();
}

template <typename T>
inline T AtomicInt<T>::increment(MemoryOrder memOrder) {
  return incrementBy(1, memOrder);
}

template <typename T>
inline T AtomicInt<T>::incrementBy(T delta, MemoryOrder memOrder) {
  switch (memOrder) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_AtomicIncrement(&value_, delta);
    }
    case kMemOrderBarrier: {
      return base::subtle::Barrier_AtomicIncrement(&value_, delta);
    }
    case kMemOrderAcquire: {
      fatalMemOrderNotSupported(
          "increment/incrementBy",
          "kMemOrderAcquire",
          "kMemOrderNoBarrier and kMemOrderBarrier");
      break;
    }
    case kMemOrderRelease: {
      fatalMemOrderNotSupported(
          "increment/incrementBy",
          "kMemOrderAcquire",
          "kMemOrderNoBarrier and kMemOrderBarrier");
      break;
    }
  }
  abort();
}

template <typename T>
inline T AtomicInt<T>::exchange(T newValue, MemoryOrder memOrder) {
  switch (memOrder) {
    case kMemOrderNoBarrier: {
      return base::subtle::NoBarrier_AtomicExchange(&value_, newValue);
    }
    case kMemOrderBarrier: {
      fatalMemOrderNotSupported("exchange");
      break;
    }
    case kMemOrderAcquire: {
      return base::subtle::Acquire_AtomicExchange(&value_, newValue);
    }
    case kMemOrderRelease: {
      return base::subtle::Release_AtomicExchange(&value_, newValue);
    }
  }
  abort();
}

template <typename T>
inline void AtomicInt<T>::storeMax(T newValue, MemoryOrder memOrder) {
  T oldValue = load(memOrder);
  while (true) {
    T maxValue = std::max(oldValue, newValue);
    T prevValue = compareAndSwap(oldValue, maxValue, memOrder);
    if (PREDICT_TRUE(oldValue == prevValue)) {
      break;
    }
    oldValue = prevValue;
  }
}

template <typename T>
inline void AtomicInt<T>::storeMin(T newValue, MemoryOrder memOrder) {
  T oldValue = load(memOrder);
  while (true) {
    T minValue = std::min(oldValue, newValue);
    T prevValue = compareAndSwap(oldValue, minValue, memOrder);
    if (PREDICT_TRUE(oldValue == prevValue)) {
      break;
    }
    oldValue = prevValue;
  }
}

} // namespace kudu
#endif /* KUDU_UTIL_ATOMIC_H */
