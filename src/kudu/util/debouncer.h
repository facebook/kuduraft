// Copyright (c) Meta Platforms, Inc. and affiliates.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>

namespace kudu {
/**
 * A debouncer is something that collates extra calls into a single call.
 *
 * Essentially, a debouncer allows a single (executing) entity to obtain a lock,
 * and a single additional (waiting) entity to wait for the lock. Additional
 * waiters (duplicates) will be rejected.
 *
 * This debouncer can thus be used to guarantee that one round of execution
 * always occurs after the some signal, but prevents duplicate execution from
 * multiple signals in a short time.
 *
 * This class can be templated with a mutex type which the executing entity will
 * hold and the waiting entity will block on. Duplicates are rejected based on a
 * atomic state flag.
 *
 * Note that this function satisfies the C++ Lockable requirement from a API
 * perspective only. It provides a try_lock function, but try_lock just attempts
 * to become the waiting entity, and will block while waiting to be the
 * executing entity. The original Lockable contract asserts that try_lock is
 * non-blocking, but that's not true here.
 *
 * As such, we can use it with RAII holders like std::scoped_lock, but blocking
 * vs non-blocking behaviour may be different.
 */
template <class Mutex>
class Debouncer {
 public:
  Debouncer() {}

  // Uncopyable type
  Debouncer(const Debouncer&) = delete;
  Debouncer& operator=(const Debouncer&) = delete;

  /**
   * Tries to wait for the executing mutex.
   *
   * If nothing is executing, we'll lock the mutex and return.
   *
   * If something is executing, then this function blocks on the executing mutex
   * until it's released, then return true.
   *
   * If something is executing as well as waiting, this function returns false,
   * signifiying that the request to execute has been deduped and denied.
   *
   * @return true if we are now the executing entity
   */
  bool try_lock() {
    bool expectedWaitingToken = false;
    if (!waitingToken_.compare_exchange_strong(expectedWaitingToken, true)) {
      return false;
    }

    executingMutex_.lock();
    waitingToken_ = false;
    return true;
  }

  /**
   * Releases the executing mutex, allowing the waiting entity to become an
   * executing entity.
   */
  void unlock() {
    executingMutex_.unlock();
  }

 private:
  /**
   * The mutex the the executing entity holds.
   */
  Mutex executingMutex_;
  /**
   * The flag that denotes is there's a waiting entity.
   */
  std::atomic_bool waitingToken_ = false;
};

/**
 * Debouncer using a standard std::mutex as the executing mutex.
 */
using MutexDebouncer = Debouncer<std::mutex>;

} // namespace kudu
