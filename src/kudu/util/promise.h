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
#ifndef KUDU_UTIL_PROMISE_H
#define KUDU_UTIL_PROMISE_H

#include "kudu/gutil/macros.h"
#include "kudu/util/countdown_latch.h"

namespace kudu {

// A promise boxes a value which is to be provided at some time in the future.
// A single producer calls set(...), and any number of consumers can call get()
// to retrieve the produced value.
//
// In Guava terms, this is a SettableFuture<T>.
template <typename T>
class Promise {
 public:
  Promise() : latch_(1) {}
  ~Promise() {}

  // Block until a value is available, and return a reference to it.
  const T& get() const {
    latch_.wait();
    return val_;
  }

  // Wait for the promised value to become available with the given timeout.
  //
  // Returns NULL if the timeout elapses before a value is available.
  // Otherwise returns a pointer to the value. This pointer's lifetime is
  // tied to the lifetime of the Promise object.
  const T* waitFor(const MonoDelta& delta) const {
    if (latch_.waitFor(delta)) {
      return &val_;
    } else {
      return NULL;
    }
  }

  // Set the value of this promise.
  // This may be called at most once.
  void set(const T& val) {
    DCHECK_EQ(latch_.count(), 1) << "Already set!";
    val_ = val;
    latch_.countDown();
  }

 private:
  CountDownLatch latch_;
  T val_;
  DISALLOW_COPY_AND_ASSIGN(Promise);
};

} // namespace kudu
#endif /* KUDU_UTIL_PROMISE_H */
