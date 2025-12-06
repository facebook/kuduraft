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
#pragma once

#include <stddef.h>

#include <mutex>

#include "kudu/gutil/port.h"
#include "kudu/util/atomic.h"
#include "kudu/util/status.h"

namespace kudu {

// Similar to std::call_once, but returns Status from the initialization
// function. The status is cached and returned on subsequent calls.
//
// Example usage:
//   class MyClass {
//     KuduOnceLambda init_once_;
//
//     Status LazyInit() {
//       return init_once_.Init([this]() {
//         // Initialization that might fail
//         RETURN_NOT_OK(SomeSetup());
//         return Status::OK();
//       });
//     }
//   };
class KuduOnceLambda {
 public:
  KuduOnceLambda() : init_succeeded_(false) {}

  // If the underlying `once_flag` has yet to be invoked, invokes the provided
  // lambda and stores its return value. Otherwise, returns the stored Status.
  template <typename Fn>
  Status Init(Fn fn) {
    std::call_once(once_flag_, [this, fn] {
      status_ = fn();
      if (PREDICT_TRUE(status_.ok())) {
        init_succeeded_.Store(true, kMemOrderRelease);
      }
    });
    return status_;
  }

  // Similar to KuduOnceDynamic, kMemOrderAcquire here and kMemOrderRelease in
  // Init(), taken together, mean that threads can safely synchronize on
  // ini_succeeded_.
  bool init_succeeded() const {
    return init_succeeded_.Load(kMemOrderAcquire);
  }

  // Returns the memory usage of this object without the object itself. Should
  // be used when embedded inside another object.
  size_t memory_footprint_excluding_this() const;

  // Returns the memory usage of this object including the object itself.
  // Should be used when allocated on the heap.
  size_t memory_footprint_including_this() const;

 private:
  AtomicBool init_succeeded_;
  std::once_flag once_flag_;
  Status status_;
};

} // namespace kudu
