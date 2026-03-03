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
#ifndef KUDU_UTIL_HIGH_WATER_MARK_H
#define KUDU_UTIL_HIGH_WATER_MARK_H

#include "kudu/gutil/macros.h"
#include "kudu/util/atomic.h"

namespace kudu {

// Lock-free integer that keeps track of the highest value seen.
// Similar to Impala's RuntimeProfile::HighWaterMarkCounter.
// HighWaterMark::maxValue() returns the highest value seen;
// HighWaterMark::currentValue() returns the current value.
class HighWaterMark {
 public:
  explicit HighWaterMark(int64_t initialValue)
      : currentValue_(initialValue), maxValue_(initialValue) {}

  // Return the current value.
  int64_t currentValue() const {
    return currentValue_.load(kMemOrderNoBarrier);
  }

  // Return the max value.
  int64_t maxValue() const {
    return maxValue_.load(kMemOrderNoBarrier);
  }

  // If current value + 'delta' is <= 'max', increment current value
  // by 'delta' and return true; return false otherwise.
  bool tryIncrementBy(int64_t delta, int64_t max) {
    while (true) {
      int64_t oldVal = currentValue();
      int64_t newVal = oldVal + delta;
      if (newVal > max) {
        return false;
      }
      if (PREDICT_TRUE(currentValue_.compareAndSet(
              oldVal, newVal, kMemOrderNoBarrier))) {
        updateMax(newVal);
        return true;
      }
    }
  }

  void incrementBy(int64_t amount) {
    updateMax(currentValue_.incrementBy(amount, kMemOrderNoBarrier));
  }

 private:
  void updateMax(int64_t value) {
    maxValue_.storeMax(value, kMemOrderNoBarrier);
  }

  AtomicInt<int64_t> currentValue_;
  AtomicInt<int64_t> maxValue_;
};

} // namespace kudu
#endif /* KUDU_UTIL_HIGH_WATER_MARK_H */
