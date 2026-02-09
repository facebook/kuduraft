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
#ifndef IMPALA_BIT_UTIL_H
#define IMPALA_BIT_UTIL_H

#include <stdint.h>
#include "kudu/gutil/port.h"

namespace kudu {

// Utility class to do standard bit tricks
// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  // Returns the ceil of value/divisor
  static inline int ceil(int value, int divisor) {
    return value / divisor + (value % divisor != 0);
  }

  // Returns the 'numBits' least-significant bits of 'v'.
  static inline uint64_t trailingBits(uint64_t v, int numBits) {
    if (PREDICT_FALSE(numBits == 0)) {
      return 0;
    }
    if (PREDICT_FALSE(numBits >= 64)) {
      return v;
    }
    int n = 64 - numBits;
    return (v << n) >> n;
  }

  static inline uint64_t shiftLeftZeroOnOverflow(uint64_t v, int numBits) {
    if (PREDICT_FALSE(numBits >= 64)) {
      return 0;
    }
    return v << numBits;
  }

  static inline uint64_t shiftRightZeroOnOverflow(uint64_t v, int numBits) {
    if (PREDICT_FALSE(numBits >= 64)) {
      return 0;
    }
    return v >> numBits;
  }
};

} // namespace kudu

#endif
