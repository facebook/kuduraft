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

#include "kudu/util/throttler.h"

#include <algorithm>
#include <mutex>

namespace kudu {

Throttler::Throttler(
    MonoTime now,
    uint64_t opRate,
    uint64_t byteRate,
    double burstFactor)
    : nextRefill_(now) {
  opRefill_ = opRate / (MonoTime::kMicrosecondsPerSecond / kRefillPeriodMicros);
  opToken_ = 0;
  opTokenMax_ = static_cast<uint64_t>(opRefill_ * burstFactor);
  byteRefill_ =
      byteRate / (MonoTime::kMicrosecondsPerSecond / kRefillPeriodMicros);
  byteToken_ = 0;
  byteTokenMax_ = static_cast<uint64_t>(byteRefill_ * burstFactor);
}

bool Throttler::take(MonoTime now, uint64_t op, uint64_t byte) {
  if (opRefill_ == 0 && byteRefill_ == 0) {
    return true;
  }
  std::lock_guard<simple_spinlock> lock(lock_);
  refill(now);
  if ((opRefill_ == 0 || op <= opToken_) &&
      (byteRefill_ == 0 || byte <= byteToken_)) {
    if (opRefill_ > 0) {
      opToken_ -= op;
    }
    if (byteRefill_ > 0) {
      byteToken_ -= byte;
    }
    return true;
  }
  return false;
}

void Throttler::refill(MonoTime now) {
  int64_t d = (now - nextRefill_).ToMicroseconds();
  if (d < 0) {
    return;
  }
  uint64_t numPeriod = d / kRefillPeriodMicros + 1;
  nextRefill_ += MonoDelta::FromMicroseconds(numPeriod * kRefillPeriodMicros);
  opToken_ += numPeriod * opRefill_;
  opToken_ = std::min(opToken_, opTokenMax_);
  byteToken_ += numPeriod * byteRefill_;
  byteToken_ = std::min(byteToken_, byteTokenMax_);
}

} // namespace kudu
