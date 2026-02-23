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

#include "kudu/clock/mock_ntp.h"

#include <mutex>
#include <ostream>

#include <glog/logging.h>

#include "kudu/util/status.h"

namespace kudu::clock {

Status MockNtp::walltimeWithError(uint64_t* nowUsec, uint64_t* errorUsec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  VLOG(1) << "Current clock time: " << mockClockTimeUsec_
          << " error: " << mockClockMaxErrorUsec_
          << ". Updating to time: " << nowUsec << " and error: " << errorUsec;
  *nowUsec = mockClockTimeUsec_;
  *errorUsec = mockClockMaxErrorUsec_;
  return Status::OK();
}

void MockNtp::setMockClockWallTimeForTests(uint64_t nowUsec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  CHECK_GE(nowUsec, mockClockTimeUsec_);
  mockClockTimeUsec_ = nowUsec;
}

void MockNtp::setMockMaxClockErrorForTests(uint64_t maxErrorUsec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  mockClockMaxErrorUsec_ = maxErrorUsec;
}

} // namespace kudu::clock
