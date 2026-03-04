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

#include "kudu/util/faststring.h"

#include <glog/logging.h>
#include <memory>

namespace kudu {

void faststring::GrowByAtLeast(size_t count) {
  // Not enough space, need to reserve more.
  // Don't reserve exactly enough space for the new string -- that makes it
  // too easy to write perf bugs where you get O(n^2) append.
  // Instead, alwayhs expand by at least 50%.

  size_t toReserve = len_ + count;
  if (len_ + count < len_ * 3 / 2) {
    toReserve = len_ * 3 / 2;
  }
  GrowArray(toReserve);
}

void faststring::GrowArray(size_t newCapacity) {
  DCHECK_GE(newCapacity, capacity_);
  std::unique_ptr<uint8_t[]> newData(new uint8_t[newCapacity]);
  if (len_ > 0) {
    memcpy(&newData[0], &data_[0], len_);
  }
  capacity_ = newCapacity;
  if (data_ != initialData_) {
    delete[] data_;
  } else {
    KUDU_ASAN_POISON_MEMORY_REGION(initialData_, arraysize(initialData_));
  }

  data_ = newData.release();
  KUDU_ASAN_POISON_MEMORY_REGION(data_ + len_, capacity_ - len_);
}

void faststring::ShrinkToFitInternal() {
  DCHECK_NE(data_, initialData_);
  if (len_ <= kInitialCapacity) {
    KUDU_ASAN_UNPOISON_MEMORY_REGION(initialData_, len_);
    memcpy(initialData_, &data_[0], len_);
    delete[] data_;
    data_ = initialData_;
    capacity_ = kInitialCapacity;
  } else {
    std::unique_ptr<uint8_t[]> newData(new uint8_t[len_]);
    memcpy(&newData[0], &data_[0], len_);
    delete[] data_;
    data_ = newData.release();
    capacity_ = len_;
  }
}

} // namespace kudu
