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
#include "kudu/util/pb_util-internal.h"

#include <ostream>
#include <string>

namespace kudu {
namespace pb_util {
namespace internal {

////////////////////////////////////////////
// SequentialFileFileInputStream
////////////////////////////////////////////

bool SequentialFileFileInputStream::Next(const void** data, int* size) {
  if (PREDICT_FALSE(!status_.ok())) {
    LOG(WARNING) << "Already failed on a previous read: " << status_.ToString();
    return false;
  }

  size_t available = (bufferUsed_ - bufferOffset_);
  if (available > 0) {
    *data = buffer_.get() + bufferOffset_;
    *size = available;
    bufferOffset_ += available;
    totalRead_ += available;
    return true;
  }

  Slice result(buffer_.get(), bufferSize_);
  status_ = rfile_->Read(&result);
  if (!status_.ok()) {
    LOG(WARNING) << "Read at " << bufferOffset_
                 << " failed: " << status_.ToString();
    return false;
  }

  bufferUsed_ = result.size();
  bufferOffset_ = bufferUsed_;
  totalRead_ += bufferUsed_;
  *data = buffer_.get();
  *size = bufferUsed_;
  return bufferUsed_ > 0;
}

bool SequentialFileFileInputStream::Skip(int count) {
  CHECK_GT(count, 0);
  int avail = (bufferUsed_ - bufferOffset_);
  if (avail > count) {
    bufferOffset_ += count;
    totalRead_ += count;
  } else {
    bufferUsed_ = 0;
    bufferOffset_ = 0;
    status_ = rfile_->Skip(count - avail);
    totalRead_ += count - avail;
  }
  return status_.ok();
}

////////////////////////////////////////////
// WritableFileOutputStream
////////////////////////////////////////////

bool WritableFileOutputStream::Next(void** data, int* size) {
  if (PREDICT_FALSE(!status_.ok())) {
    LOG(WARNING) << "Already failed on a previous write: "
                 << status_.ToString();
    return false;
  }

  size_t available = (bufferSize_ - bufferOffset_);
  if (available > 0) {
    *data = buffer_.get() + bufferOffset_;
    *size = available;
    bufferOffset_ += available;
    return true;
  }

  if (!flush()) {
    return false;
  }

  bufferOffset_ = bufferSize_;
  *data = buffer_.get();
  *size = bufferSize_;
  return true;
}

} // namespace internal
} // namespace pb_util
} // namespace kudu
