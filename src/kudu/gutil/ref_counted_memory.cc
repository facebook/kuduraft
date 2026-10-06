// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/gutil/ref_counted_memory.h"

#include <glog/logging.h>

namespace kudu {

RefCountedMemory::RefCountedMemory() {}

RefCountedMemory::~RefCountedMemory() {}

RefCountedString::RefCountedString() {}

RefCountedString::~RefCountedString() {}

const unsigned char* RefCountedString::front() const {
  return data_.empty() ? nullptr
                       : reinterpret_cast<const unsigned char*>(data_.data());
}

size_t RefCountedString::size() const {
  return data_.size();
}

} //  namespace kudu
