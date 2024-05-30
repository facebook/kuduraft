// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/atomic_refcount.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {

namespace subtle {

RefCountedBase::RefCountedBase() : ref_count_(0) {}

RefCountedBase::~RefCountedBase() {}

void RefCountedBase::AddRef() const {
  // TODO(maruel): Add back once it doesn't assert 500 times/sec.
  // Current thread books the critical section "AddRelease" without release it.
  // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
  ++ref_count_;
}

bool RefCountedBase::Release() const {
  // TODO(maruel): Add back once it doesn't assert 500 times/sec.
  // Current thread books the critical section "AddRelease" without release it.
  // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
  if (--ref_count_ == 0) {
    return true;
  }
  return false;
}

bool RefCountedThreadSafeBase::HasOneRef() const {
  return base::RefCountIsOne(
      &const_cast<RefCountedThreadSafeBase*>(this)->ref_count_);
}

RefCountedThreadSafeBase::RefCountedThreadSafeBase() : ref_count_(0) {}

RefCountedThreadSafeBase::~RefCountedThreadSafeBase() {}

void RefCountedThreadSafeBase::AddRef() const {
  base::RefCountInc(&ref_count_);
}

bool RefCountedThreadSafeBase::Release() const {
  if (!base::RefCountDec(&ref_count_)) {
    return true;
  }
  return false;
}

} // namespace subtle

} // namespace kudu
