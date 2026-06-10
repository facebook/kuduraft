// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/gutil/callback_internal.h"

#include <glog/logging.h>

namespace kudu {
namespace internal {

bool CallbackBase::is_null() const {
  return bindState_.get() == nullptr;
}

void CallbackBase::Reset() {
  polymorphicInvoke_ = nullptr;
  // NULL the bindState_ last, since it may be holding the last ref to whatever
  // object owns us, and we may be deleted after that.
  bindState_ = nullptr;
}

bool CallbackBase::Equals(const CallbackBase& other) const {
  return bindState_.get() == other.bindState_.get() &&
      polymorphicInvoke_ == other.polymorphicInvoke_;
}

CallbackBase::CallbackBase(BindStateBase* bindState)
    : bindState_(bindState), polymorphicInvoke_(nullptr) {}

CallbackBase::~CallbackBase() {}

} // namespace internal
} // namespace kudu
