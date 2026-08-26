// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/faststring.h"

namespace kudu {

void putFixed32(faststring* dst, uint32_t value) {
  inlinePutFixed32(dst, value);
}

} // namespace kudu
