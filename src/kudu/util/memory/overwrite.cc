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

#include "kudu/util/memory/overwrite.h"

#include "kudu/gutil/strings/stringpiece.h"

#include <glog/logging.h>
#include <string.h>
namespace kudu {

void overwriteWithPattern(char* p, size_t len, StringPiece pattern) {
  size_t patLen = pattern.size();
  CHECK_LT(0, patLen);
  size_t rem = len;
  const char* patPtr = pattern.data();

  for (; rem >= patLen; rem -= patLen) {
    memcpy(p, patPtr, patLen);
    p += patLen;
  }

  for (; rem > 0; rem--) {
    *p++ = *patPtr++;
  }
}

} // namespace kudu
