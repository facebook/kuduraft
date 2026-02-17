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

#pragma once

// Ignore a single leaked object, given its pointer.
// Does nothing if LeakSanitizer is not enabled.
#define KUDU_ANNONTATE_LEAKING_OBJECT_PTR(p)

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#if defined(__linux__)

#undef KUDU_ANNONTATE_LEAKING_OBJECT_PTR
#define KUDU_ANNONTATE_LEAKING_OBJECT_PTR(p) __lsan_ignore_object(p);

#endif
#endif
#endif

// API definitions from LLVM lsan_interface.h

extern "C" {
// Allocations made between calls to __lsan_disable() and __lsan_enable() will
// be treated as non-leaks. Disable/enable pairs may be nested.
void __lsan_disable();
void __lsan_enable();

// The heap object into which p points will be treated as a non-leak.
void __lsan_ignore_object(const void* p);
} // extern "C"

namespace kudu {
namespace debug {

class ScopedLSANDisabler {
 public:
  ScopedLSANDisabler() {
    __lsan_disable();
  }
  ~ScopedLSANDisabler() {
    __lsan_enable();
  }
  ScopedLSANDisabler(const ScopedLSANDisabler&) = delete;
  ScopedLSANDisabler& operator=(const ScopedLSANDisabler&) = delete;
  ScopedLSANDisabler(ScopedLSANDisabler&&) = delete;
  ScopedLSANDisabler& operator=(ScopedLSANDisabler&&) = delete;
};

} // namespace debug
} // namespace kudu
