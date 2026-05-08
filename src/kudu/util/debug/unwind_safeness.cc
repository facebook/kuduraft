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

// Override various libdl functions which can race with libunwind.
// The overridden versions set a threadlocal variable and our
// stack-tracing code checks the threadlocal before calling into
// libunwind.
//
// Based on public domain code by Aliaksey Kandratsenka at
// https://github.com/alk/unwind_safeness_helper

#include "kudu/util/debug/unwind_safeness.h"

#include <dlfcn.h>
#include <stddef.h>

#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/port.h"

#define CALL_ORIG(orig_ptr, func_name, ...) \
  ((decltype(&func_name))orig_ptr)(__VA_ARGS__)

// Don't hook dl_iterate_phdr in TSAN builds since TSAN already instruments this
// function and blocks signals while calling it. And skip it for macOS; it
// doesn't exist there.
#if !defined(KUDU_SANITIZE_THREAD) && !defined(__APPLE__)
#define HOOK_DL_ITERATE_PHDR 1
#endif

typedef int (*DlIteratePhdrCbtype)(struct dl_phdr_info*, size_t, void*);

namespace {

// Whether InitializeIfNecessary() has been called.
bool gInitted;

// The original versions of our wrapped functions.
void* gOrigDlopen;
void* gOrigDlclose;
#ifdef HOOK_DL_ITERATE_PHDR
void* gOrigDlIteratePhdr;
#endif

// The depth of calls into libdl.
__thread int gUnsafenessDepth;

// Scoped helper to track the recursion depth of calls into libdl
struct ScopedBumpDepth {
  ScopedBumpDepth() {
    gUnsafenessDepth++;
  }
  ~ScopedBumpDepth() {
    gUnsafenessDepth--;
  }
};

void* dlsymOrDie(const char* sym) {
  dlerror();
  void* ret = dlsym(RTLD_NEXT, sym);
  char* error = dlerror();
  CHECK(!error) << "failed to find symbol " << sym << ": " << error;
  return ret;
}

// Initialize the global variables which store the original references. This is
// set up as a constructor so that we're guaranteed to call this before main()
// while we are still single-threaded.
//
// NOTE: We _also_ call explicitly this from each of the wrappers, because
// there are some cases where the constructors of dynamic libraries may call
// dlopen, and there is no guarantee that our own constructor runs before
// the constructor of other libraries.
//
// A couple examples of the above:
//
// 1) In ASAN builds, the sanitizer runtime ends up calling dl_iterate_phdr from
// its
//    initialization.
// 2) OpenSSL in FIPS mode calls dlopen() within its constructor.
__attribute__((constructor)) void initIfNecessary() {
  // Dynamic library initialization is always single-threaded, so there's no
  // need for any synchronization here.
  if (gInitted)
    return;

  gOrigDlopen = dlsymOrDie("dlopen");
  gOrigDlclose = dlsymOrDie("dlclose");
#ifdef HOOK_DL_ITERATE_PHDR
  // Failing to hook dl_iterate_phdr is non-fatal.
  //
  // In toolchains where the linker is passed --as-needed by default, a
  // dynamically linked binary that doesn't directly reference any kudu_util
  // symbols will omit a DT_NEEDED entry for kudu_util. Such a binary will
  // no doubt emit a DT_NEEDED entry for libc, which means libc will wind up
  // _before_ kudu_util in dlsym's search order. The net effect: the dlsym()
  // call below will fail.
  //
  // All Ubuntu releases since Natty[1] behave in this way, except that many
  // of them are also vulnerable to a glibc bug[2] that'll cause such a
  // failure to go unreported by dlerror(). In newer releases, the failure
  // is reported and dlsymOrDie() crashes the process.
  //
  // Given that the subset of affected binaries is small, and given that
  // dynamic linkage isn't used in production anyway, we'll just treat the
  // hook attempt as a best effort. Affected binaries that actually attempt
  // to invoke dl_iterate_phdr will dereference a null pointer and crash, so
  // if this is ever becomes a problem, we'll know right away.a
  //
  // 1. https://wiki.ubuntu.com/NattyNarwhal/ToolchainTransition
  // 2. https://sourceware.org/bugzilla/show_bug.cgi?id=19509
  gOrigDlIteratePhdr = dlsym(RTLD_NEXT, "dl_iterate_phdr");
#endif
  gInitted = true;
}

} // anonymous namespace

namespace kudu {
namespace debug {

bool safeToUnwindStack() {
  return gUnsafenessDepth == 0;
}

} // namespace debug
} // namespace kudu

extern "C" {

void* dlopen(const char* filename, int flag) { // NOLINT
  initIfNecessary();
  ScopedBumpDepth d;
  return CALL_ORIG(gOrigDlopen, dlopen, filename, flag);
}

int dlclose(void* handle) { // NOLINT
  initIfNecessary();
  ScopedBumpDepth d;
  return CALL_ORIG(gOrigDlclose, dlclose, handle);
}

#ifdef HOOK_DL_ITERATE_PHDR
int dl_iterate_phdr(DlIteratePhdrCbtype callback, void* data) { // NOLINT
  initIfNecessary();
  ScopedBumpDepth d;
  return CALL_ORIG(gOrigDlIteratePhdr, dl_iterate_phdr, callback, data);
}
#endif
}
