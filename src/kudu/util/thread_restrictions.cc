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

#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/thread.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadlocal.h"

#ifdef ENABLE_THREAD_RESTRICTIONS

namespace kudu {

namespace {

struct LocalThreadRestrictions {
  LocalThreadRestrictions()
      : ioAllowed(true), waitAllowed(true), singletonAllowed(true) {}

  bool ioAllowed;
  bool waitAllowed;
  bool singletonAllowed;
};

LocalThreadRestrictions* loadTls() {
  // Disable leak check. LSAN sometimes gets false positives on thread locals.
  // See: https://github.com/google/sanitizers/issues/757
  debug::ScopedLeakCheckDisabler d;
  BLOCK_STATIC_THREAD_LOCAL(LocalThreadRestrictions, localThreadRestrictions);
  return localThreadRestrictions;
}

} // anonymous namespace

bool ThreadRestrictions::setIoAllowed(bool allowed) {
  bool previousAllowed = loadTls()->ioAllowed;
  loadTls()->ioAllowed = allowed;
  return previousAllowed;
}

void ThreadRestrictions::assertIoAllowed() {
  CHECK(loadTls()->ioAllowed)
      << "Function marked as IO-only was called from a thread that "
      << "disallows IO!  If this thread really should be allowed to "
      << "make IO calls, adjust the call to "
      << "kudu::ThreadRestrictions::setIoAllowed() in this thread's "
      << "startup. "
      << (Thread::currentThread() ? Thread::currentThread()->ToString()
                                  : "(not a kudu::Thread)");
}

bool ThreadRestrictions::setWaitAllowed(bool allowed) {
  bool previousAllowed = loadTls()->waitAllowed;
  loadTls()->waitAllowed = allowed;
  return previousAllowed;
}

void ThreadRestrictions::assertWaitAllowed() {
  CHECK(loadTls()->waitAllowed)
      << "Waiting is not allowed to be used on this thread to prevent "
      << "server-wide latency aberrations and deadlocks. "
      << (Thread::currentThread() ? Thread::currentThread()->ToString()
                                  : "(not a kudu::Thread)");
}

} // namespace kudu

#endif
