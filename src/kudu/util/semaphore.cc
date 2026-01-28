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

#include "kudu/util/semaphore.h"

#include <semaphore.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/walltime.h"
#include "kudu/util/monotime.h"

namespace kudu {

Semaphore::Semaphore(int capacity) {
  DCHECK_GE(capacity, 0);
  if (sem_init(&sem_, 0, capacity) != 0) {
    fatal("init");
  }
}

Semaphore::~Semaphore() {
  if (sem_destroy(&sem_) != 0) {
    fatal("destroy");
  }
}

void Semaphore::acquire() {
  while (true) {
    int ret;
    RETRY_ON_EINTR(ret, sem_wait(&sem_));
    if (ret == 0) {
      // TODO(todd): would be nice to track acquisition time, etc.
      return;
    }
    fatal("wait");
  }
}

bool Semaphore::tryAcquire() {
  int ret;
  RETRY_ON_EINTR(ret, sem_trywait(&sem_));
  if (ret == 0) {
    return true;
  }
  if (errno == EAGAIN) {
    return false;
  }
  fatal("trywait");
}

bool Semaphore::timedAcquire(const MonoDelta& timeout) {
  int64_t microtime = GetCurrentTimeMicros();
  microtime += timeout.ToMicroseconds();

  struct timespec absTimeout;
  MonoDelta::NanosToTimeSpec(
      microtime * MonoTime::kNanosecondsPerMicrosecond, &absTimeout);

  while (true) {
    int ret;
    RETRY_ON_EINTR(ret, sem_timedwait(&sem_, &absTimeout));
    if (ret == 0) {
      return true;
    }
    if (errno == ETIMEDOUT) {
      return false;
    }
    fatal("timedwait");
  }
}

void Semaphore::release() {
  PCHECK(sem_post(&sem_) == 0);
}

int Semaphore::getValue() {
  int val;
  PCHECK(sem_getvalue(&sem_, &val) == 0);
  return val;
}

void Semaphore::fatal(const char* action) {
  PLOG(FATAL) << "Could not " << action << " semaphore "
              << reinterpret_cast<void*>(&sem_);
  abort(); // unnecessary, but avoids gcc complaining
}

} // namespace kudu
