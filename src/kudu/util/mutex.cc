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
//
// Portions (c) 2011 The Chromium Authors.
#include <pthread.h>

#include <glog/logging.h>

#include "kudu/util/mutex.h"
#include "kudu/util/trace.h"

namespace kudu {

Mutex::Mutex() {
  // In release, go with the default lock attributes.
  pthread_mutex_init(&nativeHandle_, NULL);
}

Mutex::~Mutex() {
  int rv = pthread_mutex_destroy(&nativeHandle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv);
}

bool Mutex::tryAcquire() {
  int rv = pthread_mutex_trylock(&nativeHandle_);
  return rv == 0;
}

void Mutex::acquire() {
  // Optimize for the case when mutexes are uncontended. If they
  // are contended, we'll have to go to sleep anyway, so the extra
  // cost of branch mispredictions is moot.
  //
  // tryAcquire() is implemented as a simple CompareAndSwap inside
  // pthreads so this does not require a system call.
  if (PREDICT_TRUE(tryAcquire())) {
    return;
  }

  // If we weren't able to acquire the mutex immediately, then it's
  // worth gathering timing information about the mutex acquisition.
  kudu::MicrosecondsInt64 startTime = getMonoTimeMicros();
  int rv = pthread_mutex_lock(&nativeHandle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv); // NOLINT(whitespace/semicolon)
  kudu::MicrosecondsInt64 endTime = getMonoTimeMicros();

  int64_t waitTime = endTime - startTime;
  if (waitTime > 0) {
    TRACE_COUNTER_INCREMENT("mutex_wait_us", waitTime);
  }
}

void Mutex::release() {
  int rv = pthread_mutex_unlock(&nativeHandle_);
  DCHECK_EQ(0, rv) << ". " << strerror(rv);
}

} // namespace kudu
