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

#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/rw_semaphore.h"

using std::thread;
using std::vector;

namespace kudu {
struct SharedState {
  SharedState() : done(false), intVar(0) {}

  bool done;
  int64_t intVar;
  RwSemaphore sem;
};

// Thread which increases the value in the shared state under the write lock.
void writer(SharedState* state) {
  int i = 0;
  while (true) {
    std::lock_guard<RwSemaphore> l(state->sem);
    state->intVar += (i++);
    if (state->done) {
      break;
    }
  }
}

// Thread which verifies that the value in the shared state only increases.
void reader(SharedState* state) {
  int prevVal = 0;
  while (true) {
    shared_lock<RwSemaphore> l(state->sem);
    // The intVar should only be seen to increase.
    CHECK_GE(state->intVar, prevVal);
    prevVal = state->intVar;
    if (state->done) {
      break;
    }
  }
}

// Test which verifies basic functionality of the semaphore.
// When run under TSAN this also verifies the barriers.
TEST(RWSemaphoreTest, TestBasicOperation) {
  SharedState s;
  vector<thread*> threads;
  // Start 5 readers and writers.
  for (int i = 0; i < 5; i++) {
    threads.push_back(new thread(reader, &s));
    threads.push_back(new thread(writer, &s));
  }

  // Let them contend for a short amount of time.
  SleepFor(MonoDelta::FromMilliseconds(50));

  // Signal them to stop.
  {
    std::lock_guard<RwSemaphore> l(s.sem);
    s.done = true;
  }

  for (thread* t : threads) {
    t->join();
    delete t;
  }
}

} // namespace kudu
