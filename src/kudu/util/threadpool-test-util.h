// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Test utilities for ThreadPool testing.
#pragma once

#include <barrier>
#include <latch>
#include <memory>

#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

// Wait for all currently queued tasks in a ThreadPool to complete.
//
// This works by submitting `maxThreads` barrier tasks. When all barrier
// tasks reach the barrier, it means all threads have finished processing
// any prior work, since:
// 1. Barrier tasks are added to the end of the queue
// 2. Tasks are processed in FIFO order from the queue
// 3. When a thread picks up a barrier task, its previous task must be done
// 4. When all `maxThreads` threads are at the barrier, all prior work is done
//
// We then wait for all barrier tasks to fully complete (via a latch) to
// ensure threads have gone idle before returning.
//
// NOTE: This only waits for tasks that were queued before this call.
// New tasks submitted concurrently may or may not be waited for.
inline void waitForPool(ThreadPool& pool) {
  int maxThreads = pool.numThreads();
  auto latch = std::make_shared<std::latch>(maxThreads);
  auto barrier = std::make_shared<std::barrier<>>(maxThreads + 1);
  for (int i = 0; i < maxThreads; i++) {
    CHECK_OK(pool.SubmitFunc([barrier, latch]() {
      barrier->arrive_and_wait();
      latch->count_down();
    }));
  }
  barrier->arrive_and_wait(); // Wait until all threads have reached the barrier
  latch->wait(); // Wait until all barrier tasks have fully completed
}

} // namespace kudu
