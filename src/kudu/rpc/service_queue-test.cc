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

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_int32(num_producers, 4, "Number of producer threads");

DEFINE_int32(num_consumers, 20, "Number of consumer threads");

DEFINE_int32(max_queue_size, 50, "Max queue length");

namespace kudu {
namespace rpc {

static std::atomic<uint32_t> inProgress;

static std::atomic<uint32_t> total;

template <typename Queue>
void producerThread(Queue* queue) {
  int maxInprogress = FLAGS_max_queue_size - FLAGS_num_producers;
  while (true) {
    while (inProgress > maxInprogress) {
      base::subtle::PauseCPU();
    }
    inProgress++;
    InboundCall* call = new InboundCall(std::shared_ptr<Connection>());
    std::optional<InboundCall*> evicted;
    auto status = queue->put(call, &evicted);
    if (status == kQueueFull) {
      LOG(INFO) << "queue full: producer exiting";
      delete call;
      break;
    }

    if (PREDICT_FALSE(evicted != {})) {
      LOG(INFO) << "call evicted: producer exiting";
      delete evicted.get();
      break;
    }

    if (PREDICT_TRUE(status == kQueueShutdown)) {
      delete call;
      break;
    }
  }
}

template <typename Queue>
void consumerThread(Queue* queue) {
  unique_ptr<InboundCall> call;
  while (queue->blockingGet(&call)) {
    inProgress--;
    total++;
    call.reset();
  }
}

TEST(TestServiceQueue, LifoServiceQueuePerf) {
  LifoServiceQueue queue(FLAGS_max_queue_size);
  vector<std::thread> producers;
  vector<std::thread> consumers;

  for (int i = 0; i < FLAGS_num_producers; i++) {
    producers.emplace_back(&producerThread<LifoServiceQueue>, &queue);
  }

  for (int i = 0; i < FLAGS_num_consumers; i++) {
    consumers.emplace_back(&consumerThread<LifoServiceQueue>, &queue);
  }

  int seconds = AllowSlowTests() ? 10 : 1;
  uint64_t totalSample = 0;
  uint64_t totalQueueLen = 0;
  uint64_t totalIdleWorkers = 0;
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  int32_t before = total;

  for (int i = 0; i < seconds * 50; i++) {
    SleepFor(MonoDelta::FromMilliseconds(20));
    totalSample++;
    totalQueueLen += queue.estimatedQueueLength();
    totalIdleWorkers += queue.estimatedIdleWorkerCount();
  }

  sw.stop();
  int32_t delta = total - before;

  queue.shutdown();
  for (int i = 0; i < FLAGS_num_producers; i++) {
    producers[i].join();
  }
  for (int i = 0; i < FLAGS_num_consumers; i++) {
    consumers[i].join();
  }

  float reqsPerSecond = static_cast<float>(delta / sw.elapsed().wall_seconds());
  float userCpuMicrosPerReq =
      static_cast<float>(sw.elapsed().user / 1000.0 / delta);
  float sysCpuMicrosPerReq =
      static_cast<float>(sw.elapsed().system / 1000.0 / delta);

  LOG(INFO) << "Reqs/sec:         " << (int32_t)reqsPerSecond;
  LOG(INFO) << "User CPU per req: " << userCpuMicrosPerReq << "us";
  LOG(INFO) << "Sys CPU per req:  " << sysCpuMicrosPerReq << "us";
  LOG(INFO) << "Avg rpc queue length: "
            << totalQueueLen / static_cast<double>(totalSample);
  LOG(INFO) << "Avg idle workers:     "
            << totalIdleWorkers / static_cast<double>(totalSample);
}

} // namespace rpc
} // namespace kudu
