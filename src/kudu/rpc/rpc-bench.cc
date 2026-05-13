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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::bind;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DEFINE_int32(
    client_threads,
    16,
    "Number of client threads. For the synchronous benchmark, each thread has "
    "a single outstanding synchronous request at a time. For the async "
    "benchmark, this determines the number of client reactors.");

DEFINE_int32(
    async_call_concurrency,
    60,
    "Number of concurrent requests that will be outstanding at a time for the "
    "async benchmark. The requests are multiplexed across the number of "
    "reactors specified by the 'client_threads' flag.");

DEFINE_int32(worker_threads, 1, "Number of server worker threads");

DEFINE_int32(server_reactors, 4, "Number of server reactor threads");

DEFINE_int32(run_seconds, 1, "Seconds to run the test");

DECLARE_bool(rpc_encrypt_loopback_connections);
DEFINE_bool(
    enable_encryption,
    true,
    "Whether to enable TLS encryption for rpc-bench");

METRIC_DECLARE_histogram(reactor_load_percent);
METRIC_DECLARE_histogram(reactor_active_latency_us);

namespace kudu {
namespace rpc {

class RpcBench : public RpcTestBase {
 public:
  RpcBench() : shouldRun_(true), stop_(0) {}

  void SetUp() override {
    RpcTestBase::SetUp();
    overrideFlagForSlowTests("run_seconds", "10");

    nWorkerThreads_ = FLAGS_worker_threads;
    nServerReactorThreads_ = FLAGS_server_reactors;

    // Set up server.
    FLAGS_rpc_encrypt_loopback_connections = FLAGS_enable_encryption;
    ASSERT_OK(startTestServerWithGeneratedCode(
        &serverAddr_, FLAGS_enable_encryption));
  }

  void summarizePerf(CpuTimes elapsed, int totalReqs, bool sync) {
    float reqsPerSecond = static_cast<float>(totalReqs / elapsed.wallSeconds());
    float userCpuMicrosPerReq =
        static_cast<float>(elapsed.user / 1000.0 / totalReqs);
    float sysCpuMicrosPerReq =
        static_cast<float>(elapsed.system / 1000.0 / totalReqs);
    float cswPerReq = static_cast<float>(elapsed.contextSwitches) / totalReqs;

    HdrHistogram reactorLoad(
        *METRIC_reactor_load_percent
             .instantiate(serverMessenger_->metric_entity())
             ->histogram());
    HdrHistogram reactorLatency(
        *METRIC_reactor_active_latency_us
             .instantiate(serverMessenger_->metric_entity())
             ->histogram());

    LOG(INFO) << "Mode:            " << (sync ? "Sync" : "Async");
    if (sync) {
      LOG(INFO) << "Client threads:   " << FLAGS_client_threads;
    } else {
      LOG(INFO) << "Client reactors:  " << FLAGS_client_threads;
      LOG(INFO) << "Call concurrency: " << FLAGS_async_call_concurrency;
    }

    LOG(INFO) << "Worker threads:   " << FLAGS_worker_threads;
    LOG(INFO) << "Server reactors:  " << FLAGS_server_reactors;
    LOG(INFO) << "Encryption:       " << FLAGS_enable_encryption;
    LOG(INFO) << "----------------------------------";
    LOG(INFO) << "Reqs/sec:         " << reqsPerSecond;
    LOG(INFO) << "User CPU per req: " << userCpuMicrosPerReq << "us";
    LOG(INFO) << "Sys CPU per req:  " << sysCpuMicrosPerReq << "us";
    LOG(INFO) << "Ctx Sw. per req:  " << cswPerReq;
    LOG(INFO) << "Server Reactor load (mean):     " << reactorLoad.meanValue()
              << "%";
    LOG(INFO) << "Server Reactor load (95p):      "
              << reactorLoad.valueAtPercentile(95) << "%";
    LOG(INFO) << "Server Reactor Latency (mean):  "
              << reactorLatency.meanValue() << "us";
    LOG(INFO) << "Server Reactor Latency (95p):   "
              << reactorLatency.valueAtPercentile(95) << "us";
  }

 protected:
  friend class ClientThread;
  friend class ClientAsyncWorkload;

  Sockaddr serverAddr_;
  Atomic32 shouldRun_;
  CountDownLatch stop_;
};

class ClientThread {
 public:
  explicit ClientThread(RpcBench* bench) : bench_(bench), requestCount(0) {}

  void start() {
    thread_.reset(new thread(&ClientThread::run, this));
  }

  void join() {
    thread_->join();
  }

  void run() {
    shared_ptr<Messenger> clientMessenger;
    CHECK_OK(bench_->createMessenger(
        "Client",
        &clientMessenger,
        /*nReactors=*/1,
        FLAGS_enable_encryption));

    CalculatorServiceProxy p(clientMessenger, bench_->serverAddr_, "localhost");

    AddRequestPB req;
    AddResponsePB resp;
    while (Acquire_Load(&bench_->shouldRun_)) {
      req.set_x(requestCount);
      req.set_y(requestCount);
      RpcController controller;
      controller.set_timeout(MonoDelta::FromSeconds(10));
      CHECK_OK(p.Add(req, &resp, &controller));
      CHECK_EQ(req.x() + req.y(), resp.result());
      requestCount++;
    }
  }

  unique_ptr<thread> thread_;
  RpcBench* bench_;
  int requestCount;
};

// Test making successful RPC calls.
TEST_F(RpcBench, BenchmarkCalls) {
  Stopwatch sw(Stopwatch::kAllThreads);
  sw.start();

  vector<unique_ptr<ClientThread>> threads;
  for (int i = 0; i < FLAGS_client_threads; i++) {
    threads.emplace_back(new ClientThread(this));
    threads.back()->start();
  }

  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  Release_Store(&shouldRun_, false);

  int totalReqs = 0;

  for (auto& thr : threads) {
    thr->join();
    totalReqs += thr->requestCount;
  }
  sw.stop();

  summarizePerf(sw.elapsed(), totalReqs, true);
}

class ClientAsyncWorkload {
 public:
  ClientAsyncWorkload(RpcBench* bench, shared_ptr<Messenger> messenger)
      : bench_(bench), messenger_(std::move(messenger)), requestCount(0) {
    controller_.set_timeout(MonoDelta::FromSeconds(10));
    proxy_.reset(new CalculatorServiceProxy(
        messenger_, bench_->serverAddr_, "localhost"));
  }

  void callOneRpc() {
    if (requestCount > 0) {
      CHECK_OK(controller_.status());
      CHECK_EQ(req_.x() + req_.y(), resp_.result());
    }
    if (!Acquire_Load(&bench_->shouldRun_)) {
      bench_->stop_.countDown();
      return;
    }
    controller_.Reset();
    req_.set_x(requestCount);
    req_.set_y(requestCount);
    requestCount++;
    proxy_->AddAsync(
        req_,
        &resp_,
        &controller_,
        bind(&ClientAsyncWorkload::callOneRpc, this));
  }

  void start() {
    callOneRpc();
  }

  RpcBench* bench_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<CalculatorServiceProxy> proxy_;
  uint32_t requestCount;
  RpcController controller_;
  AddRequestPB req_;
  AddResponsePB resp_;
};

TEST_F(RpcBench, BenchmarkCallsAsync) {
  int threads = FLAGS_client_threads;
  int concurrency = FLAGS_async_call_concurrency;

  vector<shared_ptr<Messenger>> messengers;
  for (int i = 0; i < threads; i++) {
    shared_ptr<Messenger> m;
    ASSERT_OK(createMessenger(
        "Client", &m, /*nReactors=*/1, FLAGS_enable_encryption));
    messengers.emplace_back(std::move(m));
  }

  vector<unique_ptr<ClientAsyncWorkload>> workloads;
  for (int i = 0; i < concurrency; i++) {
    workloads.emplace_back(
        new ClientAsyncWorkload(this, messengers[i % threads]));
  }

  stop_.reset(concurrency);

  Stopwatch sw(Stopwatch::kAllThreads);
  sw.start();

  for (int i = 0; i < concurrency; i++) {
    workloads[i]->start();
  }

  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  Release_Store(&shouldRun_, false);

  sw.stop();

  stop_.wait();
  int totalReqs = 0;
  for (int i = 0; i < concurrency; i++) {
    totalReqs += workloads[i]->requestCount;
  }

  summarizePerf(sw.elapsed(), totalReqs, false);
}

} // namespace rpc
} // namespace kudu
