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

#include "kudu/rpc/rpc-test-base.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/port.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/thread.h"

METRIC_DECLARE_counter(rpc_connections_accepted);
METRIC_DECLARE_counter(rpcs_queue_overflow);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class MultiThreadedRpcTest : public RpcTestBase {
 public:
  // Make a single RPC call.
  void singleCall(
      Sockaddr serverAddr,
      const char* methodName,
      Status* result,
      CountDownLatch* latch) {
    LOG(INFO) << "Connecting to " << serverAddr.ToString();
    shared_ptr<Messenger> clientMessenger;
    CHECK_OK(CreateMessenger("ClientSC", &clientMessenger));
    Proxy p(
        clientMessenger,
        serverAddr,
        serverAddr.host(),
        GenericCalculatorService::static_service_name());
    *result = DoTestSyncCall(p, methodName);
    latch->CountDown();
  }

  // Make RPC calls until we see a failure.
  void hammerServer(
      Sockaddr serverAddr,
      const char* methodName,
      Status* lastResult) {
    shared_ptr<Messenger> clientMessenger;
    CHECK_OK(CreateMessenger("ClientHS", &clientMessenger));
    hammerServerWithMessenger(
        serverAddr, methodName, lastResult, clientMessenger);
  }

  void hammerServerWithMessenger(
      Sockaddr serverAddr,
      const char* methodName,
      Status* lastResult,
      const shared_ptr<Messenger>& messenger) {
    LOG(INFO) << "Connecting to " << serverAddr.ToString();
    Proxy p(
        messenger,
        serverAddr,
        serverAddr.host(),
        GenericCalculatorService::static_service_name());

    int i = 0;
    while (true) {
      i++;
      Status s = DoTestSyncCall(p, methodName);
      if (!s.ok()) {
        // Return on first failure.
        LOG(INFO) << "Call failed. Shutting down client thread. Ran " << i
                  << " calls: " << s.ToString();
        *lastResult = s;
        return;
      }
    }
  }
};

static void assertShutdown(kudu::Thread* thread, const Status* status) {
  ASSERT_OK(ThreadJoiner(thread).warnEveryMs(500).Join());
  string msg = status->ToString();
  ASSERT_TRUE(
      msg.find("Service unavailable") != string::npos ||
      msg.find("Network error") != string::npos)
      << "Status is actually: " << msg;
}

// Test making several concurrent RPC calls while shutting down.
// Simply verify that we don't hit any CHECK errors.
TEST_F(MultiThreadedRpcTest, TestShutdownDuringService) {
  // Set up server.
  Sockaddr serverAddr;
  ASSERT_OK(StartTestServer(&serverAddr));

  const int kNumThreads = 4;
  std::shared_ptr<kudu::Thread> threads[kNumThreads];
  Status statuses[kNumThreads];
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("t{}", i),
            &MultiThreadedRpcTest::hammerServer,
            this,
            serverAddr,
            GenericCalculatorService::kAddMethodName,
            &statuses[i],
            &threads[i]));
  }

  SleepFor(MonoDelta::FromMilliseconds(50));

  // Shut down server.
  server_messenger_->UnregisterAllServices();
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (int i = 0; i < kNumThreads; i++) {
    assertShutdown(threads[i].get(), &statuses[i]);
  }
}

// Test shutting down the client messenger exactly as a thread is about to start
// a new connection. This is a regression test for KUDU-104.
TEST_F(MultiThreadedRpcTest, TestShutdownClientWhileCallsPending) {
  // Set up server.
  Sockaddr serverAddr;
  ASSERT_OK(StartTestServer(&serverAddr));

  shared_ptr<Messenger> clientMessenger;
  ASSERT_OK(CreateMessenger("Client", &clientMessenger));

  std::shared_ptr<kudu::Thread> thread;
  Status status;
  ASSERT_OK(
      kudu::Thread::Create(
          "test",
          "test",
          &MultiThreadedRpcTest::hammerServerWithMessenger,
          this,
          serverAddr,
          GenericCalculatorService::kAddMethodName,
          &status,
          clientMessenger,
          &thread));

  // Shut down the messenger after a very brief sleep. This often will race so
  // that the call gets submitted to the messenger before shutdown, but the
  // negotiation won't have started yet. In a debug build this fails about half
  // the time without the bug fix. See KUDU-104.
  SleepFor(MonoDelta::FromMicroseconds(10));
  clientMessenger->Shutdown();
  clientMessenger.reset();

  ASSERT_OK(ThreadJoiner(thread.get()).warnEveryMs(500).Join());
  ASSERT_TRUE(status.IsAborted() || status.IsServiceUnavailable());
  string msg = status.ToString();
  SCOPED_TRACE(msg);
  ASSERT_TRUE(
      msg.find("Client RPC Messenger shutting down") != string::npos ||
      msg.find("reactor is shutting down") != string::npos ||
      msg.find("Unable to start connection negotiation thread") != string::npos)
      << "Status is actually: " << msg;
}

// This bogus service pool leaves the service queue full.
class BogusServicePool : public ServicePool {
 public:
  BogusServicePool(
      unique_ptr<ServiceIf> service,
      const std::shared_ptr<MetricEntity>& metricEntity,
      size_t serviceQueueLength)
      : ServicePool(std::move(service), metricEntity, serviceQueueLength) {}
  virtual Status init(int numThreads) override {
    // Do nothing
    return Status::OK();
  }
};

void incrementBackpressureOrShutdown(
    const Status* status,
    int* backpressure,
    int* shutdown) {
  string msg = status->ToString();
  if (msg.find("service queue is full") != string::npos) {
    ++(*backpressure);
  } else if (msg.find("shutting down") != string::npos) {
    ++(*shutdown);
  } else if (msg.find("got EOF from remote") != string::npos) {
    ++(*shutdown);
  } else {
    FAIL() << "Unexpected status message: " << msg;
  }
}

// Test that we get a Service Unavailable error when we max out the incoming RPC
// service queue.
TEST_F(MultiThreadedRpcTest, TestBlowOutServiceQueue) {
  const size_t kMaxConcurrency = 2;

  MessengerBuilder bld("messenger1");
  bld.set_num_reactors(kMaxConcurrency);
  bld.set_metric_entity(metricEntity_);
  CHECK_OK(bld.Build(&server_messenger_));

  shared_ptr<AcceptorPool> pool;
  ASSERT_OK(server_messenger_->AddAcceptorPool(Sockaddr(), &pool));
  ASSERT_OK(pool->Start(kMaxConcurrency));
  Sockaddr serverAddr = pool->bind_address();

  unique_ptr<ServiceIf> service(new GenericCalculatorService());
  service_name_ = service->service_name();
  service_pool_ = new BogusServicePool(
      std::move(service), server_messenger_->metric_entity(), kMaxConcurrency);
  ASSERT_OK(service_pool_->init(nWorkerThreads_));
  server_messenger_->RegisterService(service_name_, service_pool_);

  std::shared_ptr<kudu::Thread> threads[3];
  Status status[3];
  CountDownLatch latch(1);
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("t{}", i),
            &MultiThreadedRpcTest::singleCall,
            this,
            serverAddr,
            GenericCalculatorService::kAddMethodName,
            &status[i],
            &latch,
            &threads[i]));
  }

  // One should immediately fail due to backpressure. The latch is only
  // initialized to wait for the first of three threads to finish.
  latch.Wait();

  // The rest would time out after 10 sec, but we help them along.
  server_messenger_->UnregisterAllServices();
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (const auto& thread : threads) {
    ASSERT_OK(ThreadJoiner(thread.get()).warnEveryMs(500).Join());
  }

  // Verify that one error was due to backpressure.
  int errorsBackpressure = 0;
  int errorsShutdown = 0;

  for (const auto& s : status) {
    incrementBackpressureOrShutdown(&s, &errorsBackpressure, &errorsShutdown);
  }

  ASSERT_EQ(1, errorsBackpressure);
  ASSERT_EQ(2, errorsShutdown);

  // Check that RPC queue overflow metric is 1
  Counter* rpcsQueueOverflow =
      METRIC_rpcs_queue_overflow.Instantiate(server_messenger_->metric_entity())
          .get();
  ASSERT_EQ(1, rpcsQueueOverflow->value());
}

static void hammerServerWithTcpConns(const Sockaddr& addr) {
  while (true) {
    Socket socket;
    CHECK_OK(socket.Init(0));
    Status s;
    LOG_SLOW_EXECUTION(INFO, 100, "Connect took long") {
      s = socket.Connect(addr);
    }
    if (!s.ok()) {
      CHECK(s.IsNetworkError()) << "Unexpected error: " << s.ToString();
      return;
    }
    CHECK_OK(socket.Close());
  }
}

// Regression test for KUDU-128.
// Test that shuts down the server while new TCP connections are incoming.
TEST_F(MultiThreadedRpcTest, TestShutdownWithIncomingConnections) {
  // Set up server.
  Sockaddr serverAddr;
  ASSERT_OK(StartTestServer(&serverAddr));

  // Start a number of threads which just hammer the server with TCP
  // connections.
  vector<std::shared_ptr<kudu::Thread>> threads;
  for (int i = 0; i < 8; i++) {
    std::shared_ptr<kudu::Thread> newThread;
    CHECK_OK(
        kudu::Thread::Create(
            "test",
            fmt::format("t{}", i),
            &hammerServerWithTcpConns,
            serverAddr,
            &newThread));
    threads.push_back(newThread);
  }

  // Sleep until the server has started to actually accept some connections from
  // the test threads.
  std::shared_ptr<Counter> connsAccepted =
      METRIC_rpc_connections_accepted.Instantiate(
          server_messenger_->metric_entity());
  while (connsAccepted->value() == 0) {
    SleepFor(MonoDelta::FromMicroseconds(100));
  }

  // Shutdown while there are still new connections appearing.
  server_messenger_->UnregisterAllServices();
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (std::shared_ptr<kudu::Thread>& t : threads) {
    ASSERT_OK(ThreadJoiner(t.get()).warnEveryMs(500).Join());
  }
}

} // namespace rpc
} // namespace kudu
