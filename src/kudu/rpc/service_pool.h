// Licensed to the Apache Software Foundation (ASF) under one
#include <memory>
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

#ifndef KUDU_SERVICE_POOL_H
#define KUDU_SERVICE_POOL_H

#include <atomic>
#include <cstddef>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class Histogram;
class MetricEntity;
class Thread;

namespace rpc {

class InboundCall;
class RemoteMethod;
class ServiceIf;

struct RpcMethodInfo;

// A pool of threads that handle new incoming RPC calls.
// Also includes a queue that calls get pushed onto for handling by the pool.
class ServicePool : public RpcService {
 public:
  ServicePool(
      std::unique_ptr<ServiceIf> service,
      const std::shared_ptr<MetricEntity>& metricEntity,
      size_t serviceQueueLength);
  ~ServicePool() override;

  // Set a hook function to be called when any RPC gets rejected because
  // the service queue is full.
  //
  // NOTE: This hook runs on a reactor thread so must execute quickly.
  // Additionally, if a service queue is overflowing, the server is likely
  // under a lot of load, so hooks should be careful to throttle their own
  // execution.
  void setTooBusyHook(std::function<void(void)> hook) {
    tooBusyHook_ = std::move(hook);
  }

  // Start up the thread pool.
  virtual Status init(int numThreads);

  // Shut down the queue and the thread pool.
  virtual void Shutdown();

  RpcMethodInfo* LookupMethod(const RemoteMethod& method) override;

  virtual Status QueueInboundCall(std::unique_ptr<InboundCall> call) override;

  virtual void NotifyLongCallLoading(const RemoteMethod& method) override;

  virtual void NotifyLongCallLoaded(const RemoteMethod& method) override;

  const Counter* rpcsTimedOutInQueueMetricForTests() const {
    return rpcsTimedOutInQueue_.get();
  }

  const Histogram* incomingQueueTimeMetricForTests() const {
    return incomingQueueTime_.get();
  }

  const Counter* rpcsQueueOverflowMetric() const {
    return rpcsQueueOverflow_.get();
  }

  const std::string service_name() const;

  /**
   * Dump the current contents of the service queue
   */
  std::string rpcServiceQueueToString() const;

 private:
  void runThread();
  void rejectTooBusy(InboundCall* c);

  std::unique_ptr<ServiceIf> service_;
  std::vector<std::shared_ptr<kudu::Thread>> threads_;
  LifoServiceQueue serviceQueue_;
  std::shared_ptr<Histogram> incomingQueueTime_;
  std::shared_ptr<Counter> rpcsTimedOutInQueue_;
  std::shared_ptr<Counter> rpcsQueueOverflow_;

  mutable Mutex shutdownLock_;
  bool closing_;

  std::function<void(void)> tooBusyHook_;
  std::atomic<bool> loggedBusy_;

  DISALLOW_COPY_AND_ASSIGN(ServicePool);
  ServicePool(ServicePool&&) = delete;
  ServicePool& operator=(ServicePool&&) = delete;
};

} // namespace rpc
} // namespace kudu

#endif
