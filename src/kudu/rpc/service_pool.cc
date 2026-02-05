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

#include "kudu/rpc/service_pool.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

using std::string;
using std::unique_ptr;
using std::vector;

METRIC_DEFINE_histogram(
    server,
    rpc_incoming_queue_time,
    "RPC Queue Time",
    kudu::MetricUnit::kMicroseconds,
    "Number of microseconds incoming RPC requests spend in the worker queue",
    60000000LU,
    3);

METRIC_DEFINE_counter(
    server,
    rpcs_timed_out_in_queue,
    "RPC Queue Timeouts",
    kudu::MetricUnit::kRequests,
    "Number of RPCs whose timeout elapsed while waiting "
    "in the service queue, and thus were not processed.");

METRIC_DEFINE_counter(
    server,
    rpcs_queue_overflow,
    "RPC Queue Overflows",
    kudu::MetricUnit::kRequests,
    "Number of RPCs dropped because the service queue "
    "was full.");

namespace kudu {
namespace rpc {

ServicePool::ServicePool(
    unique_ptr<ServiceIf> service,
    const std::shared_ptr<MetricEntity>& entity,
    size_t serviceQueueLength)
    : service_(std::move(service)),
      serviceQueue_(serviceQueueLength),
      incomingQueueTime_(METRIC_rpc_incoming_queue_time.Instantiate(entity)),
      rpcsTimedOutInQueue_(METRIC_rpcs_timed_out_in_queue.Instantiate(entity)),
      rpcsQueueOverflow_(METRIC_rpcs_queue_overflow.Instantiate(entity)),
      closing_(false),
      loggedBusy_(false) {}

ServicePool::~ServicePool() {
  Shutdown();
}

Status ServicePool::init(int numThreads) {
  for (int i = 0; i < numThreads; i++) {
    std::shared_ptr<kudu::Thread> new_thread;
    CHECK_OK(
        kudu::Thread::Create(
            "service pool",
            "rpc_worker",
            &ServicePool::runThread,
            this,
            &new_thread));
    threads_.push_back(new_thread);
  }
  return Status::OK();
}

void ServicePool::Shutdown() {
  serviceQueue_.Shutdown();

  MutexLock lock(shutdownLock_);
  if (closing_) {
    return;
  }
  closing_ = true;
  // TODO: Use a proper thread pool implementation.
  for (std::shared_ptr<kudu::Thread>& thread : threads_) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }

  // Now we must drain the service queue.
  Status status = Status::ServiceUnavailable("Service is shutting down");
  std::unique_ptr<InboundCall> incoming;
  while (serviceQueue_.BlockingGet(&incoming)) {
    incoming.release()->RespondFailure(
        ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  }

  service_->Shutdown();
}

void ServicePool::rejectTooBusy(InboundCall* c) {
  string err_msg = fmt::format(
      "{} request on {} from {} dropped due to backpressure. "
      "The service queue is full; it has {} items.",
      c->remote_method().method_name(),
      service_->service_name(),
      c->remote_address().ToString(),
      serviceQueue_.maxSize());
  rpcsQueueOverflow_->Increment();
  KLOG_EVERY_N_SECS(WARNING, 300) << err_msg;
  c->RespondFailure(
      ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
      Status::ServiceUnavailable(err_msg));

  if (!loggedBusy_.load(std::memory_order_acquire)) {
    // throttle, as don't want to flood log with lines if
    // pool is always at edge of queue.
    KLOG_EVERY_N_SECS(WARNING, 600)
        << err_msg << " Contents of service queue:\n"
        << serviceQueue_.ToString();
    loggedBusy_ = true;
  }

  if (tooBusyHook_) {
    tooBusyHook_();
  }
}

std::string ServicePool::rpcServiceQueueToString() const {
  return serviceQueue_.ToString();
}

RpcMethodInfo* ServicePool::LookupMethod(const RemoteMethod& method) {
  return service_->LookupMethod(method);
}

Status ServicePool::QueueInboundCall(unique_ptr<InboundCall> call) {
  InboundCall* c = call.release();

  vector<uint32_t> unsupported_features;
  for (uint32_t feature : c->GetRequiredFeatures()) {
    if (!service_->SupportsFeature(feature)) {
      unsupported_features.push_back(feature);
    }
  }

  if (!unsupported_features.empty()) {
    c->RespondUnsupportedFeature(unsupported_features);
    return Status::NotSupported(
        "call requires unsupported application feature flags",
        JoinMapped(
            unsupported_features,
            [](uint32_t flag) { return std::to_string(flag); },
            ", "));
  }

  TRACE_TO(c->trace(), "Inserting onto call queue");

  // Queue message on service queue
  std::optional<InboundCall*> evicted;
  auto queue_status = serviceQueue_.Put(c, &evicted);
  if (queue_status == kQueueFull) {
    rejectTooBusy(c);
    return Status::OK();
  }

  if (PREDICT_FALSE(evicted.has_value())) {
    rejectTooBusy(*evicted);
  }

  // success in enqueu. Clear the printed state for busy
  loggedBusy_.store(false, std::memory_order_release);

  if (PREDICT_TRUE(queue_status == kQueueSuccess)) {
    // NB: do not do anything with 'c' after it is successfully queued --
    // a service thread may have already dequeued it, processed it, and
    // responded by this point, in which case the pointer would be invalid.
    return Status::OK();
  }

  Status status = Status::OK();
  if (queue_status == kQueueShutdown) {
    status = Status::ServiceUnavailable("Service is shutting down");
    c->RespondFailure(ErrorStatusPB::FATAL_SERVER_SHUTTING_DOWN, status);
  } else {
    status = Status::RuntimeError(
        fmt::format("Unknown error from BlockingQueue: {}", queue_status));
    c->RespondFailure(ErrorStatusPB::FATAL_UNKNOWN, status);
  }
  return status;
}

void ServicePool::NotifyLongCallLoading(const RemoteMethod& method) {
  service_->NotifyLongCallLoading(method);
}

void ServicePool::NotifyLongCallLoaded(const RemoteMethod& method) {
  service_->NotifyLongCallLoaded(method);
}

void ServicePool::runThread() {
  while (true) {
    std::unique_ptr<InboundCall> incoming;
    if (!serviceQueue_.BlockingGet(&incoming)) {
      VLOG(1) << "ServicePool: messenger shutting down.";
      return;
    }

    incoming->RecordHandlingStarted(incomingQueueTime_.get());
    ADOPT_TRACE(incoming->trace());

    if (PREDICT_FALSE(incoming->ClientTimedOut())) {
      TRACE_TO(
          incoming->trace(), "Skipping call since client already timed out");
      rpcsTimedOutInQueue_->Increment();

      // Respond as a failure, even though the client will probably ignore
      // the response anyway.
      incoming->RespondFailure(
          ErrorStatusPB::ERROR_SERVER_TOO_BUSY,
          Status::TimedOut("Call waited in the queue past client deadline"));

      // Must release since RespondFailure above ends up taking ownership
      // of the object.
      ignore_result(incoming.release());
      continue;
    }

    TRACE_TO(incoming->trace(), "Handling call");

    // Release the InboundCall pointer -- when the call is responded to,
    // it will get deleted at that point.
    service_->Handle(incoming.release());
  }
}

const string ServicePool::service_name() const {
  return service_->service_name();
}

} // namespace rpc
} // namespace kudu
