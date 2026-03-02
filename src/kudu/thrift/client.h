// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Utilities for working with Thrift clients.

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/port.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace apache

namespace kudu {
namespace thrift {

// Options for a Thrift client connection.
struct ClientOptions {
  // Thrift socket send timeout
  MonoDelta sendTimeout = MonoDelta::FromSeconds(60);

  // Thrift socket receive timeout.
  MonoDelta recvTimeout = MonoDelta::FromSeconds(60);

  // Thrift socket connect timeout.
  MonoDelta connTimeout = MonoDelta::FromSeconds(60);

  // Number of times an RPC is retried by the HA client after encountering
  // retriable failures, such as network failures.
  int32_t retryCount = 1;
};

std::shared_ptr<apache::thrift::protocol::TProtocol> createClientProtocol(
    const HostPort& address,
    const ClientOptions& options);

// Returns 'true' if the error should result in the Thrift client being torn
// down.
bool isFatalError(const Status& error);

// A wrapper class around a Thrift service client which provides support for HA
// service configurations, retrying, backoff, and fault-tolerance.
//
// This client manages the lifecycle of the underlying Thrift clients,
// automatically reconnecting on faults and retrying requests.
//
// This class is thread safe after start() is called.
template <typename Service>
class HaClient {
 public:
  HaClient();
  ~HaClient();

  // Starts the highly available Thrift service client instance.
  Status start(std::vector<HostPort> addresses, ClientOptions options);

  // Stops the highly available Thrift service client instance.
  void stop();

  // Synchronously executes a task with exclusive access to the thrift service
  // client.
  Status execute(std::function<Status(Service*)> task) WARN_UNUSED_RESULT;

 private:
  // Reconnects to an instance of the Thrift service, or returns an error if all
  // service instances are unavailable.
  Status reconnect();

  // Background thread which executes calls to the Thrift service.
  std::unique_ptr<ThreadPool> threadpool_;

  // Client options.
  std::vector<HostPort> addresses_;
  ClientOptions options_;

  // The actual client service instance (HmsClient or SentryClient).
  Service serviceClient_;

  // Fields which track consecutive reconnection attempts and backoff.
  MonoTime reconnectAfter_;
  Status reconnectFailure_;
  int consecutiveReconnectFailures_;
  int reconnectIdx_;
};

///////////////////////////////////////////////////////////////////////////////
// HaClient class definitions
//
// HaClient is defined inline so that it can be instantiated with HmsClient and
// SentryClient as template parameters, which live in modules which are not
// linked to the thrift module.
///////////////////////////////////////////////////////////////////////////////

template <typename Service>
HaClient<Service>::HaClient()
    : serviceClient_(HostPort("", 0), options_),
      reconnectAfter_(MonoTime::Now()),
      reconnectFailure_(Status::OK()),
      consecutiveReconnectFailures_(0),
      reconnectIdx_(0) {}

template <typename Service>
HaClient<Service>::~HaClient() {
  stop();
}

template <typename Service>
Status HaClient<Service>::start(
    std::vector<HostPort> addresses,
    ClientOptions options) {
  if (threadpool_) {
    return Status::IllegalState(
        fmt::format("{} HA client is already started", Service::kServiceName));
  }

  addresses_ = std::move(addresses);
  options_ = std::move(options);

  // The thread pool must be capped at one thread to ensure serialized access to
  // the fields of the service client (which isn't thread safe).
  RETURN_NOT_OK(ThreadPoolBuilder(Service::kServiceName)
                    .set_min_threads(1)
                    .set_max_threads(1)
                    .Build(&threadpool_));

  return Status::OK();
}

template <typename Service>
void HaClient<Service>::stop() {
  if (threadpool_) {
    threadpool_->Shutdown();
  }
}

template <typename Service>
Status HaClient<Service>::execute(std::function<Status(Service*)> task) {
  Synchronizer synchronizer;
  auto callback = synchronizer.asStdStatusCallback();

  // TODO(todd): wrapping this in a TRACE_EVENT scope and a LOG_IF_SLOW and such
  // would be helpful. Perhaps a TRACE message and/or a TRACE_COUNTER_INCREMENT
  // too to keep track of how much time is spent in calls to the Thrift client.
  // That will also require propagating the current Trace object into the 'Rpc'
  // object. Note that the Thrift client classes already have LOG_IF_SLOW calls
  // internally.

  RETURN_NOT_OK(threadpool_->SubmitFunc([=, this] {
    // The main run routine of the threadpool thread. Runs the task with
    // exclusive access to the Thrift service client. If the task fails, it will
    // be retried, unless the failure type is non-retriable or the maximum
    // number of retries has been exceeded. Also handles re-connecting the
    // Thrift service client after a fatal error.
    //
    // Since every task submitted to the (single thread) pool runs this, it's
    // essentially a single iteration of a run loop which handles service client
    // reconnection and task processing.
    //
    // Notes on error handling:
    //
    // There are three separate error scenarios below:
    //
    // * Error while (re)connecting the client - This is considered a
    // 'non-recoverable' error. The current task is immediately failed. In order
    // to avoid hot-looping and hammering the service with reconnect attempts on
    // every queued task, we set a backoff period. Any tasks which subsequently
    // run during this backoff period are also immediately failed.
    //
    // * Task results in a fatal error - a fatal error is any error caused by a
    // network or IO fault (not an application level failure). The HA client
    // will attempt to reconnect, and the task will be retried (up to a limit).
    //
    // * Task results in a non-fatal error - a non-fatal error is an application
    // level error, and causes the task to be failed immediately (no retries).

    // Keep track of the first attempt's failure. Typically the first failure is
    // the most informative.
    Status firstFailure;

    for (int attempt = 0; attempt <= options_.retryCount; attempt++) {
      if (!serviceClient_.IsConnected()) {
        if (reconnectAfter_ > MonoTime::Now()) {
          // Not yet ready to attempt reconnection; fail the task immediately.
          DCHECK(!reconnectFailure_.ok());
          return callback(reconnectFailure_);
        }

        // Attempt to reconnect.
        Status reconnectStatus = reconnect();
        if (!reconnectStatus.ok()) {
          // Reconnect failed; retry with exponential backoff capped at 10s and
          // fail the task. We don't bother with jitter here because only the
          // leader master should be attempting this in any given period per
          // cluster.
          consecutiveReconnectFailures_++;
          reconnectAfter_ = MonoTime::Now() +
              std::min(MonoDelta::FromMilliseconds(
                           100 << consecutiveReconnectFailures_),
                       MonoDelta::FromSeconds(10));
          reconnectFailure_ = std::move(reconnectStatus);
          return callback(reconnectFailure_);
        }

        consecutiveReconnectFailures_ = 0;
      }

      // Execute the task.
      Status taskStatus = task(&serviceClient_);

      // If the task succeeds, or it's a non-retriable error, return the result.
      if (taskStatus.ok() || !isFatalError(taskStatus)) {
        return callback(taskStatus);
      }

      // A fatal error occurred. Tear down the connection, and try again. We
      // don't log loudly here because odds are the reconnection will fail if
      // it's a true fault, at which point we do log loudly.
      VLOG(1) << fmt::format(
          "Call to {} failed: {}",
          Service::kServiceName,
          taskStatus.ToString());

      if (attempt == 0) {
        firstFailure = std::move(taskStatus);
      }

      WARN_NOT_OK(
          serviceClient_.Stop(),
          fmt::format("Failed to stop {} client", Service::kServiceName));
    }

    // We've exhausted the allowed retries.
    DCHECK(!firstFailure.ok());
    LOG(WARNING) << fmt::format(
        "Call to {} failed after {} retries: {}",
        Service::kServiceName,
        options_.retryCount,
        firstFailure.ToString());

    return callback(firstFailure);
  }));

  return synchronizer.wait();
}

// Note: Thrift provides a handy TSocketPool class which could be useful in
// allowing the Thrift client to transparently handle connecting to a pool of HA
// service instances. However, because TSocketPool handles choosing the instance
// during socket connect, it can't determine if the remote endpoint is actually
// a Thrift service, or just a random listening TCP socket. Nor can it do
// application-level checks like ensuring that the connected service is
// configured correctly. So, it's better to handle reconnecting and failover in
// this higher-level construct.
template <typename Service>
Status HaClient<Service>::reconnect() {
  Status s;

  // Try reconnecting to each service instance in sequence, returning the first
  // one which succeeds. In order to avoid getting 'stuck' on a partially failed
  // instance, we remember which we connected to previously and try it last.
  for (int i = 0; i < addresses_.size(); i++) {
    const auto& address = addresses_[reconnectIdx_];
    reconnectIdx_ = (reconnectIdx_ + 1) % addresses_.size();

    serviceClient_ = Service(address, options_);
    s = serviceClient_.Start();
    if (s.ok()) {
      VLOG(1) << fmt::format(
          "Connected to {} {}", Service::kServiceName, address.ToString());
      return Status::OK();
    }

    WARN_NOT_OK(
        s,
        fmt::format(
            "Failed to connect to {} ({})",
            Service::kServiceName,
            address.ToString()))
  }

  WARN_NOT_OK(
      serviceClient_.Stop(),
      fmt::format("Failed to stop {} client", Service::kServiceName));
  return s;
}
} // namespace thrift
} // namespace kudu
