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

#include "kudu/rpc/acceptor_pool.h"

#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/basictypes.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace google {
namespace protobuf {

class Message;

}
} // namespace google

METRIC_DEFINE_counter(
    server,
    rpc_connections_accepted,
    "RPC Connections Accepted",
    kudu::MetricUnit::kConnections,
    "Number of incoming TCP connections made to the RPC server");

DEFINE_int32(
    rpc_acceptor_listen_backlog,
    128,
    "Socket backlog parameter used when listening for RPC connections. "
    "This defines the maximum length to which the queue of pending "
    "TCP connections inbound to the RPC server may grow. If a connection "
    "request arrives when the queue is full, the client may receive "
    "an error. Higher values may help the server ride over bursts of "
    "new inbound connection requests.");
TAG_FLAG(rpc_acceptor_listen_backlog, advanced);

namespace kudu {
namespace rpc {

AcceptorPool::AcceptorPool(
    Messenger* messenger,
    Socket* socket,
    Sockaddr bindAddress)
    : messenger_(messenger),
      socket_(socket->Release()),
      bindAddress_(bindAddress),
      rpcConnectionsAccepted_(METRIC_rpc_connections_accepted.Instantiate(
          messenger->metric_entity())),
      closing_(false) {}

AcceptorPool::~AcceptorPool() {
  shutdown();
}

Status AcceptorPool::start(int numThreads) {
  RETURN_NOT_OK(socket_.Listen(FLAGS_rpc_acceptor_listen_backlog));

  for (int i = 0; i < numThreads; i++) {
    std::shared_ptr<kudu::Thread> newThread;
    Status s = kudu::Thread::Create(
        "acceptor pool",
        "acceptor",
        &AcceptorPool::runThread,
        this,
        &newThread);
    if (!s.ok()) {
      shutdown();
      return s;
    }
    threads_.push_back(newThread);
  }
  return Status::OK();
}

void AcceptorPool::shutdown() {
  if (Acquire_CompareAndSwap(&closing_, false, true) != false) {
    VLOG(2) << "Acceptor Pool on " << bindAddress_.ToString()
            << " already shut down";
    return;
  }

#if defined(__linux__)
  // Closing the socket will break us out of accept() if we're in it, and
  // prevent future accepts.
  WARN_NOT_OK(
      socket_.Shutdown(true, true),
      fmt::format(
          "Could not shut down acceptor socket on {}",
          bindAddress_.ToString()));
#else
  // Calling shutdown on an accepting (non-connected) socket is illegal on most
  // platforms (but not Linux). Instead, the accepting threads are interrupted
  // forcefully.
  for (const std::shared_ptr<kudu::Thread>& thread : threads_) {
    pthread_cancel(thread.get()->pthreadId());
  }
#endif

  for (const std::shared_ptr<kudu::Thread>& thread : threads_) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }
  threads_.clear();

  // Close the socket: keeping the descriptor open and, possibly, receiving late
  // not-to-be-read messages from the peer does not make much sense. The
  // Socket::Close() method is called upon destruction of the aggregated socket_
  // object as well. However, the typical ownership pattern of an AcceptorPool
  // object includes two references wrapped via a shared_ptr smart pointer: one
  // is held by Messenger, another by RpcServer. If not calling Socket::Close()
  // here, it would  necessary to wait until Messenger::Shutdown() is called for
  // the corresponding messenger object to close this socket.
  ignoreResult(socket_.Close());
}

Sockaddr AcceptorPool::bindAddress() const {
  return bindAddress_;
}

Status AcceptorPool::getBoundAddress(Sockaddr* addr) const {
  return socket_.GetSocketAddress(addr);
}

int64_t AcceptorPool::numRpcConnectionsAccepted() const {
  return rpcConnectionsAccepted_->value();
}

void AcceptorPool::runThread() {
  while (true) {
    Socket newSock;
    Sockaddr remote;
    VLOG(2) << "calling accept() on socket " << socket_.GetFd()
            << " listening on " << bindAddress_.ToString();
    Status s = socket_.Accept(&newSock, &remote, Socket::kFlagNonblocking);
    if (!s.ok()) {
      if (Release_Load(&closing_)) {
        break;
      }
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "AcceptorPool: accept failed: " << s.ToString() << THROTTLE_MSG;
      continue;
    }
    s = newSock.SetNoDelay(true);
    if (!s.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "Acceptor with remote = " << remote.ToString()
          << " failed to set TCP_NODELAY on a newly accepted socket: "
          << s.ToString() << THROTTLE_MSG;
      continue;
    }
    rpcConnectionsAccepted_->Increment();
    messenger_->registerInboundSocket(&newSock, remote);
  }
  VLOG(1) << "AcceptorPool shutting down.";
}

} // namespace rpc
} // namespace kudu
