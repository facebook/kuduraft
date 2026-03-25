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

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using kudu::rpc::AcceptorPool;
using kudu::rpc::Messenger;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_string(
    rpc_bind_addresses,
    "0.0.0.0",
    "Comma-separated list of addresses to bind to for RPC connections. "
    "Currently, ephemeral ports (i.e. port 0) are not allowed.");
TAG_FLAG(rpc_bind_addresses, stable);

DEFINE_string(
    rpc_advertised_addresses,
    "",
    "Comma-separated list of addresses to advertise externally for RPC "
    "connections. Ephemeral ports (i.e. port 0) are not allowed. This "
    "should be configured when the locally bound RPC addresses "
    "specified in --rpc_bind_addresses are not externally resolvable, "
    "for example, if Kudu is deployed in a container.");
TAG_FLAG(rpc_advertised_addresses, advanced);

DEFINE_int32(
    rpc_num_acceptors_per_address,
    1,
    "Number of RPC acceptor threads for each bound address");
TAG_FLAG(rpc_num_acceptors_per_address, advanced);

DEFINE_int32(
    rpc_num_service_threads,
    10,
    "Number of RPC worker threads to run");
TAG_FLAG(rpc_num_service_threads, advanced);

DEFINE_int32(
    rpc_service_queue_length,
    50,
    "Default length of queue for incoming RPC requests");
TAG_FLAG(rpc_service_queue_length, advanced);

DEFINE_bool(
    rpc_server_allow_ephemeral_ports,
    false,
    "Allow binding to ephemeral ports. This can cause problems, so currently "
    "only allowed in tests.");
TAG_FLAG(rpc_server_allow_ephemeral_ports, unsafe);

namespace kudu {

RpcServerOptions::RpcServerOptions()
    : rpc_bind_addresses(FLAGS_rpc_bind_addresses),
      rpc_advertised_addresses(FLAGS_rpc_advertised_addresses),
      num_acceptors_per_address(FLAGS_rpc_num_acceptors_per_address),
      num_service_threads(FLAGS_rpc_num_service_threads),
      default_port(0),
      service_queue_length(FLAGS_rpc_service_queue_length) {}

RpcServer::RpcServer(RpcServerOptions opts)
    : serverState_(kUninitialized), options_(std::move(opts)) {}

RpcServer::~RpcServer() {
  Shutdown();
}

string RpcServer::ToString() const {
  // TODO: include port numbers, etc.
  return "RpcServer";
}

Status RpcServer::Init(const shared_ptr<Messenger>& messenger) {
  CHECK_EQ(serverState_, kUninitialized);
  messenger_ = messenger;

  RETURN_NOT_OK(parseAddressList(
      options_.rpc_bind_addresses, options_.default_port, &rpcBindAddresses_));
  for (const Sockaddr& addr : rpcBindAddresses_) {
    if (isPrivilegedPort(addr.port())) {
      LOG(WARNING) << "May be unable to bind to privileged port for address "
                   << addr.ToString();
    }

    // Currently, we can't support binding to ephemeral ports outside of
    // unit tests, because consensus caches RPC ports of other servers
    // across restarts. See KUDU-334.
    if (addr.port() == 0 && !FLAGS_rpc_server_allow_ephemeral_ports) {
      LOG(FATAL) << "Binding to ephemeral ports not supported (RPC address "
                 << "configured to " << addr.ToString() << ")";
    }
  }

  if (!options_.rpc_advertised_addresses.empty()) {
    RETURN_NOT_OK(parseAddressList(
        options_.rpc_advertised_addresses,
        options_.default_port,
        &rpcAdvertisedAddresses_));

    for (const Sockaddr& addr : rpcAdvertisedAddresses_) {
      if (addr.port() == 0) {
        LOG(FATAL)
            << "Advertising an ephemeral port is not supported (RPC advertised address "
            << "configured to " << addr.ToString() << ")";
      }
    }
  }

  serverState_ = kInitialized;
  return Status::OK();
}

Status RpcServer::RegisterService(unique_ptr<rpc::ServiceIf> service) {
  CHECK(serverState_ == kInitialized || serverState_ == kBound)
      << "bad state: " << serverState_;
  string service_name = service->serviceName();
  std::shared_ptr<rpc::ServicePool> service_pool(new rpc::ServicePool(
      std::move(service),
      messenger_->metric_entity(),
      options_.service_queue_length));
  RETURN_NOT_OK(service_pool->init(options_.num_service_threads));
  auto* service_pool_raw_ptr = service_pool.get();
  service_pool->setTooBusyHook([this, service_pool_raw_ptr]() {
    if (tooBusyHook_) {
      tooBusyHook_(service_pool_raw_ptr);
    }
  });
  RETURN_NOT_OK(messenger_->RegisterService(service_name, service_pool));
  return Status::OK();
}

Status RpcServer::Bind() {
  CHECK_EQ(serverState_, kInitialized);

  // Create the Acceptor pools (one per bind address)
  vector<shared_ptr<AcceptorPool>> new_acceptor_pools;
  // Create the AcceptorPool for each bind address.
  for (const Sockaddr& bind_addr : rpcBindAddresses_) {
    shared_ptr<rpc::AcceptorPool> pool;

    Socket sock;
    RETURN_NOT_OK(sock.Init(0));
    RETURN_NOT_OK(sock.SetReuseAddr(true));
    RETURN_NOT_OK(sock.Bind(bind_addr));
    Sockaddr remote;
    RETURN_NOT_OK(sock.GetSocketAddress(&remote));
    new_acceptor_pools.push_back(
        std::make_shared<AcceptorPool>(messenger_.get(), &sock, remote));
  }
  acceptorPools_.swap(new_acceptor_pools);

  serverState_ = kBound;
  return Status::OK();
}

Status RpcServer::Start() {
  if (serverState_ == kInitialized) {
    RETURN_NOT_OK(Bind());
  }
  CHECK_EQ(serverState_, kBound);
  serverState_ = kStarted;

  for (const shared_ptr<AcceptorPool>& pool : acceptorPools_) {
    RETURN_NOT_OK(pool->start(options_.num_acceptors_per_address));
  }

  vector<Sockaddr> bound_addrs;
  RETURN_NOT_OK(GetBoundAddresses(&bound_addrs));
  string bound_addrs_str;
  for (const Sockaddr& bind_addr : bound_addrs) {
    if (!bound_addrs_str.empty()) {
      bound_addrs_str += ", ";
    }
    bound_addrs_str += bind_addr.ToString();
  }
  LOG(INFO) << "RPC server started. Bound to: " << bound_addrs_str;

  return Status::OK();
}

void RpcServer::Shutdown() {
  for (const shared_ptr<AcceptorPool>& pool : acceptorPools_) {
    pool->shutdown();
  }
  acceptorPools_.clear();

  if (messenger_) {
    messenger_->UnregisterAllServices();
  }
}

Status RpcServer::GetBoundAddresses(vector<Sockaddr>* addresses) const {
  if (serverState_ != kBound && serverState_ != kStarted) {
    return Status::ServiceUnavailable(
        fmt::format("bad state: {}", serverState_));
  }
  for (const shared_ptr<AcceptorPool>& pool : acceptorPools_) {
    Sockaddr bound_addr;
    RETURN_NOT_OK_PREPEND(
        pool->getBoundAddress(&bound_addr),
        "Unable to get bound address from AcceptorPool");
    addresses->push_back(bound_addr);
  }
  return Status::OK();
}

Status RpcServer::GetAdvertisedAddresses(vector<Sockaddr>* addresses) const {
  if (serverState_ != kBound && serverState_ != kStarted) {
    return Status::ServiceUnavailable(
        fmt::format("bad state: {}", serverState_));
  }
  if (rpcAdvertisedAddresses_.empty()) {
    return GetBoundAddresses(addresses);
  }
  *addresses = rpcAdvertisedAddresses_;
  return Status::OK();
}

const rpc::ServicePool* RpcServer::servicePool(
    const string& service_name) const {
  return kudu::down_cast<rpc::ServicePool*>(
      messenger_->rpc_service(service_name).get());
}

} // namespace kudu
