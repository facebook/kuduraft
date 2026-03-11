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

#include "kudu/tools/tool_proxy_builder.h"

#include <memory>
#include <string>
#include <vector>

#include "kudu/consensus/consensus.proxy.h" // IWYU pragma: keep
#include "kudu/rpc/messenger.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu::tools {

using consensus::ConsensusServiceProxy;
using rpc::Messenger;
using rpc::MessengerBuilder;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

template <class ProxyClass>
Status buildProxy(
    const string& address,
    uint16_t defaultPort,
    unique_ptr<ProxyClass>* proxy) {
  HostPort hp;
  RETURN_NOT_OK(hp.parseString(address, defaultPort));
  shared_ptr<Messenger> messenger;
  RETURN_NOT_OK(MessengerBuilder("tool").Build(&messenger));

  vector<Sockaddr> resolved;
  RETURN_NOT_OK(hp.resolveAddresses(&resolved));

  proxy->reset(new ProxyClass(messenger, resolved[0], hp.host()));
  return Status::OK();
}

// Explicit specialization for callers outside this compilation unit.
template Status buildProxy(
    const string& address,
    uint16_t defaultPort,
    unique_ptr<ConsensusServiceProxy>* proxy);

} // namespace kudu::tools
