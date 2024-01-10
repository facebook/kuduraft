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

#pragma once

namespace kudu::consensus {

enum class ProxyPolicy {
  // Proxy is disabled and the leader ships ops to all peers directly
  DISABLE_PROXY = 1,
  // Proxy routing is built implicily using current active raft configs.
  // One peer is choosen to act as a 'proxy peer' in a given region.
  // The rules used to build proxy topology in this policy is:
  // 1. A given region has at-most one valid proxy peer.
  // 2. The proxy peer in a region is always backed by a database i.e a peer
  //    that acts as a witness (of raft log) cannot be a proxy peer.
  // 3. A region can have no valid proxy peers, in which case the leader ships
  //    ops directly to all peers in that region.
  // 4. peers that get proxied through an intermediate (proxy) peer should
  //    not be backed by a database.
  // 5. Leader ships ops to all peers in its own region i.e no proxying in
  //    leader's region.
  SIMPLE_REGION_ROUTING_POLICY = 2,
  // Routing topology needs to be explicilty supplied by external entities
  // Read DurableRoutingTable in routing.h/routing.cc for more details
  DURABLE_ROUTING_POLICY = 3,
};
} // namespace kudu::consensus
