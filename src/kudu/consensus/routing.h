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

#include <memory>
#include <string>
#include <unordered_map>

#include <optional>

#include <folly/SharedMutex.h>
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/proxy_policy.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/rwc_lock.h"

namespace kudu {

class Status;

namespace consensus {

class RegionGroupRoutingTable;

// An interface that needs to be implemented to support different proxy policy.
// Each implementation manages the routing table/proxy topology according to the
// rules defined for that policy. Check proxy_policy.h for different supported
// policy types
class IRoutingTable {
 public:
  IRoutingTable() = default;
  virtual ~IRoutingTable() = default;

  // Returns the uuid of the next 'proxy_peer' in 'nextHopOut'.
  // 'srcUuid' is the uuid of the peer who is sending the message. 'destUuid'
  // is the uuid of the peer to which message is intended.
  virtual Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHopOut) const = 0;

  // Called each time the raft config is updated. Internal state is also updated
  // based on proxy policy.
  virtual Status updateRaftConfig(RaftConfigPB raftConfig) = 0;

  // Called each time the leaderUuid changes (due to detection of a new leader)
  virtual void updateLeader(std::string leaderUuid) = 0;

  // Updates the proxy topology that is used to route a request from source to
  // destination. may be a no-op in some routing policies.
  virtual Status updateProxyTopology(ProxyTopologyPB proxyTopology) = 0;

  // returns the current proxy topology
  virtual ProxyTopologyPB getProxyTopology() const = 0;

  // Get the proxy policy based on which this table operates
  virtual ProxyPolicy getProxyPolicy() const = 0;
};

// A class that calculates the route that a message should take when being
// proxied across a topology, given a Raft config and a leader.
//
// For example, given the following topology, where parents in the tree are
// defined by setting the proxy_from field in the Raft config:
//
//              A            G
//             / \          / \
//            B   C*       H   I
//           / \   \
//          D   E   F
//
// and given that C is the leader, this implementation will assume there is a
// direct route from C to G and thus construct a single-tree topology that
// looks like the following:
//
//               A
//             /   \
//            B     C*
//           / \   / \
//          D   E F   G
//                   / \
//                  H   I
//
// Of course, the route from C to F will be C -> F.
// Similarly, the route from C to I will be C -> G -> I.
// To reach D from C, the route will be C -> A -> B -> D.
// Naturally, the next hop from A to E will be B.
//
// This class is NOT thread-safe and must be externally synchronized..
class RoutingTable {
 public:
  // Initialize the routing table. Safe to call multiple times.
  //
  // Returns Status::Incomplete as an information warning, yet successfully
  // initializes the routing table, if any proxy_from edges specified in
  // ProxyTopologyPB do not appear RaftConfigPB. In such cases, direct routing
  // to those destinations will be used. If this is not desired, treat
  // Status::Incomplete as an error.
  //
  // All other non-OK Status codes are errors and the routing table will not be
  // left in a defined state.
  Status init(
      const RaftConfigPB& raftConfig,
      const ProxyTopologyPB& proxyTopology,
      const std::string& leaderUuid);

  // Find the UUID of the next hop, given the UUIDs of the current source
  // and the ultimate destination.
  Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHopOut) const;

  // Return a string representation of the routing topology.
  std::string toString() const;

 private:
  // A node representing a raft peer in a hierarchy with associated routing
  // rules for proxied messages.
  struct Node {
    explicit Node(RaftPeerPB peerPb) : peerPb(std::move(peerPb)) {}

    const std::string& id() const {
      return peerPb.permanent_uuid();
    }

    const RaftPeerPB peerPb;
    Node* proxyFrom = nullptr;

    // children: child uuid -> child Node
    std::unordered_map<std::string, std::unique_ptr<Node>> children;
    // routes: dest uuid -> next hop uuid
    std::unordered_map<std::string, std::string> routes;
  };

  // Construct a forest of Node trees that represent proxy_from relationships.
  // Any Node that does not have a proxy_from specified in the proxy topology
  // will appear as a root Node in the forest.
  //
  // Output:
  //   index: An index keyed by the UUID of each Node.
  //   forest: Each tree is rooted in a Node with no "proxy_from" specified.
  //
  // Returns InvalidArgument and fails if duplicate peers appear in the
  // RaftConfigPB or if multiple proxy_from edges are specified for the same
  // destination in the ProxyTopologyPB.
  //
  // Returns Incomplete as a warning, but successfully initializes the output
  // variables, if any proxy_from peers specified in ProxyTopologyPB are not
  // found in RaftConfigPB.
  Status constructForest(
      const RaftConfigPB& raftConfig,
      const ProxyTopologyPB& proxyTopology,
      std::unordered_map<std::string, Node*>* index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* forest);

  // Reorganize the given forest into a single routing tree by moving the roots
  // of Node trees that don't include the leader under the leader as children.
  // The leader must appear in the index. If it does not, InvalidArgument is
  // returned.
  Status mergeForestIntoSingleRoutingTree(
      const std::string& leaderUuid,
      const std::unordered_map<std::string, Node*>& index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* forest);

  // Recursively construct the next-hop indices at each node. We run DFS to
  // determine routes because there is only one route to each node from the
  // root.
  void constructNextHopIndicesRec(Node* cur);

  // Recursive helper for DFS to build the debug string emitted by toString().
  void toStringHelperRec(Node* cur, int level, std::string* out) const;

  bool hasExplicitRoutes_{false}; // Whether there are any topology edges.
  std::unique_ptr<Node> topologyRoot_;
  std::unordered_map<std::string, Node*> index_;
};

// Thread-safe and durable metadata layer on top of RoutingTable. Only keeps
// the ProxyTopologyPB durable. Ensures that (at most) a single instance of
// RoutingTable is active at any given moment.
//
// DurableRoutingTable differs behaviorally from RoutingTable when the leader
// is unknown. For the details, the header doc for NextHop().
//
class DurableRoutingTable : public IRoutingTable {
 public:
  ~DurableRoutingTable() override = default;

  enum class LoadOptions { kDoNotCreate, kCreateEmptyIfDoesNotExist };

  // Initialize for the first time and write to disk.
  static Status create(
      FsManager* fsManager,
      std::string tabletId,
      RaftConfigPB raftConfig,
      ProxyTopologyPB proxyTopology,
      std::shared_ptr<DurableRoutingTable>* drt);

  // Read from disk.
  static Status load(
      FsManager* fsManager,
      std::string tabletId,
      RaftConfigPB raftConfig,
      LoadOptions opts,
      std::shared_ptr<DurableRoutingTable>* drt);

  // Delete the on-disk data for the DRT.
  static Status deleteOnDiskData(
      FsManager* fsManager,
      const std::string& tabletId);

  // Called when the proxy graph changes.
  Status updateProxyTopology(ProxyTopologyPB proxyTopology) override;

  // Called when the Raft config changes.
  Status updateRaftConfig(RaftConfigPB raftConfig) override;

  // Called when the leader changes.
  void updateLeader(std::string leaderUuid) override;

  // If the leader is known and 'destUuid' is in the raft config, returns the
  // next hop along the route to reach 'destUuid'. If 'destUuid' is not a
  // member of the config, returns a Status::NotFound error. If there is no
  // known leader, but 'destUuid' is a member of the raft config, returns
  // 'destUuid' to directly route to the node, ignoring normal proxy routing
  // rules, since proxying routes are only defined when the leader is known.
  Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHopOut) const override;

  // Return the currently active proxy topology.
  ProxyTopologyPB getProxyTopology() const override;

  // Get proxy policy based on which this table operates (DurableRoutingPolicy)
  ProxyPolicy getProxyPolicy() const override;

  // Return a string representation of the routing topology.
  std::string toString() const;

 private:
  DurableRoutingTable(
      FsManager* fsManager,
      std::string tabletId,
      ProxyTopologyPB proxyTopology,
      RaftConfigPB raftConfig);

  // We flush a new ProxyTopologyPB to disk before committing the updated
  // version to memory. This method is not thread-safe and must be synchronized
  // by taking the lock or similar.
  Status flush() const;

  // Thread-safe log prefix helper.
  std::string LogPrefix() const;

  FsManager* fsManager_;
  const std::string tabletId_;

  mutable RwcLock lock_; // read-write-commit lock protecting the below fields
  ProxyTopologyPB proxyTopology_;
  RaftConfigPB raftConfig_;
  std::optional<std::string> leaderUuid_; // We don't always know who is leader.
  std::optional<RoutingTable>
      routingTable_; // When leader is unknown, the route is undefined.
};

// A simple 'region' based routing table. Check proxy_policy.h for more
// information. This table is intantiated when proxy policy is set to
// ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY.
class SimpleRegionRoutingTable : public IRoutingTable {
 public:
  ~SimpleRegionRoutingTable() override = default;

  Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHopOut) const override;

  Status updateRaftConfig(RaftConfigPB raftConfig) override;
  void updateLeader(std::string leaderUuid) override;
  ProxyTopologyPB getProxyTopology() const override;
  Status updateProxyTopology(ProxyTopologyPB proxyTopology) override;
  void setLocalPeerPb(RaftPeerPB localPeerPb);
  ProxyPolicy getProxyPolicy() const override;

  static Status create(
      RaftConfigPB raftConfig,
      RaftPeerPB localPeerPb,
      std::shared_ptr<SimpleRegionRoutingTable>* srt);

 private:
  Status rebuildProxyTopology(RaftConfigPB raftConfig);

  // Lock protecting below fields
  mutable folly::SharedMutexTracked lock_;
  ProxyTopologyPB proxyTopology_;
  RaftConfigPB raftConfig_;
  RaftPeerPB localPeerPb_;
  std::optional<std::string> leaderUuid_;
  std::unordered_map<std::string, std::string> dstToProxyMap_;
};

// A container to hols all available routing tables (implemented based on
// routing policy). All routing tables are created during bootstrap. The table
// that gets used for routing is based on 'proxy_policy_'.
class RoutingTableContainer {
 public:
  RoutingTableContainer(
      const ProxyPolicy& proxyPolicy,
      const RaftPeerPB& localPeerPb,
      RaftConfigPB raftConfig,
      std::shared_ptr<DurableRoutingTable> drt,
      const std::vector<std::unordered_set<std::string>>& regionGroups);

  // Returns the uuid of the next 'proxy_peer' in 'nextHopOut'.
  // 'srcUuid' is the uuid of the peer who is sending the message. 'destUuid'
  // is the uuid of the peer to which message is intended.
  Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHopOut) const;

  // Called each time the raft config is updated. Internal state is also updated
  // based on proxy policy.
  Status updateRaftConfig(RaftConfigPB raftConfig);

  // Called each time the leaderUuid changes (due to detection of a new leader)
  void updateLeader(std::string leaderUuid);

  // returns the current proxy topology used by the current proxyPolicy_
  ProxyTopologyPB getProxyTopology() const;

  // Updates the proxy topology that is used to route a request from source to
  // destination. may be a no-op in some routing policies.
  Status updateProxyTopology(
      ProxyTopologyPB proxyTopology,
      RaftConfigPB raftConfig,
      const std::string& leaderUuid);

  Status updateProxyRegionGroup(
      const std::vector<std::unordered_set<std::string>>& regionGroups,
      RaftConfigPB raftConfig,
      const std::string& leaderUuid);
  std::vector<std::unordered_set<std::string>> getProxyRegionGroup();

  void updateRtt(const std::string& peerUuid, std::chrono::microseconds rtt);

  // Updates the locak_peer on all tables that use it
  void setLocalPeerPb(RaftPeerPB localPeerPb);

  // returns the current proxyPolicy_
  ProxyPolicy getProxyPolicy() const;

  // Sets the proxy policy in use to 'proxyPolicy'
  // Also updates the leaderUuid and raftConfig on all managed routing tables.
  // This allows individual routing tables to update rebild their topology and
  // routing rules
  Status setProxyPolicy(
      const ProxyPolicy& proxyPolicy,
      const std::string& leaderUuid,
      RaftConfigPB raftConfig);

 private:
  std::atomic<ProxyPolicy> proxyPolicy_;
  std::shared_ptr<SimpleRegionRoutingTable> srt_;
  std::shared_ptr<RegionGroupRoutingTable> grt_;
  std::shared_ptr<DurableRoutingTable> drt_;
};

// Verify that a ProxyTopologyPB is well-formed.
// Returns OK if no duplicates, empty strings, or self-loops are detected.
// Does not attempt to perform multi-hop loop detection because the final
// routing topology is not defined without a Raft config and leader.
Status verifyProxyTopology(const ProxyTopologyPB& proxyTopology);

// Helper function to check if a peer can be a proxy peer
// Used by SimpleRegionRoutingTable and RegionGroupRoutingTable
bool canBeProxyPeer(const RaftPeerPB& peer);
} // namespace consensus
} // namespace kudu
