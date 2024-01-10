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

#include <boost/optional/optional.hpp>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/proxy_policy.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/rwc_lock.h"

namespace kudu {

class Status;

namespace consensus {

// An interface that needs to be implemented to support different proxy policy.
// Each implementation manages the routing table/proxy topology according to the
// rules defined for that policy. Check proxy_policy.h for different supported
// policy types
class IRoutingTable {
 public:
  IRoutingTable() = default;
  virtual ~IRoutingTable() = default;

  // Returns the uuid of the next 'proxy_peer' in 'next_hop'.
  // 'src_uuid' is the uuid of the peer who is sending the message. 'dest_uuid'
  // is the uuid of the peer to which message is intended.
  virtual Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const = 0;

  // Called each time the raft config is updated. Internal state is also updated
  // based on proxy policy.
  virtual Status UpdateRaftConfig(RaftConfigPB raft_config) = 0;

  // Called each time the leader_uuid changes (due to detection of a new leader)
  virtual void UpdateLeader(std::string leader_uuid) = 0;

  // Updates the proxy topology that is used to route a request from source to
  // destination. may be a no-op in some routing policies.
  virtual Status UpdateProxyTopology(ProxyTopologyPB proxy_topology) = 0;

  // returns the current proxy topology
  virtual ProxyTopologyPB GetProxyTopology() const = 0;

  // Get the proxy policy based on which this table operates
  virtual ProxyPolicy GetProxyPolicy() const = 0;
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
  Status Init(
      const RaftConfigPB& raft_config,
      const ProxyTopologyPB& proxy_topology,
      const std::string& leader_uuid);

  // Find the UUID of the next hop, given the UUIDs of the current source
  // and the ultimate destination.
  Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const;

  // Return a string representation of the routing topology.
  std::string ToString() const;

 private:
  // A node representing a raft peer in a hierarchy with associated routing
  // rules for proxied messages.
  struct Node {
    explicit Node(RaftPeerPB peer_pb) : peer_pb(std::move(peer_pb)) {}

    const std::string& id() const {
      return peer_pb.permanent_uuid();
    }

    const RaftPeerPB peer_pb;
    Node* proxy_from = nullptr;

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
  Status ConstructForest(
      const RaftConfigPB& raft_config,
      const ProxyTopologyPB& proxy_topology,
      std::unordered_map<std::string, Node*>* index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* forest);

  // Reorganize the given forest into a single routing tree by moving the roots
  // of Node trees that don't include the leader under the leader as children.
  // The leader must appear in the index. If it does not, InvalidArgument is
  // returned.
  Status MergeForestIntoSingleRoutingTree(
      const std::string& leader_uuid,
      const std::unordered_map<std::string, Node*>& index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* forest);

  // Recursively construct the next-hop indices at each node. We run DFS to
  // determine routes because there is only one route to each node from the
  // root.
  void ConstructNextHopIndicesRec(Node* cur);

  // Recursive helper for DFS to build the debug string emitted by ToString().
  void ToStringHelperRec(Node* cur, int level, std::string* out) const;

  bool has_explicit_routes_{false}; // Whether there are any topology edges.
  std::unique_ptr<Node> topology_root_;
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
  static Status Create(
      FsManager* fs_manager,
      std::string tablet_id,
      RaftConfigPB raft_config,
      ProxyTopologyPB proxy_topology,
      std::shared_ptr<DurableRoutingTable>* drt);

  // Read from disk.
  static Status Load(
      FsManager* fs_manager,
      std::string tablet_id,
      RaftConfigPB raft_config,
      LoadOptions opts,
      std::shared_ptr<DurableRoutingTable>* drt);

  // Delete the on-disk data for the DRT.
  static Status DeleteOnDiskData(
      FsManager* fs_manager,
      const std::string& tablet_id);

  // Called when the proxy graph changes.
  Status UpdateProxyTopology(ProxyTopologyPB proxy_topology) override;

  // Called when the Raft config changes.
  Status UpdateRaftConfig(RaftConfigPB raft_config) override;

  // Called when the leader changes.
  void UpdateLeader(std::string leader_uuid) override;

  // If the leader is known and 'dest_uuid' is in the raft config, returns the
  // next hop along the route to reach 'dest_uuid'. If 'dest_uuid' is not a
  // member of the config, returns a Status::NotFound error. If there is no
  // known leader, but 'dest_uuid' is a member of the raft config, returns
  // 'dest_uuid' to directly route to the node, ignoring normal proxy routing
  // rules, since proxying routes are only defined when the leader is known.
  Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const override;

  // Return the currently active proxy topology.
  ProxyTopologyPB GetProxyTopology() const override;

  // Get proxy policy based on which this table operates (DurableRoutingPolicy)
  ProxyPolicy GetProxyPolicy() const override;

  // Return a string representation of the routing topology.
  std::string ToString() const;

 private:
  DurableRoutingTable(
      FsManager* fs_manager,
      std::string tablet_id,
      ProxyTopologyPB proxy_topology,
      RaftConfigPB raft_config);

  // We flush a new ProxyTopologyPB to disk before committing the updated
  // version to memory. This method is not thread-safe and must be synchronized
  // by taking the lock or similar.
  Status Flush() const;

  // Thread-safe log prefix helper.
  std::string LogPrefix() const;

  FsManager* fs_manager_;
  const std::string tablet_id_;

  mutable RWCLock lock_; // read-write-commit lock protecting the below fields
  ProxyTopologyPB proxy_topology_;
  RaftConfigPB raft_config_;
  boost::optional<std::string>
      leader_uuid_; // We don't always know who is leader.
  boost::optional<RoutingTable>
      routing_table_; // When leader is unknown, the route is undefined.
};

// A simple 'region' based routing table. Check proxy_policy.h for more
// information. This table is intantiated when proxy policy is set to
// ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY.
class SimpleRegionRoutingTable : public IRoutingTable {
 public:
  ~SimpleRegionRoutingTable() override = default;

  Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const override;

  Status UpdateRaftConfig(RaftConfigPB raft_config) override;
  void UpdateLeader(std::string leader_uuid) override;
  ProxyTopologyPB GetProxyTopology() const override;
  Status UpdateProxyTopology(ProxyTopologyPB proxy_topology) override;
  void SetLocalPeerPB(RaftPeerPB local_peer_pb);
  ProxyPolicy GetProxyPolicy() const override;

  static Status Create(
      RaftConfigPB raft_config,
      RaftPeerPB local_peer_pb,
      std::shared_ptr<SimpleRegionRoutingTable>* srt);

 private:
  Status RebuildProxyTopology(RaftConfigPB raft_config);

  // Lock protecting below fields
  mutable RWMutex lock_;
  ProxyTopologyPB proxy_topology_;
  RaftConfigPB raft_config_;
  RaftPeerPB local_peer_pb_;
  boost::optional<std::string> leader_uuid_;
  std::unordered_map<std::string, std::string> peer_region_map_;
  std::unordered_map<std::string, std::string> dst_to_proxy_map_;
};

// A container to hols all available routing tables (implemented based on
// routing policy). All routing tables are created during bootstrap. The table
// that gets used for routing is based on 'proxy_policy_'.
class RoutingTableContainer {
 public:
  RoutingTableContainer(
      const ProxyPolicy& proxy_policy,
      const RaftPeerPB& local_peer_pb,
      RaftConfigPB raft_config,
      std::shared_ptr<DurableRoutingTable> drt);

  // Returns the uuid of the next 'proxy_peer' in 'next_hop'.
  // 'src_uuid' is the uuid of the peer who is sending the message. 'dest_uuid'
  // is the uuid of the peer to which message is intended.
  Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const;

  // Called each time the raft config is updated. Internal state is also updated
  // based on proxy policy.
  Status UpdateRaftConfig(RaftConfigPB raft_config);

  // Called each time the leader_uuid changes (due to detection of a new leader)
  void UpdateLeader(std::string leader_uuid);

  // returns the current proxy topology used by the current proxy_policy_
  ProxyTopologyPB GetProxyTopology() const;

  // Updates the proxy topology that is used to route a request from source to
  // destination. may be a no-op in some routing policies.
  Status UpdateProxyTopology(
      ProxyTopologyPB proxy_topology,
      RaftConfigPB raft_config,
      const std::string& leader_uuid);

  // Updates the locak_peer on all tables that use it
  void SetLocalPeerPB(RaftPeerPB local_peer_pb);

  // returns the current proxy_policy_
  ProxyPolicy GetProxyPolicy() const;

  // Sets the proxy policy in use to 'proxy_policy'
  // Also updates the leader_uuid and raft_config on all managed routing tables.
  // This allows individual routing tables to update rebild their topology and
  // routing rules
  Status SetProxyPolicy(
      const ProxyPolicy& proxy_policy,
      const std::string& leader_uuid,
      RaftConfigPB raft_config);

 private:
  std::atomic<ProxyPolicy> proxy_policy_;
  std::shared_ptr<SimpleRegionRoutingTable> srt_;
  std::shared_ptr<DurableRoutingTable> drt_;
};

// Verify that a ProxyTopologyPB is well-formed.
// Returns OK if no duplicates, empty strings, or self-loops are detected.
// Does not attempt to perform multi-hop loop detection because the final
// routing topology is not defined without a Raft config and leader.
Status VerifyProxyTopology(const ProxyTopologyPB& proxy_topology);
} // namespace consensus
} // namespace kudu
