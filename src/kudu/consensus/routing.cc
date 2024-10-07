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

#include "kudu/consensus/routing.h"

#include <unordered_set>

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/region_group_routing.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu::consensus {

////////////////////////////////////////////////////////////////////////////////
// RoutingTable
////////////////////////////////////////////////////////////////////////////////

Status RoutingTable::Init(
    const RaftConfigPB& raft_config,
    const ProxyTopologyPB& proxy_topology,
    const std::string& leader_uuid) {
  unordered_map<string, Node*> index;
  unordered_map<string, unique_ptr<Node>> forest;

  Status s = ConstructForest(raft_config, proxy_topology, &index, &forest);
  if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
    return s;
  }
  RETURN_NOT_OK(MergeForestIntoSingleRoutingTree(leader_uuid, index, &forest));
  ConstructNextHopIndicesRec(forest.begin()->second.get());

  has_explicit_routes_ = !proxy_topology.proxy_edges().empty();
  index_ = std::move(index);
  topology_root_ = std::move(forest.begin()->second);

  return s;
}

Status RoutingTable::ConstructForest(
    const RaftConfigPB& raft_config,
    const ProxyTopologyPB& proxy_topology,
    std::unordered_map<std::string, Node*>* index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  RETURN_NOT_OK_PREPEND(
      VerifyProxyTopology(proxy_topology), "invalid proxy topology");

  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(raft_config), "invalid raft config");

  unordered_map<string, string>
      dest_to_proxy_from; // keyed by directed edge destination
  for (const auto& edge : proxy_topology.proxy_edges()) {
    InsertOrDie(&dest_to_proxy_from, edge.peer_uuid(), edge.proxy_from_uuid());
  }

  // Initially, construct a forest comprised of all peers in the Raft config,
  // with no proxy_from relationships represented.
  std::unordered_map<std::string, std::unique_ptr<Node>> tmp_forest;
  std::unordered_map<std::string, Node*> tmp_index;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    unique_ptr<Node> node(new Node(peer));
    tmp_index.emplace(peer.permanent_uuid(), node.get());
    tmp_forest.emplace(peer.permanent_uuid(), std::move(node));
  }

  // proxy_from nodes specified in ProxyTopologyPB that were not found in
  // RaftConfigPB.
  vector<string> proxy_from_nodes_not_found;

  // Now, organize the forest into parent-child relationships, where the parent
  // is represented as proxy_from in each ProxyTopologyPB edge, and the child
  // is the destination. Any node without a valid proxy_from (either not
  // specified in ProxyTopologyPB or specified as a peer that isn't currently a
  // member of the Raft config) will be left as a tree root in the forest.
  for (const RaftPeerPB& peer : raft_config.peers()) {
    const string* proxy_from_uuid =
        FindOrNull(dest_to_proxy_from, peer.permanent_uuid());
    if (!proxy_from_uuid) {
      continue; // No 'proxy_from' specified for this peer.
    }

    // Node has proxy_from set, so we must link them and assign object
    // ownership as a child of the proxy_from Node.
    Node* proxy_from_ptr =
        FindWithDefault(tmp_index, *proxy_from_uuid, nullptr);
    if (!proxy_from_ptr) {
      // We skip over rules specifying proxy_from as a node not in the Raft
      // config and we warn about it.
      proxy_from_nodes_not_found.push_back(*proxy_from_uuid);
      continue;
    }

    // Move destination out of forest map and into the proxy_from Node as a
    // child.
    const string& node_uuid = peer.permanent_uuid();
    auto iter = tmp_forest.find(node_uuid);
    DCHECK(iter != tmp_forest.end());
    unique_ptr<Node> node = std::move(iter->second);
    tmp_forest.erase(iter->first);
    node->proxy_from = proxy_from_ptr;
    auto result = proxy_from_ptr->children.emplace(node_uuid, std::move(node));
    DCHECK(result.second) << "unexpected duplicate uuid: " << node_uuid;
  }

  *index = std::move(tmp_index);
  *forest = std::move(tmp_forest);

  // This is just a warning, not an error.
  if (!proxy_from_nodes_not_found.empty()) {
    return Status::Incomplete(
        "the following proxy_from nodes specified in the proxy topology were "
        "not found in the active Raft config and have been ignored",
        JoinStrings(proxy_from_nodes_not_found, ", "));
  }

  return Status::OK();
}

Status RoutingTable::MergeForestIntoSingleRoutingTree(
    const std::string& leader_uuid,
    const std::unordered_map<std::string, Node*>& index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  Node* leader = FindWithDefault(index, leader_uuid, nullptr);
  if (!leader) {
    return Status::InvalidArgument(
        "invalid config: cannot find leader", leader_uuid);
  }

  // Find the ultimate proxy root of the leader, if the leader as a proxy
  // assigned to it.
  Node* source_root = leader;
  while (source_root->proxy_from) {
    source_root = source_root->proxy_from;
  }

  // Make all trees, except the one the leader is in, children of the leader.
  // The result is a single tree.
  auto iter = forest->begin();
  while (iter != forest->end()) {
    if (iter->first == source_root->id()) {
      ++iter;
      continue;
    }
    const string& child_uuid = iter->first;
    iter->second->proxy_from = leader;
    leader->children.emplace(child_uuid, std::move(iter->second));
    iter = forest->erase(iter);
  }

  DCHECK_EQ(1, forest->size());
  return Status::OK();
}

void RoutingTable::ConstructNextHopIndicesRec(Node* cur) {
  for (const auto& child_entry : cur->children) {
    const string& child_uuid = child_entry.first;
    const auto& child = child_entry.second;
    ConstructNextHopIndicesRec(child.get());
    // Absorb child routes.
    for (const auto& child_route : child->routes) {
      const string& dest_uuid = child_route.first;
      cur->routes.emplace(dest_uuid, child_uuid);
    }
  }
  // Add self-route as a base case.
  cur->routes.emplace(cur->id(), cur->id());
}

Status RoutingTable::NextHop(
    const string& src_uuid,
    const string& dest_uuid,
    string* next_hop) const {
  // Base case: use direct routing if no routing topology is defined. If we
  // don't do this, if the leader has a proxy topology defined, and a proxy node
  // does not, then the proxy node will think the shortest path to the
  // destination is the leader, resulting in a routing loop. In the general case
  // this can happen due to proxy topology inconsistencies across the cluster
  // anyway, but it's nice to get non-pathological behavior in this case.
  if (!has_explicit_routes_) {
    *next_hop = dest_uuid;
    return Status::OK();
  }

  DCHECK(has_explicit_routes_); // Some proxy topology is defined.
  Node* src = FindWithDefault(index_, src_uuid, nullptr);
  if (!src) {
    return Status::NotFound(Substitute("unknown source uuid: $0", src_uuid));
  }
  Node* dest = FindWithDefault(index_, dest_uuid, nullptr);
  if (!dest) {
    return Status::NotFound(
        Substitute("unknown destination uuid: $0", dest_uuid));
  }

  // Search children.
  string* next_uuid = FindOrNull(src->routes, dest_uuid);
  if (next_uuid) {
    *next_hop = *next_uuid;
    return Status::OK();
  }

  // If we can't route via a child, route via a parent.
  DCHECK(src->proxy_from);
  *next_hop = src->proxy_from->id();
  return Status::OK();
}

std::string RoutingTable::ToString() const {
  string out;
  out.reserve(4096);
  // DFS.
  ToStringHelperRec(topology_root_.get(), /*level=*/0, &out);
  return out;
}

void RoutingTable::ToStringHelperRec(Node* cur, int level, std::string* out)
    const {
  for (int i = level - 1; i >= 0; i--) {
    if (i > 0) {
      *out += "   ";
    } else {
      *out += "-> ";
    }
  }
  *out += strings::Substitute(
      "$0 ($1)\n",
      cur->peer_pb.permanent_uuid(),
      SecureShortDebugString(cur->peer_pb.last_known_addr()));
  for (const auto& entry : cur->children) {
    ToStringHelperRec(entry.second.get(), level + 1, out);
  }
}

////////////////////////////////////////////////////////////////////////////////
// DurableRoutingTable
////////////////////////////////////////////////////////////////////////////////

Status DurableRoutingTable::Create(
    FsManager* fs_manager,
    std::string tablet_id,
    RaftConfigPB raft_config,
    ProxyTopologyPB proxy_topology,
    std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);
  if (fs_manager->env()->FileExists(path)) {
    return Status::AlreadyPresent(Substitute("File $0 already exists", path));
  }

  auto tmp_drt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(
      fs_manager,
      std::move(tablet_id),
      std::move(proxy_topology),
      std::move(raft_config)));
  RETURN_NOT_OK(tmp_drt->Flush()); // no lock needed as object is unpublished
  *drt = std::move(tmp_drt);
  return Status::OK();
}

// Read from disk.
Status DurableRoutingTable::Load(
    FsManager* fs_manager,
    std::string tablet_id,
    RaftConfigPB raft_config,
    LoadOptions opts,
    std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);

  ProxyTopologyPB proxy_topology;
  Status s = pb_util::ReadPBContainerFromPath(
      fs_manager->env(), path, &proxy_topology);
  if (PREDICT_FALSE(
          s.IsNotFound() && opts == LoadOptions::kCreateEmptyIfDoesNotExist)) {
    s = Create(fs_manager, tablet_id, raft_config, {}, drt);
  }
  RETURN_NOT_OK(s);

  *drt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(
      fs_manager,
      std::move(tablet_id),
      std::move(proxy_topology),
      std::move(raft_config)));
  return Status::OK();
}

Status DurableRoutingTable::DeleteOnDiskData(
    FsManager* fs_manager,
    const string& tablet_id) {
  string path = fs_manager->GetProxyMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(
      fs_manager->env()->DeleteFile(path),
      Substitute(
          "Unable to delete durable routing table file for tablet $0",
          tablet_id));
  return Status::OK();
}

Status DurableRoutingTable::UpdateProxyTopology(
    ProxyTopologyPB proxy_topology) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  if (leader_uuid_) {
    Status s = routing_table.Init(raft_config_, proxy_topology, *leader_uuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
  }

  // Only flush the proxy graph protobuf to disk when it changes.
  if (!MessageDifferencer::Equals(proxy_topology, proxy_topology_)) {
    VLOG_WITH_PREFIX(3) << "proxy routes updated, flushing to disk...";
    RETURN_NOT_OK(Flush());
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock
      .cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  proxy_topology_ = std::move(proxy_topology);

  if (leader_uuid_) {
    routing_table_ = std::move(routing_table);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routing_table_->ToString();
  } else {
    routing_table_ = {};
    LOG_WITH_PREFIX(INFO)
        << "proxy routing temporarily disabled: no known leader";
  }

  return Status::OK();
}

Status DurableRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  bool leader_in_config = false;
  if (leader_uuid_) {
    leader_in_config = IsRaftConfigMember(*leader_uuid_, raft_config);
  }
  if (leader_in_config) {
    Status s = routing_table.Init(raft_config, proxy_topology_, *leader_uuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routing_table.ToString();
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock
      .cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  raft_config_ = std::move(raft_config);

  if (leader_in_config) {
    routing_table_ = std::move(routing_table);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routing_table_->ToString();
  } else {
    routing_table_ = {};
    LOG_WITH_PREFIX(INFO)
        << "proxy routing temporarily disabled: the leader is not in the config";
  }

  return Status::OK();
}

void DurableRoutingTable::UpdateLeader(string leader_uuid) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  RoutingTable routing_table;
  bool initialized = false;
  if (IsRaftConfigMember(leader_uuid, raft_config_)) {
    // Rebuild the routing table. If this fails, remember the new leader anyway.
    Status s = routing_table.Init(raft_config_, proxy_topology_, leader_uuid);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
      initialized = true;
    } else if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX(WARNING)
          << "unable to initialize proxy routing table: " << s.ToString();
    } else {
      initialized = true;
    }
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock
      .cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  leader_uuid_ = std::move(leader_uuid);
  if (initialized) {
    routing_table_ = std::move(routing_table);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes: \n"
                          << routing_table_->ToString();
  } else {
    routing_table_ = {};
    VLOG_WITH_PREFIX(2)
        << "proxy routing disabled: no valid proxy topology is set";
  }
}

Status DurableRoutingTable::NextHop(
    const std::string& src_uuid,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  shared_lock<RWCLock> l(lock_);
  if (routing_table_) {
    return routing_table_->NextHop(src_uuid, dest_uuid, next_hop);
  }
  if (!IsRaftConfigMember(dest_uuid, raft_config_)) {
    return Status::NotFound(Substitute(
        "peer with uuid $0 not found in consensus config", dest_uuid));
  }

  *next_hop = dest_uuid;
  return Status::OK();
}

ProxyTopologyPB DurableRoutingTable::GetProxyTopology() const {
  shared_lock<RWCLock> l(lock_);
  return proxy_topology_;
}

string DurableRoutingTable::ToString() const {
  shared_lock<RWCLock> l(lock_);
  if (routing_table_) {
    return routing_table_->ToString();
  }
  return "";
}

DurableRoutingTable::DurableRoutingTable(
    FsManager* fs_manager,
    string tablet_id,
    ProxyTopologyPB proxy_topology,
    RaftConfigPB raft_config)
    : fs_manager_(fs_manager),
      tablet_id_(std::move(tablet_id)),
      proxy_topology_(std::move(proxy_topology)),
      raft_config_(std::move(raft_config)) {
  // TODO(mpercy): Do we have any validation to perform here?
}

Status DurableRoutingTable::Flush() const {
  // TODO(mpercy): This entire method is copy / pasted from
  // ConsensusMetadata::Flush(). Factor out?

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::CreateDirIfMissing(fs_manager_->env(), dir, &created_dir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parent_dir),
        "Unable to fsync consensus parent dir " + parent_dir);
  }

  string path = fs_manager_->GetProxyMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fs_manager_->env(),
          path,
          proxy_topology_,
          pb_util::OVERWRITE,
          pb_util::SYNC),
      Substitute(
          "Unable to write proxy metadata file for tablet $0 to path $1",
          tablet_id_,
          path));
  return Status::OK();
}

string DurableRoutingTable::LogPrefix() const {
  return strings::Substitute("T $0 P $1: ", tablet_id_, fs_manager_->uuid());
}

ProxyPolicy DurableRoutingTable::GetProxyPolicy() const {
  return ProxyPolicy::DURABLE_ROUTING_POLICY;
}

////////////////////////////////////////////////////////////////////////////////
// SimpleRegionRoutingTable
////////////////////////////////////////////////////////////////////////////////
Status SimpleRegionRoutingTable::Create(
    RaftConfigPB raft_config,
    RaftPeerPB local_peer_pb,
    std::shared_ptr<SimpleRegionRoutingTable>* srt) {
  auto simple_routing_table = std::make_shared<SimpleRegionRoutingTable>();
  simple_routing_table->SetLocalPeerPB(std::move(local_peer_pb));
  simple_routing_table->UpdateRaftConfig(std::move(raft_config));
  *srt = std::move(simple_routing_table);

  return Status::OK();
}

Status SimpleRegionRoutingTable::RebuildProxyTopology(
    RaftConfigPB raft_config) {
  ProxyTopologyPB proxy_topology;
  const std::string& local_peer_region = local_peer_pb_.attrs().region();

  // Take a copy of current map
  std::unordered_map<std::string, std::string> current_dst_to_proxy_map;
  {
    shared_lock<RWMutex> l(lock_);
    current_dst_to_proxy_map = dst_to_proxy_map_;
  }

  // 1. Identify the 'proxy peer' for each region. The peer that is backed by a
  // database in a region acts as a 'proxy peer' for the region. [Update when
  // region splitting is supported]. If there are multiple such peers in a
  // region, then pick the first peer as the 'proxy peer' for the region [this
  // cannot happen once region splitting is supported]
  // 2. Also build a map of "peer-uuid to peer-region" for all peers
  std::unordered_map<std::string, std::string> region_proxy_peer_map;
  std::unordered_map<std::string, std::string> peer_region_map;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    if (peer.attrs().backing_db_present()) {
      region_proxy_peer_map.emplace(
          peer.attrs().region(), peer.permanent_uuid());
    }
    peer_region_map.emplace(peer.permanent_uuid(), peer.attrs().region());
  }

  // For every destination peer, choose the peer from which it will be proxied
  // from. Some rules (see proxy_policy.h):
  // 1. A peer with a backing database is never proxied. Leader ships messages
  // directly to all such peers
  // 2. A peer which is in the same region as the node that is shipping messages
  // will not be proxied i.e all peers in the same region as the 'source' will
  // get messages directly from the 'source'
  // 3. A peer which is in a region without any valid 'proxy peer' will recieve
  // messages directly from the 'source'
  // 4. Also note that to avoid flapping stable proxy routes, if a peer is
  // already being proxied and the proxy peer is part of the new config, then
  // the  proxy host for such a peer is left unchanged
  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  for (const RaftPeerPB& dest_peer : raft_config.peers()) {
    std::string dest_peer_region = dest_peer.attrs().region();
    if (dest_peer.attrs().backing_db_present()) {
      // Peers that have a backing database are not proxied (rule #1)
      continue;
    } else {
      const auto& proxy_peer_uuid =
          region_proxy_peer_map.find(dest_peer_region);
      if (proxy_peer_uuid == region_proxy_peer_map.end() ||
          dest_peer_region == local_peer_region) {
        // Region without a valid 'proxy' peer or peers that are in the same
        // region as this peer are not proxied (rule #2 and #3)
        continue;
      } else {
        // Add a new edge into the topology
        ProxyEdgePB* proxy_edge = proxy_topology.add_proxy_edges();
        proxy_edge->set_peer_uuid(dest_peer.permanent_uuid());

        // Check if this 'destination peer' is being currently proxied.
        // If yes, check if current 'proxy peer' exists in the new config.
        // If yes, then do not change the 'proxy peer' for this
        // 'destination peer'.
        const auto& current_proxy_peer =
            current_dst_to_proxy_map.find(dest_peer.permanent_uuid());
        if (current_proxy_peer != current_dst_to_proxy_map.end()) {
          // Check if the proxy peer exists in the new config.
          const auto& current_proxy_peer_region =
              peer_region_map.find(current_proxy_peer->second);
          if (current_proxy_peer_region != peer_region_map.end()) {
            // Continue to route through the existing 'proxy peer'
            proxy_edge->set_proxy_from_uuid(current_proxy_peer->second);
            dst_to_proxy_map.emplace(
                dest_peer.permanent_uuid(), current_proxy_peer->second);
            continue;
          }
        }

        // 'dest_peer' will be proxied through 'proxy_peer_uuid'
        proxy_edge->set_proxy_from_uuid(proxy_peer_uuid->second);
        dst_to_proxy_map.emplace(
            dest_peer.permanent_uuid(), proxy_peer_uuid->second);
      }
    }
  }

  std::lock_guard<RWMutex> l(lock_);
  proxy_topology_ = std::move(proxy_topology);
  dst_to_proxy_map_ = std::move(dst_to_proxy_map);
  raft_config_ = std::move(raft_config);

  return Status::OK();
}

Status SimpleRegionRoutingTable::NextHop(
    const std::string& /* src_uuid */,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  shared_lock<RWMutex> l(lock_);
  const auto& proxy_uuid = dst_to_proxy_map_.find(dest_uuid);
  if (proxy_uuid == dst_to_proxy_map_.end()) {
    // Could not find this destination, route directly to the destination
    *next_hop = dest_uuid;
    return Status::OK();
  }

  *next_hop = proxy_uuid->second;
  return Status::OK();
}

Status SimpleRegionRoutingTable::UpdateProxyTopology(
    ProxyTopologyPB proxy_topology) {
  // SimpleRegionRoutingTable uses config to update proxy maps. Hence cannot
  // update topology directly.
  // TODO: provide a way to override designated per-region proxy peer
  return Status::OK();
}

ProxyTopologyPB SimpleRegionRoutingTable::GetProxyTopology() const {
  shared_lock<RWMutex> l(lock_);
  return proxy_topology_;
}

Status SimpleRegionRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  return RebuildProxyTopology(std::move(raft_config));
}

void SimpleRegionRoutingTable::UpdateLeader(string leader_uuid) {
  std::lock_guard<RWMutex> l(lock_);
  leader_uuid_ = std::move(leader_uuid);
}

void SimpleRegionRoutingTable::SetLocalPeerPB(RaftPeerPB local_peer_pb) {
  std::lock_guard<RWMutex> l(lock_);
  local_peer_pb_ = std::move(local_peer_pb);
}

ProxyPolicy SimpleRegionRoutingTable::GetProxyPolicy() const {
  return ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY;
}

////////////////////////////////////////////////////////////////////////////////
// RoutingTableContainer implementation
////////////////////////////////////////////////////////////////////////////////
RoutingTableContainer::RoutingTableContainer(
    const ProxyPolicy& proxy_policy,
    const RaftPeerPB& local_peer_pb,
    RaftConfigPB raft_config,
    std::shared_ptr<DurableRoutingTable> drt,
    const std::vector<std::unordered_set<std::string>>& region_groups) {
  proxy_policy_ = proxy_policy;
  drt_ = std::move(drt);

  std::shared_ptr<SimpleRegionRoutingTable> srt;
  SimpleRegionRoutingTable::Create(raft_config, local_peer_pb, &srt);
  srt_ = std::move(srt);

  std::shared_ptr<RegionGroupRoutingTable> rgrt;
  RegionGroupRoutingTable::Create(
      raft_config, local_peer_pb, region_groups, &rgrt);
  grt_ = std::move(rgrt);
}

Status RoutingTableContainer::NextHop(
    const std::string& src_uuid,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->NextHop(src_uuid, dest_uuid, next_hop);
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->NextHop(src_uuid, dest_uuid, next_hop);
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->NextHop(src_uuid, dest_uuid, next_hop);
    case ProxyPolicy::DISABLE_PROXY:
      *next_hop = dest_uuid;
      return Status::OK();
    default:
      break; // placate the compiler
  }

  return Status::NotSupported("The specified proxy_policy is not supported");
}

Status RoutingTableContainer::UpdateProxyTopology(
    ProxyTopologyPB proxy_topology,
    RaftConfigPB raft_config,
    const std::string& leader_uuid) {
  // Explicit routing topology can only be used by durable routing table
  // Update the leader uuid before updating proxy_topology
  drt_->UpdateLeader(leader_uuid);
  RETURN_NOT_OK(drt_->UpdateRaftConfig(std::move(raft_config)));
  return drt_->UpdateProxyTopology(std::move(proxy_topology));
}

std::vector<std::unordered_set<std::string>>
RoutingTableContainer::GetProxyRegionGroup() {
  return grt_->GetProxyRegionGroup();
}

Status RoutingTableContainer::UpdateProxyRegionGroup(
    const std::vector<std::unordered_set<std::string>>& region_groups,
    RaftConfigPB raft_config,
    const std::string& leader_uuid) {
  return grt_->UpdateProxyRegionGroup(
      region_groups, std::move(raft_config), leader_uuid);
}

void RoutingTableContainer::UpdateRtt(
    const std::string& peer_uuid,
    std::chrono::microseconds rtt) {
  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      grt_->UpdateRtt(peer_uuid, rtt);
      break;
    default:
      break; // placate the compiler
  }
}

ProxyTopologyPB RoutingTableContainer::GetProxyTopology() const {
  ProxyTopologyPB topology_pb;

  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->GetProxyTopology();
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->GetProxyTopology();
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->GetProxyTopology();
    default:
      break; // placate the compiler
  }

  return topology_pb;
}

Status RoutingTableContainer::UpdateRaftConfig(RaftConfigPB raft_config) {
  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->UpdateRaftConfig(std::move(raft_config));
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->UpdateRaftConfig(std::move(raft_config));
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->UpdateRaftConfig(std::move(raft_config));
    default:
      break; // placate the compiler
  }

  return Status::NotSupported("The specified proxy_policy is not supported");
}

void RoutingTableContainer::UpdateLeader(string leader_uuid) {
  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      drt_->UpdateLeader(std::move(leader_uuid));
      break;
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      srt_->UpdateLeader(std::move(leader_uuid));
      break;
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      grt_->UpdateLeader(std::move(leader_uuid));
      break;
    default:
      break; // placate the compiler
  }
}

void RoutingTableContainer::SetLocalPeerPB(RaftPeerPB local_peer_pb) {
  ProxyPolicy policy = proxy_policy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return; // No-Op for drt
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      srt_->SetLocalPeerPB(std::move(local_peer_pb));
      break;
    default:
      break; // placate the compiler
  }
}

ProxyPolicy RoutingTableContainer::GetProxyPolicy() const {
  return proxy_policy_.load();
}

Status RoutingTableContainer::SetProxyPolicy(
    const ProxyPolicy& proxy_policy,
    const std::string& leader_uuid,
    RaftConfigPB raft_config) {
  drt_->UpdateLeader(leader_uuid);
  srt_->UpdateLeader(leader_uuid);

  RETURN_NOT_OK(drt_->UpdateRaftConfig(raft_config));
  RETURN_NOT_OK(srt_->UpdateRaftConfig(raft_config));

  RETURN_NOT_OK(grt_->UpdateRaftConfigAndLeader(raft_config, leader_uuid));

  proxy_policy_ = proxy_policy;

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// Global functions.
////////////////////////////////////////////////////////////////////////////////

Status VerifyProxyTopology(const ProxyTopologyPB& proxy_topology) {
  unordered_set<string> seen;
  for (const auto& entry : proxy_topology.proxy_edges()) {
    if (entry.peer_uuid().empty()) {
      return Status::InvalidArgument(Substitute(
          "empty peer_uuid specified: $0", SecureShortDebugString(entry)));
    }
    if (entry.proxy_from_uuid().empty()) {
      return Status::InvalidArgument(Substitute(
          "empty proxy_from_uuid specified: $0",
          SecureShortDebugString(entry)));
    }
    if (entry.peer_uuid() == entry.proxy_from_uuid()) {
      return Status::InvalidArgument(Substitute(
          "illegal self-loop specified: $0", SecureShortDebugString(entry)));
    }
    if (!InsertIfNotPresent(&seen, entry.peer_uuid())) {
      return Status::InvalidArgument(
          Substitute("duplicate peer_uuid specified: $0", entry.peer_uuid()));
    }
  }
  return Status::OK();
}

} // namespace kudu::consensus
