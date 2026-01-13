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

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/region_group_routing.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

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
      destToProxyFrom; // keyed by directed edge destination
  for (const auto& edge : proxy_topology.proxy_edges()) {
    auto [it, inserted] =
        destToProxyFrom.emplace(edge.peer_uuid(), edge.proxy_from_uuid());
    DCHECK(inserted) << "duplicate key: " << edge.peer_uuid();
  }

  // Initially, construct a forest comprised of all peers in the Raft config,
  // with no proxy_from relationships represented.
  std::unordered_map<std::string, std::unique_ptr<Node>> tmpForest;
  std::unordered_map<std::string, Node*> tmpIndex;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    unique_ptr<Node> node(new Node(peer));
    tmpIndex.emplace(peer.permanent_uuid(), node.get());
    tmpForest.emplace(peer.permanent_uuid(), std::move(node));
  }

  // proxy_from nodes specified in ProxyTopologyPB that were not found in
  // RaftConfigPB.
  vector<string> proxyFromNodesNotFound;

  // Now, organize the forest into parent-child relationships, where the parent
  // is represented as proxy_from in each ProxyTopologyPB edge, and the child
  // is the destination. Any node without a valid proxy_from (either not
  // specified in ProxyTopologyPB or specified as a peer that isn't currently a
  // member of the Raft config) will be left as a tree root in the forest.
  for (const RaftPeerPB& peer : raft_config.peers()) {
    auto it = destToProxyFrom.find(peer.permanent_uuid());
    if (it == destToProxyFrom.end()) {
      continue; // No 'proxy_from' specified for this peer.
    }
    const string* proxyFromUuid = &it->second;

    // Node has proxy_from set, so we must link them and assign object
    // ownership as a child of the proxy_from Node.
    Node* proxyFromPtr = nullptr;
    auto tmpIndexIt = tmpIndex.find(*proxyFromUuid);
    if (tmpIndexIt != tmpIndex.end()) {
      proxyFromPtr = tmpIndexIt->second;
    }
    if (!proxyFromPtr) {
      // We skip over rules specifying proxy_from as a node not in the Raft
      // config and we warn about it.
      proxyFromNodesNotFound.push_back(*proxyFromUuid);
      continue;
    }

    // Move destination out of forest map and into the proxy_from Node as a
    // child.
    const string& nodeUuid = peer.permanent_uuid();
    auto iter = tmpForest.find(nodeUuid);
    DCHECK(iter != tmpForest.end());
    unique_ptr<Node> node = std::move(iter->second);
    tmpForest.erase(iter->first);
    node->proxy_from = proxyFromPtr;
    auto result = proxyFromPtr->children.emplace(nodeUuid, std::move(node));
    DCHECK(result.second) << "unexpected duplicate uuid: " << nodeUuid;
  }

  *index = std::move(tmpIndex);
  *forest = std::move(tmpForest);

  // This is just a warning, not an error.
  if (!proxyFromNodesNotFound.empty()) {
    return Status::Incomplete(
        "the following proxy_from nodes specified in the proxy topology were "
        "not found in the active Raft config and have been ignored",
        JoinStrings(proxyFromNodesNotFound, ", "));
  }

  return Status::OK();
}

Status RoutingTable::MergeForestIntoSingleRoutingTree(
    const std::string& leader_uuid,
    const std::unordered_map<std::string, Node*>& index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  Node* leader = nullptr;
  auto leaderIt = index.find(leader_uuid);
  if (leaderIt != index.end()) {
    leader = leaderIt->second;
  }
  if (!leader) {
    return Status::InvalidArgument(
        "invalid config: cannot find leader", leader_uuid);
  }

  // Find the ultimate proxy root of the leader, if the leader as a proxy
  // assigned to it.
  Node* sourceRoot = leader;
  while (sourceRoot->proxy_from) {
    sourceRoot = sourceRoot->proxy_from;
  }

  // Make all trees, except the one the leader is in, children of the leader.
  // The result is a single tree.
  auto iter = forest->begin();
  while (iter != forest->end()) {
    if (iter->first == sourceRoot->id()) {
      ++iter;
      continue;
    }
    const string& childUuid = iter->first;
    iter->second->proxy_from = leader;
    leader->children.emplace(childUuid, std::move(iter->second));
    iter = forest->erase(iter);
  }

  DCHECK_EQ(1, forest->size());
  return Status::OK();
}

void RoutingTable::ConstructNextHopIndicesRec(Node* cur) {
  for (const auto& childEntry : cur->children) {
    const string& childUuid = childEntry.first;
    const auto& child = childEntry.second;
    ConstructNextHopIndicesRec(child.get());
    // Absorb child routes.
    for (const auto& childRoute : child->routes) {
      const string& destUuid = childRoute.first;
      cur->routes.emplace(destUuid, childUuid);
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
  Node* src = nullptr;
  auto srcIt = index_.find(src_uuid);
  if (srcIt != index_.end()) {
    src = srcIt->second;
  }
  if (!src) {
    return Status::NotFound(fmt::format("unknown source uuid: {}", src_uuid));
  }
  Node* dest = nullptr;
  auto destIt = index_.find(dest_uuid);
  if (destIt != index_.end()) {
    dest = destIt->second;
  }
  if (!dest) {
    return Status::NotFound(
        fmt::format("unknown destination uuid: {}", dest_uuid));
  }

  // Search children.
  auto it = src->routes.find(dest_uuid);
  if (it != src->routes.end()) {
    *next_hop = it->second;
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
  *out += fmt::format(
      "{} ({})\n",
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
    return Status::AlreadyPresent(fmt::format("File {} already exists", path));
  }

  auto tmpDrt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(
      fs_manager,
      std::move(tablet_id),
      std::move(proxy_topology),
      std::move(raft_config)));
  RETURN_NOT_OK(tmpDrt->Flush()); // no lock needed as object is unpublished
  *drt = std::move(tmpDrt);
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
      fmt::format(
          "Unable to delete durable routing table file for tablet {}",
          tablet_id));
  return Status::OK();
}

Status DurableRoutingTable::UpdateProxyTopology(
    ProxyTopologyPB proxy_topology) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routingTable;
  if (leader_uuid_) {
    Status s = routingTable.Init(raft_config_, proxy_topology, *leader_uuid_);
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
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.CommitUnlock(); });

  proxy_topology_ = std::move(proxy_topology);

  if (leader_uuid_) {
    routing_table_ = std::move(routingTable);
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
  auto releaseWriteLock = folly::makeGuard([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routingTable;
  bool leaderInConfig = false;
  if (leader_uuid_) {
    leaderInConfig = IsRaftConfigMember(*leader_uuid_, raft_config);
  }
  if (leaderInConfig) {
    Status s = routingTable.Init(raft_config, proxy_topology_, *leader_uuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routingTable.ToString();
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.CommitUnlock(); });

  raft_config_ = std::move(raft_config);

  if (leaderInConfig) {
    routing_table_ = std::move(routingTable);
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
  auto releaseWriteLock = folly::makeGuard([&] { lock_.WriteUnlock(); });

  RoutingTable routingTable;
  bool initialized = false;
  if (IsRaftConfigMember(leader_uuid, raft_config_)) {
    // Rebuild the routing table. If this fails, remember the new leader anyway.
    Status s = routingTable.Init(raft_config_, proxy_topology_, leader_uuid);
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
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.CommitUnlock(); });

  leader_uuid_ = std::move(leader_uuid);
  if (initialized) {
    routing_table_ = std::move(routingTable);
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
    return Status::NotFound(
        fmt::format(
            "peer with uuid {} not found in consensus config", dest_uuid));
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
  bool createdDir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::CreateDirIfMissing(fs_manager_->env(), dir, &createdDir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(createdDir)) {
    string parentDir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parentDir),
        "Unable to fsync consensus parent dir " + parentDir);
  }

  string path = fs_manager_->GetProxyMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fs_manager_->env(),
          path,
          proxy_topology_,
          pb_util::OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write proxy metadata file for tablet {} to path {}",
          tablet_id_,
          path));
  return Status::OK();
}

string DurableRoutingTable::LogPrefix() const {
  return fmt::format("T {} P {}: ", tablet_id_, fs_manager_->uuid());
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
  auto simpleRoutingTable = std::make_shared<SimpleRegionRoutingTable>();
  simpleRoutingTable->SetLocalPeerPB(std::move(local_peer_pb));
  simpleRoutingTable->UpdateRaftConfig(std::move(raft_config));
  *srt = std::move(simpleRoutingTable);

  return Status::OK();
}

Status SimpleRegionRoutingTable::RebuildProxyTopology(
    RaftConfigPB raft_config) {
  ProxyTopologyPB proxy_topology;
  const std::string& localPeerRegion = local_peer_pb_.attrs().region();

  // Take a copy of current map
  std::unordered_map<std::string, std::string> currentDstToProxyMap;
  {
    std::shared_lock l(lock_);
    currentDstToProxyMap = dst_to_proxy_map_;
  }

  // 1. Identify the 'proxy peer' for each region. The peer that is backed by a
  // database in a region acts as a 'proxy peer' for the region. [Update when
  // region splitting is supported]. If there are multiple such peers in a
  // region, then pick the first peer as the 'proxy peer' for the region [this
  // cannot happen once region splitting is supported]
  // 2. Also build a map of "peer-uuid to peer-region" for all peers
  std::unordered_map<std::string, std::string> regionProxyPeerMap;
  std::unordered_map<std::string, std::string> peerRegionMap;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    if (CanbeProxyPeer(peer)) {
      regionProxyPeerMap.emplace(peer.attrs().region(), peer.permanent_uuid());
    }
    peerRegionMap.emplace(peer.permanent_uuid(), peer.attrs().region());
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
  std::unordered_map<std::string, std::string> dstToProxyMap;
  for (const RaftPeerPB& destPeer : raft_config.peers()) {
    std::string destPeerRegion = destPeer.attrs().region();
    if (CanbeProxyPeer(destPeer)) {
      // Peers that have a backing database are not proxied (rule #1)
      continue;
    } else {
      const auto& proxyPeerUuid = regionProxyPeerMap.find(destPeerRegion);
      if (proxyPeerUuid == regionProxyPeerMap.end() ||
          destPeerRegion == localPeerRegion) {
        // Region without a valid 'proxy' peer or peers that are in the same
        // region as this peer are not proxied (rule #2 and #3)
        continue;
      } else {
        // Add a new edge into the topology
        ProxyEdgePB* proxyEdge = proxy_topology.add_proxy_edges();
        proxyEdge->set_peer_uuid(destPeer.permanent_uuid());

        // Check if this 'destination peer' is being currently proxied.
        // If yes, check if current 'proxy peer' exists in the new config.
        // If yes, then do not change the 'proxy peer' for this
        // 'destination peer'.
        const auto& currentProxyPeer =
            currentDstToProxyMap.find(destPeer.permanent_uuid());
        if (currentProxyPeer != currentDstToProxyMap.end()) {
          // Check if the proxy peer exists in the new config.
          const auto& currentProxyPeerRegion =
              peerRegionMap.find(currentProxyPeer->second);
          if (currentProxyPeerRegion != peerRegionMap.end()) {
            // Continue to route through the existing 'proxy peer'
            proxyEdge->set_proxy_from_uuid(currentProxyPeer->second);
            dstToProxyMap.emplace(
                destPeer.permanent_uuid(), currentProxyPeer->second);
            continue;
          }
        }

        // 'dest_peer' will be proxied through 'proxy_peer_uuid'
        proxyEdge->set_proxy_from_uuid(proxyPeerUuid->second);
        dstToProxyMap.emplace(destPeer.permanent_uuid(), proxyPeerUuid->second);
      }
    }
  }

  std::lock_guard l(lock_);
  proxy_topology_ = std::move(proxy_topology);
  dst_to_proxy_map_ = std::move(dstToProxyMap);
  raft_config_ = std::move(raft_config);

  return Status::OK();
}

Status SimpleRegionRoutingTable::NextHop(
    const std::string& /* src_uuid */,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  std::shared_lock l(lock_);
  const auto& proxyUuid = dst_to_proxy_map_.find(dest_uuid);
  if (proxyUuid == dst_to_proxy_map_.end()) {
    // Could not find this destination, route directly to the destination
    *next_hop = dest_uuid;
    return Status::OK();
  }

  *next_hop = proxyUuid->second;
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
  std::shared_lock l(lock_);
  return proxy_topology_;
}

Status SimpleRegionRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  return RebuildProxyTopology(std::move(raft_config));
}

void SimpleRegionRoutingTable::UpdateLeader(string leader_uuid) {
  std::lock_guard l(lock_);
  leader_uuid_ = std::move(leader_uuid);
}

void SimpleRegionRoutingTable::SetLocalPeerPB(RaftPeerPB local_peer_pb) {
  std::lock_guard l(lock_);
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
  ProxyTopologyPB topologyPb;

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

  return topologyPb;
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
      return Status::InvalidArgument(
          fmt::format(
              "empty peer_uuid specified: {}", SecureShortDebugString(entry)));
    }
    if (entry.proxy_from_uuid().empty()) {
      return Status::InvalidArgument(
          fmt::format(
              "empty proxy_from_uuid specified: {}",
              SecureShortDebugString(entry)));
    }
    if (entry.peer_uuid() == entry.proxy_from_uuid()) {
      return Status::InvalidArgument(
          fmt::format(
              "illegal self-loop specified: {}",
              SecureShortDebugString(entry)));
    }
    if (!seen.insert(entry.peer_uuid()).second) {
      return Status::InvalidArgument(
          fmt::format("duplicate peer_uuid specified: {}", entry.peer_uuid()));
    }
  }
  return Status::OK();
}

bool CanbeProxyPeer(const RaftPeerPB& peer) {
  return IsBackingDbPresent(peer) && !IsStandbyMember(peer);
}

} // namespace kudu::consensus
