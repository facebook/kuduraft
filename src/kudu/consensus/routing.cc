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

Status RoutingTable::init(
    const RaftConfigPB& raftConfig,
    const ProxyTopologyPB& proxyTopology,
    const std::string& leaderUuid) {
  unordered_map<string, Node*> index;
  unordered_map<string, unique_ptr<Node>> forest;

  Status s = constructForest(raftConfig, proxyTopology, &index, &forest);
  if (PREDICT_FALSE(!s.ok() && !s.IsIncomplete())) {
    return s;
  }
  RETURN_NOT_OK(mergeForestIntoSingleRoutingTree(leaderUuid, index, &forest));
  constructNextHopIndicesRec(forest.begin()->second.get());

  hasExplicitRoutes_ = !proxyTopology.proxy_edges().empty();
  index_ = std::move(index);
  topologyRoot_ = std::move(forest.begin()->second);

  return s;
}

Status RoutingTable::constructForest(
    const RaftConfigPB& raftConfig,
    const ProxyTopologyPB& proxyTopology,
    std::unordered_map<std::string, Node*>* index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  RETURN_NOT_OK_PREPEND(
      verifyProxyTopology(proxyTopology), "invalid proxy topology");

  RETURN_NOT_OK_PREPEND(verifyRaftConfig(raftConfig), "invalid raft config");

  unordered_map<string, string>
      destToProxyFrom; // keyed by directed edge destination
  for (const auto& edge : proxyTopology.proxy_edges()) {
    auto [it, inserted] =
        destToProxyFrom.emplace(edge.peer_uuid(), edge.proxy_from_uuid());
    DCHECK(inserted) << "duplicate key: " << edge.peer_uuid();
  }

  // Initially, construct a forest comprised of all peers in the Raft config,
  // with no proxy_from relationships represented.
  std::unordered_map<std::string, std::unique_ptr<Node>> tmpForest;
  std::unordered_map<std::string, Node*> tmpIndex;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
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
  for (const RaftPeerPB& peer : raftConfig.peers()) {
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
    node->proxyFrom = proxyFromPtr;
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

Status RoutingTable::mergeForestIntoSingleRoutingTree(
    const std::string& leaderUuid,
    const std::unordered_map<std::string, Node*>& index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* forest) {
  Node* leader = nullptr;
  auto leaderIt = index.find(leaderUuid);
  if (leaderIt != index.end()) {
    leader = leaderIt->second;
  }
  if (!leader) {
    return Status::InvalidArgument(
        "invalid config: cannot find leader", leaderUuid);
  }

  // Find the ultimate proxy root of the leader, if the leader as a proxy
  // assigned to it.
  Node* sourceRoot = leader;
  while (sourceRoot->proxyFrom) {
    sourceRoot = sourceRoot->proxyFrom;
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
    iter->second->proxyFrom = leader;
    leader->children.emplace(childUuid, std::move(iter->second));
    iter = forest->erase(iter);
  }

  DCHECK_EQ(1, forest->size());
  return Status::OK();
}

void RoutingTable::constructNextHopIndicesRec(Node* cur) {
  for (const auto& childEntry : cur->children) {
    const string& childUuid = childEntry.first;
    const auto& child = childEntry.second;
    constructNextHopIndicesRec(child.get());
    // Absorb child routes.
    for (const auto& childRoute : child->routes) {
      const string& destUuid = childRoute.first;
      cur->routes.emplace(destUuid, childUuid);
    }
  }
  // Add self-route as a base case.
  cur->routes.emplace(cur->id(), cur->id());
}

Status RoutingTable::nextHop(
    const string& srcUuid,
    const string& destUuid,
    string* nextHopOut) const {
  // Base case: use direct routing if no routing topology is defined. If we
  // don't do this, if the leader has a proxy topology defined, and a proxy node
  // does not, then the proxy node will think the shortest path to the
  // destination is the leader, resulting in a routing loop. In the general case
  // this can happen due to proxy topology inconsistencies across the cluster
  // anyway, but it's nice to get non-pathological behavior in this case.
  if (!hasExplicitRoutes_) {
    *nextHopOut = destUuid;
    return Status::OK();
  }

  DCHECK(hasExplicitRoutes_); // Some proxy topology is defined.
  Node* src = nullptr;
  auto srcIt = index_.find(srcUuid);
  if (srcIt != index_.end()) {
    src = srcIt->second;
  }
  if (!src) {
    return Status::NotFound(fmt::format("unknown source uuid: {}", srcUuid));
  }
  Node* dest = nullptr;
  auto destIt = index_.find(destUuid);
  if (destIt != index_.end()) {
    dest = destIt->second;
  }
  if (!dest) {
    return Status::NotFound(
        fmt::format("unknown destination uuid: {}", destUuid));
  }

  // Search children.
  auto it = src->routes.find(destUuid);
  if (it != src->routes.end()) {
    *nextHopOut = it->second;
    return Status::OK();
  }

  // If we can't route via a child, route via a parent.
  DCHECK(src->proxyFrom);
  *nextHopOut = src->proxyFrom->id();
  return Status::OK();
}

std::string RoutingTable::toString() const {
  string out;
  out.reserve(4096);
  // DFS.
  toStringHelperRec(topologyRoot_.get(), /*level=*/0, &out);
  return out;
}

void RoutingTable::toStringHelperRec(Node* cur, int level, std::string* out)
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
      cur->peerPb.permanent_uuid(),
      SecureShortDebugString(cur->peerPb.last_known_addr()));
  for (const auto& entry : cur->children) {
    toStringHelperRec(entry.second.get(), level + 1, out);
  }
}

////////////////////////////////////////////////////////////////////////////////
// DurableRoutingTable
////////////////////////////////////////////////////////////////////////////////

Status DurableRoutingTable::create(
    FsManager* fsManager,
    std::string tabletId,
    RaftConfigPB raftConfig,
    ProxyTopologyPB proxyTopology,
    std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fsManager->GetProxyMetadataPath(tabletId);
  if (fsManager->env()->FileExists(path)) {
    return Status::AlreadyPresent(fmt::format("File {} already exists", path));
  }

  auto tmpDrt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(
      fsManager,
      std::move(tabletId),
      std::move(proxyTopology),
      std::move(raftConfig)));
  RETURN_NOT_OK(tmpDrt->flush()); // no lock needed as object is unpublished
  *drt = std::move(tmpDrt);
  return Status::OK();
}

// Read from disk.
Status DurableRoutingTable::load(
    FsManager* fsManager,
    std::string tabletId,
    RaftConfigPB raftConfig,
    LoadOptions opts,
    std::shared_ptr<DurableRoutingTable>* drt) {
  string path = fsManager->GetProxyMetadataPath(tabletId);

  ProxyTopologyPB proxyTopology;
  Status s =
      pb_util::ReadPBContainerFromPath(fsManager->env(), path, &proxyTopology);
  if (PREDICT_FALSE(
          s.IsNotFound() && opts == LoadOptions::kCreateEmptyIfDoesNotExist)) {
    s = create(fsManager, tabletId, raftConfig, {}, drt);
  }
  RETURN_NOT_OK(s);

  *drt = std::shared_ptr<DurableRoutingTable>(new DurableRoutingTable(
      fsManager,
      std::move(tabletId),
      std::move(proxyTopology),
      std::move(raftConfig)));
  return Status::OK();
}

Status DurableRoutingTable::deleteOnDiskData(
    FsManager* fsManager,
    const string& tabletId) {
  string path = fsManager->GetProxyMetadataPath(tabletId);
  RETURN_NOT_OK_PREPEND(
      fsManager->env()->DeleteFile(path),
      fmt::format(
          "Unable to delete durable routing table file for tablet {}",
          tabletId));
  return Status::OK();
}

Status DurableRoutingTable::updateProxyTopology(ProxyTopologyPB proxyTopology) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  // Rebuild the routing table.
  RoutingTable routingTable;
  if (leaderUuid_) {
    Status s = routingTable.init(raftConfig_, proxyTopology, *leaderUuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
  }

  // Only flush the proxy graph protobuf to disk when it changes.
  if (!MessageDifferencer::Equals(proxyTopology, proxyTopology_)) {
    VLOG_WITH_PREFIX(3) << "proxy routes updated, flushing to disk...";
    RETURN_NOT_OK(flush());
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  proxyTopology_ = std::move(proxyTopology);

  if (leaderUuid_) {
    routingTable_ = std::move(routingTable);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routingTable_->toString();
  } else {
    routingTable_ = {};
    LOG_WITH_PREFIX(INFO)
        << "proxy routing temporarily disabled: no known leader";
  }

  return Status::OK();
}

Status DurableRoutingTable::updateRaftConfig(RaftConfigPB raftConfig) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  // Rebuild the routing table.
  RoutingTable routingTable;
  bool leaderInConfig = false;
  if (leaderUuid_) {
    leaderInConfig = isRaftConfigMember(*leaderUuid_, raftConfig);
  }
  if (leaderInConfig) {
    Status s = routingTable.init(raftConfig, proxyTopology_, *leaderUuid_);
    if (PREDICT_FALSE(s.IsIncomplete())) {
      // Log but continue for Incomplete, which is a warning.
      LOG_WITH_PREFIX(WARNING) << s.ToString();
    } else {
      RETURN_NOT_OK(s);
    }
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routingTable.toString();
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  raftConfig_ = std::move(raftConfig);

  if (leaderInConfig) {
    routingTable_ = std::move(routingTable);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes:\n"
                          << routingTable_->toString();
  } else {
    routingTable_ = {};
    LOG_WITH_PREFIX(INFO)
        << "proxy routing temporarily disabled: the leader is not in the config";
  }

  return Status::OK();
}

void DurableRoutingTable::updateLeader(string leaderUuid) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  RoutingTable routingTable;
  bool initialized = false;
  if (isRaftConfigMember(leaderUuid, raftConfig_)) {
    // Rebuild the routing table. If this fails, remember the new leader anyway.
    Status s = routingTable.init(raftConfig_, proxyTopology_, leaderUuid);
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
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  leaderUuid_ = std::move(leaderUuid);
  if (initialized) {
    routingTable_ = std::move(routingTable);
    LOG_WITH_PREFIX(INFO) << "updated proxy routes: \n"
                          << routingTable_->toString();
  } else {
    routingTable_ = {};
    VLOG_WITH_PREFIX(2)
        << "proxy routing disabled: no valid proxy topology is set";
  }
}

Status DurableRoutingTable::nextHop(
    const std::string& srcUuid,
    const std::string& destUuid,
    std::string* nextHopOut) const {
  shared_lock<RwcLock> l(lock_);
  if (routingTable_) {
    return routingTable_->nextHop(srcUuid, destUuid, nextHopOut);
  }
  if (!isRaftConfigMember(destUuid, raftConfig_)) {
    return Status::NotFound(
        fmt::format(
            "peer with uuid {} not found in consensus config", destUuid));
  }

  *nextHopOut = destUuid;
  return Status::OK();
}

ProxyTopologyPB DurableRoutingTable::getProxyTopology() const {
  shared_lock<RwcLock> l(lock_);
  return proxyTopology_;
}

string DurableRoutingTable::toString() const {
  shared_lock<RwcLock> l(lock_);
  if (routingTable_) {
    return routingTable_->toString();
  }
  return "";
}

DurableRoutingTable::DurableRoutingTable(
    FsManager* fsManager,
    string tabletId,
    ProxyTopologyPB proxyTopology,
    RaftConfigPB raftConfig)
    : fsManager_(fsManager),
      tabletId_(std::move(tabletId)),
      proxyTopology_(std::move(proxyTopology)),
      raftConfig_(std::move(raftConfig)) {
  // TODO(mpercy): Do we have any validation to perform here?
}

Status DurableRoutingTable::flush() const {
  // TODO(mpercy): This entire method is copy / pasted from
  // ConsensusMetadata::Flush(). Factor out?

  // Create directories if needed.
  string dir = fsManager_->GetConsensusMetadataDir();
  bool createdDir = false;
  RETURN_NOT_OK_PREPEND(
      env_util::createDirIfMissing(fsManager_->env(), dir, &createdDir),
      "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(createdDir)) {
    string parentDir = DirName(dir);
    RETURN_NOT_OK_PREPEND(
        Env::Default()->SyncDir(parentDir),
        "Unable to fsync consensus parent dir " + parentDir);
  }

  string path = fsManager_->GetProxyMetadataPath(tabletId_);
  RETURN_NOT_OK_PREPEND(
      pb_util::WritePBContainerToPath(
          fsManager_->env(),
          path,
          proxyTopology_,
          pb_util::OVERWRITE,
          pb_util::SYNC),
      fmt::format(
          "Unable to write proxy metadata file for tablet {} to path {}",
          tabletId_,
          path));
  return Status::OK();
}

string DurableRoutingTable::LogPrefix() const {
  return fmt::format("T {} P {}: ", tabletId_, fsManager_->uuid());
}

ProxyPolicy DurableRoutingTable::getProxyPolicy() const {
  return ProxyPolicy::DURABLE_ROUTING_POLICY;
}

////////////////////////////////////////////////////////////////////////////////
// SimpleRegionRoutingTable
////////////////////////////////////////////////////////////////////////////////
Status SimpleRegionRoutingTable::create(
    RaftConfigPB raftConfig,
    RaftPeerPB localPeerPb,
    std::shared_ptr<SimpleRegionRoutingTable>* srt) {
  auto simpleRoutingTable = std::make_shared<SimpleRegionRoutingTable>();
  simpleRoutingTable->setLocalPeerPb(std::move(localPeerPb));
  simpleRoutingTable->updateRaftConfig(std::move(raftConfig));
  *srt = std::move(simpleRoutingTable);

  return Status::OK();
}

Status SimpleRegionRoutingTable::rebuildProxyTopology(RaftConfigPB raftConfig) {
  ProxyTopologyPB proxyTopology;
  const std::string& localPeerRegion = localPeerPb_.attrs().region();

  // Take a copy of current map
  std::unordered_map<std::string, std::string> currentDstToProxyMap;
  {
    std::shared_lock l(lock_);
    currentDstToProxyMap = dstToProxyMap_;
  }

  // 1. Identify the 'proxy peer' for each region. The peer that is backed by a
  // database in a region acts as a 'proxy peer' for the region. [Update when
  // region splitting is supported]. If there are multiple such peers in a
  // region, then pick the first peer as the 'proxy peer' for the region [this
  // cannot happen once region splitting is supported]
  // 2. Also build a map of "peer-uuid to peer-region" for all peers
  std::unordered_map<std::string, std::string> regionProxyPeerMap;
  std::unordered_map<std::string, std::string> peerRegionMap;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
    if (canBeProxyPeer(peer)) {
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
  for (const RaftPeerPB& destPeer : raftConfig.peers()) {
    std::string destPeerRegion = destPeer.attrs().region();
    if (canBeProxyPeer(destPeer)) {
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
        ProxyEdgePB* proxyEdge = proxyTopology.add_proxy_edges();
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
  proxyTopology_ = std::move(proxyTopology);
  dstToProxyMap_ = std::move(dstToProxyMap);
  raftConfig_ = std::move(raftConfig);

  return Status::OK();
}

Status SimpleRegionRoutingTable::nextHop(
    const std::string& /* srcUuid */,
    const std::string& destUuid,
    std::string* nextHopOut) const {
  std::shared_lock l(lock_);
  const auto& proxyUuid = dstToProxyMap_.find(destUuid);
  if (proxyUuid == dstToProxyMap_.end()) {
    // Could not find this destination, route directly to the destination
    *nextHopOut = destUuid;
    return Status::OK();
  }

  *nextHopOut = proxyUuid->second;
  return Status::OK();
}

Status SimpleRegionRoutingTable::updateProxyTopology(
    ProxyTopologyPB proxyTopology) {
  // SimpleRegionRoutingTable uses config to update proxy maps. Hence cannot
  // update topology directly.
  // TODO: provide a way to override designated per-region proxy peer
  return Status::OK();
}

ProxyTopologyPB SimpleRegionRoutingTable::getProxyTopology() const {
  std::shared_lock l(lock_);
  return proxyTopology_;
}

Status SimpleRegionRoutingTable::updateRaftConfig(RaftConfigPB raftConfig) {
  return rebuildProxyTopology(std::move(raftConfig));
}

void SimpleRegionRoutingTable::updateLeader(string leaderUuid) {
  std::lock_guard l(lock_);
  leaderUuid_ = std::move(leaderUuid);
}

void SimpleRegionRoutingTable::setLocalPeerPb(RaftPeerPB localPeerPb) {
  std::lock_guard l(lock_);
  localPeerPb_ = std::move(localPeerPb);
}

ProxyPolicy SimpleRegionRoutingTable::getProxyPolicy() const {
  return ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY;
}

////////////////////////////////////////////////////////////////////////////////
// RoutingTableContainer implementation
////////////////////////////////////////////////////////////////////////////////
RoutingTableContainer::RoutingTableContainer(
    const ProxyPolicy& proxyPolicy,
    const RaftPeerPB& localPeerPb,
    RaftConfigPB raftConfig,
    std::shared_ptr<DurableRoutingTable> drt,
    const std::vector<std::unordered_set<std::string>>& regionGroups) {
  proxyPolicy_ = proxyPolicy;
  drt_ = std::move(drt);

  std::shared_ptr<SimpleRegionRoutingTable> srt;
  SimpleRegionRoutingTable::create(raftConfig, localPeerPb, &srt);
  srt_ = std::move(srt);

  std::shared_ptr<RegionGroupRoutingTable> rgrt;
  RegionGroupRoutingTable::create(raftConfig, localPeerPb, regionGroups, &rgrt);
  grt_ = std::move(rgrt);
}

Status RoutingTableContainer::nextHop(
    const std::string& srcUuid,
    const std::string& destUuid,
    std::string* nextHopOut) const {
  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->nextHop(srcUuid, destUuid, nextHopOut);
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->nextHop(srcUuid, destUuid, nextHopOut);
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->nextHop(srcUuid, destUuid, nextHopOut);
    case ProxyPolicy::DISABLE_PROXY:
      *nextHopOut = destUuid;
      return Status::OK();
    default:
      break; // placate the compiler
  }

  return Status::NotSupported("The specified proxy_policy is not supported");
}

Status RoutingTableContainer::updateProxyTopology(
    ProxyTopologyPB proxyTopology,
    RaftConfigPB raftConfig,
    const std::string& leaderUuid) {
  // Explicit routing topology can only be used by durable routing table
  // Update the leader uuid before updating proxyTopology
  drt_->updateLeader(leaderUuid);
  RETURN_NOT_OK(drt_->updateRaftConfig(std::move(raftConfig)));
  return drt_->updateProxyTopology(std::move(proxyTopology));
}

std::vector<std::unordered_set<std::string>>
RoutingTableContainer::getProxyRegionGroup() {
  return grt_->getProxyRegionGroup();
}

Status RoutingTableContainer::updateProxyRegionGroup(
    const std::vector<std::unordered_set<std::string>>& regionGroups,
    RaftConfigPB raftConfig,
    const std::string& leaderUuid) {
  return grt_->updateProxyRegionGroup(
      regionGroups, std::move(raftConfig), leaderUuid);
}

void RoutingTableContainer::updateRtt(
    const std::string& peerUuid,
    std::chrono::microseconds rtt) {
  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      grt_->updateRtt(peerUuid, rtt);
      break;
    default:
      break; // placate the compiler
  }
}

ProxyTopologyPB RoutingTableContainer::getProxyTopology() const {
  ProxyTopologyPB topologyPb;

  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->getProxyTopology();
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->getProxyTopology();
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->getProxyTopology();
    default:
      break; // placate the compiler
  }

  return topologyPb;
}

Status RoutingTableContainer::updateRaftConfig(RaftConfigPB raftConfig) {
  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return drt_->updateRaftConfig(std::move(raftConfig));
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      return srt_->updateRaftConfig(std::move(raftConfig));
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      return grt_->updateRaftConfig(std::move(raftConfig));
    default:
      break; // placate the compiler
  }

  return Status::NotSupported("The specified proxy_policy is not supported");
}

void RoutingTableContainer::updateLeader(string leaderUuid) {
  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      drt_->updateLeader(std::move(leaderUuid));
      break;
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      srt_->updateLeader(std::move(leaderUuid));
      break;
    case ProxyPolicy::REGION_GROUP_ROUTING_POLICY:
      grt_->updateLeader(std::move(leaderUuid));
      break;
    default:
      break; // placate the compiler
  }
}

void RoutingTableContainer::setLocalPeerPb(RaftPeerPB localPeerPb) {
  ProxyPolicy policy = proxyPolicy_.load();

  switch (policy) {
    case ProxyPolicy::DURABLE_ROUTING_POLICY:
      return; // No-Op for drt
    case ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY:
      srt_->setLocalPeerPb(std::move(localPeerPb));
      break;
    default:
      break; // placate the compiler
  }
}

ProxyPolicy RoutingTableContainer::getProxyPolicy() const {
  return proxyPolicy_.load();
}

Status RoutingTableContainer::setProxyPolicy(
    const ProxyPolicy& proxyPolicy,
    const std::string& leaderUuid,
    RaftConfigPB raftConfig) {
  drt_->updateLeader(leaderUuid);
  srt_->updateLeader(leaderUuid);

  RETURN_NOT_OK(drt_->updateRaftConfig(raftConfig));
  RETURN_NOT_OK(srt_->updateRaftConfig(raftConfig));

  RETURN_NOT_OK(grt_->updateRaftConfigAndLeader(raftConfig, leaderUuid));

  proxyPolicy_ = proxyPolicy;

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
// Global functions.
////////////////////////////////////////////////////////////////////////////////

Status verifyProxyTopology(const ProxyTopologyPB& proxyTopology) {
  unordered_set<string> seen;
  for (const auto& entry : proxyTopology.proxy_edges()) {
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

bool canBeProxyPeer(const RaftPeerPB& peer) {
  return isBackingDbPresent(peer) && !isStandbyMember(peer);
}

} // namespace kudu::consensus
