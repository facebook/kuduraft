// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/region_group_routing.h"
#include "kudu/consensus/routing.h"

#include <unordered_set>

#include <folly/String.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include "common/logging/logging.h"

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/consensus/quorum_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using google::protobuf::util::MessageDifferencer;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace consensus {

////////////////////////////////////////////////////////////////////////////////
// RegionGroupRoutingTable
////////////////////////////////////////////////////////////////////////////////
Status RegionGroupRoutingTable::create(
    RaftConfigPB raftConfig,
    RaftPeerPB localPeerPb,
    const std::vector<std::unordered_set<std::string>>& regionGroups,
    std::shared_ptr<RegionGroupRoutingTable>* rgrt) {
  *rgrt = std::shared_ptr<RegionGroupRoutingTable>(new RegionGroupRoutingTable(
      std::move(raftConfig), std::move(localPeerPb), regionGroups));
  return Status::OK();
}

RegionGroupRoutingTable::RegionGroupRoutingTable(
    RaftConfigPB raftConfig,
    RaftPeerPB localPeerPb,
    const std::vector<std::unordered_set<std::string>>& regionGroups) {
  std::vector<std::string> regionStrs;
  for (const auto& regions : regionGroups) {
    regionStrs.emplace_back(folly::join(",", regions));
  }
  LOG(INFO) << "Creating RegionGroupRoutingTable with region groups: "
            << folly::join(";", regionStrs);
  regionGroups_ = regionGroups;
  localPeerPb_ = std::move(localPeerPb);
  raftConfig_ = std::move(raftConfig);
}

bool RegionGroupRoutingTable::hasRttValue(const std::string& peerUuid) const {
  auto itr = peerRttMap_.find(peerUuid);
  if (itr == peerRttMap_.end()) {
    return false;
  }
  return itr->second.avgRtt.count() > 0;
}

std::string RegionGroupRoutingTable::getGroupProxyPeerByRtt(
    const std::unordered_set<std::string>& regions,
    const std::unordered_map<std::string, std::vector<std::string>>&
        regionPeerMap) const {
  if (regions.empty() || regionPeerMap.empty()) {
    return "";
  }
  std::string proxyPeerUuid;
  int64_t minRtt = INT_MAX;
  for (const auto& region : regions) {
    if (!regionPeerMap.contains(region)) {
      continue;
    }
    for (const auto& peerUuid : regionPeerMap.at(region)) {
      auto itr = peerRttMap_.find(peerUuid);
      if (itr == peerRttMap_.end()) {
        continue;
      }
      if (itr->second.avgRtt.count() < minRtt || proxyPeerUuid.empty()) {
        minRtt = itr->second.avgRtt.count();
        proxyPeerUuid = itr->first;
      }
    }
  }
  return proxyPeerUuid;
}

Status RegionGroupRoutingTable::buildProxyTopology(
    const RaftConfigPB& raftConfig,
    const RaftPeerPB& localPeerPb,
    const std::optional<std::string>& leaderUuid,
    const std::vector<std::unordered_set<std::string>>& regionGroups,
    const std::unordered_map<std::string, std::string>& currentDstToProxyMap,
    std::unordered_map<std::string, std::string>& dstToProxyMap,
    ProxyTopologyPB& proxyTopology,
    std::unordered_map<std::string, RaftPeerPB>& peersMap) {
  std::vector<std::string> regionStrs;
  for (const auto& regions : regionGroups) {
    regionStrs.emplace_back(folly::join(",", regions));
  }
  LOG(INFO) << "buildProxyTopology: " << folly::join(";", regionStrs);
  const std::string& localPeerRegion = localPeerPb.attrs().region();
  // Assume leader does the route properly,
  // non leader replica just need to forward the request to the destination.
  // So it doesn't need any proxy map.
  if (!leaderUuid.has_value() || *leaderUuid != localPeerPb.permanent_uuid() ||
      !isRaftConfigMember(*leaderUuid, raftConfig)) {
    dstToProxyMap.clear();
    return Status::OK();
  }

  // Handle the case that local peer is the leader.

  // 1. Identify the 'proxy peer' for each region. The peer that is backed by
  // a database in a region acts as a 'proxy peer' for the region.
  // 2. Also build a map of "peer-region to peer-uuid vector of all peers in
  // that region" for all peers backed by a database. This map will be used to
  // identify the 'proxy peer' for each region group.
  std::unordered_map<std::string, std::vector<std::string>> regionPeerMap;
  std::string leaderRegion;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
    if (canBeProxyPeer(peer)) {
      regionPeerMap[peer.attrs().region()].push_back(peer.permanent_uuid());
    }
    peersMap.emplace(peer.permanent_uuid(), peer);
    if (peer.permanent_uuid() == leaderUuid) {
      leaderRegion = peer.attrs().region();
    }
  }

  // Identify the 'proxy peer' for each region group.
  std::unordered_map<std::string, std::string> groupProxyRegionMap;
  for (const auto& regionGroup : regionGroups) {
    if (regionGroup.empty()) {
      continue;
    }
    if (regionGroup.contains(leaderRegion)) {
      // For the region group where the leader is in, route directly from the
      // leader to the peer without proxying.
      continue;
    }
    std::string selectedGroupProxyPeer =
        getGroupProxyPeerByRtt(regionGroup, regionPeerMap);
    if (!selectedGroupProxyPeer.empty()) {
      for (const auto& region : regionGroup) {
        groupProxyRegionMap[region] = selectedGroupProxyPeer;
      }
    }
  }
  for (const RaftPeerPB& destPeer : raftConfig.peers()) {
    const std::string& destPeerRegion = destPeer.attrs().region();
    // peer without a backing db should use the peer with backing db in the
    // same region as the proxy
    if (!canBeProxyPeer(destPeer)) {
      const auto& proxyPeerUuid = regionPeerMap.find(destPeerRegion);
      if (proxyPeerUuid == regionPeerMap.end() ||
          proxyPeerUuid->second.empty() || destPeerRegion == localPeerRegion) {
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
          if (peersMap.contains(currentProxyPeer->second)) {
            // Continue to route through the existing 'proxy peer'
            proxyEdge->set_proxy_from_uuid(currentProxyPeer->second);
            dstToProxyMap.emplace(
                destPeer.permanent_uuid(), currentProxyPeer->second);
            continue;
          }
        }

        // 'destPeer' will be proxied through 'proxyPeerUuid'
        proxyEdge->set_proxy_from_uuid(*proxyPeerUuid->second.begin());
        dstToProxyMap.emplace(
            destPeer.permanent_uuid(), *proxyPeerUuid->second.begin());
      }
      continue;
    }
    auto itr = groupProxyRegionMap.find(destPeerRegion);
    if (itr != groupProxyRegionMap.end() &&
        itr->second != destPeer.permanent_uuid() &&
        hasRttValue(destPeer.permanent_uuid())) {
      ProxyEdgePB* proxyEdge = proxyTopology.add_proxy_edges();
      proxyEdge->set_peer_uuid(destPeer.permanent_uuid());
      proxyEdge->set_proxy_from_uuid(itr->second);
      dstToProxyMap[destPeer.permanent_uuid()] = itr->second;
    }
    // for all peers in the same region as the leader, route directly from the
    // leader to the peer without proxying
  }

  return Status::OK();
}

Status RegionGroupRoutingTable::nextHop(
    const std::string& /* srcUuid */,
    const std::string& destUuid,
    std::string* nextHop) const {
  shared_lock<RwcLock> l(lock_);
  const auto& proxyUuid = dstToProxyMap_.find(destUuid);
  if (proxyUuid == dstToProxyMap_.end()) {
    // Could not find this destination, route directly to the destination
    *nextHop = destUuid;
    return Status::OK();
  }

  *nextHop = proxyUuid->second;
  return Status::OK();
}

Status RegionGroupRoutingTable::updateProxyTopology(
    ProxyTopologyPB /*proxyTopology*/) {
  // See updateProxyRegionGroup for updating the proxy topology.
  return Status::NotSupported(
      "RegionGroupRoutingTable::updateProxyTopology not supported.");
}

Status RegionGroupRoutingTable::updateProxyRegionGroup(
    const std::vector<std::unordered_set<std::string>>& regionGroups,
    RaftConfigPB raftConfig,
    const std::string& leaderUuid) {
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });
  std::unordered_map<std::string, std::string> dstToProxyMap;
  std::unordered_map<std::string, RaftPeerPB> peersMap;
  ProxyTopologyPB proxyTopology;
  buildProxyTopology(
      raftConfig,
      localPeerPb_,
      leaderUuid,
      regionGroups,
      dstToProxyMap_,
      dstToProxyMap,
      proxyTopology,
      peersMap);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dstToProxyMap);
  proxyTopology_ = std::move(proxyTopology);
  raftConfig_ = std::move(raftConfig);
  peersMap_ = std::move(peersMap);
  regionGroups_ = regionGroups;
  leaderUuid_ = leaderUuid;
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");

  return Status::OK();
}

ProxyTopologyPB RegionGroupRoutingTable::getProxyTopology() const {
  shared_lock<RwcLock> l(lock_);
  return proxyTopology_;
}

Status RegionGroupRoutingTable::updateRaftConfig(RaftConfigPB raftConfig) {
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });
  std::unordered_map<std::string, std::string> dstToProxyMap;
  std::unordered_map<std::string, RaftPeerPB> peersMap;
  ProxyTopologyPB proxyTopology;
  buildProxyTopology(
      raftConfig,
      localPeerPb_,
      leaderUuid_,
      regionGroups_,
      dstToProxyMap_,
      dstToProxyMap,
      proxyTopology,
      peersMap);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dstToProxyMap);
  proxyTopology_ = std::move(proxyTopology);
  raftConfig_ = std::move(raftConfig);
  peersMap_ = std::move(peersMap);

  return Status::OK();
}

void RegionGroupRoutingTable::updateLeader(string leaderUuid) {
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  std::unordered_map<std::string, std::string> dstToProxyMap;
  std::unordered_map<std::string, RaftPeerPB> peersMap;
  ProxyTopologyPB proxyTopology;
  buildProxyTopology(
      raftConfig_,
      localPeerPb_,
      leaderUuid,
      regionGroups_,
      dstToProxyMap_,
      dstToProxyMap,
      proxyTopology,
      peersMap);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dstToProxyMap);
  proxyTopology_ = std::move(proxyTopology);
  peersMap_ = std::move(peersMap);
  leaderUuid_ = std::move(leaderUuid);
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");
}

Status RegionGroupRoutingTable::updateRaftConfigAndLeader(
    RaftConfigPB raftConfig,
    std::string leaderUuid) {
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  std::unordered_map<std::string, std::string> dstToProxyMap;
  std::unordered_map<std::string, RaftPeerPB> peersMap;
  ProxyTopologyPB proxyTopology;
  buildProxyTopology(
      raftConfig,
      localPeerPb_,
      leaderUuid,
      regionGroups_,
      dstToProxyMap_,
      dstToProxyMap,
      proxyTopology,
      peersMap);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dstToProxyMap);
  proxyTopology_ = std::move(proxyTopology);
  peersMap_ = std::move(peersMap);
  leaderUuid_ = std::move(leaderUuid);
  raftConfig_ = std::move(raftConfig);
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");

  return Status::OK();
}

ProxyPolicy RegionGroupRoutingTable::getProxyPolicy() const {
  return ProxyPolicy::REGION_GROUP_ROUTING_POLICY;
}

bool RegionGroupRoutingTable::isLeaderNoLock() const {
  return leaderUuid_.has_value() &&
      localPeerPb_.permanent_uuid() == leaderUuid_;
}

bool RegionGroupRoutingTable::isSameRegionGroup(
    const std::string& regionA,
    const std::string& regionB) const {
  if (regionA == regionB) {
    return true;
  }
  for (const auto& regionGroup : regionGroups_) {
    if (regionGroup.contains(regionA) && regionGroup.contains(regionB)) {
      return true;
    }
  }
  return false;
}

/*static*/
ProxyTopologyPB RegionGroupRoutingTable::deriveProxyTopologyByProxyMap(
    const std::unordered_map<std::string, std::string>& dstToProxyMap) {
  ProxyTopologyPB proxyTopology;
  for (const auto& [dstUuid, proxyUuid] : dstToProxyMap) {
    ProxyEdgePB* proxyEdge = proxyTopology.add_proxy_edges();
    proxyEdge->set_peer_uuid(dstUuid);
    proxyEdge->set_proxy_from_uuid(proxyUuid);
  }
  return proxyTopology;
}

/*static*/
bool RegionGroupRoutingTable::tryUpdateProxyMap(
    const std::string& proxyUuid,
    const std::unordered_set<std::string>& dbPeersInSameGroup,
    std::unordered_map<std::string, std::string>& dstToProxyMap) {
  bool needsUpdate = false;
  for (const auto& peer : dbPeersInSameGroup) {
    auto itr = dstToProxyMap.find(peer);
    if (peer == proxyUuid) {
      if (itr != dstToProxyMap.end()) {
        dstToProxyMap.erase(itr);
        needsUpdate = true;
      }
      continue;
    }
    if (itr == dstToProxyMap.end() || itr->second != proxyUuid) {
      dstToProxyMap[peer] = proxyUuid;
      LOG_EVERY_MS(INFO, 1000) << "Update proxy map for peer " << peer
                               << " to new proxy=" << proxyUuid;
      needsUpdate = true;
    }
  }
  return needsUpdate;
}

void RegionGroupRoutingTable::updateRtt(
    const std::string& peerUuid,
    std::chrono::microseconds rtt) {
  // TODO(chenjin) - this is high frequency operation, need to validate
  // if lock overhead is acceptable.
  lock_.writeLock();
  auto releaseWriteLock = folly::makeGuard([&] { lock_.writeUnlock(); });

  auto peerItr = peersMap_.find(peerUuid);
  // unknown peer, ignore the update
  if (peerItr == peersMap_.end()) {
    return;
  }
  // peer without a backing db, ignore the update
  if (!canBeProxyPeer(peerItr->second)) {
    return;
  }

  const std::string& peerRegion = peerItr->second.attrs().region();

  // peer in the same region as the local peer, ignore the update
  const std::string& localPeerRegion = localPeerPb_.attrs().region();
  if (isSameRegionGroup(peerRegion, localPeerRegion)) {
    return;
  }

  lock_.upgradeToCommitLock();
  releaseWriteLock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto releaseCommitLock = folly::makeGuard([&] { lock_.commitUnlock(); });
  auto rttUpdated = peerRttMap_[peerUuid].updateRtt(rtt);
  if (!rttUpdated || peerRegion.empty() || !isLeaderNoLock()) {
    return;
  }
  int64_t newRttUs = peerRttMap_[peerUuid].avgRtt.count();
  if (newRttUs <= 0) {
    return;
  }

  RegionGroup rg(raftConfig_, regionGroups_);
  std::unordered_set<std::string> dbPeersInSameGroup;
  auto [curProxyUuid, oldMinRtt] =
      rg.getRegionProxyRtt(peerRttMap_, peerRegion, dbPeersInSameGroup);
  dbPeersInSameGroup.insert(peerUuid);
  if (dbPeersInSameGroup.size() <= 1) {
    return;
  }
  std::string proxyUuid;
  // no need to update the proxy if the rtt of current peer is not much lower
  // than the old proxy peer
  if (curProxyUuid.empty() || newRttUs + 5000 <= oldMinRtt) {
    proxyUuid = peerUuid;
  } else {
    proxyUuid = curProxyUuid;
  }

  auto dstToProxyMap = dstToProxyMap_;
  if (!tryUpdateProxyMap(proxyUuid, dbPeersInSameGroup, dstToProxyMap)) {
    return;
  }

  auto topology = deriveProxyTopologyByProxyMap(dstToProxyMap);
  dstToProxyMap_ = std::move(dstToProxyMap);
  proxyTopology_ = std::move(topology);
}

} // namespace consensus
} // namespace kudu
