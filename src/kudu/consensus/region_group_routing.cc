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
    RaftConfigPB raft_config,
    RaftPeerPB local_peer_pb,
    const std::vector<std::unordered_set<std::string>>& region_groups,
    std::shared_ptr<RegionGroupRoutingTable>* rgrt) {
  *rgrt = std::shared_ptr<RegionGroupRoutingTable>(new RegionGroupRoutingTable(
      std::move(raft_config), std::move(local_peer_pb), region_groups));
  return Status::OK();
}

RegionGroupRoutingTable::RegionGroupRoutingTable(
    RaftConfigPB raft_config,
    RaftPeerPB local_peer_pb,
    const std::vector<std::unordered_set<std::string>>& region_groups) {
  std::vector<std::string> region_strs;
  for (const auto& regions : region_groups) {
    region_strs.emplace_back(folly::join(",", regions));
  }
  LOG(INFO) << "Creating RegionGroupRoutingTable with region groups: "
            << folly::join(";", region_strs);
  regionGroups_ = region_groups;
  localPeerPb_ = std::move(local_peer_pb);
  raftConfig_ = std::move(raft_config);
}

bool RegionGroupRoutingTable::HasRttValue(const std::string& peer_uuid) const {
  auto itr = peerRttMap_.find(peer_uuid);
  if (itr == peerRttMap_.end()) {
    return false;
  }
  return itr->second.avgRtt.count() > 0;
}

std::string RegionGroupRoutingTable::GetGroupProxyPeerByRtt(
    const std::unordered_set<std::string>& regions,
    const std::unordered_map<std::string, std::vector<std::string>>&
        region_peer_map) const {
  if (regions.empty() || region_peer_map.empty()) {
    return "";
  }
  std::string proxy_peer_uuid;
  int64_t min_rtt = INT_MAX;
  for (const auto& region : regions) {
    if (!region_peer_map.contains(region)) {
      continue;
    }
    for (const auto& peer_uuid : region_peer_map.at(region)) {
      auto itr = peerRttMap_.find(peer_uuid);
      if (itr == peerRttMap_.end()) {
        continue;
      }
      if (itr->second.avgRtt.count() < min_rtt || proxy_peer_uuid.empty()) {
        min_rtt = itr->second.avgRtt.count();
        proxy_peer_uuid = itr->first;
      }
    }
  }
  return proxy_peer_uuid;
}

Status RegionGroupRoutingTable::BuildProxyTopology(
    const RaftConfigPB& raft_config,
    const RaftPeerPB& local_peer_pb,
    const std::optional<std::string>& leader_uuid,
    const std::vector<std::unordered_set<std::string>>& region_groups,
    const std::unordered_map<std::string, std::string>&
        current_dst_to_proxy_map,
    std::unordered_map<std::string, std::string>& dst_to_proxy_map,
    ProxyTopologyPB& proxy_topology,
    std::unordered_map<std::string, RaftPeerPB>& peers_map) {
  std::vector<std::string> region_strs;
  for (const auto& regions : region_groups) {
    region_strs.emplace_back(folly::join(",", regions));
  }
  LOG(INFO) << "BuildProxyTopology: " << folly::join(";", region_strs);
  const std::string& local_peer_region = local_peer_pb.attrs().region();
  // Assume leader does the route properly,
  // non leader replica just need to forward the request to the destination.
  // So it doesn't need any proxy map.
  if (!leader_uuid.has_value() ||
      *leader_uuid != local_peer_pb.permanent_uuid() ||
      !isRaftConfigMember(*leader_uuid, raft_config)) {
    dst_to_proxy_map.clear();
    return Status::OK();
  }

  // Handle the case that local peer is the leader.

  // 1. Identify the 'proxy peer' for each region. The peer that is backed by
  // a database in a region acts as a 'proxy peer' for the region.
  // 2. Also build a map of "peer-region to peer-uuid vector of all peers in
  // that region" for all peers backed by a database. This map will be used to
  // identify the 'proxy peer' for each region group.
  std::unordered_map<std::string, std::vector<std::string>> region_peer_map;
  std::string leader_region;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    if (canBeProxyPeer(peer)) {
      region_peer_map[peer.attrs().region()].push_back(peer.permanent_uuid());
    }
    peers_map.emplace(peer.permanent_uuid(), peer);
    if (peer.permanent_uuid() == leader_uuid) {
      leader_region = peer.attrs().region();
    }
  }

  // Identify the 'proxy peer' for each region group.
  std::unordered_map<std::string, std::string> group_proxy_region_map;
  for (const auto& region_group : region_groups) {
    if (region_group.empty()) {
      continue;
    }
    if (region_group.contains(leader_region)) {
      // For the region group where the leader is in, route directly from the
      // leader to the peer without proxying.
      continue;
    }
    std::string selected_group_proxy_peer =
        GetGroupProxyPeerByRtt(region_group, region_peer_map);
    if (!selected_group_proxy_peer.empty()) {
      for (const auto& region : region_group) {
        group_proxy_region_map[region] = selected_group_proxy_peer;
      }
    }
  }
  for (const RaftPeerPB& dest_peer : raft_config.peers()) {
    const std::string& dest_peer_region = dest_peer.attrs().region();
    // peer without a backing db should use the peer with backing db in the
    // same region as the proxy
    if (!canBeProxyPeer(dest_peer)) {
      const auto& proxy_peer_uuid = region_peer_map.find(dest_peer_region);
      if (proxy_peer_uuid == region_peer_map.end() ||
          proxy_peer_uuid->second.empty() ||
          dest_peer_region == local_peer_region) {
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
          if (peers_map.contains(current_proxy_peer->second)) {
            // Continue to route through the existing 'proxy peer'
            proxy_edge->set_proxy_from_uuid(current_proxy_peer->second);
            dst_to_proxy_map.emplace(
                dest_peer.permanent_uuid(), current_proxy_peer->second);
            continue;
          }
        }

        // 'dest_peer' will be proxied through 'proxy_peer_uuid'
        proxy_edge->set_proxy_from_uuid(*proxy_peer_uuid->second.begin());
        dst_to_proxy_map.emplace(
            dest_peer.permanent_uuid(), *proxy_peer_uuid->second.begin());
      }
      continue;
    }
    auto itr = group_proxy_region_map.find(dest_peer_region);
    if (itr != group_proxy_region_map.end() &&
        itr->second != dest_peer.permanent_uuid() &&
        HasRttValue(dest_peer.permanent_uuid())) {
      ProxyEdgePB* proxy_edge = proxy_topology.add_proxy_edges();
      proxy_edge->set_peer_uuid(dest_peer.permanent_uuid());
      proxy_edge->set_proxy_from_uuid(itr->second);
      dst_to_proxy_map[dest_peer.permanent_uuid()] = itr->second;
    }
    // for all peers in the same region as the leader, route directly from the
    // leader to the peer without proxying
  }

  return Status::OK();
}

Status RegionGroupRoutingTable::nextHop(
    const std::string& /* src_uuid */,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  shared_lock<RWCLock> l(lock_);
  const auto& proxy_uuid = dstToProxyMap_.find(dest_uuid);
  if (proxy_uuid == dstToProxyMap_.end()) {
    // Could not find this destination, route directly to the destination
    *next_hop = dest_uuid;
    return Status::OK();
  }

  *next_hop = proxy_uuid->second;
  return Status::OK();
}

Status RegionGroupRoutingTable::updateProxyTopology(
    ProxyTopologyPB /*proxy_topolog*/) {
  // See updateProxyRegionGroup for updating the proxy topology.
  return Status::NotSupported(
      "RegionGroupRoutingTable::updateProxyTopology not supported.");
}

Status RegionGroupRoutingTable::updateProxyRegionGroup(
    const std::vector<std::unordered_set<std::string>>& region_groups,
    RaftConfigPB raft_config,
    const std::string& leader_uuid) {
  lock_.writeLock();
  auto release_write_lock = folly::makeGuard([&] { lock_.writeUnlock(); });
  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config,
      localPeerPb_,
      leader_uuid,
      region_groups,
      dstToProxyMap_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  release_write_lock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dst_to_proxy_map);
  proxyTopology_ = std::move(proxy_topology);
  raftConfig_ = std::move(raft_config);
  peersMap_ = std::move(peers_map);
  regionGroups_ = region_groups;
  leaderUuid_ = leader_uuid;
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");

  return Status::OK();
}

ProxyTopologyPB RegionGroupRoutingTable::getProxyTopology() const {
  shared_lock<RWCLock> l(lock_);
  return proxyTopology_;
}

Status RegionGroupRoutingTable::updateRaftConfig(RaftConfigPB raft_config) {
  lock_.writeLock();
  auto release_write_lock = folly::makeGuard([&] { lock_.writeUnlock(); });
  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config,
      localPeerPb_,
      leaderUuid_,
      regionGroups_,
      dstToProxyMap_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  release_write_lock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dst_to_proxy_map);
  proxyTopology_ = std::move(proxy_topology);
  raftConfig_ = std::move(raft_config);
  peersMap_ = std::move(peers_map);

  return Status::OK();
}

void RegionGroupRoutingTable::updateLeader(string leader_uuid) {
  lock_.writeLock();
  auto release_write_lock = folly::makeGuard([&] { lock_.writeUnlock(); });

  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raftConfig_,
      localPeerPb_,
      leader_uuid,
      regionGroups_,
      dstToProxyMap_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  release_write_lock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dst_to_proxy_map);
  proxyTopology_ = std::move(proxy_topology);
  peersMap_ = std::move(peers_map);
  leaderUuid_ = std::move(leader_uuid);
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");
}

Status RegionGroupRoutingTable::updateRaftConfigAndLeader(
    RaftConfigPB raft_config,
    std::string leader_uuid) {
  lock_.writeLock();
  auto release_write_lock = folly::makeGuard([&] { lock_.writeUnlock(); });

  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config,
      localPeerPb_,
      leader_uuid,
      regionGroups_,
      dstToProxyMap_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.upgradeToCommitLock();
  release_write_lock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = folly::makeGuard([&] { lock_.commitUnlock(); });

  dstToProxyMap_ = std::move(dst_to_proxy_map);
  proxyTopology_ = std::move(proxy_topology);
  peersMap_ = std::move(peers_map);
  leaderUuid_ = std::move(leader_uuid);
  raftConfig_ = std::move(raft_config);
  LOG(INFO) << "Updated leader to " << leaderUuid_.value_or("unknown");

  return Status::OK();
}

ProxyPolicy RegionGroupRoutingTable::getProxyPolicy() const {
  return ProxyPolicy::REGION_GROUP_ROUTING_POLICY;
}

bool RegionGroupRoutingTable::IsLeaderNoLock() const {
  return leaderUuid_.has_value() &&
      localPeerPb_.permanent_uuid() == leaderUuid_;
}

bool RegionGroupRoutingTable::isSameRegionGroup(
    const std::string& regionA,
    const std::string& regionB) const {
  if (regionA == regionB) {
    return true;
  }
  for (const auto& region_group : regionGroups_) {
    if (region_group.contains(regionA) && region_group.contains(regionB)) {
      return true;
    }
  }
  return false;
}

/*static*/
ProxyTopologyPB RegionGroupRoutingTable::DeriveProxyTopologyByProxyMap(
    const std::unordered_map<std::string, std::string>& dst_to_proxy_map) {
  ProxyTopologyPB proxy_topology;
  for (const auto& [dst_uuid, proxy_uuid] : dst_to_proxy_map) {
    ProxyEdgePB* proxy_edge = proxy_topology.add_proxy_edges();
    proxy_edge->set_peer_uuid(dst_uuid);
    proxy_edge->set_proxy_from_uuid(proxy_uuid);
  }
  return proxy_topology;
}

/*static*/
bool RegionGroupRoutingTable::TryUpdateProxyMap(
    const std::string& proxy_uuid,
    const std::unordered_set<std::string>& db_peers_in_same_group,
    std::unordered_map<std::string, std::string>& dst_to_proxy_map) {
  bool needs_update = false;
  for (const auto& peer : db_peers_in_same_group) {
    auto itr = dst_to_proxy_map.find(peer);
    if (peer == proxy_uuid) {
      if (itr != dst_to_proxy_map.end()) {
        dst_to_proxy_map.erase(itr);
        needs_update = true;
      }
      continue;
    }
    if (itr == dst_to_proxy_map.end() || itr->second != proxy_uuid) {
      dst_to_proxy_map[peer] = proxy_uuid;
      LOG_EVERY_MS(INFO, 1000) << "Update proxy map for peer " << peer
                               << " to new proxy=" << proxy_uuid;
      needs_update = true;
    }
  }
  return needs_update;
}

void RegionGroupRoutingTable::updateRtt(
    const std::string& peer_uuid,
    std::chrono::microseconds rtt) {
  // TODO(chenjin) - this is high frequency operation, need to validate
  // if lock overhead is acceptable.
  lock_.writeLock();
  auto release_write_lock = folly::makeGuard([&] { lock_.writeUnlock(); });

  auto peer_itr = peersMap_.find(peer_uuid);
  // unknown peer, ignore the update
  if (peer_itr == peersMap_.end()) {
    return;
  }
  // peer without a backing db, ignore the update
  if (!canBeProxyPeer(peer_itr->second)) {
    return;
  }

  const std::string& peer_region = peer_itr->second.attrs().region();

  // peer in the same region as the local peer, ignore the update
  const std::string& local_peer_region = localPeerPb_.attrs().region();
  if (isSameRegionGroup(peer_region, local_peer_region)) {
    return;
  }

  lock_.upgradeToCommitLock();
  release_write_lock
      .dismiss(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = folly::makeGuard([&] { lock_.commitUnlock(); });
  auto rtt_updated = peerRttMap_[peer_uuid].UpdateRtt(rtt);
  if (!rtt_updated || peer_region.empty() || !IsLeaderNoLock()) {
    return;
  }
  int64_t new_rtt_us = peerRttMap_[peer_uuid].avgRtt.count();
  if (new_rtt_us <= 0) {
    return;
  }

  RegionGroup rg(raftConfig_, regionGroups_);
  std::unordered_set<std::string> db_peers_in_same_group;
  auto [cur_proxy_uuid, old_min_rtt] =
      rg.GetRegionProxyRtt(peerRttMap_, peer_region, db_peers_in_same_group);
  db_peers_in_same_group.insert(peer_uuid);
  if (db_peers_in_same_group.size() <= 1) {
    return;
  }
  std::string proxy_uuid;
  // no need to update the proxy if the rtt of current peer is not much lower
  // than the old proxy peer
  if (cur_proxy_uuid.empty() || new_rtt_us + 5000 <= old_min_rtt) {
    proxy_uuid = peer_uuid;
  } else {
    proxy_uuid = cur_proxy_uuid;
  }

  auto dst_to_proxy_map = dstToProxyMap_;
  if (!TryUpdateProxyMap(
          proxy_uuid, db_peers_in_same_group, dst_to_proxy_map)) {
    return;
  }

  auto topology = DeriveProxyTopologyByProxyMap(dst_to_proxy_map);
  dstToProxyMap_ = std::move(dst_to_proxy_map);
  proxyTopology_ = std::move(topology);
}

} // namespace consensus
} // namespace kudu
