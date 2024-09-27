// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/region_group_routing.h"
#include "kudu/consensus/routing.h"

#include <unordered_set>

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/locks.h"
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

namespace kudu {
namespace consensus {

////////////////////////////////////////////////////////////////////////////////
// RegionGroupRoutingTable
////////////////////////////////////////////////////////////////////////////////
Status RegionGroupRoutingTable::Create(
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
  region_groups_ = region_groups;
  local_peer_pb_ = std::move(local_peer_pb);
  raft_config_ = std::move(raft_config);
  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config_,
      local_peer_pb_,
      leader_uuid_,
      region_groups_,
      dst_to_proxy_map_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  dst_to_proxy_map_ = std::move(dst_to_proxy_map);
  proxy_topology_ = std::move(proxy_topology);
  peers_map_ = std::move(peers_map);
}

bool RegionGroupRoutingTable::HasRttValue(const std::string& peer_uuid) const {
  auto itr = peer_rtt_map_.find(peer_uuid);
  if (itr == peer_rtt_map_.end()) {
    return false;
  }
  return itr->second.avg_rtt.count() > 0;
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
    if (region_peer_map.find(region) == region_peer_map.end()) {
      continue;
    }
    for (const auto& peer_uuid : region_peer_map.at(region)) {
      auto itr = peer_rtt_map_.find(peer_uuid);
      if (itr == peer_rtt_map_.end()) {
        continue;
      }
      if (itr->second.avg_rtt.count() < min_rtt || proxy_peer_uuid.empty()) {
        min_rtt = itr->second.avg_rtt.count();
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
  const std::string& local_peer_region = local_peer_pb.attrs().region();
  // Use the same routes as SimpleRegionRoutingTable when local peer is not the
  // leader. SimpleRegionRoutingTable is enough here since it doesn't do nested
  // proxying. Request to a proxy node should be routed to the destination node
  // directly without further proxying. Assume leader does the route properly,
  // non leader replica just need to forward the request to the destination.
  if (!leader_uuid.has_value() ||
      *leader_uuid != local_peer_pb.permanent_uuid() ||
      !IsRaftConfigMember(*leader_uuid, raft_config)) {
    std::unordered_map<std::string, std::string> region_proxy_peer_map;
    for (const RaftPeerPB& peer : raft_config.peers()) {
      if (peer.attrs().backing_db_present()) {
        region_proxy_peer_map.emplace(
            peer.attrs().region(), peer.permanent_uuid());
      }
      peers_map.emplace(peer.permanent_uuid(), peer);
    }

    for (const RaftPeerPB& dest_peer : raft_config.peers()) {
      std::string dest_peer_region = dest_peer.attrs().region();
      if (dest_peer.attrs().backing_db_present()) {
        continue;
      } else {
        const auto& proxy_peer_uuid =
            region_proxy_peer_map.find(dest_peer_region);
        if (proxy_peer_uuid == region_proxy_peer_map.end() ||
            dest_peer_region == local_peer_region) {
          continue;
        } else {
          ProxyEdgePB* proxy_edge = proxy_topology.add_proxy_edges();
          proxy_edge->set_peer_uuid(dest_peer.permanent_uuid());

          const auto& current_proxy_peer =
              current_dst_to_proxy_map.find(dest_peer.permanent_uuid());
          if (current_proxy_peer != current_dst_to_proxy_map.end()) {
            const auto& current_proxy_peer_region =
                peers_map.find(current_proxy_peer->second);
            if (current_proxy_peer_region != peers_map.end()) {
              proxy_edge->set_proxy_from_uuid(current_proxy_peer->second);
              dst_to_proxy_map.emplace(
                  dest_peer.permanent_uuid(), current_proxy_peer->second);
              continue;
            }
          }

          proxy_edge->set_proxy_from_uuid(proxy_peer_uuid->second);
          dst_to_proxy_map.emplace(
              dest_peer.permanent_uuid(), proxy_peer_uuid->second);
        }
      }
    }
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
    if (peer.attrs().backing_db_present()) {
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
    if (region_group.find(leader_region) != region_group.end()) {
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
    if (!dest_peer.attrs().backing_db_present()) {
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
          if (peers_map.find(current_proxy_peer->second) != peers_map.end()) {
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

Status RegionGroupRoutingTable::NextHop(
    const std::string& /* src_uuid */,
    const std::string& dest_uuid,
    std::string* next_hop) const {
  shared_lock<RWCLock> l(lock_);
  const auto& proxy_uuid = dst_to_proxy_map_.find(dest_uuid);
  if (proxy_uuid == dst_to_proxy_map_.end()) {
    // Could not find this destination, route directly to the destination
    *next_hop = dest_uuid;
    return Status::OK();
  }

  *next_hop = proxy_uuid->second;
  return Status::OK();
}

Status RegionGroupRoutingTable::UpdateProxyTopology(
    ProxyTopologyPB /*proxy_topolog*/) {
  // TODO(chenjin) - we need an API to accept region group.
  // Proxy topology update is no-op.
  return Status::OK();
}

ProxyTopologyPB RegionGroupRoutingTable::GetProxyTopology() const {
  shared_lock<RWCLock> l(lock_);
  return proxy_topology_;
}

Status RegionGroupRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });
  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config,
      local_peer_pb_,
      leader_uuid_,
      region_groups_,
      dst_to_proxy_map_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock
      .cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  dst_to_proxy_map_ = std::move(dst_to_proxy_map);
  proxy_topology_ = std::move(proxy_topology);
  raft_config_ = std::move(raft_config);
  peers_map_ = std::move(peers_map);

  return Status::OK();
}

void RegionGroupRoutingTable::UpdateLeader(string leader_uuid) {
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  std::unordered_map<std::string, std::string> dst_to_proxy_map;
  std::unordered_map<std::string, RaftPeerPB> peers_map;
  ProxyTopologyPB proxy_topology;
  BuildProxyTopology(
      raft_config_,
      local_peer_pb_,
      leader_uuid,
      region_groups_,
      dst_to_proxy_map_,
      dst_to_proxy_map,
      proxy_topology,
      peers_map);

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock
      .cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  dst_to_proxy_map_ = std::move(dst_to_proxy_map);
  proxy_topology_ = std::move(proxy_topology);
  peers_map_ = std::move(peers_map);
  leader_uuid_ = std::move(leader_uuid);
}

ProxyPolicy RegionGroupRoutingTable::GetProxyPolicy() const {
  return ProxyPolicy::REGION_GROUP_ROUTING_POLICY;
}

bool RegionGroupRoutingTable::IsLeaderNoLock() const {
  return leader_uuid_.has_value() &&
      local_peer_pb_.permanent_uuid() == leader_uuid_;
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

void RegionGroupRoutingTable::UpdateRtt(
    const std::string& peer_uuid,
    std::chrono::microseconds rtt) {
  std::unordered_set<std::string> db_peers_in_same_group;
  {
    // TODO(chenjin) - this is high frequency operation, need to validate
    // if lock overhead is acceptable.
    lock_.WriteLock();
    auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

    auto peer_itr = peers_map_.find(peer_uuid);
    // unknown peer, ignore the update
    if (peer_itr == peers_map_.end()) {
      return;
    }
    // peer without a backing db, ignore the update
    if (!peer_itr->second.attrs().backing_db_present()) {
      return;
    }

    const std::string& peer_region = peer_itr->second.attrs().region();
    RegionGroup rg(raft_config_, region_groups_);
    int64_t old_min_rtt = rg.GetRegionProxyRtt(
        peer_rtt_map_, peer_region, db_peers_in_same_group);

    lock_.UpgradeToCommitLock();
    release_write_lock
        .cancel(); // Unlocking the commit lock releases the write lock.
    auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });
    auto rtt_updated = peer_rtt_map_[peer_uuid].UpdateRtt(rtt);
    if (!rtt_updated || peer_region.empty() || !IsLeaderNoLock()) {
      return;
    }
    int64_t new_rtt_us = peer_rtt_map_[peer_uuid].avg_rtt.count();
    // no need to update the proxy topology if the new rtt is not much lower
    // than the current min rtt
    if (new_rtt_us <= 0 || db_peers_in_same_group.empty() ||
        new_rtt_us + 5000 >= old_min_rtt) {
      return;
    }
  }

  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });
  bool hasUpdate = false;
  auto dst_to_proxy_map = dst_to_proxy_map_;
  for (const auto& peer : db_peers_in_same_group) {
    auto itr = dst_to_proxy_map.find(peer);
    if (itr == dst_to_proxy_map.end() || itr->second != peer_uuid) {
      dst_to_proxy_map[peer] = peer_uuid;
      hasUpdate = true;
    }
  }
  if (!hasUpdate) {
    return;
  }
  auto topology = DeriveProxyTopologyByProxyMap(dst_to_proxy_map);
  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel();
  dst_to_proxy_map_ = std::move(dst_to_proxy_map);
  proxy_topology_ = std::move(topology);
  lock_.CommitUnlock();
}

} // namespace consensus
} // namespace kudu
