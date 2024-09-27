#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <optional>

#include "kudu/consensus/routing.h"

namespace kudu {

class Status;

namespace consensus {

// A region group based routing table. Check proxy_policy.h for more
// information. This table is intantiated when proxy policy is set to
// ProxyPolicy::SIMPLE_REGION_ROUTING_POLICY.
class RegionGroupRoutingTable : public IRoutingTable {
 public:
  FRIEND_TEST(RegionGroupRoutingTableTest, RttTrackerTest);
  FRIEND_TEST(RegionGroupRoutingTableTest, HelpFuncTest);
  FRIEND_TEST(RegionGroupRoutingTableTest, BuildProxyTopologyTest);

  ~RegionGroupRoutingTable() override = default;

  Status NextHop(
      const std::string& src_uuid,
      const std::string& dest_uuid,
      std::string* next_hop) const override;

  Status UpdateRaftConfig(RaftConfigPB raft_config) override;
  void UpdateLeader(std::string leader_uuid) override;
  ProxyTopologyPB GetProxyTopology() const override;
  Status UpdateProxyTopology(ProxyTopologyPB proxy_topology) override;
  ProxyPolicy GetProxyPolicy() const override;

  static Status Create(
      RaftConfigPB raft_config,
      RaftPeerPB local_peer_pb,
      const std::vector<std::unordered_set<std::string>>& region_groups,
      std::shared_ptr<RegionGroupRoutingTable>* rgrt);

  // Update rtt latency value from local replica to the peer replica.
  // For leader replica, this might be used to update the proxy map if
  // the closest peer to leader of a region group is changed.
  void UpdateRtt(const std::string& peer_uuid, std::chrono::microseconds rtt);

 private:
  // Helper class to track rtt between remote peer and local replica.
  // It collects samples first then calculate the average rtt based on those
  // samples. To avoid inaccurate rtt value due to network glitch, it only
  // updates the rtt value when the number of samples is large enough.
  // The class is not thread safe, caller needs to make sure it's
  // called with synchoronization.
  struct RttTracker {
    std::chrono::microseconds avg_rtt{0};
    std::chrono::time_point<std::chrono::steady_clock> last_updated;
    int64_t total_updates_since_last_update{0};
    int64_t avg_rtt_us_since_last_update{0};

    // To avoid frequent updates and outliners which might cause unnecessary
    // proxy map updates, we only update the rtt value when it has enough
    // samples and the last update is old enough.
    // It will first calculate the average rtt based on the samples and stored
    // that in avg_rtt_us_since_last_update. Then it will update the avg_rtt
    // when there are enough samples and the last update is old enough.
    bool UpdateRtt(std::chrono::microseconds rtt) {
      auto now = std::chrono::steady_clock::now();
      avg_rtt_us_since_last_update =
          (avg_rtt_us_since_last_update * total_updates_since_last_update +
           rtt.count()) /
          (total_updates_since_last_update + 1);
      total_updates_since_last_update++;
      static const int64_t kMaxCachedUpdates = 10000000;
      if (total_updates_since_last_update > kMaxCachedUpdates ||
          (now - last_updated > std::chrono::seconds(30) &&
           total_updates_since_last_update > 5)) {
        avg_rtt = std::chrono::microseconds(avg_rtt_us_since_last_update);
        avg_rtt_us_since_last_update = 0;
        total_updates_since_last_update = 0;
        last_updated = now;
        return true;
      }
      return false;
    }
  };

  class RegionGroup {
   public:
    RegionGroup(
        const RaftConfigPB& raft_config,
        const std::vector<std::unordered_set<std::string>>& region_groups)
        : raft_config_(raft_config), region_groups_(region_groups) {}

    // Get the proxy peer in the region group by the lowest rtt.
    // @param peer_rtt_map: the map from peer uuid to the rtt tracker
    // @param peer_region: the region of the peer
    // @param db_peers_in_same_group: the set of db peers in the same group
    //        with the peer
    // @return the rtt of the proxy peer in the region group, return -1 if
    //         it can't find proxy peer.
    int64_t GetRegionProxyRtt(
        const std::unordered_map<std::string, RttTracker>& peer_rtt_map,
        const std::string& peer_region,
        std::unordered_set<std::string>& db_peers_in_same_group) const {
      const std::unordered_set<std::string>* region_group_ptr = nullptr;
      for (const auto& region_group : region_groups_) {
        if (region_group.find(peer_region) != region_group.end()) {
          region_group_ptr = &region_group;
          break;
        }
      }
      if (region_group_ptr == nullptr) {
        return -1;
      }
      int64_t min_rtt = INT64_MAX;
      for (const auto& peer : raft_config_.peers()) {
        if (region_group_ptr->find(peer.attrs().region()) !=
                region_group_ptr->end() &&
            peer.attrs().backing_db_present()) {
          auto itr = peer_rtt_map.find(peer.permanent_uuid());
          if (itr != peer_rtt_map.end() && itr->second.avg_rtt.count() > 0) {
            min_rtt = std::min(min_rtt, itr->second.avg_rtt.count());
            db_peers_in_same_group.insert(peer.permanent_uuid());
          }
        }
      }
      return min_rtt;
    }

   private:
    const RaftConfigPB& raft_config_;
    const std::vector<std::unordered_set<std::string>>& region_groups_;
  };

  RegionGroupRoutingTable(
      RaftConfigPB raft_config,
      RaftPeerPB local_peer_pb,
      const std::vector<std::unordered_set<std::string>>& region_groups);

  // Build the proxy topology based on the current raft config, leader
  // and peer_rtt_map. It is supposed to be called under read lock so
  // that it can get consistent rtt data for each peer.
  Status BuildProxyTopology(
      const RaftConfigPB& raft_config,
      const RaftPeerPB& local_peer_pb,
      const std::optional<std::string>& leader_uuid,
      const std::vector<std::unordered_set<std::string>>& region_groups,
      const std::unordered_map<std::string, std::string>&
          current_dst_to_proxy_map,
      std::unordered_map<std::string, std::string>& dst_to_proxy_map,
      ProxyTopologyPB& proxy_topology,
      std::unordered_map<std::string, RaftPeerPB>& peers_map);

  // Get the proxy peer in the region group by the lowest rtt.
  // @param regions: the region group
  // @param region_peer_map: the map from region to the list of peers in the
  //        region
  // @return the proxy peer uuid for the region group, return empty string if
  //         it can't find proxy peer.
  std::string GetGroupProxyPeerByRtt(
      const std::unordered_set<std::string>& regions,
      const std::unordered_map<std::string, std::vector<std::string>>&
          region_peer_map) const;
  bool HasRttValue(const std::string& peer_uuid) const;
  bool IsLeaderNoLock() const;
  static ProxyTopologyPB DeriveProxyTopologyByProxyMap(
      const std::unordered_map<std::string, std::string>& dst_to_proxy_map);

  mutable RWCLock lock_; // read-write-commit lock protecting the below fields
  ProxyTopologyPB proxy_topology_;
  std::vector<std::unordered_set<std::string>> region_groups_;
  RaftConfigPB raft_config_;
  std::unordered_map<std::string, RaftPeerPB> peers_map_;
  RaftPeerPB local_peer_pb_;
  std::optional<std::string> leader_uuid_;
  std::unordered_map<std::string, std::string> dst_to_proxy_map_;
  std::unordered_map<std::string, RttTracker> peer_rtt_map_;
};

} // namespace consensus
} // namespace kudu
