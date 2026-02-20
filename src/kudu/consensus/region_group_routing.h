#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <optional>

#include "kudu/consensus/quorum_util.h"
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
  FRIEND_TEST(RegionGroupRoutingTableTest, SameRegionGroupTest);
  FRIEND_TEST(RegionGroupRoutingTableTest, BuildProxyTopologyTest);
  FRIEND_TEST(RegionGroupRoutingTableTest, TryUpdateProxyMapTest);

  ~RegionGroupRoutingTable() override = default;

  Status nextHop(
      const std::string& srcUuid,
      const std::string& destUuid,
      std::string* nextHop) const override;

  Status updateRaftConfig(RaftConfigPB raftConfig) override;
  void updateLeader(std::string leaderUuid) override;
  Status updateRaftConfigAndLeader(
      RaftConfigPB raftConfig,
      std::string leaderUuid);
  ProxyTopologyPB getProxyTopology() const override;
  Status updateProxyTopology(ProxyTopologyPB proxyTopology) override;
  Status updateProxyRegionGroup(
      const std::vector<std::unordered_set<std::string>>& regionGroups,
      RaftConfigPB raftConfig,
      const std::string& leaderUuid);
  std::vector<std::unordered_set<std::string>> getProxyRegionGroup() const {
    std::shared_lock l(lock_);
    return regionGroups_;
  }
  ProxyPolicy getProxyPolicy() const override;

  static Status create(
      RaftConfigPB raftConfig,
      RaftPeerPB localPeerPb,
      const std::vector<std::unordered_set<std::string>>& regionGroups,
      std::shared_ptr<RegionGroupRoutingTable>* rgrt);

  // Update rtt latency value from local replica to the peer replica.
  // For leader replica, this might be used to update the proxy map if
  // the closest peer to leader of a region group is changed.
  void updateRtt(const std::string& peerUuid, std::chrono::microseconds rtt);

 private:
  // Helper class to track rtt between remote peer and local replica.
  // It collects samples first then calculate the average rtt based on those
  // samples. To avoid inaccurate rtt value due to network glitch, it only
  // updates the rtt value when the number of samples is large enough.
  // The class is not thread safe, caller needs to make sure it's
  // called with synchoronization.
  struct RttTracker {
    std::chrono::microseconds avgRtt{0};
    std::chrono::time_point<std::chrono::steady_clock> lastUpdated;
    int64_t totalUpdatesSinceLastUpdate{0};
    int64_t avgRttUsSinceLastUpdate{0};

    // To avoid frequent updates and outliners which might cause unnecessary
    // proxy map updates, we only update the rtt value when it has enough
    // samples and the last update is old enough.
    // It will first calculate the average rtt based on the samples and stored
    // that in avgRttUsSinceLastUpdate. Then it will update the avgRtt
    // when there are enough samples and the last update is old enough.
    bool updateRtt(std::chrono::microseconds rtt) {
      auto now = std::chrono::steady_clock::now();
      avgRttUsSinceLastUpdate =
          (avgRttUsSinceLastUpdate * totalUpdatesSinceLastUpdate +
           rtt.count()) /
          (totalUpdatesSinceLastUpdate + 1);
      totalUpdatesSinceLastUpdate++;
      static const int64_t kMaxCachedUpdates = 10000000;
      if (totalUpdatesSinceLastUpdate > kMaxCachedUpdates ||
          (now - lastUpdated > std::chrono::seconds(30) &&
           totalUpdatesSinceLastUpdate > 5)) {
        avgRtt = std::chrono::microseconds(avgRttUsSinceLastUpdate);
        avgRttUsSinceLastUpdate = 0;
        totalUpdatesSinceLastUpdate = 0;
        lastUpdated = now;
        return true;
      }
      return false;
    }
  };

  class RegionGroup {
   public:
    RegionGroup(
        const RaftConfigPB& raftConfig,
        const std::vector<std::unordered_set<std::string>>& regionGroups)
        : raftConfig_(raftConfig), regionGroups_(regionGroups) {}

    // Get the proxy peer in the region group by the lowest rtt.
    // @param peerRttMap: the map from peer uuid to the rtt tracker
    // @param peerRegion: the region of the peer
    // @param dbPeersInSameGroup: the set of db peers in the same group
    //        with the peer
    // @return the pair of proxy peer for the region group and its rtt to
    //         leader, return -1 if it can't find proxy peer.
    std::pair<std::string, int64_t> getRegionProxyRtt(
        const std::unordered_map<std::string, RttTracker>& peerRttMap,
        const std::string& peerRegion,
        std::unordered_set<std::string>& dbPeersInSameGroup) const {
      const std::unordered_set<std::string>* regionGroupPtr = nullptr;
      for (const auto& regionGroup : regionGroups_) {
        if (regionGroup.contains(peerRegion)) {
          regionGroupPtr = &regionGroup;
          break;
        }
      }
      if (regionGroupPtr == nullptr) {
        return std::make_pair("", -1);
      }
      int64_t minRtt = INT64_MAX;
      std::string proxy;
      for (const auto& peer : raftConfig_.peers()) {
        if (regionGroupPtr->find(peer.attrs().region()) !=
                regionGroupPtr->end() &&
            canBeProxyPeer(peer)) {
          auto itr = peerRttMap.find(peer.permanent_uuid());
          if (itr != peerRttMap.end() && itr->second.avgRtt.count() > 0) {
            if (minRtt > itr->second.avgRtt.count()) {
              minRtt = itr->second.avgRtt.count();
              proxy = peer.permanent_uuid();
            }
            dbPeersInSameGroup.insert(peer.permanent_uuid());
          }
        }
      }
      return std::make_pair(proxy, minRtt);
    }

   private:
    const RaftConfigPB& raftConfig_;
    const std::vector<std::unordered_set<std::string>>& regionGroups_;
  };

  RegionGroupRoutingTable(
      RaftConfigPB raftConfig,
      RaftPeerPB localPeerPb,
      const std::vector<std::unordered_set<std::string>>& regionGroups);

  // Build the proxy topology based on the current raft config, leader
  // and peerRttMap. It is supposed to be called under read lock so
  // that it can get consistent rtt data for each peer.
  Status buildProxyTopology(
      const RaftConfigPB& raftConfig,
      const RaftPeerPB& localPeerPb,
      const std::optional<std::string>& leaderUuid,
      const std::vector<std::unordered_set<std::string>>& regionGroups,
      const std::unordered_map<std::string, std::string>& currentDstToProxyMap,
      std::unordered_map<std::string, std::string>& dstToProxyMap,
      ProxyTopologyPB& proxyTopology,
      std::unordered_map<std::string, RaftPeerPB>& peersMap);

  // Get the proxy peer in the region group by the lowest rtt.
  // @param regions: the region group
  // @param regionPeerMap: the map from region to the list of peers in the
  //        region
  // @return the proxy peer uuid for the region group, return empty string if
  //         it can't find proxy peer.
  std::string getGroupProxyPeerByRtt(
      const std::unordered_set<std::string>& regions,
      const std::unordered_map<std::string, std::vector<std::string>>&
          regionPeerMap) const;
  bool hasRttValue(const std::string& peerUuid) const;
  bool isLeaderNoLock() const;
  bool isSameRegionGroup(const std::string& regionA, const std::string& regionB)
      const;
  static ProxyTopologyPB deriveProxyTopologyByProxyMap(
      const std::unordered_map<std::string, std::string>& dstToProxyMap);
  // Given a proxy peer and the peers in the same group of the proxy, check
  // if existing proxy map needs to be updated. Return true if it is updated
  // and the dstToProxyMap will also be updated.
  // There are few cases the map will be updated:
  //   - add proxy to a peer if it doesn't exist in the map
  //   - update proxy for a peer if its old proxy is different
  //   - cleanup proxy for the new proxy peer, itself doesn't need use other
  //     peer as its proxy
  static bool tryUpdateProxyMap(
      const std::string& proxyUuid,
      const std::unordered_set<std::string>& dbPeersInSameGroup,
      std::unordered_map<std::string, std::string>& dstToProxyMap);

  mutable RwcLock lock_; // read-write-commit lock protecting the below fields
  ProxyTopologyPB proxyTopology_;
  std::vector<std::unordered_set<std::string>> regionGroups_;
  RaftConfigPB raftConfig_;
  std::unordered_map<std::string, RaftPeerPB> peersMap_;
  RaftPeerPB localPeerPb_;
  std::optional<std::string> leaderUuid_;
  std::unordered_map<std::string, std::string> dstToProxyMap_;
  std::unordered_map<std::string, RttTracker> peerRttMap_;
};

} // namespace consensus
} // namespace kudu
