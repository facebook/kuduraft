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

#include "kudu/consensus/region_group_routing.h"
#include "kudu/consensus/routing.h"

#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "kudu/consensus/consensus-test-util.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;

namespace kudu {
namespace consensus {

static void
addEdge(ProxyTopologyPB* proxyTopology, string peer, string upstreamUuid) {
  ProxyEdgePB* edge = proxyTopology->add_proxy_edges();
  edge->set_peer_uuid(std::move(peer));
  edge->set_proxy_from_uuid(std::move(upstreamUuid));
}

TEST(RoutingTest, TestRoutingTable) {
  RaftConfigPB raftConfig = BuildRaftConfigPBForTests(/*num_voters=*/6);
  raftConfig.set_opid_index(1); // required for validation
  ProxyTopologyPB proxyTopology;
  addEdge(&proxyTopology, /*peer=*/"peer-1", /*upstreamUuid=*/"peer-0");
  addEdge(&proxyTopology, /*peer=*/"peer-3", /*upstreamUuid=*/"peer-2");
  addEdge(&proxyTopology, /*peer=*/"peer-4", /*upstreamUuid=*/"peer-3");
  addEdge(&proxyTopology, /*peer=*/"peer-5", /*upstreamUuid=*/"peer-3");

  // Specify a leader that has a parent (proxy_from).
  const string kLeaderUuid = "peer-3";

  RoutingTable routingTable;
  ASSERT_OK(routingTable.init(raftConfig, proxyTopology, kLeaderUuid));

  string nextHop;
  ASSERT_OK(routingTable.nextHop("peer-3", "peer-5", &nextHop));
  ASSERT_EQ("peer-5", nextHop);
  ASSERT_OK(routingTable.nextHop("peer-3", "peer-1", &nextHop));
  ASSERT_EQ("peer-0", nextHop);
  ASSERT_OK(routingTable.nextHop("peer-5", "peer-1", &nextHop));
  ASSERT_EQ("peer-3", nextHop);
  ASSERT_OK(routingTable.nextHop("peer-2", "peer-4", &nextHop));
  ASSERT_EQ("peer-3", nextHop);
}

// Test the case where an instance of "proxy_from" is not in the Raft config.
TEST(RoutingTest, TestProxyFromNotInRaftConfig) {
  const string kLeaderUuid = "peer-0";
  const string kBogusUuid = "bogus";

  RaftConfigPB raftConfig = BuildRaftConfigPBForTests(/*num_voters=*/2);
  raftConfig.set_opid_index(1); // required for validation
  ProxyTopologyPB proxyTopology;
  addEdge(&proxyTopology, /*peer=*/"peer-1", /*upstreamUuid=*/kBogusUuid);

  RoutingTable routingTable;
  Status s = routingTable.init(raftConfig, proxyTopology, kLeaderUuid);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "have been ignored: " + kBogusUuid);

  string nextHop;
  ASSERT_OK(routingTable.nextHop(
      /*src_uuid=*/"peer-0", /*dest_uuid=*/"peer-1", &nextHop));
  ASSERT_EQ("peer-1", nextHop); // Direct routing fallback.
}

// If a node has no routing table, and receives a request from the leader to
// proxy a message, the proxy node should proxy directly.
TEST(RoutingTest, TestStaleRouter) {
  RaftConfigPB raftConfig = BuildRaftConfigPBForTests(/*num_voters=*/3);
  raftConfig.set_opid_index(1); // required for validation

  const string kLeaderUuid = "peer-0";

  ProxyTopologyPB proxyTopology;
  RoutingTable routingTable;
  ASSERT_OK(routingTable.init(raftConfig, proxyTopology, kLeaderUuid));

  string nextHop;
  ASSERT_OK(routingTable.nextHop(
      /*src_uuid=*/"peer-1", /*dest_uuid=*/"peer-2", &nextHop));
  ASSERT_EQ("peer-2", nextHop); // Direct routing fallback.
}

TEST(RegionGroupRoutingTableTest, RttTrackerTest) {
  RegionGroupRoutingTable::RttTracker tracker;
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(20)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(21)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));

  EXPECT_GE(tracker.avgRttUsSinceLastUpdate, 19);
  EXPECT_LT(tracker.avgRttUsSinceLastUpdate, 21);

  LOG(INFO) << "Update totalUpdatesSinceLastUpdate to trigger the update.";
  tracker.totalUpdatesSinceLastUpdate = 10000001;
  EXPECT_TRUE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_EQ(tracker.avgRttUsSinceLastUpdate, 0);
  EXPECT_EQ(tracker.totalUpdatesSinceLastUpdate, 0);
  EXPECT_EQ(tracker.avgRtt.count(), 19);
}

TEST(RegionGroupRoutingTableTest, HelpFuncTest) {
  std::vector<std::string> databaseRegions = {
      "prn", "atn", "frc", "ftw", "lla", "odn"};
  RaftConfigPB raftConfig =
      BuildRaftConfigPBForRoutingProxyTests(databaseRegions);
  RaftPeerPB localPeerPb;
  for (const auto& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "prn") {
        localPeerPb = peer;
        break;
      }
    }
  }

  std::vector<std::unordered_set<std::string>> regionGroups;
  regionGroups.emplace_back(std::unordered_set<std::string>{"lla", "odn"});

  RegionGroupRoutingTable routingTable(raftConfig, localPeerPb, regionGroups);
  auto proxyTopology =
      routingTable.DeriveProxyTopologyByProxyMap(routingTable.dstToProxyMap_);
  for (const auto& edge : proxyTopology.proxy_edges()) {
    auto itr = routingTable.dstToProxyMap_.find(edge.peer_uuid());
    EXPECT_TRUE(itr != routingTable.dstToProxyMap_.end());
    EXPECT_EQ(itr->second, edge.proxy_from_uuid());
  }

  std::string expectedProxyPeerUuid;
  std::unordered_map<std::string, std::vector<std::string>> regionPeerMap;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      regionPeerMap[peer.attrs().region()].push_back(peer.permanent_uuid());
      if (peer.attrs().region() == "lla") {
        routingTable.peerRttMap_[peer.permanent_uuid()].avgRtt =
            std::chrono::microseconds(150);
      } else if (peer.attrs().region() == "odn") {
        routingTable.peerRttMap_[peer.permanent_uuid()].avgRtt =
            std::chrono::microseconds(100);
        expectedProxyPeerUuid = peer.permanent_uuid();
      }
    }
  }
  auto proxyPeerUuid =
      routingTable.GetGroupProxyPeerByRtt({"lla", "odn"}, regionPeerMap);
  EXPECT_FALSE(proxyPeerUuid.empty());
  EXPECT_EQ(proxyPeerUuid, expectedProxyPeerUuid);
}

TEST(RegionGroupRoutingTableTest, SameRegionGroupTest) {
  std::vector<std::string> databaseRegions = {
      "prn", "atn", "frc", "ftw", "lla", "odn"};
  RaftConfigPB raftConfig =
      BuildRaftConfigPBForRoutingProxyTests(databaseRegions);
  RaftPeerPB localPeerPb;
  for (const auto& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "prn") {
        localPeerPb = peer;
        break;
      }
    }
  }

  std::vector<std::unordered_set<std::string>> regionGroups;
  regionGroups.emplace_back(std::unordered_set<std::string>{"lla", "odn"});
  regionGroups.emplace_back(
      std::unordered_set<std::string>{"prn", "atn", "frc", "ftw"});

  RegionGroupRoutingTable routingTable(raftConfig, localPeerPb, regionGroups);
  LOG(INFO) << "Test isSameRegionGroup.";
  EXPECT_TRUE(routingTable.isSameRegionGroup("lla", "odn"));
  EXPECT_TRUE(routingTable.isSameRegionGroup("lla", "lla"));
  EXPECT_FALSE(routingTable.isSameRegionGroup("lla", "prn"));
  EXPECT_FALSE(routingTable.isSameRegionGroup("lla", ""));
  EXPECT_TRUE(routingTable.isSameRegionGroup("atn", "prn"));
}

TEST(RegionGroupRoutingTableTest, BuildProxyTopologyTest) {
  std::vector<std::string> databaseRegions = {
      "prn", "atn", "frc", "ftw", "lla", "odn", "cln"};
  RaftConfigPB raftConfig =
      BuildRaftConfigPBForRoutingProxyTests(databaseRegions);
  RaftPeerPB localPeerPb, leaderPeerPb;
  for (const auto& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "prn") {
        localPeerPb = peer;
        leaderPeerPb = peer;
      }
    }
  }

  std::vector<std::unordered_set<std::string>> regionGroups;
  regionGroups.emplace_back(
      std::unordered_set<std::string>{"lla", "odn", "cln"});
  regionGroups.emplace_back(
      std::unordered_set<std::string>{"prn", "atn", "frc", "ftw"});

  RegionGroupRoutingTable routingTable(raftConfig, localPeerPb, regionGroups);
  auto proxyTopology = routingTable.getProxyTopology();
  for (const auto& edge : proxyTopology.proxy_edges()) {
    auto itr = routingTable.dstToProxyMap_.find(edge.peer_uuid());
    LOG(INFO) << "peer_uuid: " << edge.peer_uuid();
    EXPECT_TRUE(itr != routingTable.dstToProxyMap_.end());
    EXPECT_EQ(itr->second, edge.proxy_from_uuid());
  }

  LOG(INFO) << "Update rtt for peers in lla and odn so that one of them can be "
            << "selected as proxy peer.";
  std::string expectedProxyPeerUuid;
  std::string llaPeerUuid, odnPeerUuid, clnPeerUuid;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "lla") {
        llaPeerUuid = peer.permanent_uuid();
        routingTable.peerRttMap_[peer.permanent_uuid()].avgRtt =
            std::chrono::microseconds(150000);
      } else if (peer.attrs().region() == "odn") {
        odnPeerUuid = peer.permanent_uuid();
        routingTable.peerRttMap_[peer.permanent_uuid()].avgRtt =
            std::chrono::microseconds(100000);
        expectedProxyPeerUuid = peer.permanent_uuid();
      } else if (peer.attrs().region() == "cln") {
        clnPeerUuid = peer.permanent_uuid();
      }
    }
  }
  routingTable.updateLeader(leaderPeerPb.permanent_uuid());
  auto itr = routingTable.dstToProxyMap_.find(llaPeerUuid);
  EXPECT_TRUE(itr != routingTable.dstToProxyMap_.end());
  EXPECT_EQ(itr->second, odnPeerUuid);

  LOG(INFO)
      << "Simulate rtt update for peer in cln which will update the proxy.";
  EXPECT_TRUE(
      routingTable.peersMap_.find(clnPeerUuid) != routingTable.peersMap_.end());
  routingTable.peerRttMap_[clnPeerUuid].totalUpdatesSinceLastUpdate = 10000000;
  routingTable.peerRttMap_[clnPeerUuid].avgRttUsSinceLastUpdate = 70000;
  routingTable.updateRtt(clnPeerUuid, std::chrono::microseconds(70000));
  EXPECT_EQ(routingTable.peerRttMap_[clnPeerUuid].avgRtt.count(), 70000);
  itr = routingTable.dstToProxyMap_.find(llaPeerUuid);
  EXPECT_TRUE(itr != routingTable.dstToProxyMap_.end());
  EXPECT_EQ(itr->second, clnPeerUuid);
  itr = routingTable.dstToProxyMap_.find(odnPeerUuid);
  EXPECT_TRUE(itr != routingTable.dstToProxyMap_.end());
  EXPECT_EQ(itr->second, clnPeerUuid);

  LOG(INFO) << "Test the case where the peer "
            << "is in the same region group as leader.";
  std::string frcPeerUuid, atnPeerUuid, ftwPeerUuid;
  for (const RaftPeerPB& peer : raftConfig.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "frc") {
        frcPeerUuid = peer.permanent_uuid();
      } else if (peer.attrs().region() == "atn") {
        atnPeerUuid = peer.permanent_uuid();
      } else if (peer.attrs().region() == "ftw") {
        ftwPeerUuid = peer.permanent_uuid();
      }
    }
  }
  routingTable.updateRtt(frcPeerUuid, std::chrono::microseconds(40000));
  EXPECT_EQ(
      routingTable.peerRttMap_.find(frcPeerUuid),
      routingTable.peerRttMap_.end());
  routingTable.updateRtt(atnPeerUuid, std::chrono::microseconds(10000));
  EXPECT_EQ(
      routingTable.peerRttMap_.find(atnPeerUuid),
      routingTable.peerRttMap_.end());
  routingTable.updateRtt(ftwPeerUuid, std::chrono::microseconds(10000));
  EXPECT_EQ(
      routingTable.peerRttMap_.find(ftwPeerUuid),
      routingTable.peerRttMap_.end());
  routingTable.updateRtt(
      leaderPeerPb.permanent_uuid(), std::chrono::microseconds(1000));
  EXPECT_EQ(
      routingTable.peerRttMap_.find(leaderPeerPb.permanent_uuid()),
      routingTable.peerRttMap_.end());
}

TEST(RegionGroupRoutingTableTest, TryUpdateProxyMapTest) {
  std::unordered_map<std::string, std::string> dstToProxyMap;
  const std::string kProxyUuid = "test_uuid_1";
  std::unordered_set<std::string> dbPeersInSameGroup;
  LOG(INFO) << "Test the case where proxy_uuid is not in the map.";
  EXPECT_FALSE(
      RegionGroupRoutingTable::TryUpdateProxyMap(
          kProxyUuid, dbPeersInSameGroup, dstToProxyMap));

  LOG(INFO) << "Test the case where proxy_uuid set as "
            << "proxy for peer in same group.";
  dbPeersInSameGroup.insert("test_uuid_2");
  dbPeersInSameGroup.insert(kProxyUuid);
  dbPeersInSameGroup.insert("test_uuid_3");

  EXPECT_TRUE(
      RegionGroupRoutingTable::TryUpdateProxyMap(
          kProxyUuid, dbPeersInSameGroup, dstToProxyMap));
  EXPECT_EQ(dstToProxyMap.size(), 2);
  EXPECT_EQ(dstToProxyMap["test_uuid_2"], kProxyUuid);
  EXPECT_EQ(dstToProxyMap["test_uuid_3"], kProxyUuid);

  LOG(INFO) << "Test the case where proxy_uuid doesn't set as "
            << "proxy for peer in different group.";
  dbPeersInSameGroup.clear();
  dbPeersInSameGroup.insert("test_uuid_2");
  dbPeersInSameGroup.insert("test_uuid_3");
  EXPECT_FALSE(
      RegionGroupRoutingTable::TryUpdateProxyMap(
          kProxyUuid, dbPeersInSameGroup, dstToProxyMap));

  dbPeersInSameGroup.insert(kProxyUuid);
  EXPECT_TRUE(
      RegionGroupRoutingTable::TryUpdateProxyMap(
          "test_uuid_2", dbPeersInSameGroup, dstToProxyMap));
  EXPECT_EQ(dstToProxyMap.size(), 2);
  EXPECT_EQ(dstToProxyMap["test_uuid_3"], "test_uuid_2");
  EXPECT_EQ(dstToProxyMap[kProxyUuid], "test_uuid_2");

  EXPECT_FALSE(
      RegionGroupRoutingTable::TryUpdateProxyMap(
          "test_uuid_2", dbPeersInSameGroup, dstToProxyMap));
}
} // namespace consensus
} // namespace kudu
