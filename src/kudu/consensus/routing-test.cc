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
AddEdge(ProxyTopologyPB* proxy_topology, string peer, string upstream_uuid) {
  ProxyEdgePB* edge = proxy_topology->add_proxy_edges();
  edge->set_peer_uuid(std::move(peer));
  edge->set_proxy_from_uuid(std::move(upstream_uuid));
}

TEST(RoutingTest, TestRoutingTable) {
  RaftConfigPB raft_config = BuildRaftConfigPBForTests(/*num_voters=*/6);
  raft_config.set_opid_index(1); // required for validation
  ProxyTopologyPB proxy_topology;
  AddEdge(&proxy_topology, /*to=*/"peer-1", /*proxy_from=*/"peer-0");
  AddEdge(&proxy_topology, /*to=*/"peer-3", /*proxy_from=*/"peer-2");
  AddEdge(&proxy_topology, /*to=*/"peer-4", /*proxy_from=*/"peer-3");
  AddEdge(&proxy_topology, /*to=*/"peer-5", /*proxy_from=*/"peer-3");

  // Specify a leader that has a parent (proxy_from).
  const string kLeaderUuid = "peer-3";

  RoutingTable routing_table;
  ASSERT_OK(routing_table.Init(raft_config, proxy_topology, kLeaderUuid));

  string next_hop;
  ASSERT_OK(routing_table.NextHop("peer-3", "peer-5", &next_hop));
  ASSERT_EQ("peer-5", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-3", "peer-1", &next_hop));
  ASSERT_EQ("peer-0", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-5", "peer-1", &next_hop));
  ASSERT_EQ("peer-3", next_hop);
  ASSERT_OK(routing_table.NextHop("peer-2", "peer-4", &next_hop));
  ASSERT_EQ("peer-3", next_hop);
}

// Test the case where an instance of "proxy_from" is not in the Raft config.
TEST(RoutingTest, TestProxyFromNotInRaftConfig) {
  const string kLeaderUuid = "peer-0";
  const string kBogusUuid = "bogus";

  RaftConfigPB raft_config = BuildRaftConfigPBForTests(/*num_voters=*/2);
  raft_config.set_opid_index(1); // required for validation
  ProxyTopologyPB proxy_topology;
  AddEdge(&proxy_topology, /*to=*/"peer-1", /*proxy_from=*/kBogusUuid);

  RoutingTable routing_table;
  Status s = routing_table.Init(raft_config, proxy_topology, kLeaderUuid);
  ASSERT_FALSE(s.ok()) << s.ToString();
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "have been ignored: " + kBogusUuid);

  string next_hop;
  ASSERT_OK(routing_table.NextHop(
      /*src_uuid=*/"peer-0", /*dest_uuid=*/"peer-1", &next_hop));
  ASSERT_EQ("peer-1", next_hop); // Direct routing fallback.
}

// If a node has no routing table, and receives a request from the leader to
// proxy a message, the proxy node should proxy directly.
TEST(RoutingTest, TestStaleRouter) {
  RaftConfigPB raft_config = BuildRaftConfigPBForTests(/*num_voters=*/3);
  raft_config.set_opid_index(1); // required for validation

  const string kLeaderUuid = "peer-0";

  ProxyTopologyPB proxy_topology;
  RoutingTable routing_table;
  ASSERT_OK(routing_table.Init(raft_config, proxy_topology, kLeaderUuid));

  string next_hop;
  ASSERT_OK(routing_table.NextHop(
      /*src_uuid=*/"peer-1", /*dest_uuid=*/"peer-2", &next_hop));
  ASSERT_EQ("peer-2", next_hop); // Direct routing fallback.
}

TEST(RegionGroupRoutingTableTest, RttTrackerTest) {
  RegionGroupRoutingTable::RttTracker tracker;
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(20)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(21)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_FALSE(tracker.UpdateRtt(std::chrono::microseconds(19)));

  EXPECT_GE(tracker.avg_rtt_us_since_last_update, 19);
  EXPECT_LT(tracker.avg_rtt_us_since_last_update, 21);

  LOG(INFO) << "Update total_updates_since_last_update to trigger the update.";
  tracker.total_updates_since_last_update = 10000001;
  EXPECT_TRUE(tracker.UpdateRtt(std::chrono::microseconds(19)));
  EXPECT_EQ(tracker.avg_rtt_us_since_last_update, 0);
  EXPECT_EQ(tracker.total_updates_since_last_update, 0);
  EXPECT_EQ(tracker.avg_rtt.count(), 19);
}

TEST(RegionGroupRoutingTableTest, HelpFuncTest) {
  std::vector<std::string> database_regions = {
      "prn", "atn", "frc", "ftw", "lla", "odn"};
  RaftConfigPB raft_config =
      BuildRaftConfigPBForRoutingProxyTests(database_regions);
  RaftPeerPB local_peer_pb;
  for (const auto& peer : raft_config.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "prn") {
        local_peer_pb = peer;
        break;
      }
    }
  }

  std::vector<std::unordered_set<std::string>> region_groups;
  region_groups.emplace_back(std::unordered_set<std::string>{"lla", "odn"});

  RegionGroupRoutingTable routing_table(
      raft_config, local_peer_pb, region_groups);
  auto proxy_topology = routing_table.DeriveProxyTopologyByProxyMap(
      routing_table.dst_to_proxy_map_);
  for (const auto& edge : proxy_topology.proxy_edges()) {
    auto itr = routing_table.dst_to_proxy_map_.find(edge.peer_uuid());
    EXPECT_TRUE(itr != routing_table.dst_to_proxy_map_.end());
    EXPECT_EQ(itr->second, edge.proxy_from_uuid());
  }

  std::string expected_proxy_peer_uuid;
  std::unordered_map<std::string, std::vector<std::string>> region_peer_map;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    if (peer.attrs().backing_db_present()) {
      region_peer_map[peer.attrs().region()].push_back(peer.permanent_uuid());
      if (peer.attrs().region() == "lla") {
        routing_table.peer_rtt_map_[peer.permanent_uuid()].avg_rtt =
            std::chrono::microseconds(150);
      } else if (peer.attrs().region() == "odn") {
        routing_table.peer_rtt_map_[peer.permanent_uuid()].avg_rtt =
            std::chrono::microseconds(100);
        expected_proxy_peer_uuid = peer.permanent_uuid();
      }
    }
  }
  auto proxy_peer_uuid =
      routing_table.GetGroupProxyPeerByRtt({"lla", "odn"}, region_peer_map);
  EXPECT_FALSE(proxy_peer_uuid.empty());
  EXPECT_EQ(proxy_peer_uuid, expected_proxy_peer_uuid);
}

TEST(RegionGroupRoutingTableTest, BuildProxyTopologyTest) {
  std::vector<std::string> database_regions = {
      "prn", "atn", "frc", "ftw", "lla", "odn", "cln"};
  RaftConfigPB raft_config =
      BuildRaftConfigPBForRoutingProxyTests(database_regions);
  RaftPeerPB local_peer_pb, leader_peer_pb;
  for (const auto& peer : raft_config.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "prn") {
        local_peer_pb = peer;
        leader_peer_pb = peer;
      }
    }
  }

  std::vector<std::unordered_set<std::string>> region_groups;
  region_groups.emplace_back(
      std::unordered_set<std::string>{"lla", "odn", "cln"});

  RegionGroupRoutingTable routing_table(
      raft_config, local_peer_pb, region_groups);
  auto proxy_topology = routing_table.GetProxyTopology();
  for (const auto& edge : proxy_topology.proxy_edges()) {
    auto itr = routing_table.dst_to_proxy_map_.find(edge.peer_uuid());
    LOG(INFO) << "peer_uuid: " << edge.peer_uuid();
    EXPECT_TRUE(itr != routing_table.dst_to_proxy_map_.end());
    EXPECT_EQ(itr->second, edge.proxy_from_uuid());
  }

  LOG(INFO) << "Update rtt for peers in lla and odn so that one of them can be "
            << "selected as proxy peer.";
  std::string expected_proxy_peer_uuid;
  std::string lla_peer_uuid, odn_peer_uuid, cln_peer_uuid;
  for (const RaftPeerPB& peer : raft_config.peers()) {
    if (peer.attrs().backing_db_present()) {
      if (peer.attrs().region() == "lla") {
        lla_peer_uuid = peer.permanent_uuid();
        routing_table.peer_rtt_map_[peer.permanent_uuid()].avg_rtt =
            std::chrono::microseconds(150000);
      } else if (peer.attrs().region() == "odn") {
        odn_peer_uuid = peer.permanent_uuid();
        routing_table.peer_rtt_map_[peer.permanent_uuid()].avg_rtt =
            std::chrono::microseconds(100000);
        expected_proxy_peer_uuid = peer.permanent_uuid();
      } else if (peer.attrs().region() == "cln") {
        cln_peer_uuid = peer.permanent_uuid();
      }
    }
  }
  routing_table.UpdateLeader(leader_peer_pb.permanent_uuid());
  auto itr = routing_table.dst_to_proxy_map_.find(lla_peer_uuid);
  EXPECT_TRUE(itr != routing_table.dst_to_proxy_map_.end());
  EXPECT_EQ(itr->second, odn_peer_uuid);

  LOG(INFO)
      << "Simulate rtt update for peer in cln which will update the proxy.";
  EXPECT_TRUE(
      routing_table.peers_map_.find(cln_peer_uuid) !=
      routing_table.peers_map_.end());
  routing_table.peer_rtt_map_[cln_peer_uuid].total_updates_since_last_update =
      10000000;
  routing_table.peer_rtt_map_[cln_peer_uuid].avg_rtt_us_since_last_update =
      70000;
  routing_table.UpdateRtt(cln_peer_uuid, std::chrono::microseconds(70000));
  EXPECT_EQ(routing_table.peer_rtt_map_[cln_peer_uuid].avg_rtt.count(), 70000);
  itr = routing_table.dst_to_proxy_map_.find(lla_peer_uuid);
  EXPECT_TRUE(itr != routing_table.dst_to_proxy_map_.end());
  EXPECT_EQ(itr->second, cln_peer_uuid);
  itr = routing_table.dst_to_proxy_map_.find(odn_peer_uuid);
  EXPECT_TRUE(itr != routing_table.dst_to_proxy_map_.end());
  EXPECT_EQ(itr->second, cln_peer_uuid);
}
} // namespace consensus
} // namespace kudu
