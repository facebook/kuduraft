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

#include "kudu/consensus/quorum_util.h"

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/common/common.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace consensus {

// Handy notation of membership types used by addPeer(), etc.
constexpr auto N =
    RaftPeerPB::NON_VOTER; // NOLINT(readability-identifier-naming)
constexpr auto U =
    RaftPeerPB::UNKNOWN_MEMBER_TYPE; // NOLINT(readability-identifier-naming)
constexpr auto V = RaftPeerPB::VOTER; // NOLINT(readability-identifier-naming)

constexpr auto MHP_H =
    MajorityHealthPolicy::Honor; // NOLINT(readability-identifier-naming)
constexpr auto MHP_I =
    MajorityHealthPolicy::Ignore; // NOLINT(readability-identifier-naming)

// The various possible health statuses.
constexpr auto kHealthStatuses = {'?', '-', 'x', '+'};

using Attr = std::pair<string, bool>;

static void setOverallHealth(HealthReportPB* healthReport, char overallHealth) {
  switch (overallHealth) {
    case '+':
      healthReport->set_overall_health(HealthReportPB::HEALTHY);
      break;
    case '-':
      healthReport->set_overall_health(HealthReportPB::FAILED);
      break;
    case 'x':
      healthReport->set_overall_health(HealthReportPB::FAILED_UNRECOVERABLE);
      break;
    case '?':
      healthReport->set_overall_health(HealthReportPB::UNKNOWN);
      break;
    default:
      FAIL() << overallHealth << ": unexpected replica health status";
  }
}

std::ostream& operator<<(std::ostream& os, MajorityHealthPolicy policy) {
  switch (policy) {
    case MajorityHealthPolicy::Honor:
      os << "MajorityHealthPolicy::Honor";
      break;
    case MajorityHealthPolicy::Ignore:
      os << "MajorityHealthPolicy::Ignore";
      break;
    default:
      os << policy << ": unsupported health policy";
      break;
  }
  return os;
}

// Add a consensus peer into the specified configuration.
static void addPeer(
    RaftConfigPB* config,
    const string& uuid,
    RaftPeerPB::MemberType type,
    std::optional<char> overallHealth = {},
    vector<Attr> attrs = {}) {
  RaftPeerPB* peer = config->add_peers();
  peer->set_permanent_uuid(uuid);
  peer->mutable_last_known_addr()->set_host(uuid + ".example.com");
  peer->set_member_type(type);
  if (overallHealth) {
    unique_ptr<HealthReportPB> healthReport(new HealthReportPB);
    setOverallHealth(healthReport.get(), *overallHealth);
    peer->set_allocated_health_report(healthReport.release());
  }
  if (!attrs.empty()) {
    unique_ptr<RaftPeerAttrsPB> attrsPb(new RaftPeerAttrsPB);
    for (const auto& attr : attrs) {
      if (attr.first == "PROMOTE") {
        attrsPb->set_promote(attr.second);
      } else if (attr.first == "REPLACE") {
        attrsPb->set_replace(attr.second);
      } else {
        FAIL() << attr.first << ": unexpected attribute to set";
      }
    }
    peer->set_allocated_attrs(attrsPb.release());
  }
}

using RaftMemberSpec = pair<string, RaftPeerPB::MemberType>;

static RaftConfigPB createConfig(const vector<RaftMemberSpec>& specs) {
  RaftConfigPB config;
  for (const auto& spec : specs) {
    addPeer(&config, spec.first, spec.second);
  }
  return config;
}

static void promotePeer(RaftConfigPB* config, const string& peerUuid) {
  RaftPeerPB* peerPb;
  const Status s = getRaftConfigMember(config, peerUuid, &peerPb);
  if (!s.ok()) {
    FAIL() << peerUuid << ": " << s.ToString();
  }
  peerPb->set_member_type(V);
  // peerPb->mutable_attrs()->clear_promote();
  peerPb->mutable_attrs()->set_promote(false);
}

static void removePeer(RaftConfigPB* config, const string& peerUuid) {
  if (!removeFromRaftConfig(config, peerUuid)) {
    FAIL() << peerUuid << ": peer is not in the config";
  }
}

static void
setPeerHealth(RaftConfigPB* config, const string& uuid, char health) {
  RaftPeerPB* peerPb;
  const Status s = getRaftConfigMember(config, uuid, &peerPb);
  if (!s.ok()) {
    FAIL() << "unexpected failure from getRaftConfigMember(): " << s.ToString();
  }
  setOverallHealth(peerPb->mutable_health_report(), health);
}

// Test that we return the right electable UUIDs from a config
TEST(QuorumUtilTest, TestGetElectableUuids) {
  // Test case 1: All voters with backing DB
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');

    for (int i = 0; i < config.peers_size(); i++) {
      config.mutable_peers(i)->mutable_attrs()->set_backing_db_present(true);
    }

    std::unordered_set<std::string> electable = getElectableUuids(config);
    ASSERT_EQ(3, electable.size());
    EXPECT_TRUE(electable.contains("A"));
    EXPECT_TRUE(electable.contains("B"));
    EXPECT_TRUE(electable.contains("C"));
  }

  // Test case 2: Mix of voters and non-voters with backing DB
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", N, '+'); // Non-voter

    for (int i = 0; i < config.peers_size(); i++) {
      config.mutable_peers(i)->mutable_attrs()->set_backing_db_present(true);
    }

    std::unordered_set<std::string> electable = getElectableUuids(config);
    ASSERT_EQ(2, electable.size());
    EXPECT_TRUE(electable.contains("A"));
    EXPECT_TRUE(electable.contains("B"));
    EXPECT_FALSE(electable.contains("C"));
  }

  // Test case 3: Voters with mixed backing DB status
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');

    config.mutable_peers(0)->mutable_attrs()->set_backing_db_present(true);
    config.mutable_peers(1)->mutable_attrs()->set_backing_db_present(false);
    config.mutable_peers(2)->mutable_attrs()->set_backing_db_present(true);

    std::unordered_set<std::string> electable = getElectableUuids(config);
    ASSERT_EQ(2, electable.size());
    EXPECT_TRUE(electable.contains("A"));
    EXPECT_FALSE(electable.contains("B"));
    EXPECT_TRUE(electable.contains("C"));
  }

  // Test case 4: Empty config
  {
    RaftConfigPB config;
    std::unordered_set<std::string> electable = getElectableUuids(config);
    ASSERT_EQ(0, electable.size());
  }
}

TEST(QuorumUtilTest, TestMemberExtraction) {
  RaftConfigPB config;
  addPeer(&config, "A", V);
  addPeer(&config, "B", V);
  addPeer(&config, "C", V);

  // Basic test for getRaftConfigMember().
  RaftPeerPB* peerPb;
  Status s = getRaftConfigMember(&config, "invalid", &peerPb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(getRaftConfigMember(&config, "A", &peerPb));
  ASSERT_EQ("A", peerPb->permanent_uuid());

  // Basic test for getRaftConfigLeader().
  ConsensusStatePB cstate;
  *cstate.mutable_committed_config() = config;
  s = getRaftConfigLeader(&cstate, &peerPb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  cstate.set_leader_uuid("B");
  ASSERT_OK(getRaftConfigLeader(&cstate, &peerPb));
  ASSERT_EQ("B", peerPb->permanent_uuid());
}

TEST(QuorumUtilTest, TestDiffConsensusStates) {
  ConsensusStatePB oldCs;
  addPeer(oldCs.mutable_committed_config(), "A", V);
  addPeer(oldCs.mutable_committed_config(), "B", V);
  addPeer(oldCs.mutable_committed_config(), "C", V);
  oldCs.set_current_term(1);
  oldCs.set_leader_uuid("A");
  oldCs.mutable_committed_config()->set_opid_index(1);

  // Simple case of no change.
  EXPECT_EQ("no change", diffConsensusStates(oldCs, oldCs));

  // Simulate a leader change.
  {
    auto newCs = oldCs;
    newCs.set_leader_uuid("B");
    newCs.set_current_term(2);

    EXPECT_EQ(
        "term changed from 1 to 2, "
        "leader changed from A (A.example.com) to B (B.example.com)",
        diffConsensusStates(oldCs, newCs));
  }

  // Simulate eviction of a peer.
  {
    auto newCs = oldCs;
    newCs.mutable_committed_config()->set_opid_index(2);
    newCs.mutable_committed_config()->mutable_peers()->RemoveLast();

    EXPECT_EQ(
        "config changed from index 1 to 2, "
        "VOTER C (C.example.com) evicted",
        diffConsensusStates(oldCs, newCs));
  }

  // Simulate addition of a peer.
  {
    auto newCs = oldCs;
    newCs.mutable_committed_config()->set_opid_index(2);
    addPeer(newCs.mutable_committed_config(), "D", N);

    EXPECT_EQ(
        "config changed from index 1 to 2, "
        "NON_VOTER D (D.example.com) added",
        diffConsensusStates(oldCs, newCs));
  }

  // Simulate change of a peer's member type.
  {
    auto newCs = oldCs;
    newCs.mutable_committed_config()->set_opid_index(2);
    newCs.mutable_committed_config()
        ->mutable_peers()
        ->Mutable(2)
        ->set_member_type(N);

    EXPECT_EQ(
        "config changed from index 1 to 2, "
        "C (C.example.com) changed from VOTER to NON_VOTER",
        diffConsensusStates(oldCs, newCs));
  }

  // Simulate change from no leader to a leader
  {
    auto noLeaderCs = oldCs;
    noLeaderCs.clear_leader_uuid();
    auto newCs = oldCs;
    newCs.set_current_term(2);

    EXPECT_EQ(
        "term changed from 1 to 2, "
        "leader changed from <none> to A (A.example.com)",
        diffConsensusStates(noLeaderCs, newCs));
  }

  // Simulate gaining a pending config
  {
    auto pendingConfigCs = oldCs;
    pendingConfigCs.mutable_pending_config();
    EXPECT_EQ(
        "now has a pending config: ",
        diffConsensusStates(oldCs, pendingConfigCs));
  }

  // Simulate losing a pending config
  {
    auto pendingConfigCs = oldCs;
    pendingConfigCs.mutable_pending_config();
    EXPECT_EQ(
        "no longer has a pending config: ",
        diffConsensusStates(pendingConfigCs, oldCs));
  }

  // Simulate a change in a pending config
  {
    auto beforeCs = oldCs;
    addPeer(beforeCs.mutable_pending_config(), "A", V);
    auto afterCs = beforeCs;
    afterCs.mutable_pending_config()
        ->mutable_peers()
        ->Mutable(0)
        ->set_member_type(N);

    EXPECT_EQ(
        "pending config changed, A (A.example.com) changed from VOTER to NON_VOTER",
        diffConsensusStates(beforeCs, afterCs));
  }
}

// Unit test for the variants of getConsensusRole().
TEST(QuorumUtilTest, TestGetConsensusRole) {
  const auto kLeader = RaftPeerPB::LEADER;
  const auto kFollower = RaftPeerPB::FOLLOWER;
  const auto kLearner = RaftPeerPB::LEARNER;
  const auto kNonParticipant = RaftPeerPB::NON_PARTICIPANT;

  // 3-argument variant of getConsensusRole().
  const auto config1 = createConfig({{"A", V}, {"B", V}, {"C", N}});
  ASSERT_EQ(kLeader, getConsensusRole("A", "A", config1));
  ASSERT_EQ(kFollower, getConsensusRole("B", "A", config1));
  ASSERT_EQ(kFollower, getConsensusRole("A", "", config1));
  ASSERT_EQ(kLearner, getConsensusRole("C", "A", config1));
  ASSERT_EQ(kLearner, getConsensusRole("C", "C", config1)); // Illegal.
  ASSERT_EQ(kNonParticipant, getConsensusRole("D", "A", config1));
  ASSERT_EQ(kNonParticipant, getConsensusRole("D", "D", config1)); // Illegal.
  ASSERT_EQ(kNonParticipant, getConsensusRole("", "A", config1)); // Illegal.
  ASSERT_EQ(kNonParticipant, getConsensusRole("", "", config1)); // Illegal.

  // 2-argument variant of getConsensusRole().
  const auto config2 = createConfig({{"A", V}, {"B", V}, {"C", V}});
  ConsensusStatePB cstate;
  *cstate.mutable_committed_config() = config1;
  *cstate.mutable_pending_config() = config2;
  cstate.set_leader_uuid("A");
  ASSERT_EQ(kLeader, getConsensusRole("A", cstate));
  ASSERT_EQ(kFollower, getConsensusRole("B", cstate));
  ASSERT_EQ(kFollower, getConsensusRole("C", cstate));
  ASSERT_EQ(kNonParticipant, getConsensusRole("D", cstate));
  cstate.set_leader_uuid("D");
  ASSERT_EQ(kNonParticipant, getConsensusRole("D", cstate)); // Illegal.
}

TEST(QuorumUtilTest, TestIsRaftConfigVoter) {
  RaftConfigPB config;
  addPeer(&config, "A", V);
  addPeer(&config, "B", N);
  addPeer(&config, "C", U);

  // The case when membership type is not specified. That sort of configuration
  // would not pass VerifyRaftConfig(), though. Anyway, that should result
  // in non-voter since the member_type is initialized with UNKNOWN_MEMBER_TYPE.
  const string noMemberTypePeerUuid = "D";
  RaftPeerPB* noMemberTypePeer = config.add_peers();
  noMemberTypePeer->set_permanent_uuid(noMemberTypePeerUuid);
  noMemberTypePeer->mutable_last_known_addr()->set_host(
      noMemberTypePeerUuid + ".example.com");

  ASSERT_TRUE(isRaftConfigVoter("A", config));
  ASSERT_FALSE(isRaftConfigVoter("B", config));
  ASSERT_FALSE(isRaftConfigVoter("C", config));
  ASSERT_FALSE(isRaftConfigVoter(noMemberTypePeerUuid, config));

  RaftPeerPB* peerA;
  ASSERT_OK(getRaftConfigMember(&config, "A", &peerA));
  RaftPeerPB* peerB;
  ASSERT_OK(getRaftConfigMember(&config, "B", &peerB));
  ASSERT_FALSE(replicaTypesEqual(*peerA, *peerB));
  ASSERT_TRUE(replicaTypesEqual(*peerB, *peerB));
  RaftPeerPB* peerC;
  ASSERT_OK(getRaftConfigMember(&config, "C", &peerC));
  ASSERT_FALSE(replicaTypesEqual(*peerB, *peerC));
}

// Tests paremeterized by the policy on the replica majority's health.
class QuorumUtilHealthPolicyParamTest
    : public ::testing::Test,
      public ::testing::WithParamInterface<MajorityHealthPolicy> {};
INSTANTIATE_TEST_CASE_P(
    ,
    QuorumUtilHealthPolicyParamTest,
    ::testing::Values(MHP_H, MHP_I));

// Verify basic functionality of the kudu::consensus::shouldAddReplica() utility
// function.
TEST_P(QuorumUtilHealthPolicyParamTest, ShouldAddReplica) {
  const auto policy = GetParam();
  {
    RaftConfigPB config;
    addPeer(&config, "A", V);
    addPeer(&config, "B", V);
    addPeer(&config, "C", V);
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 4, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 4, policy));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, '?');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 4, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 4, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, healthStatus);
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", V, healthStatus);
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 4, policy));
      EXPECT_FALSE(shouldAddReplica(config, 2, policy));
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 4, policy));
      EXPECT_TRUE(shouldAddReplica(config, 2, policy));
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, '-');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", N, '+');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", N, '+');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      // Should add a replica if ignoring the health status of the majority.
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", N, '+');
    // The configuration is over-replicated already.
    EXPECT_FALSE(shouldAddReplica(config, 1, policy));
    if (policy == MHP_H) {
      // Not enough voters to commit the change.
      EXPECT_FALSE(shouldAddReplica(config, 2, policy));
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 2, policy));
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(shouldAddReplica(config, 1, policy));
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    if (policy == MHP_H) {
      // The configuration is under-replicated, but there are not enough healthy
      // voters to commit the configuration change.
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", N, healthStatus, {{"PROMOTE", true}});
    EXPECT_FALSE(shouldAddReplica(config, 1, policy));
    if (policy == MHP_H) {
      EXPECT_FALSE(shouldAddReplica(config, 2, policy));
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 2, policy));
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '?');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    // The catalog manager should wait for a definite health status of replica
    // 'C' before making decision whether to add replica for replacement or not.
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", N, '+');
    EXPECT_TRUE(shouldAddReplica(config, 4, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_TRUE(shouldAddReplica(config, 4, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    addPeer(&config, "D", N, healthStatus);
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    addPeer(&config, "D", N, '+');
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    // The non-voter replica does not have the PROMOTE attribute,
    // so a new one is needed.
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 4, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 4, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    addPeer(&config, "D", N, healthStatus, {{"PROMOTE", true}});
    EXPECT_FALSE(shouldAddReplica(config, 2, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, healthStatus);
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, healthStatus, {{"PROMOTE", true}});
    addPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, healthStatus);
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, healthStatus, {{"PROMOTE", true}});
    addPeer(&config, "E", N, '+', {{"PROMOTE", false}});
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", V, healthStatus);
    if (policy == MHP_H) {
      // If honoring the health of the replica's majority, the catalog manager
      // will not add a new non-voter replica until the situation is resolved.
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, healthStatus);
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, healthStatus);
    addPeer(&config, "E", V, '+');
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 4, policy));
    EXPECT_TRUE(shouldAddReplica(config, 5, policy));
  }
}

// Verify logic of the kudu::consensus::shouldEvictReplica(), anticipating
// removal of a voter replica.
TEST(QuorumUtilTest, ShouldEvictReplicaVoters) {
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '-');
    addPeer(&config, "C", V, '+');
    // Not safe to evict because we don't have enough healthy nodes to commit
    // the eviction.
    EXPECT_FALSE(shouldEvictReplica(config, "C", 1, MHP_H));
    EXPECT_FALSE(shouldEvictReplica(config, "C", 2, MHP_H));

    // Should evict if ignoring the health status of the majority.
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "C", 1, MHP_I, &toEvict));
    EXPECT_EQ("B", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "C", 2, MHP_I, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V);
    addPeer(&config, "C", V);
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, MHP_H));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 2, MHP_H));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+', {{"REPLACE", false}});
    addPeer(&config, "C", V, '-');
    addPeer(&config, "D", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 4, MHP_H));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_H, &toEvict));
    EXPECT_EQ("C", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 4, MHP_H));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_H, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  for (char healthStatus : kHealthStatuses) {
    SCOPED_TRACE(fmt::format("replica health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '-');
    addPeer(&config, "D", V, healthStatus, {{"REPLACE", true}});
    // For replication factors <= 3 we will be able to commit the eviction of D
    // with only A and B, regardless of D's health and regardless of the
    // desired replication factor.
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, MHP_H, &toEvict));
    // The priority of voter replica replacement (decreasing):
    //   * failed & slated for replacement
    //   * failed
    //   * ...
    if (healthStatus == '-' || healthStatus == 'x') {
      EXPECT_EQ("D", toEvict);
    } else {
      EXPECT_EQ("C", toEvict);
    }
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_H, &toEvict));
    if (healthStatus == '-' || healthStatus == 'x') {
      EXPECT_EQ("D", toEvict);
    } else {
      EXPECT_EQ("C", toEvict);
    }
    if (healthStatus == 'x') {
      // Unrecoverably failed replica should be evicted even if the
      // configuration is not over-replicated if it's safe to commit the
      // configuration change.
      ASSERT_TRUE(shouldEvictReplica(config, "A", 4, MHP_H));
      EXPECT_EQ("D", toEvict);
    } else {
      // Since we are not over-replicated, we will not evict in this case.
      EXPECT_FALSE(shouldEvictReplica(config, "A", 4, MHP_H));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '?');
    addPeer(&config, "D", V, '+', {{"REPLACE", true}});

    // For the replication factor 3, it's too early to evict 'C': it might be
    // in a good health, actually (reported, say, next heartbeat). Evicting 'D'
    // at this step is not a good idea neither: the 'C' might appear to fail,
    // and then it's better to keep 'D' around to provide the required
    // replication factor. It's necessary to wait for more deterministic status
    // of replica 'C' before making proper eviction decision.
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, MHP_H));

    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, MHP_H, &toEvict));
    EXPECT_EQ("D", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 4, MHP_H));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_H, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, 'x');
    addPeer(&config, "C", V, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_H, &toEvict));
    EXPECT_EQ("B", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, MHP_H, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '-');
    addPeer(&config, "C", V, 'x');

    // No majority to commit the change.
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, MHP_H));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 2, MHP_H));

    // If ignoring the safety rules, it tries to evict even if the majority
    // of replicas are not online. Among failed replicas, replicas failed
    // irreverisbly are evicted first.
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, MHP_I, &toEvict));
    EXPECT_EQ("C", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, MHP_I, &toEvict));
    EXPECT_EQ("C", toEvict);
  }
}

// Verify logic of the kudu::consensus::shouldEvictReplica(), anticipating
// removal of a voter replica.
TEST_P(QuorumUtilHealthPolicyParamTest, ShouldEvictReplicaVoters) {
  const auto policy = GetParam();
  {
    RaftConfigPB config;
    addPeer(&config, "A", V);
    addPeer(&config, "B", V);
    addPeer(&config, "C", V);
    EXPECT_FALSE(shouldEvictReplica(config, "", 2, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "", 3, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, healthStatus);
    EXPECT_FALSE(shouldEvictReplica(config, "", 3, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "", 2, policy));
  }
  for (auto healthStatus : {'-', 'x'}) {
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, healthStatus);
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, policy));
    EXPECT_EQ("C", toEvict);
    if (healthStatus == '-') {
      EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    } else {
      ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
      EXPECT_EQ("C", toEvict);
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '?');
    addPeer(&config, "B", V, '-');
    addPeer(&config, "C", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "C", 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '?');
    addPeer(&config, "C", V, 'x');
    if (policy == MHP_H) {
      EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    } else {
      string toEvict;
      ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
      EXPECT_EQ("C", toEvict);
    }
  }
}

// Verify logic of the kudu::consensus::shouldEvictReplica(), anticipating
// removal of a non-voter replica (generic for all health policies).
TEST_P(QuorumUtilHealthPolicyParamTest, ShouldEvictReplicaNonVoters) {
  const auto policy = GetParam();
  {
    RaftConfigPB config;
    addPeer(&config, "A", V);
    EXPECT_FALSE(shouldEvictReplica(config, "", 1, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 1, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", N);
    EXPECT_FALSE(shouldEvictReplica(config, "A", 2, policy));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", N, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", N, '-', {{"PROMOTE", true}});
    string toEvict;
    // It's always safe to evict an unhealthy non-voter if we have enough
    // healthy voters to commit the config change.
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", N, '-');
    addPeer(&config, "C", N);
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", N, '?');
    addPeer(&config, "C", N, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 2, policy));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", N);
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 2, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V);
    addPeer(&config, "C", N);
    EXPECT_FALSE(shouldEvictReplica(config, "A", 2, policy));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    if (policy == MHP_H) {
      // Would evict a non-voter first, but it's not known whether the majority
      // of the voter replicas are on-line to commence the operation: that's
      // because the state of B is unknown. So, in this case the voter replica B
      // will be removed first.
      EXPECT_EQ("B", toEvict);
    } else {
      EXPECT_EQ("C", toEvict);
    }

    removePeer(&config, "B");
    // Now, having just a single online replica, it's possible to evict the
    // failed non-voter replica C.
    ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
    EXPECT_EQ("C", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '-');
    addPeer(&config, "B", V);
    addPeer(&config, "C", N);
    EXPECT_FALSE(shouldEvictReplica(config, "", 2, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "", 1, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '-');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(shouldEvictReplica(config, "B", 3, policy));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 2, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
  }
  {
    // Make sure failed non-voter replicas are removed from the configuration to
    // avoid polluting all tablet servers with failed non-voter replicas.
    RaftConfigPB config;
    addPeer(&config, "A", V, '-');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '-', {{"PROMOTE", true}});
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 4, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "C", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '-');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '?', {{"PROMOTE", true}});
    EXPECT_FALSE(shouldEvictReplica(config, "B", 3, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "B", 4, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '-');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, 'x', {{"PROMOTE", true}});
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "B", 4, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, 'x');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, 'x');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "B", 4, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, 'x');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 3, policy, &toEvict));
    EXPECT_EQ("A", toEvict);
    ASSERT_TRUE(shouldEvictReplica(config, "B", 4, policy, &toEvict));
    EXPECT_EQ("A", toEvict);
  }
}

TEST_P(QuorumUtilHealthPolicyParamTest, DontEvictLeader) {
  const vector<string> replicas = {"A", "B", "C", "D"};
  RaftConfigPB config;
  addPeer(&config, replicas[0], V, '+');
  addPeer(&config, replicas[1], V, '+');
  addPeer(&config, replicas[2], V, '+');
  addPeer(&config, replicas[3], V, '+');

  const auto policy = GetParam();
  // Exhaustively loop through all nodes, each as leader, when over-replicated
  // and ensure that the leader never gets evicted.
  for (const auto& leader : replicas) {
    SCOPED_TRACE(fmt::format("leader {}", leader));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, leader, 3, policy, &toEvict));
    ASSERT_NE(leader, toEvict);
  }
}

// This is a scenario for tablet configurations with more than the required
// number of voter replicas.  For different health policies, the results depend
// on whether the majority of replicas is on-line.
TEST_P(QuorumUtilHealthPolicyParamTest, TooManyVoters) {
  const auto policy = GetParam();
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '?');
    addPeer(&config, "D", V, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+');
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '-');
    addPeer(&config, "D", V, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_TRUE(toEvict == "C" || toEvict == "D") << toEvict;
    if (policy == MHP_H) {
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    } else {
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
}

// Basic scenarios involving replicas with the REPLACE attribute set.
TEST_P(QuorumUtilHealthPolicyParamTest, ReplaceAttributeBasic) {
  const auto policy = GetParam();
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    EXPECT_TRUE(shouldAddReplica(config, 1, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 1, policy));
  }
  {
    // Regression test scenario for KUDU-2443.
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    EXPECT_FALSE(shouldAddReplica(config, 1, policy));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 1, policy));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "B", 1, policy, &toEvict));
    EXPECT_EQ("A", toEvict);
  }
  {
    for (auto healthStatus : {'+', '-', '?', 'x'}) {
      SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
      RaftConfigPB config;
      addPeer(&config, "A", V, '+', {{"REPLACE", true}});
      addPeer(&config, "B", N, healthStatus);
      EXPECT_TRUE(shouldAddReplica(config, 1, policy));
      if (healthStatus == '+' || healthStatus == '?') {
        EXPECT_FALSE(shouldEvictReplica(config, "A", 1, policy));
      } else {
        string toEvict;
        ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
        EXPECT_EQ("B", toEvict);
      }
    }
  }
  // If a non-voter replica with PROMOTE=true is already in the Raft config,
  // no need to add an additional one if the health status of the non-voter
  // replica is HEALTHY or UNKNOWN.
  {
    for (auto healthStatus : {'+', '-', '?', 'x'}) {
      SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
      RaftConfigPB config;
      addPeer(&config, "A", V, '+', {{"REPLACE", true}});
      addPeer(&config, "B", N, healthStatus, {{"PROMOTE", true}});
      if (healthStatus == '+' || healthStatus == '?') {
        EXPECT_FALSE(shouldAddReplica(config, 1, policy));
      } else {
        EXPECT_TRUE(shouldAddReplica(config, 1, policy));
      }
      if (healthStatus == '+' || healthStatus == '?') {
        EXPECT_FALSE(shouldEvictReplica(config, "A", 1, policy));
      } else {
        string toEvict;
        ASSERT_TRUE(shouldEvictReplica(config, "A", 1, policy, &toEvict));
        EXPECT_EQ("B", toEvict);
      }
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));

    for (const auto& leaderReplica : {"B", "C", "D"}) {
      string toEvict;
      SCOPED_TRACE(fmt::format("leader {}", leaderReplica));
      ASSERT_TRUE(
          shouldEvictReplica(config, leaderReplica, 3, policy, &toEvict));
      EXPECT_EQ("A", toEvict);
    }
  }
  for (auto healthStatus : {'-', '?', 'x'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, healthStatus, {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    for (const auto& leaderReplica : {"B", "C", "D"}) {
      SCOPED_TRACE(
          fmt::format(
              "health status '{}', leader {}", healthStatus, leaderReplica));
      string toEvict;
      ASSERT_TRUE(
          shouldEvictReplica(config, leaderReplica, 3, policy, &toEvict));
      EXPECT_EQ("A", toEvict);
      EXPECT_FALSE(shouldAddReplica(config, 3, policy));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    addPeer(&config, "E", V, '+');
    // There should be no attempt to evict the leader.
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_NE("A", toEvict);
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));

    for (const auto& leaderReplica : {"B", "C", "D", "E"}) {
      string toEvict2;
      SCOPED_TRACE(fmt::format("leader {}", leaderReplica));
      ASSERT_TRUE(
          shouldEvictReplica(config, leaderReplica, 3, policy, &toEvict2));
      EXPECT_EQ("A", toEvict2);
    }
  }
  for (auto replicaHealth : kHealthStatuses) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, replicaHealth, {{"REPLACE", true}});
    SCOPED_TRACE(fmt::format("replica health status '{}'", replicaHealth));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    if (replicaHealth == '+') {
      EXPECT_NE("A", toEvict);
    } else {
      EXPECT_EQ("D", toEvict);
    }
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'?', '-', 'x'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, healthStatus, {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '+');
    for (const auto& leaderReplica : {"B", "C", "D"}) {
      SCOPED_TRACE(
          fmt::format(
              "health status '{}', leader {}", healthStatus, leaderReplica));
      string toEvict;
      ASSERT_TRUE(
          shouldEvictReplica(config, leaderReplica, 3, policy, &toEvict));
      EXPECT_EQ("A", toEvict);
      EXPECT_TRUE(shouldAddReplica(config, 3, policy));
    }
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '?');
    EXPECT_FALSE(shouldEvictReplica(config, "B", 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'?', '-', 'x'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '?');
    addPeer(&config, "D", V, healthStatus, {{"REPLACE", true}});
    addPeer(&config, "E", V, '+');
    SCOPED_TRACE(fmt::format("health status '{}'", healthStatus));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
}

// Test specific to the scenarios where the leader replica itself marked with
// the 'REPLACE' attribute.
TEST_P(QuorumUtilHealthPolicyParamTest, LeaderReplicaWithReplaceAttribute) {
  const auto policy = GetParam();
  // Healthy excess voter replicas (both voters and non-voters) should not be
  // evicted when the leader is marked with the 'REPLACE' attribute.
  for (auto healthStatus : {'+', '?'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, healthStatus, {{"PROMOTE", true}});
    SCOPED_TRACE(
        fmt::format("non-voter replica with status '{}'", healthStatus));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  for (auto healthStatus : {'+', '?'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, healthStatus);
    SCOPED_TRACE(fmt::format("voter replica with status '{}'", healthStatus));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  for (auto promote : {false, true}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '-', {{"PROMOTE", promote}});
    SCOPED_TRACE(
        fmt::format(
            "failed non-voter replica with PROMOTE attribute {}",
            promote ? "set" : "unset"));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    // Current algorithm is conservative in the cases like below, but we might
    // evict non-voter replica 'E' which does not have the PROMOTE attribute.
    // TODO(aserbin): clarify on this.
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    addPeer(&config, "E", N, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    // The non-voter replica does not have the 'promote' attribute, so
    // it should be evicted since it's not going to become a voter anyway,
    // and we don't support standby non-voter replicas at this point.
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    addPeer(&config, "E", N, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("E", toEvict);
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    // In the case below the non-voter replica 'D' is not needed. The
    // configuration like that might be the result of an attempt to replace 'D'
    // which was previously reported as failed. However, by the time the newly
    // added replica caught up with the leader, replica 'E' was back on-line.
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    addPeer(&config, "E", V, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
}

// This test is specific for various scenarios when multiple replicas have the
// REPLACE attribute set (for all health policies).
TEST_P(QuorumUtilHealthPolicyParamTest, MultipleReplicasWithReplaceAttribute) {
  const auto policy = GetParam();
  for (auto replicaType : {N, V}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", replicaType, '-');
    SCOPED_TRACE(
        fmt::format(
            "replica of {} type", RaftPeerPB::MemberType_Name(replicaType)));
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto replicaHealth : {'+', '?'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", N, replicaHealth);
    SCOPED_TRACE(
        fmt::format(
            "NON_VOTER replica with health status '{}'", replicaHealth));
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (const auto& leaderReplica : {"A", "B", "C"}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '?');
    SCOPED_TRACE(fmt::format("leader {}", leaderReplica));
    EXPECT_FALSE(shouldEvictReplica(config, leaderReplica, 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (const auto& leaderReplica : {"A", "C"}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    addPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    SCOPED_TRACE(fmt::format("leader {}", leaderReplica));
    EXPECT_FALSE(shouldEvictReplica(config, leaderReplica, 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", V, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("B", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_TRUE(toEvict == "B" || toEvict == "C");
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '+');
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    // The non-voter replica does not have the PROMOTE attribute, so it the
    // configuration should be considered under-replicated.
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  for (auto replicaStatus : {'+', '?'}) {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, replicaStatus, {{"PROMOTE", true}});
    EXPECT_FALSE(shouldEvictReplica(config, "A", 3, policy));
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+');
    addPeer(&config, "C", V, '+');
    addPeer(&config, "D", N, '-');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_EQ("D", toEvict);
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '+');
    addPeer(&config, "E", V, '+');
    string toEvict;
    ASSERT_TRUE(shouldEvictReplica(config, "A", 3, policy, &toEvict));
    EXPECT_TRUE(toEvict == "B" || toEvict == "C");
    EXPECT_TRUE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", V, '+');
    addPeer(&config, "E", V, '+');
    addPeer(&config, "F", V, '+');

    for (const string& leaderReplica : {"A", "B", "C", "D", "E", "F"}) {
      string toEvict;
      ASSERT_TRUE(
          shouldEvictReplica(config, leaderReplica, 3, policy, &toEvict));
      EXPECT_TRUE(toEvict == "A" || toEvict == "B" || toEvict == "C");
      if (leaderReplica == "A" || leaderReplica == "B" ||
          leaderReplica == "C") {
        EXPECT_NE(leaderReplica, toEvict);
      }
    }
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
  {
    RaftConfigPB config;
    addPeer(&config, "A", V, '+', {{"REPLACE", true}});
    addPeer(&config, "B", V, '+', {{"REPLACE", true}});
    addPeer(&config, "C", V, '+', {{"REPLACE", true}});
    addPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    addPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    addPeer(&config, "F", N, '+', {{"PROMOTE", true}});

    for (const string& leaderReplica : {"A", "B", "C"}) {
      // All non-voters are in good shape and not a single one has been
      // promoted yet.
      ASSERT_FALSE(shouldEvictReplica(config, leaderReplica, 3, policy));
    }
    // No more replicas are needed for the replacement.
    EXPECT_FALSE(shouldAddReplica(config, 3, policy));
  }
}

// Verify logic of the kudu::consensus::shouldEvictReplica(), anticipating
// removal of a non-voter replica (specific for particular health policies).
TEST(QuorumUtilTest, ShouldEvictReplicaNonVoters) {
  RaftConfigPB config;
  addPeer(&config, "A", V, '+');
  addPeer(&config, "B", V, '-');
  addPeer(&config, "C", N, '-', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", 2, MHP_H));
  string toEvict;
  ASSERT_TRUE(shouldEvictReplica(config, "A", 2, MHP_I, &toEvict));
  EXPECT_EQ("C", toEvict);
  // Would evict a non-voter first, but replica B is reported as failed and
  // the configuration does not have enough healthy voter replicas to have a
  // majority of votes. So, the voter replica B will be removed first.
  ASSERT_TRUE(shouldEvictReplica(config, "A", 1, MHP_H, &toEvict));
  EXPECT_EQ("B", toEvict);

  removePeer(&config, "B");
  // Now, having just a single online replica, it's possible to evict the
  // failed non-voter replica C.
  ASSERT_TRUE(shouldEvictReplica(config, "A", 1, MHP_H, &toEvict));
  EXPECT_EQ("C", toEvict);
}

// A scenario of replica replacement where replicas fall behind the log segment
// GC threshold and are replaced accordingly. This scenario is written to
// address scenarios like of KUDU-2342.
TEST(QuorumUtilTest, NewlyAddedNonVoterFallsBehindLogGC) {
  constexpr auto kReplicationFactor = 3;
  constexpr auto kPolicy = MajorityHealthPolicy::Honor;

  RaftConfigPB config;
  addPeer(&config, "A", V, '+');
  addPeer(&config, "B", V, '+');
  addPeer(&config, "C", V, '+');

  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica B falls behind the log segment GC threshold. Since this is an
  // irreverisble failure, system tries to evict the replica right away.
  setPeerHealth(&config, "B", 'x');
  string toEvict;
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("B", toEvict);

  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Adding a non-voter to replace B.
  addPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new non-voter replica becomes healthy.
  setPeerHealth(&config, "D", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new non-voter replica falls behind the log segment GC threshold. The
  // system should evict it before trying to add a replacement replica.
  setPeerHealth(&config, "D", 'x');
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("D", toEvict);
  removePeer(&config, toEvict);

  // A new non-voter replica is needed.
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Adding a non-voter to replace D.
  addPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new non-voter replica 'E' becomes healthy.
  setPeerHealth(&config, "E", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The newly added replica gets promoted to voter.
  promotePeer(&config, "E");
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new voter replica E falls behind the log segment GC threshold. The
  // replica should be evicted.
  setPeerHealth(&config, "E", 'x');
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("E", toEvict);

  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The system should add a replacement for the evicted replica.
  addPeer(&config, "F", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new non-voter replica 'F' becomes healthy.
  setPeerHealth(&config, "F", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The newly added replica 'F' gets promoted to voter, all is well now.
  promotePeer(&config, "F");
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));
}

// A scenario of replica replacement where the replica added for replacement
// of a failed one also fails. The system should end up replacing both failed
// replicas.
TEST(QuorumUtilTest, NewlyPromotedReplicaCrashes) {
  constexpr auto kReplicationFactor = 3;
  constexpr auto kPolicy = MajorityHealthPolicy::Honor;

  RaftConfigPB config;
  addPeer(&config, "A", V, '+');
  addPeer(&config, "B", V, '+');
  addPeer(&config, "C", V, '+');

  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica B fails.
  setPeerHealth(&config, "B", '-');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Adding a non-voter to replace B.
  addPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new non-voter replica becomes healthy.
  setPeerHealth(&config, "D", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The newly added non-voter replica is promoted.
  promotePeer(&config, "D");
  {
    // B would be evicted, if it's reported as is.
    string toEvict;
    ASSERT_TRUE(
        shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
    EXPECT_EQ("B", toEvict);
  }
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // However, the newly promoted replica crashes prior to B getting evicted.
  // The system should add a new replica for replacement.

  // We cannot evict because we don't have enough healthy voters to commit
  // the eviction config change.
  setPeerHealth(&config, "D", '?');
  string toEvict;
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("B", toEvict);
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  setPeerHealth(&config, "D", '-');
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_TRUE(toEvict == "B" || toEvict == "D") << toEvict;
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  addPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  setPeerHealth(&config, "E", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  promotePeer(&config, "E");
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_TRUE(toEvict == "B" || toEvict == "D") << toEvict;
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  removePeer(&config, toEvict);
  // The processs converges: 3 voter replicas, all are healthy.
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));
}

// A scenario to verify that the catalog manager does not do anything unexpected
// in the 3-4-3 replica management mode when replica's health is flapping
// between HEALTHY and UNKNOWN (e.g., when leader replica changes).
TEST(QuorumUtilTest, ReplicaHealthFlapping) {
  constexpr auto kReplicationFactor = 3;
  constexpr auto kPolicy = MajorityHealthPolicy::Honor;

  // The initial tablet report after the tablet replica A has started and
  // become the leader.
  RaftConfigPB config;
  addPeer(&config, "A", V, '+');
  addPeer(&config, "B", V, '?');
  addPeer(&config, "C", V, '?');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica B is reported as healthy.
  setPeerHealth(&config, "B", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica C is reported as healthy.
  setPeerHealth(&config, "C", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica B becomes the new leader.
  setPeerHealth(&config, "A", '?');
  setPeerHealth(&config, "B", '+');
  setPeerHealth(&config, "C", '?');
  EXPECT_FALSE(shouldEvictReplica(config, "B", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica A is reported as healthy; replica C fails.
  setPeerHealth(&config, "A", '+');
  setPeerHealth(&config, "B", '+');
  setPeerHealth(&config, "C", '-');
  EXPECT_FALSE(shouldEvictReplica(config, "B", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // A new non-voter replica has been added to replace failed replica C.
  addPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "B", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica A becomes the new leader.
  setPeerHealth(&config, "A", '+');
  setPeerHealth(&config, "B", '?');
  setPeerHealth(&config, "C", '?');
  setPeerHealth(&config, "D", '?');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // The new leader has contacted on-line replicas.
  setPeerHealth(&config, "A", '+');
  setPeerHealth(&config, "B", '+');
  setPeerHealth(&config, "C", '?');
  setPeerHealth(&config, "D", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica D catches up with the leader's WAL and gets promoted.
  promotePeer(&config, "D");
  string toEvict;
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("C", toEvict);
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica D becomes the new leader.
  setPeerHealth(&config, "A", '?');
  setPeerHealth(&config, "B", '?');
  setPeerHealth(&config, "C", '?');
  setPeerHealth(&config, "D", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "D", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  setPeerHealth(&config, "A", '+');
  setPeerHealth(&config, "B", '+');
  setPeerHealth(&config, "C", '?');
  setPeerHealth(&config, "D", '+');
  ASSERT_TRUE(
      shouldEvictReplica(config, "D", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("C", toEvict);
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  setPeerHealth(&config, "C", '-');
  ASSERT_TRUE(
      shouldEvictReplica(config, "D", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_EQ("C", toEvict);
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  removePeer(&config, "C");
  EXPECT_FALSE(shouldEvictReplica(config, "D", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));
}

// A scenario to simulate the process of migrating all replicas of a tablet,
// where all replicas are marked for replacement simultaneously. This is a
// possible scenario when decommissioning multiple tablet servers/nodes at once.
TEST(QuorumUtilTest, ReplaceAllTabletReplicas) {
  constexpr auto kReplicationFactor = 3;
  constexpr auto kPolicy = MajorityHealthPolicy::Honor;

  // The initial tablet report after the tablet replica 'A' has started and
  // become the leader.
  RaftConfigPB config;
  addPeer(&config, "A", V, '+', {{"REPLACE", true}});
  addPeer(&config, "B", V, '+', {{"REPLACE", true}});
  addPeer(&config, "C", V, '+', {{"REPLACE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // First non-voter replica added.
  addPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Second non-voter replica added.
  addPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Third non-voter replica added.
  addPeer(&config, "F", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  setPeerHealth(&config, "D", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica 'D' catches up with the leader's WAL and gets promoted.
  promotePeer(&config, "D");
  string toEvict;
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_TRUE(toEvict == "B" || toEvict == "C");
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Evicting the replica selected by shouldEvictReplica() above.
  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Non-voter replica 'F' become unavailable.
  setPeerHealth(&config, "F", '-');
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  ASSERT_EQ("F", toEvict);
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Evicting the failed non-voter replica, selected by shouldEvictReplica()
  // above.
  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_TRUE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Adding a new non-voter replica.
  addPeer(&config, "G", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // A newly added non-voter replica is in good shape.
  setPeerHealth(&config, "G", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica 'E' is reported in good health.
  setPeerHealth(&config, "E", '+');
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica 'E' catches up with the leader's WAL and gets promoted.
  promotePeer(&config, "E");
  ASSERT_TRUE(
      shouldEvictReplica(config, "A", kReplicationFactor, kPolicy, &toEvict));
  EXPECT_TRUE(toEvict == "B" || toEvict == "C");
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Evicting the replica selected by shouldEvictReplica() above.
  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Replica 'G' catches up, but replica 'A' cannot yet be evicted since it's
  // a leader replica.
  promotePeer(&config, "G");
  EXPECT_FALSE(shouldEvictReplica(config, "A", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Leadership changes from 'A' to 'G', so now it's possible to evict 'A'.
  ASSERT_TRUE(
      shouldEvictReplica(config, "G", kReplicationFactor, kPolicy, &toEvict));
  ASSERT_EQ("A", toEvict);
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));

  // Evicting the replica selected by shouldEvictReplica() above. With that,
  // the replacement process of all the marked replicas is complete; no further
  // changes is necessary for the tablet's Raft configuration.
  removePeer(&config, toEvict);
  EXPECT_FALSE(shouldEvictReplica(config, "G", kReplicationFactor, kPolicy));
  EXPECT_FALSE(shouldAddReplica(config, kReplicationFactor, kPolicy));
}

} // namespace consensus
} // namespace kudu
