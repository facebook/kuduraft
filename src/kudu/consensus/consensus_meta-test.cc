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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using std::string;
using std::unique_ptr;
using std::vector;

const char* kTabletId = "test-consensus-metadata";
const int64_t kInitialTerm = 3;

class ConsensusMetadataTest : public KuduTest {
 public:
  ConsensusMetadataTest() : fs_manager_(env_, GetTestPath("fs_root")) {}

  virtual void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test configuration.
    config_.set_opid_index(kInvalidOpIdIndex);
    RaftPeerPB* peer = config_.add_peers();
    peer->set_permanent_uuid(fs_manager_.uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

 protected:
  // Assert that the given cmeta has a single configuration with the given
  // metadata values.
  void assertValuesEqual(
      const std::shared_ptr<ConsensusMetadata>& cmeta,
      int64_t opIdIndex,
      const string& permanantUuid,
      int64_t term);

  FsManager fs_manager_;
  RaftConfigPB config_;
};

void ConsensusMetadataTest::assertValuesEqual(
    const std::shared_ptr<ConsensusMetadata>& cmeta,
    int64_t opIdIndex,
    const string& permanantUuid,
    int64_t term) {
  // Sanity checks.
  ASSERT_EQ(1, cmeta->CommittedConfig().peers_size());

  // Value checks.
  ASSERT_EQ(opIdIndex, cmeta->CommittedConfig().opid_index());
  ASSERT_EQ(
      permanantUuid,
      cmeta->CommittedConfig().peers().begin()->permanent_uuid());
  ASSERT_EQ(term, cmeta->currentTerm());
}

// Test the basic "happy case" of creating and then loading a file.
TEST_F(ConsensusMetadataTest, TestCreateLoad) {
  // Create the file.
  {
    ASSERT_OK(
        ConsensusMetadata::Create(
            &fs_manager_,
            kTabletId,
            fs_manager_.uuid(),
            config_,
            kInitialTerm));
  }

  // Load the file.
  std::shared_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(
      ConsensusMetadata::Load(
          &fs_manager_, kTabletId, fs_manager_.uuid(), &cmeta));
  NO_FATALS(assertValuesEqual(
      cmeta, kInvalidOpIdIndex, fs_manager_.uuid(), kInitialTerm));
  ASSERT_GT(cmeta->on_disk_size(), 0);
}

// Test deferred creation.
TEST_F(ConsensusMetadataTest, TestDeferredCreateLoad) {
  // Create the cmeta object, but not the file.
  std::shared_ptr<ConsensusMetadata> writer;
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_,
          kTabletId,
          fs_manager_.uuid(),
          config_,
          kInitialTerm,
          ConsensusMetadataCreateMode::NoFlushOnCreate,
          &writer));

  // Try to load the file: it should not be there.
  std::shared_ptr<ConsensusMetadata> reader;
  Status s = ConsensusMetadata::Load(
      &fs_manager_, kTabletId, fs_manager_.uuid(), &reader);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // Flush; now the file will be there.
  ASSERT_OK(writer->Flush());
  ASSERT_OK(
      ConsensusMetadata::Load(
          &fs_manager_, kTabletId, fs_manager_.uuid(), &reader));
  NO_FATALS(assertValuesEqual(
      reader, kInvalidOpIdIndex, fs_manager_.uuid(), kInitialTerm));
}

// Ensure that Create() will not overwrite an existing file.
TEST_F(ConsensusMetadataTest, TestCreateNoOverwrite) {
  // Create the consensus metadata file.
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_, kTabletId, fs_manager_.uuid(), config_, kInitialTerm));
  // Try to create it again.
  Status s = ConsensusMetadata::Create(
      &fs_manager_, kTabletId, fs_manager_.uuid(), config_, kInitialTerm);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_MATCHES(
      s.ToString(), "Unable to write consensus meta file.*already exists");
}

// Ensure that we get an error when loading a file that doesn't exist.
TEST_F(ConsensusMetadataTest, TestFailedLoad) {
  Status s =
      ConsensusMetadata::Load(&fs_manager_, kTabletId, fs_manager_.uuid());
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();
  LOG(INFO) << "Expected failure: " << s.ToString();
}

// Check that changes are not written to disk until Flush() is called.
TEST_F(ConsensusMetadataTest, TestFlush) {
  const int64_t kNewTerm = 4;
  std::shared_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_,
          kTabletId,
          fs_manager_.uuid(),
          config_,
          kInitialTerm,
          ConsensusMetadataCreateMode::FlushOnCreate,
          &cmeta));
  cmeta->setCurrentTerm(kNewTerm);

  // We are sort of "breaking the rules" by having multiple ConsensusMetadata
  // objects in flight that point to the same file, but for a test this is fine
  // since it's read-only.
  {
    std::shared_ptr<ConsensusMetadata> cmetaRead;
    ASSERT_OK(
        ConsensusMetadata::Load(
            &fs_manager_, kTabletId, fs_manager_.uuid(), &cmetaRead));
    NO_FATALS(assertValuesEqual(
        cmetaRead, kInvalidOpIdIndex, fs_manager_.uuid(), kInitialTerm));
    ASSERT_GT(cmeta->on_disk_size(), 0);
  }

  ASSERT_OK(cmeta->Flush());
  size_t cmetaSize = cmeta->on_disk_size();

  {
    std::shared_ptr<ConsensusMetadata> cmetaRead;
    ASSERT_OK(
        ConsensusMetadata::Load(
            &fs_manager_, kTabletId, fs_manager_.uuid(), &cmetaRead));
    NO_FATALS(assertValuesEqual(
        cmetaRead, kInvalidOpIdIndex, fs_manager_.uuid(), kNewTerm));
    ASSERT_EQ(cmetaSize, cmetaRead->on_disk_size());
  }
}

// Builds a distributed configuration of voters with the given uuids.
RaftConfigPB buildConfig(const vector<string>& uuids) {
  RaftConfigPB config;
  for (const string& uuid : uuids) {
    RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(uuid);
    peer->set_member_type(RaftPeerPB::VOTER);
    CHECK_OK(hostPortToPb(
        HostPort("255.255.255.255", 0), peer->mutable_last_known_addr()));
  }
  return config;
}

// Test ConsensusMetadata active role calculation.
TEST_F(ConsensusMetadataTest, TestActiveRole) {
  vector<string> uuids = {"a", "b", "c", "d"};
  string peerUuid = "e";
  RaftConfigPB config1 =
      buildConfig(uuids); // We aren't a member of this config...
  config1.set_opid_index(0);

  std::shared_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_,
          kTabletId,
          peerUuid,
          config1,
          kInitialTerm,
          ConsensusMetadataCreateMode::FlushOnCreate,
          &cmeta));

  ASSERT_EQ(4, cmeta->CountVotersInConfig(COMMITTED_CONFIG));
  ASSERT_EQ(0, cmeta->GetConfigOpIdIndex(COMMITTED_CONFIG));

  // Not a participant.
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
  ASSERT_FALSE(cmeta->IsMemberInConfig(peerUuid, COMMITTED_CONFIG));
  ASSERT_FALSE(cmeta->IsVoterInConfig(peerUuid, COMMITTED_CONFIG));

  // Follower.
  uuids.push_back(peerUuid);
  RaftConfigPB config2 = buildConfig(uuids); // But we are a member of this one.
  config2.set_opid_index(1);
  cmeta->set_committed_config(config2);

  ASSERT_EQ(5, cmeta->CountVotersInConfig(COMMITTED_CONFIG));
  ASSERT_EQ(1, cmeta->GetConfigOpIdIndex(COMMITTED_CONFIG));

  ASSERT_EQ(RaftPeerPB::FOLLOWER, cmeta->active_role());
  ASSERT_TRUE(cmeta->IsVoterInConfig(peerUuid, COMMITTED_CONFIG));

  // Pending should mask committed.
  cmeta->set_pending_config(config1);
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());

  ASSERT_TRUE(cmeta->IsMemberInConfig(peerUuid, COMMITTED_CONFIG));
  ASSERT_TRUE(cmeta->IsVoterInConfig(peerUuid, COMMITTED_CONFIG));
  for (auto configState : {ACTIVE_CONFIG, PENDING_CONFIG}) {
    ASSERT_FALSE(cmeta->IsMemberInConfig(peerUuid, configState));
    ASSERT_FALSE(cmeta->IsVoterInConfig(peerUuid, configState));
  }
  cmeta->clear_pending_config();
  ASSERT_EQ(RaftPeerPB::FOLLOWER, cmeta->active_role());
  ASSERT_TRUE(cmeta->IsMemberInConfig(peerUuid, ACTIVE_CONFIG));
  ASSERT_TRUE(cmeta->IsVoterInConfig(peerUuid, ACTIVE_CONFIG));

  // Leader.
  cmeta->set_leader_uuid(peerUuid);
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());

  // Again, pending should mask committed.
  cmeta->set_pending_config(config1);
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
  cmeta->set_pending_config(config2); // pending == committed.
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());
  cmeta->set_committed_config(
      config1); // committed now excludes this node, but is masked...
  ASSERT_EQ(RaftPeerPB::LEADER, cmeta->active_role());

  // ... until we clear pending, then we find committed now excludes us.
  cmeta->clear_pending_config();
  ASSERT_EQ(RaftPeerPB::NON_PARTICIPANT, cmeta->active_role());
}

// Ensure that invocations of ToConsensusStatePB() return the expected state
// in the returned object.
TEST_F(ConsensusMetadataTest, TestToConsensusStatePB) {
  vector<string> uuids = {"a", "b", "c", "d"};
  string peerUuid = "e";

  RaftConfigPB committedConfig =
      buildConfig(uuids); // We aren't a member of this config...
  committedConfig.set_opid_index(1);
  std::shared_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_,
          kTabletId,
          peerUuid,
          committedConfig,
          kInitialTerm,
          ConsensusMetadataCreateMode::FlushOnCreate,
          &cmeta));

  uuids.push_back(peerUuid);
  RaftConfigPB pendingConfig = buildConfig(uuids);
  pendingConfig.set_opid_index(2);

  // Set the pending configuration to be one containing the current leader (who
  // is not in the committed configuration). Ensure that the leader shows up in
  // the pending configuration.
  cmeta->set_pending_config(pendingConfig);
  cmeta->set_leader_uuid(peerUuid);
  ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
  ASSERT_OK(verifyConsensusState(cstate));

  // Set a new leader to be a member of the committed configuration.
  cmeta->set_leader_uuid("a");
  ConsensusStatePB newCstate = cmeta->ToConsensusStatePB();
  ASSERT_FALSE(newCstate.leader_uuid().empty());
  ASSERT_OK(verifyConsensusState(newCstate));

  // An empty leader UUID means no leader and we should not set the
  // corresponding PB field in that case. Regression test for KUDU-2147.
  cmeta->clear_pending_config();
  cmeta->set_leader_uuid("");
  newCstate = cmeta->ToConsensusStatePB();
  ASSERT_TRUE(newCstate.leader_uuid().empty());
  ASSERT_OK(verifyConsensusState(newCstate));
}

// Helper for TestMergeCommittedConsensusStatePB.
static void assertConsensusMergeExpected(
    const std::shared_ptr<ConsensusMetadata>& cmeta,
    const ConsensusStatePB& cstate,
    int64_t expectedTerm,
    const string& expectedVotedFor) {
  // See header docs for ConsensusMetadata::MergeCommittedConsensusStatePB() for
  // a "spec" of these assertions.
  ASSERT_TRUE(!cmeta->has_pending_config());
  ASSERT_EQ(
      pb_util::SecureShortDebugString(cmeta->CommittedConfig()),
      pb_util::SecureShortDebugString(cstate.committed_config()));
  ASSERT_EQ("", cmeta->leader_uuid());
  ASSERT_EQ(expectedTerm, cmeta->currentTerm());
  if (expectedVotedFor.empty()) {
    ASSERT_FALSE(cmeta->hasVotedFor());
  } else {
    ASSERT_EQ(expectedVotedFor, cmeta->votedFor());
  }
}

// Ensure that MergeCommittedConsensusStatePB() works as advertised.
TEST_F(ConsensusMetadataTest, TestMergeCommittedConsensusStatePB) {
  vector<string> uuids = {"a", "b", "c", "d"};

  RaftConfigPB committedConfig =
      buildConfig(uuids); // We aren't a member of this config...
  committedConfig.set_opid_index(1);
  std::shared_ptr<ConsensusMetadata> cmeta;
  ASSERT_OK(
      ConsensusMetadata::Create(
          &fs_manager_,
          kTabletId,
          "e",
          committedConfig,
          1,
          ConsensusMetadataCreateMode::FlushOnCreate,
          &cmeta));

  uuids.emplace_back("e");
  RaftConfigPB pendingConfig = buildConfig(uuids);
  cmeta->set_pending_config(pendingConfig);
  cmeta->set_leader_uuid("e");
  cmeta->setVotedFor("e");

  // Keep the term and votes because the merged term is lower.
  ConsensusStatePB remoteState;
  remoteState.set_current_term(0);
  *remoteState.mutable_committed_config() = buildConfig({"x", "y", "z"});
  cmeta->MergeCommittedConsensusStatePB(remoteState);
  NO_FATALS(assertConsensusMergeExpected(cmeta, remoteState, 1, "e"));

  // Same as above because the merged term is the same as the cmeta term.
  remoteState.set_current_term(1);
  *remoteState.mutable_committed_config() = buildConfig({"f", "g", "h"});
  cmeta->MergeCommittedConsensusStatePB(remoteState);
  NO_FATALS(assertConsensusMergeExpected(cmeta, remoteState, 1, "e"));

  // Higher term, so wipe out the prior state.
  remoteState.set_current_term(2);
  *remoteState.mutable_committed_config() = buildConfig({"i", "j", "k"});
  cmeta->set_pending_config(pendingConfig);
  cmeta->MergeCommittedConsensusStatePB(remoteState);
  NO_FATALS(assertConsensusMergeExpected(cmeta, remoteState, 2, ""));
}

} // namespace consensus
} // namespace kudu
