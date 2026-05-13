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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/consensus/leader_election.h"

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

namespace rpc {
class Messenger;
} // namespace rpc

namespace consensus {

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace {

// Timeout for leader elections, in seconds.
constexpr int kLeaderElectionTimeoutSecs = 10;

// Generate a list of voter UUIDs for testing.
inline vector<string> genVoterUuids(int num_voters) {
  vector<string> voter_uuids;
  voter_uuids.reserve(num_voters);
  for (int i = 0; i < num_voters; ++i) {
    voter_uuids.push_back(fmt::format("peer-{}", i));
  }
  return voter_uuids;
}

} // namespace

////////////////////////////////////////
// LeaderElectionTest
////////////////////////////////////////

class VoteLoggerImplTest : public VoteLoggerInterface {
 public:
  explicit VoteLoggerImplTest() {}
  void logElectionStarted(
      const VoteRequestPB& voteRequest,
      const RaftConfigPB& config) {}
  void logVoteReceived(const VoteResponsePB& voteResponse) {}
  void logElectionDecided(const ElectionResult& electionResult) {}
  void advanceEpoch(int64_t epoch) {}
};

using ProxyMap = unordered_map<string, PeerProxy*>;

// A proxy factory that serves proxies from a map.
class FromMapPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit FromMapPeerProxyFactory(const ProxyMap* proxyMap)
      : proxyMap_(proxyMap) {}

  Status newProxy(const RaftPeerPB& peer_pb, shared_ptr<PeerProxy>* proxy)
      override {
    auto it = proxyMap_->find(peer_pb.permanent_uuid());
    PeerProxy* proxyPtr = (it != proxyMap_->end()) ? it->second : nullptr;
    if (!proxyPtr) {
      return Status::NotFound("no proxy for peer");
    }
    proxy->reset(proxyPtr);
    return Status::OK();
  }

  const shared_ptr<rpc::Messenger>& messenger() const override {
    return nullMessenger_;
  }

 private:
  // FYI, the tests may add and remove nodes from this map while we hold a
  // reference to it.
  const ProxyMap* const proxyMap_;

  shared_ptr<rpc::Messenger> nullMessenger_;
};

class LeaderElectionTest : public KuduTest {
 public:
  LeaderElectionTest()
      : tabletId_("test-tablet"),
        proxyFactory_(new FromMapPeerProxyFactory(&proxies_)),
        latch_(1) {
    CHECK_OK(
        ThreadPoolBuilder("test-peer-pool").set_max_threads(5).Build(&pool_));
  }

  void electionCallback(const ElectionResult& result);

 protected:
  void initUuids(int num_voters);
  void initNoOpPeerProxies();
  void initJointConsensusNoOpPeerProxies(int num_added_voters);
  void initDelayableMockedProxies(bool enableDelay);
  unique_ptr<VoteCounter>
  initVoteCounter(int num_voters, int majority_size, bool do_self_vote = true);

  // Voter 0 is the high-term voter.
  std::shared_ptr<LeaderElection> setUpElectionWithHighTermVoter(
      ConsensusTerm election_term);

  // Predetermine the election results using the specified number of
  // grant / deny / error responses.
  // num_grant must be at least 1, for the candidate to vote for itself.
  // num_grant + num_deny + num_error must add up to an odd number.
  std::shared_ptr<LeaderElection> setUpElectionWithGrantDenyErrorVotes(
      ConsensusTerm election_term,
      int num_grant,
      int num_deny,
      int num_error);

  const string tabletId_;
  string candidateUuid_;
  vector<string> voterUuids_; // exclude the candidate's uuid

  RaftConfigPB config_;
  ProxyMap proxies_;
  unique_ptr<PeerProxyFactory> proxyFactory_;
  unique_ptr<ThreadPool> pool_;

  CountDownLatch latch_;
  unique_ptr<ElectionResult> result_;
};

void LeaderElectionTest::electionCallback(const ElectionResult& result) {
  result_.reset(new ElectionResult(result));
  latch_.countDown();
}

void LeaderElectionTest::initUuids(int num_voters) {
  voterUuids_ = genVoterUuids(num_voters);
  CHECK(!voterUuids_.empty());
  candidateUuid_ = voterUuids_.back();
  voterUuids_.pop_back();
}

void LeaderElectionTest::initNoOpPeerProxies() {
  config_.Clear();
  for (const string& uuid : voterUuids_) {
    RaftPeerPB* peer_pb = config_.add_peers();
    peer_pb->set_permanent_uuid(uuid);
    peer_pb->set_member_type(RaftPeerPB::VOTER);
    PeerProxy* proxy = new NoOpTestPeerProxy(pool_.get(), *peer_pb);
    auto [it, inserted] = proxies_.insert({uuid, proxy});
    CHECK(inserted);
  }
}

void LeaderElectionTest::initJointConsensusNoOpPeerProxies(
    int num_added_voters) {
  CHECK_GT(num_added_voters, 0);
  CHECK_LT(num_added_voters, voterUuids_.size());
  config_.Clear();
  int numCurrentVoters = (int)voterUuids_.size() - num_added_voters;
  // Generate proxies for the current voters (C_old)
  RaftPeerPB* candidatePeerPb = config_.add_peers();
  candidatePeerPb->set_permanent_uuid(candidateUuid_);
  candidatePeerPb->set_member_type(RaftPeerPB::VOTER);
  for (int i = 0; i < numCurrentVoters; ++i) {
    const string& uuid = voterUuids_[i];
    RaftPeerPB* peer_pb = config_.add_peers();
    peer_pb->set_permanent_uuid(uuid);
    peer_pb->set_member_type(RaftPeerPB::VOTER);
    auto proxy = new NoOpTestPeerProxy(pool_.get(), *peer_pb);
    auto [it, inserted] = proxies_.insert({uuid, proxy});
    CHECK(inserted);
  }
  // Generate proxies for the current *and* added voters (C_new)
  for (int i = 0; i < num_added_voters; ++i) {
    const int peerIdx = numCurrentVoters + i;
    const string& uuid = voterUuids_[peerIdx];
    RaftPeerPB* peer_pb = config_.add_next_config_peers();
    peer_pb->set_permanent_uuid(uuid);
    peer_pb->set_member_type(RaftPeerPB::VOTER);
    PeerProxy* proxy = new NoOpTestPeerProxy(pool_.get(), *peer_pb);
    auto [it, inserted] = proxies_.insert({peer_pb->permanent_uuid(), proxy});
    CHECK(inserted);
  }
  for (const RaftPeerPB& peer_pb : config_.peers()) {
    RaftPeerPB* duplicatePeerPb = config_.add_next_config_peers();
    duplicatePeerPb->CopyFrom(peer_pb);
  }
  CHECK_EQ(
      config_.next_config_peers_size(),
      config_.peers_size() + num_added_voters);
}

void LeaderElectionTest::initDelayableMockedProxies(bool enableDelay) {
  config_.Clear();
  for (const string& uuid : voterUuids_) {
    RaftPeerPB* peer_pb = config_.add_peers();
    peer_pb->set_permanent_uuid(uuid);
    peer_pb->set_member_type(RaftPeerPB::VOTER);
    auto proxy = new DelayablePeerProxy<MockedPeerProxy>(
        pool_.get(), new MockedPeerProxy(pool_.get()));
    if (enableDelay) {
      proxy->delayResponse();
    }
    auto [it, inserted] = proxies_.insert({uuid, proxy});
    CHECK(inserted);
  }
}

unique_ptr<VoteCounter> LeaderElectionTest::initVoteCounter(
    int num_voters,
    int majority_size,
    bool do_self_vote) {
  unique_ptr<VoteCounter> counter(new VoteCounter(num_voters, majority_size));
  if (do_self_vote) {
    bool duplicate;
    VoteInfo vote_info;
    vote_info.vote = VOTE_GRANTED;
    CHECK_OK(counter->RegisterVote(candidateUuid_, vote_info, &duplicate));
    CHECK(!duplicate);
  }
  return counter;
}

std::shared_ptr<LeaderElection>
LeaderElectionTest::setUpElectionWithHighTermVoter(
    ConsensusTerm election_term) {
  const int kNumVoters = 3;
  const int kMajoritySize = 2;

  initUuids(kNumVoters);
  initDelayableMockedProxies(true);
  unique_ptr<VoteCounter> counter = initVoteCounter(kNumVoters, kMajoritySize);

  VoteResponsePB response;
  response.set_responder_uuid(voterUuids_[0]);
  response.set_responder_term(election_term + 1);
  response.set_vote_granted(false);
  response.mutable_consensus_error()->set_code(ConsensusErrorPB::INVALID_TERM);
  statusToPb(
      Status::InvalidArgument("Bad term"),
      response.mutable_consensus_error()->mutable_status());
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[0]])
      ->proxy()
      ->setVoteResponse(response);

  response.Clear();
  response.set_responder_uuid(voterUuids_[1]);
  response.set_responder_term(election_term);
  response.set_vote_granted(true);
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[1]])
      ->proxy()
      ->setVoteResponse(response);

  VoteRequestPB request;
  request.set_candidate_uuid(candidateUuid_);
  request.set_candidate_term(election_term);
  request.set_tablet_id(tabletId_);

  std::shared_ptr<LeaderElection> election(new LeaderElection(
      config_,
      proxyFactory_.get(),
      std::move(request),
      std::move(counter),
      MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
      std::bind(
          &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
      std::make_shared<VoteLoggerImplTest>()));
  return election;
}

std::shared_ptr<LeaderElection>
LeaderElectionTest::setUpElectionWithGrantDenyErrorVotes(
    ConsensusTerm election_term,
    int num_grant,
    int num_deny,
    int num_error) {
  const int kNumVoters = num_grant + num_deny + num_error;
  CHECK_GE(num_grant, 1); // Gotta vote for yourself.
  CHECK_EQ(1, kNumVoters % 2); // RaftConfig size must be odd.
  const int kMajoritySize = (kNumVoters / 2) + 1;

  initUuids(kNumVoters);
  initDelayableMockedProxies(false); // Don't delay the vote responses.
  unique_ptr<VoteCounter> counter = initVoteCounter(kNumVoters, kMajoritySize);
  int numGrantFollowers = num_grant - 1;

  // Set up mocked responses based on the params specified in the method
  // arguments.
  int voterIndex = 0;
  while (voterIndex < voterUuids_.size()) {
    VoteResponsePB response;
    if (numGrantFollowers > 0) {
      response.set_responder_uuid(voterUuids_[voterIndex]);
      response.set_responder_term(election_term);
      response.set_vote_granted(true);
      --numGrantFollowers;
    } else if (num_deny > 0) {
      response.set_responder_uuid(voterUuids_[voterIndex]);
      response.set_responder_term(election_term);
      response.set_vote_granted(false);
      response.mutable_consensus_error()->set_code(
          ConsensusErrorPB::LAST_OPID_TOO_OLD);
      statusToPb(
          Status::InvalidArgument("Last OpId"),
          response.mutable_consensus_error()->mutable_status());
      --num_deny;
    } else if (num_error > 0) {
      statusToPb(
          Status::NotFound("Unknown Tablet"),
          response.mutable_error()->mutable_status());
      --num_error;
    } else {
      LOG(FATAL) << "Unexpected fallthrough";
    }

    kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
        proxies_[voterUuids_[voterIndex]])
        ->proxy()
        ->setVoteResponse(response);
    ++voterIndex;
  }

  VoteRequestPB request;
  request.set_candidate_uuid(candidateUuid_);
  request.set_candidate_term(election_term);
  request.set_tablet_id(tabletId_);

  std::shared_ptr<LeaderElection> election(new LeaderElection(
      config_,
      proxyFactory_.get(),
      std::move(request),
      std::move(counter),
      MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
      std::bind(
          &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
      std::make_shared<VoteLoggerImplTest>()));
  return election;
}

// All peers respond "yes", no failures.
TEST_F(LeaderElectionTest, TestPerfectElection) {
  // Try configuration sizes of 1, 3, 5.
  vector<int> configSizes = {1, 3, 5};
  for (int num_voters : configSizes) {
    LOG(INFO) << "Testing election with config size of " << num_voters;
    int majority_size = (num_voters / 2) + 1;
    ConsensusTerm election_term =
        10L + num_voters; // Just to be able to differentiate.

    initUuids(num_voters);
    initNoOpPeerProxies();
    unique_ptr<VoteCounter> counter =
        initVoteCounter(num_voters, majority_size);

    VoteRequestPB request;
    request.set_candidate_uuid(candidateUuid_);
    request.set_candidate_term(election_term);
    request.set_tablet_id(tabletId_);

    std::shared_ptr<LeaderElection> election(new LeaderElection(
        config_,
        proxyFactory_.get(),
        std::move(request),
        std::move(counter),
        MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
        std::bind(
            &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
        std::make_shared<VoteLoggerImplTest>()));
    election->Run();
    latch_.wait();

    ASSERT_EQ(election_term, result_->vote_request.candidate_term());
    ASSERT_EQ(VOTE_GRANTED, result_->decision);

    waitForPool(*pool_);
    proxies_.clear(); // We don't delete them; The election VoterState object
                      // ends up owning them.
    latch_.reset(1);
  }
}

// Test leader election when we encounter a peer with a higher term before we
// have arrived at a majority decision.
TEST_F(LeaderElectionTest, TestHigherTermBeforeDecision) {
  const ConsensusTerm kElectionTerm = 2;
  std::shared_ptr<LeaderElection> election =
      setUpElectionWithHighTermVoter(kElectionTerm);
  election->Run();

  // This guy has a higher term.
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[0]])
      ->respond(TestPeerProxy::kRequestVote);
  latch_.wait();

  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_EQ(kElectionTerm + 1, result_->highest_voter_term);
  LOG(INFO) << "Election lost. Reason: " << result_->message;

  // This guy will vote "yes".
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[1]])
      ->respond(TestPeerProxy::kRequestVote);

  waitForPool(*pool_); // Wait for the election callbacks to finish
                       // before we destroy proxies.
}

// Test leader election when we encounter a peer with a higher term after we
// have arrived at a majority decision of "yes".
TEST_F(LeaderElectionTest, TestHigherTermAfterDecision) {
  const ConsensusTerm kElectionTerm = 2;
  std::shared_ptr<LeaderElection> election =
      setUpElectionWithHighTermVoter(kElectionTerm);
  election->Run();

  // This guy will vote "yes".
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[1]])
      ->respond(TestPeerProxy::kRequestVote);
  latch_.wait();

  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_GRANTED, result_->decision);
  ASSERT_EQ(kElectionTerm, result_->highest_voter_term);
  ASSERT_EQ("achieved majority votes", result_->message);
  LOG(INFO) << "Election won.";

  // This guy has a higher term.
  kudu::down_cast<DelayablePeerProxy<MockedPeerProxy>*>(
      proxies_[voterUuids_[0]])
      ->respond(TestPeerProxy::kRequestVote);

  waitForPool(*pool_); // Wait for the election callbacks to finish
                       // before we destroy proxies.
}

// Out-of-date OpId "vote denied" case.
TEST_F(LeaderElectionTest, TestWithDenyVotes) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumGrant = 2;
  const int kNumDeny = 3;
  const int kNumError = 0;
  std::shared_ptr<LeaderElection> election =
      setUpElectionWithGrantDenyErrorVotes(
          kElectionTerm, kNumGrant, kNumDeny, kNumError);
  LOG(INFO) << "Running";
  election->Run();

  latch_.wait();
  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_EQ(kElectionTerm, result_->highest_voter_term);
  ASSERT_EQ("could not achieve majority", result_->message);
  LOG(INFO) << "Election denied.";

  waitForPool(*pool_); // Wait for the election callbacks to finish
                       // before we destroy proxies.
}

// Count errors as denied votes.
TEST_F(LeaderElectionTest, TestWithErrorVotes) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumGrant = 1;
  const int kNumDeny = 0;
  const int kNumError = 4;
  std::shared_ptr<LeaderElection> election =
      setUpElectionWithGrantDenyErrorVotes(
          kElectionTerm, kNumGrant, kNumDeny, kNumError);
  election->Run();

  latch_.wait();
  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_EQ(0, result_->highest_voter_term); // no valid votes
  ASSERT_EQ("could not achieve majority", result_->message);
  LOG(INFO) << "Election denied.";

  waitForPool(*pool_); // Wait for the election callbacks to finish
                       // before we destroy proxies.
}

// Leader election fails due to failures on PeerProxy creation.
TEST_F(LeaderElectionTest, TestFailToCreateProxy) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumVoters = 3;
  const int kMajoritySize = 2;

  // Initialize the UUIDs and the proxies (which also sets up the config PB).
  initUuids(kNumVoters);
  initNoOpPeerProxies();

  // Remove all the proxies. This will make our peer factory return a bad
  // Status.
  // TODO(modernization): Consider std::vector<std::unique_ptr<PeerProxy>> for
  // automatic cleanup
  for (auto& entry : proxies_) {
    delete entry.second;
  }
  proxies_.clear();

  // Our election should now fail as if the votes were denied.
  VoteRequestPB request;
  request.set_candidate_uuid(candidateUuid_);
  request.set_candidate_term(kElectionTerm);
  request.set_tablet_id(tabletId_);

  unique_ptr<VoteCounter> counter = initVoteCounter(kNumVoters, kMajoritySize);
  std::shared_ptr<LeaderElection> election(new LeaderElection(
      config_,
      proxyFactory_.get(),
      std::move(request),
      std::move(counter),
      MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
      std::bind(
          &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
      std::make_shared<VoteLoggerImplTest>()));
  election->Run();
  latch_.wait();
  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_EQ(0, result_->highest_voter_term); // no votes
  ASSERT_EQ("could not achieve majority", result_->message);
}

// All peers in the old and new config respond "yes", no failures.
TEST_F(LeaderElectionTest, TestJointConsensusPerfectElection) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumCurrVoters = 3;
  const int kNumAddedVoters = 2; // making new config having 5 peers
  const int kOldMajoritySize = 2; // majority of 3 peers
  const int kNewMajoritySize = 3; // majority of 5 peers

  // Initialize UUIDs and transitional config (C_old_new), having 3 peers in the
  // old config and 5 peers in the new config (additional 2 peers).
  //  C_old = {peer-0, peer-1, *peer-4}, peer-4 is the candidate.
  //  C_new = {peer-0, peer-1, peer-2, peer-3, *peer-4}.
  initUuids(kNumCurrVoters + kNumAddedVoters);
  EXPECT_EQ(candidateUuid_, "peer-4");
  initJointConsensusNoOpPeerProxies(/*num_added_voters=*/kNumAddedVoters);
  ASSERT_EQ(kNumCurrVoters, config_.peers_size());
  ASSERT_EQ(kNumCurrVoters + kNumAddedVoters, config_.next_config_peers_size());

  // Prepare the election request from the candidate.
  VoteRequestPB request;
  request.set_candidate_uuid(candidateUuid_);
  request.set_candidate_term(kElectionTerm);
  request.set_tablet_id(tabletId_);

  // Prepare vanilla vote counter for the old and new config.
  // They share some of the peer proxies.
  unique_ptr<VoteCounter> oldConfCounter =
      initVoteCounter(kNumCurrVoters, kOldMajoritySize, /*do_self_vote=*/false);
  unique_ptr<VoteCounter> newConfCounter = initVoteCounter(
      kNumCurrVoters + kNumAddedVoters,
      kNewMajoritySize,
      /*do_self_vote=*/false);

  // Prepare the joint-consensus vote counter.
  unique_ptr<JointConsensusVoteCounter> jointCounter;
  jointCounter.reset(new JointConsensusVoteCounter(
      config_, std::move(oldConfCounter), std::move(newConfCounter)));

  // Self vote for the candidate.
  bool isCandidateDuplicate = false;
  auto status = jointCounter->RegisterVote(
      candidateUuid_, {VOTE_GRANTED}, &isCandidateDuplicate);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_FALSE(isCandidateDuplicate);

  // Initialize and run the leader election process.
  std::shared_ptr<LeaderElection> election(new LeaderElection(
      config_,
      proxyFactory_.get(),
      std::move(request),
      std::move(jointCounter),
      MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
      std::bind(
          &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
      std::make_shared<VoteLoggerImplTest>()));
  election->Run();
  latch_.wait();
  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_GRANTED, result_->decision);

  waitForPool(*pool_);
  proxies_.clear(); // We don't delete them; The election VoterState object
                    // ends up owning them.
  latch_.reset(1);
}

// The case where we gat a majority of votes in the old config, but not in the
// new config, causing the candidate to loss the election.
TEST_F(LeaderElectionTest, TestJointConsensusElectionLoss) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumCurrVoters = 3;
  const int kNumAddedVoters = 2; // making new config having 5 peers
  const int kOldMajoritySize = 2; // majority of 3 peers
  const int kNewMajoritySize = 3; // majority of 5 peers

  // The config change scenario is below.
  //  C_old = {peer-0, peer-1, *peer-4}, peer-4 is the candidate.
  //  C_new = {peer-0, peer-1, peer-2, peer-3, *peer-4}.
  //
  // We emulate peer-0, peer-2, and peer-3 to vote "no", making us achieve
  // majority in the old config {peer-1, peer-4}, but not in the new config.
  // We emulate "no" votes by removing proxies for those peers. That is possible
  // because RPC error is treated as "no" vote.
  initUuids(kNumCurrVoters + kNumAddedVoters);
  EXPECT_EQ(candidateUuid_, "peer-4");
  initJointConsensusNoOpPeerProxies(/*num_added_voters=*/kNumAddedVoters);
  delete EraseKeyReturnValuePtr(&proxies_, "peer-0");
  delete EraseKeyReturnValuePtr(&proxies_, "peer-2");
  delete EraseKeyReturnValuePtr(&proxies_, "peer-3");
  EXPECT_EQ(proxies_.size(), kOldMajoritySize - 1);

  // Prepare the election request from the candidate.
  VoteRequestPB request;
  request.set_candidate_uuid(candidateUuid_);
  request.set_candidate_term(kElectionTerm);
  request.set_tablet_id(tabletId_);

  // Prepare vanilla vote counter for the old and new config.
  unique_ptr<VoteCounter> oldConfCounter =
      initVoteCounter(kNumCurrVoters, kOldMajoritySize, /*do_self_vote=*/false);
  unique_ptr<VoteCounter> newConfCounter = initVoteCounter(
      kNumCurrVoters + kNumAddedVoters,
      kNewMajoritySize,
      /*do_self_vote=*/false);

  // Prepare the joint-consensus vote counter.
  unique_ptr<JointConsensusVoteCounter> jointCounter;
  jointCounter.reset(new JointConsensusVoteCounter(
      config_, std::move(oldConfCounter), std::move(newConfCounter)));

  // Self vote for the candidate.
  bool isCandidateDuplicate = false;
  auto status = jointCounter->RegisterVote(
      candidateUuid_, {VOTE_GRANTED}, &isCandidateDuplicate);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_FALSE(isCandidateDuplicate);

  // Initialize and run the leader election process.
  std::shared_ptr<LeaderElection> election(new LeaderElection(
      config_,
      proxyFactory_.get(),
      std::move(request),
      std::move(jointCounter),
      MonoDelta::FromSeconds(kLeaderElectionTimeoutSecs),
      std::bind(
          &LeaderElectionTest::electionCallback, this, std::placeholders::_1),
      std::make_shared<VoteLoggerImplTest>()));
  election->Run();
  latch_.wait();
  ASSERT_EQ(kElectionTerm, result_->vote_request.candidate_term());
  ASSERT_EQ(VOTE_DENIED, result_->decision); // assert we have election loss

  waitForPool(*pool_);
  proxies_.clear();
  latch_.reset(1);
}

////////////////////////////////////////
// VoteCounterTest
////////////////////////////////////////

class VoteCounterTest : public KuduTest {
 protected:
  static void assertUndecided(const VoteCounter& counter);
  static void
  assertVoteCount(const VoteCounter& counter, int yes_votes, int no_votes);
};

void VoteCounterTest::assertUndecided(const VoteCounter& counter) {
  ElectionDecisionState decision_state = counter.GetDecision();
  ASSERT_FALSE(decision_state.decided());
}

void VoteCounterTest::assertVoteCount(
    const VoteCounter& counter,
    int yes_votes,
    int no_votes) {
  ASSERT_EQ(yes_votes, counter.yes_votes_);
  ASSERT_EQ(no_votes, counter.no_votes_);
  ASSERT_EQ(yes_votes + no_votes, counter.GetTotalVotesCounted());
}

// Test basic vote counting functionality with an early majority.
TEST_F(VoteCounterTest, TestVoteCounter_EarlyDecision) {
  const int kNumVoters = 3;
  const int kMajoritySize = 2;
  vector<string> voter_uuids = genVoterUuids(kNumVoters);

  // "Yes" decision.
  {
    // Start off undecided.
    VoteCounter counter(kNumVoters, kMajoritySize);
    ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // First yes vote.
    bool duplicate;
    VoteInfo vote_info;
    vote_info.vote = VOTE_GRANTED;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 1, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second yes vote wins it in a configuration of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ElectionDecisionState decisionState = counter.GetDecision();
    ASSERT_TRUE(decisionState.decided());
    ASSERT_EQ(decisionState.decision, ElectionDecision::WON);
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 2, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }

  // "No" decision.
  {
    // Start off undecided.
    VoteCounter counter(kNumVoters, kMajoritySize);
    ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // First no vote.
    bool duplicate;
    VoteInfo vote_info;
    vote_info.vote = VOTE_DENIED;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 0, 1));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second no vote loses it in a configuration of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ElectionDecisionState decisionState = counter.GetDecision();
    ASSERT_TRUE(decisionState.decided());
    ASSERT_EQ(decisionState.decision, ElectionDecision::LOST);
    ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 0, 2));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }
}

// Test basic vote counting functionality with the last vote being the deciding
// vote.
TEST_F(VoteCounterTest, TestVoteCounter_LateDecision) {
  const int kNumVoters = 5;
  const int kMajoritySize = 3;
  vector<string> voter_uuids = genVoterUuids(kNumVoters);

  // Start off undecided.
  VoteCounter counter(kNumVoters, kMajoritySize);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 0, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Add single yes vote, still undecided.
  bool duplicate;
  VoteInfo vote_info;
  vote_info.vote = VOTE_GRANTED;
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt duplicate vote.
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
  ASSERT_TRUE(duplicate);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt to change vote.
  vote_info.vote = VOTE_DENIED;
  Status s = counter.RegisterVote(voter_uuids[0], vote_info, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "voted a different way twice");
  LOG(INFO) << "Expected vote-changed error: " << s.ToString();
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Add more votes...
  ASSERT_OK(counter.RegisterVote(voter_uuids[1], vote_info, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 1, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  vote_info.vote = VOTE_GRANTED;
  ASSERT_OK(counter.RegisterVote(voter_uuids[2], vote_info, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 2, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  vote_info.vote = VOTE_DENIED;
  ASSERT_OK(counter.RegisterVote(voter_uuids[3], vote_info, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(assertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 2, 2));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Win the election.
  vote_info.vote = VOTE_GRANTED;
  ASSERT_OK(counter.RegisterVote(voter_uuids[4], vote_info, &duplicate));
  ASSERT_FALSE(duplicate);
  ElectionDecisionState decisionState = counter.GetDecision();
  ASSERT_TRUE(decisionState.decided());
  ASSERT_EQ(decisionState.decision, ElectionDecision::WON);
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());

  // Attempt to vote with > the whole configuration.
  s = counter.RegisterVote("some-random-node", vote_info, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(
      s.ToString(), "cause the number of votes to exceed the expected number");
  LOG(INFO) << "Expected voters-exceeded error: " << s.ToString();
  ASSERT_TRUE(counter.GetDecision().decided());
  ASSERT_NO_FATAL_FAILURE(assertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());
}

// Test vote counting with an even number of voters.
TEST_F(VoteCounterTest, TestVoteCounter_EvenVoters) {
  const int kNumVoters = 2;
  const int kMajoritySize = 2;
  vector<string> voter_uuids = genVoterUuids(kNumVoters);

  // "Yes" decision.
  {
    VoteCounter counter(kNumVoters, kMajoritySize);
    NO_FATALS(assertUndecided(counter));
    NO_FATALS(assertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Initial yes vote.
    bool duplicate;
    VoteInfo vote_info;
    vote_info.vote = VOTE_GRANTED;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    NO_FATALS(assertUndecided(counter));
    NO_FATALS(assertVoteCount(counter, 1, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second yes vote wins it.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ElectionDecisionState decisionState = counter.GetDecision();
    ASSERT_TRUE(decisionState.decided());
    ASSERT_EQ(decisionState.decision, ElectionDecision::WON);
    NO_FATALS(assertVoteCount(counter, 2, 0));
    ASSERT_TRUE(counter.AreAllVotesIn());
  }

  // "No" decision.
  {
    VoteCounter counter(kNumVoters, kMajoritySize);
    NO_FATALS(assertUndecided(counter));
    NO_FATALS(assertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // The first "no" vote guarantees a failed election when num voters == 2.
    bool duplicate;
    VoteInfo vote_info;
    vote_info.vote = VOTE_DENIED;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], vote_info, &duplicate));
    ASSERT_FALSE(duplicate);
    ElectionDecisionState decisionState = counter.GetDecision();
    ASSERT_TRUE(decisionState.decided());
    ASSERT_EQ(decisionState.decision, ElectionDecision::LOST);
    NO_FATALS(assertVoteCount(counter, 0, 1));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }
}

} // namespace consensus
} // namespace kudu
