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

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
// #include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
// #include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/rpc/messenger.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
// METRIC_DEFINE_entity(tablet);
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace consensus {

using log::Log;
using log::LogOptions;
using rpc::Messenger;
using rpc::MessengerBuilder;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using testing::StrictMock;

const char* kTabletId = "test-peers-tablet";
const char* kLeaderUuid = "peer-0";
const char* kFollowerUuid = "peer-1";

class ConsensusPeersTest : public KuduTest {
 public:
  ConsensusPeersTest()
      : metric_entity_(
            METRIC_ENTITY_server.Instantiate(&metric_registry_, "peer-test")) {
    CHECK_OK(ThreadPoolBuilder("test-raft-pool").Build(&raft_pool_));
    raft_pool_token_ =
        raft_pool_->NewToken(ThreadPool::ExecutionMode::Concurrent);
  }

  virtual void SetUp() override {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    log_ = std::make_shared<StrictMock<StatefulMockLog>>(
        log::LogOptions(), fs_manager_.get(), "", kTabletId, nullptr);

    RaftConfigPB raftConfig;
    raftConfig.add_peers()->mutable_permanent_uuid()->assign(kLeaderUuid);
    raftConfig.add_peers()->mutable_permanent_uuid()->assign(kFollowerUuid);
    ASSERT_OK(
        DurableRoutingTable::create(
            fs_manager_.get(), kTabletId, raftConfig, {}, &routing_table_));
    clock_.reset(new clock::HybridClock());
    ASSERT_OK(clock_->init());

    routing_table_container_ = std::make_shared<RoutingTableContainer>(
        ProxyPolicy::DURABLE_ROUTING_POLICY,
        FakeRaftPeerPB(kLeaderUuid),
        raftConfig,
        routing_table_,
        std::vector<std::unordered_set<std::string>>());

    std::shared_ptr<TimeManager> timeManager =
        std::make_shared<TimeManager>(clock_, Timestamp::kMin);

    persistent_vars_manager_ =
        std::make_shared<PersistentVarsManager>(fs_manager_.get());
    ASSERT_OK(persistent_vars_manager_->createPersistentVars(kTabletId));

    message_queue_.reset(new PeerMessageQueue(
        metric_entity_,
        log_,
        timeManager,
        persistent_vars_manager_,
        FakeRaftPeerPB(kLeaderUuid),
        routing_table_container_,
        kTabletId,
        raft_pool_->NewToken(ThreadPool::ExecutionMode::Serial),
        MinimumOpId(),
        MinimumOpId()));

    MessengerBuilder bld("test");
    ASSERT_OK(bld.Build(&messenger_));
  }

  virtual void TearDown() override {
    messenger_->Shutdown();
    if (raft_pool_) {
      // Make sure to drain any tasks from the pool we're using for our
      // delayable proxy before destructing the queue.
      waitForPool(*raft_pool_);
    }
  }

  DelayablePeerProxy<NoOpTestPeerProxy>* newRemotePeer(
      const string& peerName,
      shared_ptr<Peer>* peer) {
    RaftPeerPB peerPb;
    peerPb.set_permanent_uuid(peerName);
    peerPb.set_member_type(RaftPeerPB::VOTER);
    auto proxyPtr = new DelayablePeerProxy<NoOpTestPeerProxy>(
        raft_pool_.get(), new NoOpTestPeerProxy(raft_pool_.get(), peerPb));
    shared_ptr<PeerProxy> proxy(proxyPtr);
    peer_proxy_pool_.Put(peerName, proxy);
    CHECK_OK(
        Peer::NewRemotePeer(
            std::move(peerPb),
            kTabletId,
            kLeaderUuid,
            message_queue_.get(),
            &peer_proxy_pool_,
            raft_pool_token_.get(),
            std::move(proxy),
            messenger_,
            peer));
    return proxyPtr;
  }

  void checkLastRemoteEntry(
      DelayablePeerProxy<NoOpTestPeerProxy>* proxy,
      int term,
      int index) {
    OpId id;
    id.CopyFrom(proxy->proxy()->last_received());
    ASSERT_EQ(id.term(), term);
    ASSERT_EQ(id.index(), index);
  }

  // Registers a callback triggered when the op with the provided term and index
  // is committed in the test consensus impl.
  // This must be called _before_ the operation is committed.
  void waitForCommitIndex(int index) {
    ASSERT_EVENTUALLY(
        [&]() { ASSERT_GE(message_queue_->GetCommittedIndex(), index); });
  }

 protected:
  MetricRegistry metric_registry_;
  std::shared_ptr<MetricEntity> metric_entity_;
  unique_ptr<FsManager> fs_manager_;
  std::shared_ptr<Log> log_;
  std::shared_ptr<PersistentVarsManager> persistent_vars_manager_;
  shared_ptr<DurableRoutingTable> routing_table_;
  shared_ptr<RoutingTableContainer> routing_table_container_;
  unique_ptr<ThreadPool> raft_pool_;
  unique_ptr<PeerMessageQueue> message_queue_;
  LogOptions options_;
  unique_ptr<ThreadPoolToken> raft_pool_token_;
  std::shared_ptr<clock::Clock> clock_;
  shared_ptr<Messenger> messenger_;
  PeerProxyPool peer_proxy_pool_;
};

// Tests that a remote peer is correctly built and tracked
// by the message queue.
// After the operations are considered done the proxy (which
// simulates the other endpoint) should reflect the replicated
// messages.
TEST_F(ConsensusPeersTest, TestRemotePeer) {
  // We use a majority size of 2 since we make one fake remote peer
  // in addition to our real local log.
  message_queue_->SetLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));

  shared_ptr<Peer> remotePeer;
  DelayablePeerProxy<NoOpTestPeerProxy>* proxy =
      newRemotePeer(kFollowerUuid, &remotePeer);

  // Append a bunch of messages to the queue
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 20);

  // signal the peer there are requests pending.
  ASSERT_OK(remotePeer->SignalRequest());
  // now wait on the status of the last operation
  // this will complete once the peer has logged all
  // requests.
  NO_FATALS(waitForCommitIndex(20));
  // verify that the replicated watermark corresponds to the last replicated
  // message.
  NO_FATALS(checkLastRemoteEntry(proxy, 2, 20));
}

TEST_F(ConsensusPeersTest, TestRemotePeers) {
  RaftConfigPB raftConfig;
  raftConfig.add_peers()->mutable_permanent_uuid()->assign(kLeaderUuid);
  raftConfig.add_peers()->mutable_permanent_uuid()->assign("peer-1");
  raftConfig.add_peers()->mutable_permanent_uuid()->assign("peer-2");
  ASSERT_OK(routing_table_->updateRaftConfig(raftConfig));

  message_queue_->SetLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));

  // Create a set of remote peers
  shared_ptr<Peer> remotePeer1;
  DelayablePeerProxy<NoOpTestPeerProxy>* remotePeer1Proxy =
      newRemotePeer("peer-1", &remotePeer1);

  shared_ptr<Peer> remotePeer2;
  DelayablePeerProxy<NoOpTestPeerProxy>* remotePeer2Proxy =
      newRemotePeer("peer-2", &remotePeer2);

  // Delay the response from the second remote peer.
  remotePeer2Proxy->DelayResponse();

  // Append one message to the queue.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);

  OpId first = MakeOpId(0, 1);

  remotePeer1->SignalRequest();
  remotePeer2->SignalRequest();

  // Now wait for the message to be replicated, this should succeed since
  // majority = 2 and only one peer was delayed. The majority is made up
  // of remote-peer1 and the local log.
  waitForCommitIndex(first.index());

  ASSERT_OPID_EQ(first, message_queue_->GetLastOpIdInLog());
  checkLastRemoteEntry(remotePeer1Proxy, first.term(), first.index());

  remotePeer2Proxy->Respond(TestPeerProxy::kUpdate);
  // Wait until all peers have replicated the message, otherwise
  // when we add the next one remote_peer2 might find the next message
  // in the queue and will replicate it, which is not what we want.
  while (message_queue_->GetAllReplicatedIndex() != first.index()) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  // Now append another message to the queue
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 2, 1);

  // We should not see it committed, even after 10ms,
  // since only the local peer replicates the message.
  SleepFor(MonoDelta::FromMilliseconds(10));
  ASSERT_LT(message_queue_->GetCommittedIndex(), 2);

  // Signal one of the two remote peers.
  remotePeer1->SignalRequest();
  // We should now be able to wait for it to replicate, since two peers (a
  // majority) have replicated the message.
  waitForCommitIndex(2);
}

// Regression test for KUDU-699: even if a peer isn't making progress,
// and thus always has data pending, we should be able to close the peer.
TEST_F(ConsensusPeersTest, TestCloseWhenRemotePeerDoesntMakeProgress) {
  message_queue_->SetLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));

  auto mockProxy = make_shared<MockedPeerProxy>(raft_pool_.get());
  peer_proxy_pool_.Put(kFollowerUuid, mockProxy);
  shared_ptr<Peer> peer;
  ASSERT_OK(
      Peer::NewRemotePeer(
          FakeRaftPeerPB(kFollowerUuid),
          kTabletId,
          kLeaderUuid,
          message_queue_.get(),
          &peer_proxy_pool_,
          raft_pool_token_.get(),
          mockProxy,
          messenger_,
          &peer));

  // Make the peer respond without making any progress -- it always returns
  // that it has only replicated op 0.0. When we see the response, we always
  // decide that more data is pending, and we want to send another request.
  ConsensusResponsePB peerResp;
  peerResp.set_responder_uuid(kFollowerUuid);
  peerResp.set_responder_term(0);
  peerResp.mutable_status()->mutable_last_received()->CopyFrom(MakeOpId(0, 0));
  peerResp.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(0, 0));
  peerResp.mutable_status()->set_last_committed_idx(0);

  mockProxy->set_update_response(peerResp);

  // Add an op to the queue and start sending requests to the peer.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  peer->SignalRequest(true);

  // We should be able to close the peer even though it has more data pending.
  peer->Close();
}

TEST_F(ConsensusPeersTest, TestDontSendOneRpcPerWriteWhenPeerIsDown) {
  message_queue_->SetLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));

  auto mockProxy = make_shared<MockedPeerProxy>(raft_pool_.get());
  peer_proxy_pool_.Put(kFollowerUuid, mockProxy);
  shared_ptr<Peer> peer;
  ASSERT_OK(
      Peer::NewRemotePeer(
          FakeRaftPeerPB(kFollowerUuid),
          kTabletId,
          kLeaderUuid,
          message_queue_.get(),
          &peer_proxy_pool_,
          raft_pool_token_.get(),
          mockProxy,
          messenger_,
          &peer));

  // Initial response has to be successful -- otherwise we'll consider the peer
  // "new" and only send heartbeat RPCs.
  ConsensusResponsePB initialResp;
  initialResp.set_responder_uuid(kFollowerUuid);
  initialResp.set_responder_term(0);
  initialResp.mutable_status()->mutable_last_received()->CopyFrom(
      MakeOpId(1, 1));
  initialResp.mutable_status()
      ->mutable_last_received_current_leader()
      ->CopyFrom(MakeOpId(1, 1));
  // We have to set the last_committed_index to 1 to avoid a tight loop
  // where the peer manager keeps trying to update the peer's committed
  // index.
  initialResp.mutable_status()->set_last_committed_idx(1);
  mockProxy->set_update_response(initialResp);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_, 1, 1);
  peer->SignalRequest(true);

  // Now wait for the message to be replicated, this should succeed since
  // the local (leader) peer always acks and the follower also acked this time.
  waitForCommitIndex(1);

  // Set up the peer to respond with an error.
  ConsensusResponsePB errorResp;
  errorResp.mutable_error()->set_code(ServerErrorPB::UNKNOWN_ERROR);
  statusToPb(
      Status::NotFound("fake error"),
      errorResp.mutable_error()->mutable_status());
  mockProxy->set_update_response(errorResp);

  // Add a bunch of messages to the queue.
  for (int i = 2; i <= 100; i++) {
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_, i, 1);
    peer->SignalRequest(false);
    SleepFor(MonoDelta::FromMilliseconds(2));
  }

  // Check that we didn't attempt to send one UpdateConsensus call per
  // Write. 100 writes might have taken a second or two, though, so it's
  // OK to have called UpdateConsensus() a few times due to regularly
  // scheduled heartbeats.
  ASSERT_LT(mockProxy->update_count(), 5);
}

} // namespace consensus
} // namespace kudu
