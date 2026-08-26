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
#include <cstdint>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/async_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool-test-util.h" // @manual - duplicate header owner

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(follower_unavailable_considered_failed_sec);

using kudu::consensus::HealthReportPB;
using std::atomic;
using std::deque;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using testing::StrictMock;

namespace kudu {
namespace consensus {

static const char* kLeaderUuid = "peer-0";
static const char* kPeerUuid = "peer-1";
static const char* kTestTablet = "test-tablet";
static const char* kLeaderQuorumId = "r0";

class ConsensusQueueTest : public KuduTest {
 public:
  ConsensusQueueTest()
      : metricEntity_(
            METRIC_ENTITY_server.instantiate(&metricRegistry_, "queue-test")),
        registry_(new log::LogAnchorRegistry) {}

  virtual void SetUp() override {
    KuduTest::SetUp();
    fsManager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    ASSERT_OK(fsManager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fsManager_->Open());

    log_ = std::make_shared<StrictMock<StatefulMockLog>>(
        log::LogOptions(), fsManager_.get(), "", kTestTablet, nullptr);

    RaftConfigPB raftConfig;
    raftConfig.add_peers()->mutable_permanent_uuid()->assign(kLeaderUuid);
    raftConfig.add_peers()->mutable_permanent_uuid()->assign(kPeerUuid);
    ASSERT_OK(
        DurableRoutingTable::create(
            fsManager_.get(), kTestTablet, raftConfig, {}, &routingTable_));

    persistentVarsManager_ =
        std::make_shared<PersistentVarsManager>(fsManager_.get());
    ASSERT_OK(persistentVarsManager_->createPersistentVars(kTestTablet));

    routingTableContainer_ = std::make_shared<RoutingTableContainer>(
        ProxyPolicy::DurableRoutingPolicy,
        fakeRaftPeerPb(kLeaderUuid),
        raftConfig,
        routingTable_,
        std::vector<std::unordered_set<std::string>>());

    clock_.reset(new clock::HybridClock());
    ASSERT_OK(clock_->init());

    ASSERT_OK(ThreadPoolBuilder("raft").build(&raftPool_));
    testLocalPeerPb_ = fakeRaftPeerPb(kLeaderUuid);
    closeAndReopenQueue(MinimumOpId(), MinimumOpId());
  }

  void closeAndReopenQueue(
      const OpId& replicatedOpId,
      const OpId& committedOpId) {
    std::shared_ptr<clock::Clock> clock =
        std::make_shared<clock::HybridClock>();
    ASSERT_OK(clock->init());
    std::shared_ptr<TimeManager> timeManager =
        std::make_shared<TimeManager>(clock, Timestamp::kMin);

    queue_.reset(new PeerMessageQueue(
        metricEntity_,
        log_,
        timeManager,
        persistentVarsManager_,
        testLocalPeerPb_,
        routingTableContainer_,
        kTestTablet,
        raftPool_->NewToken(ThreadPool::ExecutionMode::Serial),
        replicatedOpId,
        committedOpId));
  }

  virtual void TearDown() override {
    queue_->Close();
  }

  Status appendReplicateMsg(int term, int index, int payloadSize) {
    return queue_->appendOperation(makeScopedRefptrReplicate(
        createDummyReplicate(term, index, clock_->now(), payloadSize),
        Source::Memory));
  }

  RaftPeerPB makePeer(
      const std::string& peerUuid,
      RaftPeerPB::MemberType memberType) {
    RaftPeerPB peerPb;
    *peerPb.mutable_permanent_uuid() = peerUuid;
    peerPb.set_member_type(memberType);
    return peerPb;
  }

  // Updates the peer's watermark in the queue so that it matches
  // the operation we want, since the queue always assumes that
  // when a peer gets tracked it's always tracked starting at the
  // last operation in the queue
  void updatePeerWatermarkToOp(
      ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      const OpId& lastReceived,
      const OpId& lastReceivedCurrentLeader,
      int lastCommittedIdx,
      bool* sendMoreImmediately) {
    queue_->trackPeer(makePeer(kPeerUuid, RaftPeerPB::VOTER));
    response->set_responder_uuid(kPeerUuid);

    // Ask for a request. The queue assumes the peer is up-to-date so
    // this should contain no operations.
    vector<ReplicateRefPtr> refs;
    bool needsTabletCopy;
    std::string nextHopUuid;
    ASSERT_OK(queue_->RequestForPeer(
        kPeerUuid,
        /*read_ops=*/true,
        request,
        &refs,
        &needsTabletCopy,
        &nextHopUuid));
    ASSERT_FALSE(needsTabletCopy);
    ASSERT_EQ(request->ops_size(), 0);

    // Refuse saying that the log matching property check failed and
    // that our last operation is actually 'lastReceived'.
    refuseWithLogPropertyMismatch(
        response, lastReceived, lastReceivedCurrentLeader);
    response->mutable_status()->set_last_committed_idx(lastCommittedIdx);
    *sendMoreImmediately =
        queue_->ResponseFromPeer(response->responder_uuid(), *response);
    request->Clear();
    response->mutable_status()->Clear();
  }

  // Like the above but uses the last received index as the commtited index.
  void updatePeerWatermarkToOp(
      ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      const OpId& lastReceived,
      const OpId& lastReceivedCurrentLeader,
      bool* sendMoreImmediately) {
    return updatePeerWatermarkToOp(
        request,
        response,
        lastReceived,
        lastReceivedCurrentLeader,
        lastReceived.index(),
        sendMoreImmediately);
  }

  void refuseWithLogPropertyMismatch(
      ConsensusResponsePB* response,
      const OpId& lastReceived,
      const OpId& lastReceivedCurrentLeader) {
    ConsensusStatusPB* status = response->mutable_status();
    status->mutable_last_received()->CopyFrom(lastReceived);
    status->mutable_last_received_current_leader()->CopyFrom(
        lastReceivedCurrentLeader);
    ConsensusErrorPB* error = status->mutable_error();
    error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
    statusToPb(Status::IllegalState("LMP failed."), error->mutable_status());
  }

  void waitForLocalPeerToAckIndex(int index) {
    while (true) {
      const auto leader = queue_->getTrackedPeerForTests(kLeaderUuid);
      if (leader.lastReceived.index() >= index) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
  }

  // Sets the last received op on the response, as well as the last committed
  // index.
  void setLastReceivedAndLastCommitted(
      ConsensusResponsePB* response,
      const OpId& lastReceived,
      const OpId& lastReceivedCurrentLeader,
      int lastCommittedIdx) {
    *response->mutable_status()->mutable_last_received() = lastReceived;
    *response->mutable_status()->mutable_last_received_current_leader() =
        lastReceivedCurrentLeader;
    response->mutable_status()->set_last_committed_idx(lastCommittedIdx);
  }

  // Like the above but uses the same lastReceived for current term.
  void setLastReceivedAndLastCommitted(
      ConsensusResponsePB* response,
      const OpId& lastReceived,
      int lastCommittedIdx) {
    setLastReceivedAndLastCommitted(
        response, lastReceived, lastReceived, lastCommittedIdx);
  }

  // Like the above but just sets the last committed index to have the same
  // index as the last received op.
  void setLastReceivedAndLastCommitted(
      ConsensusResponsePB* response,
      const OpId& lastReceived) {
    setLastReceivedAndLastCommitted(
        response, lastReceived, lastReceived.index());
  }

 protected:
  // Identity the queue is opened with. Assign before closeAndReopenQueue() to
  // give the local peer attributes, which flexi-raft quorum membership needs.
  RaftPeerPB testLocalPeerPb_;
  unique_ptr<FsManager> fsManager_;
  MetricRegistry metricRegistry_;
  std::shared_ptr<MetricEntity> metricEntity_;
  std::shared_ptr<log::Log> log_;
  unique_ptr<ThreadPool> raftPool_;
  unique_ptr<TimeManager> timeManager_;
  shared_ptr<DurableRoutingTable> routingTable_;
  std::shared_ptr<PersistentVarsManager> persistentVarsManager_;
  shared_ptr<RoutingTableContainer> routingTableContainer_;
  unique_ptr<PeerMessageQueue> queue_;
  std::shared_ptr<log::LogAnchorRegistry> registry_;
  std::shared_ptr<clock::Clock> clock_;
};

// Tests that the queue is able to track a peer when it starts tracking a peer
// after the initial message in the queue. In particular this creates a queue
// with several messages and then starts to track a peer whose watermark
// falls in the middle of the current messages in the queue.
TEST_F(ConsensusQueueTest, TestStartTrackingAfterStart) {
  queue_->setLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, buildRaftConfigPbForTests(2));
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 100);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool sendMoreImmediately = false;

  // Peer already has some messages, last one being 7.50
  OpId lastReceived = MakeOpId(7, 50);
  OpId lastReceivedCurrentLeader = MinimumOpId();

  updatePeerWatermarkToOp(
      &request,
      &response,
      lastReceived,
      lastReceivedCurrentLeader,
      &sendMoreImmediately);
  ASSERT_TRUE(sendMoreImmediately);

  // Getting a new request should get all operations after 7.50
  vector<ReplicateRefPtr> refs;
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(50, request.ops_size());

  setLastReceivedAndLastCommitted(&response, request.ops(49).id());
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(sendMoreImmediately) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops_size(), nullptr);
}

// Tests that the peers gets the messages pages, with the size of a page
// being 'consensus_max_batch_size_bytes'
//
// FIXME(mpercy): This is another test that fails (we have another in
// log_cache-test) due to a hacky perf optimization in LogCache where we expect
// the request PB to be a certain type instead of doing reflection, which means
// the NoopRequestPB shows up as zero length.
TEST_F(ConsensusQueueTest, DISABLED_TestGetPagedMessages) {
  queue_->setLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, buildRaftConfigPbForTests(2));

  // helper to estimate request size so that we can set the max batch size
  // appropriately Note: This estimator must be precise, as it is used to set
  // the max batch size. In order for the estimate to be correct, all members of
  // the request protobuf must be set. If not all fields are set, this will set
  // the batch size to be too small to hold the expected number of ops.
  ConsensusRequestPB pageSizeEstimator;
  pageSizeEstimator.set_caller_term(14);
  pageSizeEstimator.set_committed_index(0);
  pageSizeEstimator.set_all_replicated_index(0);
  pageSizeEstimator.set_last_idx_appended_to_leader(0);
  pageSizeEstimator.mutable_preceding_id()->CopyFrom(MinimumOpId());

  // We're going to add 100 messages to the queue so we make each page fetch 9
  // of those, for a total of 12 pages. The last page should have a single op.
  const int kOpsPerRequest = 9;
  for (int i = 0; i < kOpsPerRequest; i++) {
    pageSizeEstimator.mutable_ops()->AddAllocated(
        createDummyReplicate(0, 0, clock_->now(), 0).release());
  }

  // Save the current flag state.
  gflags::FlagSaver saver;
  FLAGS_consensus_max_batch_size_bytes = pageSizeEstimator.ByteSize();

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool sendMoreImmediately = false;

  updatePeerWatermarkToOp(
      &request, &response, MinimumOpId(), MinimumOpId(), &sendMoreImmediately);
  ASSERT_TRUE(sendMoreImmediately);

  // Append the messages after the queue is tracked. Otherwise the ops might
  // get evicted from the cache immediately and the requests below would
  // result in async log reads instead of cache hits.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 100);

  OpId last;
  for (int i = 0; i < 11; i++) {
    VLOG(1) << "Making request " << i;
    vector<ReplicateRefPtr> refs;
    bool needsTabletCopy;
    std::string nextHopUuid;
    ASSERT_OK(queue_->RequestForPeer(
        kPeerUuid,
        /*read_ops=*/true,
        &request,
        &refs,
        &needsTabletCopy,
        &nextHopUuid));
    ASSERT_FALSE(needsTabletCopy);
    ASSERT_EQ(kOpsPerRequest, request.ops_size());
    last = request.ops(request.ops_size() - 1).id();
    setLastReceivedAndLastCommitted(&response, last);
    VLOG(1) << "Faking received up through " << last;
    sendMoreImmediately =
        queue_->ResponseFromPeer(response.responder_uuid(), response);
    ASSERT_TRUE(sendMoreImmediately);
  }
  vector<ReplicateRefPtr> refs;
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(1, request.ops_size());
  last = request.ops(request.ops_size() - 1).id();
  setLastReceivedAndLastCommitted(&response, last);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(sendMoreImmediately);

  // extract the ops from the request to avoid double free
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops_size(), nullptr);
}

TEST_F(ConsensusQueueTest, TestPeersDontAckBeyondWatermarks) {
  queue_->setLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, buildRaftConfigPbForTests(3));
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 100);

  // Wait for the local peer to append all messages
  waitForLocalPeerToAckIndex(100);

  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  // Since we're tracking a single peer still this should have moved the all
  // replicated watermark to the last op appended to the local log.
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 100);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId firstMsg = MakeOpId(7, 50);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool sendMoreImmediately = false;

  updatePeerWatermarkToOp(
      &request, &response, firstMsg, MinimumOpId(), &sendMoreImmediately);
  ASSERT_TRUE(sendMoreImmediately);

  // Tracking a peer a new peer should have moved the all replicated watermark
  // back.
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);

  vector<ReplicateRefPtr> refs;
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(50, request.ops_size());

  appendReplicateMessagesToQueue(queue_.get(), clock_, 101, 100);

  setLastReceivedAndLastCommitted(&response, request.ops(49).id());
  response.set_responder_term(28);

  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately)
      << "Queue didn't have anymore requests pending";

  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 100);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 100);

  // if we ask for a new request, it should come back with the rest of the
  // messages
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(100, request.ops_size());

  OpId expected = request.ops(99).id();

  setLastReceivedAndLastCommitted(&response, expected);
  response.set_responder_term(expected.term());
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(sendMoreImmediately)
      << "Queue didn't have anymore requests pending";

  waitForLocalPeerToAckIndex(expected.index());

  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), expected.index());
  ASSERT_EQ(queue_->getAllReplicatedIndex(), expected.index());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops_size(), nullptr);
}

TEST_F(ConsensusQueueTest, TestQueueAdvancesCommittedIndex) {
  queue_->setLeaderMode(
      kMinimumOpIdIndex, kMinimumTerm, buildRaftConfigPbForTests(5));
  // Track 4 additional peers (in addition to the local peer)
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::VOTER));

  // Append 10 messages to the queue.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);
  waitForLocalPeerToAckIndex(10);

  // Since only the local log has ACKed at this point,
  // the committed_index should be MinimumOpId().
  ASSERT_EQ(queue_->getCommittedIndex(), 0);

  // NOTE: We don't need to get operations from the queue. The queue
  // only cares about what the peer reported as received, not what was sent.
  ConsensusResponsePB response;
  response.set_responder_term(1);

  bool sendMoreImmediately;
  OpId lastSent = MakeOpId(0, 5);

  // Ack the first five operations for peer-1.
  response.set_responder_uuid("peer-1");
  setLastReceivedAndLastCommitted(&response, lastSent, MinimumOpId().index());

  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately);

  // Committed index should be the same
  ASSERT_EQ(queue_->getCommittedIndex(), 0);

  // Ack the first five operations for peer-2.
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately);

  // A majority has now replicated up to 0.5: local, 'peer-1', and 'peer-2'.
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  // However, this leader has appended operations in term 1, so we can't
  // advance the committed index yet.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  // Moreover, 'peer-3' and 'peer-4' have not acked yet, so the "all-replicated"
  // index also cannot advance.
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Ack all operations for peer-3.
  response.set_responder_uuid("peer-3");
  lastSent = MakeOpId(1, 10);
  setLastReceivedAndLastCommitted(&response, lastSent, MinimumOpId().index());
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);

  // peer-3 now has all operations, and the commit index hasn't advanced.
  EXPECT_FALSE(sendMoreImmediately);

  // Watermarks should remain the same as above: we still have not
  // majority-replicated anything in the current term, so committed index cannot
  // advance.
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Ack the remaining operations for peer-4.
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // Now that a majority of peers have replicated an operation in the queue's
  // term the committed index should advance.
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 10);
  ASSERT_EQ(queue_->getCommittedIndex(), 10);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Ensure that the acks for a non-voter don't count toward the majority.
TEST_F(ConsensusQueueTest, TestNonVoterAcksDontCountTowardMajority) {
  const auto kOtherVoterPeer = "peer-1";
  const auto kNonVoterPeer = "non-voter-peer-0";

  // 1. Add a non-voter to the config where there are 2 voters.
  queue_->setLeaderMode(
      kMinimumOpIdIndex,
      kMinimumTerm,
      buildRaftConfigPbForTests(
          /*numVoters=*/2,
          /*numNonVoters=*/1));
  // Track 2 additional peers (in addition to the local peer)
  queue_->trackPeer(makePeer(kOtherVoterPeer, RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer(kNonVoterPeer, RaftPeerPB::NON_VOTER));

  // 2. Add some writes. Only the local leader immediately acks them, which is
  // not enough to commit in a 2-voter + 1 non-voter config.
  //
  // Append 10 messages to the queue.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  const int kNumMessages = 10;
  appendReplicateMessagesToQueue(
      queue_.get(),
      clock_,
      /*first=*/1,
      /*count=*/kNumMessages);
  waitForLocalPeerToAckIndex(kNumMessages);

  // Since only the local log has acked at this point, the committed_index
  // should be 0.
  const int64_t kNoneCommittedIndex = 0;
  ASSERT_EQ(kNoneCommittedIndex, queue_->getCommittedIndex());

  // 3. Ack the operations from the NON_VOTER peer. The writes will not have
  // been committed yet, because the 2nd VOTER has not yet acked them.
  ConsensusResponsePB response;
  response.set_responder_uuid(kNonVoterPeer);
  const int64_t kCurrentTerm = 1;
  response.set_responder_term(kCurrentTerm);
  setLastReceivedAndLastCommitted(
      &response,
      /*lastReceived=*/MakeOpId(kCurrentTerm, kNumMessages),
      /*lastCommittedIdx=*/kNoneCommittedIndex);

  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(sendMoreImmediately);

  // Committed index should be the same.
  ASSERT_EQ(kNoneCommittedIndex, queue_->getCommittedIndex());

  // 4. Send an identical ack from the 2nd VOTER peer. This should cause the
  // operation to be committed.
  response.set_responder_uuid(kOtherVoterPeer);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately); // The committed index has increased.

  // The committed index should include the full set of ops now.
  ASSERT_EQ(kNumMessages, queue_->getCommittedIndex());

  setLastReceivedAndLastCommitted(
      &response,
      /*lastReceived=*/MakeOpId(kCurrentTerm, kNumMessages),
      /*lastCommittedIdx=*/kNumMessages);

  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(sendMoreImmediately);
}

// In this test we append a sequence of operations to a log
// and then start tracking a peer whose first required operation
// is before the first operation in the queue.
TEST_F(ConsensusQueueTest, TestQueueLoadsOperationsForPeer) {
  OpId opId = MakeOpId(1, 1);

  const int kOpsToAppend = 100;
  for (int i = 1; i <= kOpsToAppend; i++) {
    ASSERT_OK(log::appendNoOpToLogSync(clock_, log_.get(), &opId));
    // Roll the log every 10 ops
    // (Skipped with mock log)
    // if (i % 10 == 0) {
    //   ASSERT_OK(log_->AllocateSegmentAndRollOver());
    // }
  }

  ASSERT_OPID_EQ(MakeOpId(1, kOpsToAppend + 1), opId);
  OpId lastLoggedOpId = MakeOpId(opId.term(), opId.index() - 1);

  // Now reset the queue so that we can pass a new committed index,
  // the last operation in the log.
  closeAndReopenQueue(lastLoggedOpId, lastLoggedOpId);

  queue_->setLeaderMode(
      lastLoggedOpId.index(),
      lastLoggedOpId.term(),
      buildRaftConfigPbForTests(3));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool sendMoreImmediately = false;

  // The peer will actually be behind the first operation in the queue.
  // In this case about 50 operations before.
  OpId peersLastOp;
  peersLastOp.set_term(1);
  peersLastOp.set_index(50);

  // Now we start tracking the peer, this negotiation round should let
  // the queue know how far along the peer is.
  ASSERT_NO_FATAL_FAILURE(updatePeerWatermarkToOp(
      &request, &response, peersLastOp, MinimumOpId(), &sendMoreImmediately));

  // The queue should reply that there are more messages for the peer.
  ASSERT_TRUE(sendMoreImmediately);

  // When we get another request for the peer the queue should load
  // the missing operations.
  vector<ReplicateRefPtr> refs;
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(request.ops_size(), 50);

  // The messages still belong to the queue so we have to release them.
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops().size(), nullptr);
}

// This tests that the queue is able to handle operation overwriting, i.e. when
// a newly tracked peer reports the last received operations as some operation
// that doesn't exist in the leader's log. In particular it tests the case where
// a new leader starts at term 2 with only a part of the operations of the
// previous leader having been committed.
TEST_F(ConsensusQueueTest, TestQueueHandlesOperationOverwriting) {
  OpId opId = MakeOpId(1, 1);
  // Append 10 messages in term 1 to the log.
  for (int i = 1; i <= 10; i++) {
    ASSERT_OK(log::appendNoOpToLogSync(clock_, log_.get(), &opId));
    // Roll the log every 3 ops
    // (Skipped with mock log)
    // if (i % 3 == 0) {
    //   ASSERT_OK(log_->AllocateSegmentAndRollOver());
    // }
  }

  opId = MakeOpId(2, 11);
  // Now append 10 more messages in term 2.
  for (int i = 11; i <= 20; i++) {
    ASSERT_OK(log::appendNoOpToLogSync(clock_, log_.get(), &opId));
    // Roll the log every 3 ops
    // (Skipped with mock log)
    // if (i % 3 == 0) {
    //   ASSERT_OK(log_->AllocateSegmentAndRollOver());
    // }
  }

  OpId lastInLog = MakeOpId(opId.term(), opId.index() - 1);
  int64_t committedIndex = 15;

  // Now reset the queue so that we can pass a new committed index (15).
  closeAndReopenQueue(lastInLog, MakeOpId(2, committedIndex));

  queue_->setLeaderMode(
      committedIndex, lastInLog.term(), buildRaftConfigPbForTests(3));

  // Now get a request for a simulated old leader, which contains more
  // operations in term 1 than the new leader has. The queue should realize that
  // the old leader's last received doesn't exist and send it operations
  // starting at the old leader's committed index.
  ConsensusRequestPB request;
  ConsensusResponsePB response;
  vector<ReplicateRefPtr> refs;
  response.set_responder_uuid(kPeerUuid);
  bool sendMoreImmediately = false;

  queue_->trackPeer(makePeer(kPeerUuid, RaftPeerPB::VOTER));

  // Ask for a request. The queue assumes the peer is up-to-date so
  // this should contain no operations.
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(request.ops_size(), 0);
  ASSERT_OPID_EQ(request.preceding_id(), MakeOpId(2, 20));
  ASSERT_EQ(request.committed_index(), committedIndex);

  // The old leader was still in term 1 but it increased its term with our
  // request.
  response.set_responder_term(2);

  // We emulate that the old leader had 25 total operations in Term 1 (15 more
  // than we knew about) which were never committed, and that its last known
  // committed index was 5.
  ConsensusStatusPB* status = response.mutable_status();
  status->mutable_last_received()->CopyFrom(MakeOpId(1, 25));
  status->mutable_last_received_current_leader()->CopyFrom(MinimumOpId());
  status->set_last_committed_idx(5);
  ConsensusErrorPB* error = status->mutable_error();
  error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
  statusToPb(Status::IllegalState("LMP failed."), error->mutable_status());

  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  request.Clear();

  // The queue should reply that there are more operations pending.
  ASSERT_TRUE(sendMoreImmediately);

  // We're waiting for a two nodes. The all committed watermark should be
  // 0.0 since we haven't had a successful exchange with the 'remote' peer.
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Test even when a correct peer responds (meaning we actually get to execute
  // watermark advancement) we sill have the same all-replicated watermark.
  ASSERT_OK(queue_->appendOperation(
      std::make_shared<RefCountedReplicate>(
          createDummyReplicate(2, 21, clock_->now(), 0), Source::Memory)));
  waitForLocalPeerToAckIndex(21);

  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Generate another request for the remote peer, which should include
  // all of the ops since the peer's last-known committed index.
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_OPID_EQ(MakeOpId(1, 5), request.preceding_id());
  ASSERT_EQ(16, request.ops_size());

  // Now when we respond the watermarks should advance.
  response.mutable_status()->clear_error();
  setLastReceivedAndLastCommitted(&response, MakeOpId(2, 21), 5);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately);

  // Now the watermark should have advanced.
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 21);

  // The messages still belong to the queue so we have to release them.
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops().size(), nullptr);
}

// Test for a bug where we wouldn't move any watermark back, when overwriting
// operations, which would cause a check failure on the write immediately
// following the overwriting write.
TEST_F(ConsensusQueueTest, TestQueueMovesWatermarksBackward) {
  queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));
  // Append a bunch of messages and update as if they were also appeneded to the
  // leader.
  queue_->updateLastIndexAppendedToLeader(10);
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);
  waitForLocalPeerToAckIndex(10);

  // Now rewrite some of the operations and wait for the log to append.
  Synchronizer synch;
  CHECK_OK(queue_->appendOperations(
      {std::make_shared<RefCountedReplicate>(
          createDummyReplicate(2, 5, clock_->now(), 0), Source::Memory)},
      synch.asStatusCallback()));

  // Wait for the operation to be in the log.
  ASSERT_OK(synch.wait());
  waitForPool(*raftPool_);

  // Having appended index 5, the follower is still 5 ops behind the leader.
  ASSERT_EQ(5, queue_->metrics_.num_ops_behind_leader->value());

  // Without the fix the following append would trigger a check failure
  // in log cache.
  synch.reset();
  CHECK_OK(queue_->appendOperations(
      {std::make_shared<RefCountedReplicate>(
          createDummyReplicate(2, 6, clock_->now(), 0), Source::Memory)},
      synch.asStatusCallback()));

  // Wait for the operation to be in the log.
  ASSERT_OK(synch.wait());
  waitForPool(*raftPool_);

  // Having appended index 6, the follower is still 4 ops behind the leader.
  ASSERT_EQ(4, queue_->metrics_.num_ops_behind_leader->value());

  // The replication watermark on a follower should not advance by virtue of
  // appending entries to the log.
  ASSERT_EQ(0, queue_->getAllReplicatedIndex());
}

// A follower's committed index must never roll back. A leader can legitimately
// advertise a lower committed index than the follower already has -- e.g. a new
// leader that has not yet re-committed in its term, or an empty heartbeat whose
// preceding id is below our committed index -- and updateFollowerWatermarks
// must keep the higher value.
TEST_F(ConsensusQueueTest, TestFollowerCommittedIndexDoesNotRewind) {
  queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));

  queue_->updateFollowerWatermarks(
      /*committed_index=*/100,
      /*all_replicated_index=*/100,
      /*region_durable_index=*/100);
  ASSERT_EQ(100, queue_->getCommittedIndex());

  // A lower committed index from the leader must not roll us back.
  queue_->updateFollowerWatermarks(
      /*committed_index=*/0,
      /*all_replicated_index=*/100,
      /*region_durable_index=*/100);
  ASSERT_EQ(100, queue_->getCommittedIndex());
}

// Test for watermark advancement during joint-consensus phase with
// transitional config. The transitional config is below:
//   C_old      = {peer-0, peer-1, peer-2}
//   C_new      = {peer-0, peer-1, peer-2, peer-3, peer-4}
//   C_old_new  = {{peer-0, peer-1, peer-2}
//                 {peer-0, peer-1, peer-2, peer-3, peer-4}}
// During joint-consensus phase, the commit watermark should only
// be advanced after considering peers in the old and new config.
TEST_F(ConsensusQueueTest, TestQueueAdvancesUnderTransitionalConfig) {
  // 'peer-0' is the leader (see `kLeaderUuid`)
  queue_->setLeaderMode(
      kMinimumTerm,
      kMinimumOpIdIndex,
      buildTransitionalRaftConfigPbForTests(3, 5));
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::VOTER));

  // Append 5 messages to the queue.
  // This should add messages 0.1 -> 0.5 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 5);
  waitForLocalPeerToAckIndex(5);

  // Before receiving non-local ACKs, the watermark stays constant
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a majority in C_old: {peer-0, peer-1},
  // note that peer-0 is ourself so its already local-peer ACK'ed.
  OpId lastSent = MakeOpId(0, 5);
  ConsensusResponsePB response;
  response.set_responder_term(0);
  response.set_responder_uuid("peer-1");
  setLastReceivedAndLastCommitted(
      &response, lastSent, (int)MinimumOpId().index());
  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);

  // No need to send more messages as commit index hasn't advanced.
  EXPECT_FALSE(sendMoreImmediately);

  // Before receiving non-local ACKs from a mojority in new config, the
  // watermark should stay constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a majority in C_new: : {peer-0, peer-1, peer-4}
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving a majority ACKs from peers in old *and* new config,
  // the commit and majority watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from all the other remaining peers
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);
  response.set_responder_uuid("peer-3");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving ACKs from *all* peers in the old *and* new config,
  // the commit, majority, and all_replicated watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Test for watermark advancement during joint-consensus phase with
// transitional config where some peers transition from non-voter into voter.
//   C_old      = {peer-0, peer-1, peer-2, *peer-3, *peer-4}
//   C_new      = {peer-0, peer-1, peer-2, peer-3, peer-4}
// The '*' sign above indicates non-voter role.
TEST_F(ConsensusQueueTest, TestQueueAdvancesUnderTransitionalConfigToVoter) {
  queue_->setLeaderMode(
      kMinimumTerm,
      kMinimumOpIdIndex,
      buildTransitionalRaftConfigPbForTests(
          /*numOldVoters=*/3,
          /*numNewVoters=*/5,
          /*numOldNonVoters=*/2,
          /*numNewNonVoters=*/0));

  // Note that the voter type in the TrackedPeers below is not used
  // for watermark calculation, which directly uses the peers in config.
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::NON_VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::NON_VOTER));

  // Append 5 messages to the queue.
  // This should add messages 0.1 -> 0.5 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 5);
  waitForLocalPeerToAckIndex(5);

  // Before receiving non-local ACKs, the watermark stays constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a voter majority in C_old: {peer-0, peer-1},
  // note that peer-0 is ourself so its already local-peer ACK'ed.
  OpId lastSent = MakeOpId(0, 5);
  ConsensusResponsePB response;
  response.set_responder_term(0);
  response.set_responder_uuid("peer-1");
  setLastReceivedAndLastCommitted(
      &response, lastSent, (int)MinimumOpId().index());
  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);

  // Before receiving non-local ACKs from a mojority in new config, the
  // watermark should stay constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a voter majority in C_new: : {peer-0, peer-1, peer-4}
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving a majority ACKs from peers in old *and* new config,
  // the commit and majority watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from all the other remaining peers
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);
  response.set_responder_uuid("peer-3");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving ACKs from *all* peers in the old *and* new config,
  // the commit, majority, and all_replicated watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Test for watermark advancement during joint-consensus phase with
// transitional config where some peers transition from voter into non-voter.
//   C_old      = {peer-0, peer-1, peer-2, peer-3, peer-4}
//   C_new      = {peer-0, peer-1, peer-2, *peer-3, *peer-4}
// The '*' sign above indicates non-voter role.
TEST_F(ConsensusQueueTest, TestQueueAdvancesUnderTransitionalConfigToNonVoter) {
  queue_->setLeaderMode(
      kMinimumTerm,
      kMinimumOpIdIndex,
      buildTransitionalRaftConfigPbForTests(
          /*numOldVoters=*/3,
          /*numNewVoters=*/5,
          /*numOldNonVoters=*/2,
          /*numNewNonVoters=*/0));
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::VOTER));

  // Append 5 messages to the queue.
  // This should add messages 0.1 -> 0.5 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 5);
  waitForLocalPeerToAckIndex(5);

  // Before receiving non-local ACKs, the watermark stays constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a voter majority in C_old: {peer-0, peer-1, peer-4},
  // note that peer-0 is ourself so its already local-peer ACK'ed.
  OpId lastSent = MakeOpId(0, 5);
  ConsensusResponsePB response;
  response.set_responder_term(0);
  response.set_responder_uuid("peer-1");
  setLastReceivedAndLastCommitted(
      &response, lastSent, (int)MinimumOpId().index());
  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // In this case, a subset of {peer-0, peer-1, peer-4} is a voter majority in
  // the new config. Thus, the watermarks should be advanced as majority in both
  // old and new config is already satisfied.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from all the other remaining peers
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);
  response.set_responder_uuid("peer-3");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving ACKs from *all* peers in the old *and* new config,
  // the commit, majority, and all_replicated watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Similar as the TestQueueAdvancesUnderTransitionalConfigToNonVoter, having
// transition from voter into non-voter.
//   C_old      = {peer-0, peer-1, peer-2, peer-3, peer-4}
//   C_new      = {peer-0, peer-1, peer-2, *peer-3, *peer-4}
// The '*' sign above indicates non-voter role.
//
// However, here the queue first get a majority of C_old, which is not a
// majority of C_new. The leader is unlucky.
TEST_F(ConsensusQueueTest, TestQueueAdvancesUnderTransConfigUnluckyNonVoter) {
  queue_->setLeaderMode(
      kMinimumTerm,
      kMinimumOpIdIndex,
      buildTransitionalRaftConfigPbForTests(
          /*numOldVoters=*/3,
          /*numNewVoters=*/5,
          /*numOldNonVoters=*/2,
          /*numNewNonVoters=*/0));
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::VOTER));

  // Append 5 messages to the queue.
  // This should add messages 0.1 -> 0.5 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 5);
  waitForLocalPeerToAckIndex(5);

  // Before receiving non-local ACKs, the watermark stays constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from a voter majority in C_old: {peer-0, peer-3, peer-4},
  // note that peer-0 is ourself so its already local-peer ACK'ed.
  OpId lastSent = MakeOpId(0, 5);
  ConsensusResponsePB response;
  response.set_responder_term(0);
  response.set_responder_uuid("peer-3");
  setLastReceivedAndLastCommitted(
      &response, lastSent, (int)MinimumOpId().index());
  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);

  // In this case, there is no subset of {peer-0, peer-3, peer-4} that can form
  // a voter majority for C_new, so the watermarks should stay the same.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from peer-2, forming a majority in C_new.
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // {peer-0, peer-2} is a voter majority in C_new, the watermarks advance
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from all the other remaining peers
  response.set_responder_uuid("peer-1");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving ACKs from *all* peers in the old *and* new config,
  // the commit, majority, and all_replicated watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Using transitional config with even number of voters.
//   C_old      = {peer-0, peer-1, peer-2, peer-3}
//   C_new      = {peer-0, peer-1, peer-2, peer-3, peer-4, peer-5}
// This checks that majority is calculated correctly for even number of voters.
TEST_F(ConsensusQueueTest, TestQueueAdvancesUnderTransitionalConfigEvenVoters) {
  queue_->setLeaderMode(
      kMinimumTerm,
      kMinimumOpIdIndex,
      buildTransitionalRaftConfigPbForTests(
          /*numOldVoters=*/4,
          /*numNewVoters=*/6));
  queue_->trackPeer(makePeer("peer-1", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-2", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-3", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-4", RaftPeerPB::VOTER));
  queue_->trackPeer(makePeer("peer-5", RaftPeerPB::VOTER));

  // Append 5 messages to the queue.
  // This should add messages 0.1 -> 0.5 to the queue.
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 5);
  waitForLocalPeerToAckIndex(5);

  // Before receiving non-local ACKs, the watermark stays constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACK from peer-1
  OpId lastSent = MakeOpId(0, 5);
  ConsensusResponsePB response;
  response.set_responder_term(0);
  response.set_responder_uuid("peer-1");
  setLastReceivedAndLastCommitted(
      &response, lastSent, (int)MinimumOpId().index());
  bool sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);

  // Majority out of 4 voters is 3, thus having 2 peers are not enough to
  // advance the watermarks for C_old
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACK from peer-2, creating a majority (3 out of 4) of C_old
  response.set_responder_uuid("peer-2");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_FALSE(sendMoreImmediately);

  // Eventhough we have majority in C_old, we still dont have majority of C_new,
  // making watermarks stay constant.
  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 0);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACK from peer-3, creating a majority (4 out of 6) of C_new
  response.set_responder_uuid("peer-3");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // Since we achieve majority *both* in C_old and C_new, the watermaks advance
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 0);

  // Receive ACKs from all the other remaining peers
  response.set_responder_uuid("peer-4");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);
  response.set_responder_uuid("peer-5");
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(sendMoreImmediately);

  // After receiving ACKs from *all* peers in the old *and* new config,
  // the commit, majority, and all_replicated watermarks should be advanced.
  ASSERT_EQ(queue_->getCommittedIndex(), 5);
  ASSERT_EQ(queue_->getMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), 5);
}

// Tests that we're advancing the watermarks properly and only when the peer
// has a prefix of our log. This also tests for a specific bug that we had.
// Here's the scenario: Peer would report:
//   - last received 75.49
//   - last committed 72.31
//
// Queue has messages:
// 72.31-72.45
// 73.46-73.51
// 76.52-76.53
//
// The queue has more messages than the peer, but the peer has messages
// that the queue doesn't and which will be overwritten.
//
// In the first round of negotiation the peer would report LMP mismatch.
// In the second round the queue would try to send it messages starting at 75.49
// but since that message didn't exist in the queue's log it would instead send
// messages starting at 72.31. However, because the batches were big it was only
// able to send a few messages (e.g. up to 72.40).
//
// Since in this last exchange everything went ok (the peer still doesn't know
// that messages will be overwritten later), the queue would mark the exchange
// as successful and the peer's last received would be taken into account when
// calculating watermarks, which was incorrect.
//
// FIXME(mpercy): This test also likely fails due to the above-mentioned
// hacky LogCache optimization, resulting in miscounting the size of
// NoopRequestPB messages in the queue. That's just a guess, though, but it's a
// delicate test so I think that's the most likely culprit.
TEST_F(
    ConsensusQueueTest,
    DISABLED_TestOnlyAdvancesWatermarkWhenPeerHasAPrefixOfOurLog) {
  FLAGS_consensus_max_batch_size_bytes = 1024 * 10;

  const int kInitialCommittedIndex = 30;
  closeAndReopenQueue(MakeOpId(72, 30), MakeOpId(82, 30));
  queue_->setLeaderMode(
      kInitialCommittedIndex, 76, buildRaftConfigPbForTests(3));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  vector<ReplicateRefPtr> refs;

  bool sendMoreImmediately;
  // We expect the majority replicated watermark to start at the committed
  // index.
  int64_t expectedMajorityReplicated = kInitialCommittedIndex;
  // We expect the all replicated watermark to be reset when we track a new
  // peer.
  int64_t expectedAllReplicated = 0;

  ASSERT_EQ(
      queue_->getMajorityReplicatedIndexForTests(), expectedMajorityReplicated);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), expectedAllReplicated);

  updatePeerWatermarkToOp(
      &request,
      &response,
      MakeOpId(75, 49),
      MinimumOpId(),
      31,
      &sendMoreImmediately);
  ASSERT_TRUE(sendMoreImmediately);

  for (int i = 31; i <= 53; i++) {
    if (i <= 45) {
      appendReplicateMsg(72, i, 1024);
      continue;
    }
    if (i <= 51) {
      appendReplicateMsg(73, i, 1024);
      continue;
    }
    appendReplicateMsg(76, i, 1024);
  }

  waitForLocalPeerToAckIndex(53);

  // When we get operations for this peer we should get them starting
  // immediately after the committed index, for a total of 9 operations.
  bool needsTabletCopy;
  std::string nextHopUuid;
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(request.ops_size(), 9);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(72, 32));
  const OpId* lastOp = &request.ops(request.ops_size() - 1).id();

  // When the peer acks that it received an operation that is not in our current
  // term, it gets ignored in terms of watermark advancement.
  setLastReceivedAndLastCommitted(&response, MakeOpId(75, 49), *lastOp, 31);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(sendMoreImmediately);

  // We've sent (and received and ack) up to 72.40 from the remote peer
  expectedMajorityReplicated = expectedAllReplicated = 40;

  ASSERT_EQ(
      queue_->getMajorityReplicatedIndexForTests(), expectedMajorityReplicated);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), expectedAllReplicated);

  // Another request for this peer should get another page of messages. Still
  // not on the queue's term (and thus without advancing watermarks).
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops().size(), nullptr);
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(request.ops_size(), 9);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(72, 41));
  lastOp = &request.ops(request.ops_size() - 1).id();

  setLastReceivedAndLastCommitted(&response, MakeOpId(75, 49), *lastOp, 31);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);

  // We've now sent (and received an ack) up to 73.39
  expectedMajorityReplicated = expectedAllReplicated = 49;

  ASSERT_EQ(
      queue_->getMajorityReplicatedIndexForTests(), expectedMajorityReplicated);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), expectedAllReplicated);

  // The last page of request should overwrite the peer's operations and the
  // response should finally advance the watermarks.
  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops().size(), nullptr);
  ASSERT_OK(queue_->RequestForPeer(
      kPeerUuid,
      /*read_ops=*/true,
      &request,
      &refs,
      &needsTabletCopy,
      &nextHopUuid));
  ASSERT_FALSE(needsTabletCopy);
  ASSERT_EQ(request.ops_size(), 4);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(73, 50));

  // We're done, both watermarks should be at the end.
  expectedMajorityReplicated = expectedAllReplicated = 53;

  setLastReceivedAndLastCommitted(&response, MakeOpId(76, 53), 31);
  sendMoreImmediately =
      queue_->ResponseFromPeer(response.responder_uuid(), response);

  ASSERT_EQ(
      queue_->getMajorityReplicatedIndexForTests(), expectedMajorityReplicated);
  ASSERT_EQ(queue_->getAllReplicatedIndex(), expectedAllReplicated);

  request.mutable_ops()->UnsafeArenaExtractSubrange(
      0, request.ops().size(), nullptr);
}

TEST_F(ConsensusQueueTest, TestFollowerCommittedIndexAndMetrics) {
  queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));

  // Emulate a follower sending a request to replicate 10 messages.
  queue_->updateLastIndexAppendedToLeader(10);
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);
  waitForLocalPeerToAckIndex(10);

  // The committed_index should be MinimumOpId() since
  // UpdateFollowerCommittedIndex has not been called.
  ASSERT_EQ(0, queue_->getCommittedIndex());

  // Update the committed index. In real life, this would be done by the
  // consensus implementation when it receives an updated committed index from
  // the leader.
  queue_->updateFollowerWatermarks(
      /*committed_index=*/10,
      /*all_replicated_index=*/10,
      /*region_durable_index=*/-1);
  ASSERT_EQ(10, queue_->getCommittedIndex());

  // Check the metrics have the right values based on the updated committed
  // index.
  ASSERT_EQ(0, queue_->metrics_.num_majority_done_ops->value());
  ASSERT_EQ(0, queue_->metrics_.num_in_progress_ops->value());
  ASSERT_EQ(0, queue_->metrics_.num_ops_behind_leader->value());

  // Emulate the leader appending up to index 15. The num_ops_behind_leader
  // should jump to 5.
  queue_->updateLastIndexAppendedToLeader(15);
  ASSERT_EQ(5, queue_->metrics_.num_ops_behind_leader->value());
}

TEST_F(ConsensusQueueTest, ZeroCommitQuorum) {
  FLAGS_enable_flexi_raft = true;
  queue_->setAdjustVoterDistribution(false);
  auto config = buildQuorumIdRaftConfigPbForTests({
      {0, {kLeaderQuorumId, RaftPeerPB::VOTER}},
      {1, {kLeaderQuorumId, RaftPeerPB::VOTER}},
      {2, {kLeaderQuorumId, RaftPeerPB::VOTER}},
  });
  config.mutable_voter_distribution()->insert({kLeaderQuorumId, -2});
  queue_->setLeaderMode(kMinimumOpIdIndex, kMinimumTerm, std::move(config));
  appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);

  // Wait for the local peer to append all messages
  waitForLocalPeerToAckIndex(10);

  EXPECT_EQ(queue_->getMajorityReplicatedIndexForTests(), 10);
}
// ---------------------------------------------------------------------------
// Leadership confirmation for linearizable reads.
//
// The watermark answers "when was a majority of this leader's commit quorum
// last proven to recognize it?". A linearizable read compares that against the
// instant it began. Without it a partitioned leader would serve a stale
// snapshot quite happily, because its local role stays LEADER until something
// tells it otherwise.
// ---------------------------------------------------------------------------

using ConfirmationResult = PeerMessageQueue::ConfirmationResult;

class ConsensusQueueConfirmationTest : public ConsensusQueueTest {
 public:
  void SetUp() override {
    ConsensusQueueTest::SetUp();
    // Confirmation must work with leases off.
    FLAGS_enable_raft_leader_lease = false;
  }

  // Mirrors Peer::doProcessResponse: record when the RPC was sent, then hand
  // the response to the queue.
  void respond(
      const string& uuid,
      ConsensusResponsePB* response,
      const MonoTime& rpcStart) {
    response->set_responder_uuid(uuid);
    queue_->ResponseFromPeer(uuid, *response, rpcStart);
  }

  void ackFrom(
      const string& uuid,
      const OpId& lastReceived,
      const MonoTime& rpcStart) {
    ConsensusResponsePB response;
    setLastReceivedAndLastCommitted(&response, lastReceived);
    respond(uuid, &response, rpcStart);
  }

  void refuseWithInvalidTerm(
      const string& uuid,
      const OpId& lastReceived,
      const MonoTime& rpcStart) {
    ConsensusResponsePB response;
    response.set_responder_term(kMinimumTerm);
    ConsensusStatusPB* status = response.mutable_status();
    *status->mutable_last_received() = lastReceived;
    *status->mutable_last_received_current_leader() = lastReceived;
    status->set_last_committed_idx(lastReceived.index());
    ConsensusErrorPB* error = status->mutable_error();
    error->set_code(ConsensusErrorPB::INVALID_TERM);
    statusToPb(Status::IllegalState("Invalid term."), error->mutable_status());
    respond(uuid, &response, rpcStart);
  }

  // Takes the term from the snapshot, as a real reader does: appending
  // operations advances the queue's term past the one setLeaderMode was given.
  ConfirmationResult confirmation(const MonoTime& anchor) {
    return queue_->checkQuorumConfirmation(
        queue_->getLeaderReadSnapshot().currentTerm, anchor);
  }

  // Brings the queue up as leader of a 'numVoters' ring with the remote voters
  // tracked, and appends enough operations for the local peer to have acked.
  void setupQueue(int numVoters) {
    queue_->setLeaderMode(
        kMinimumOpIdIndex, kMinimumTerm, buildRaftConfigPbForTests(numVoters));
    for (int i = 1; i < numVoters; i++) {
      queue_->trackPeer(makePeer(fmt::format("peer-{}", i), RaftPeerPB::VOTER));
    }
    appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);
    waitForLocalPeerToAckIndex(10);
  }

  // Blocks until a reader has entered waitForQuorumConfirmation.
  //
  // Tests that act on a parked reader must order against its arrival, not
  // against a guessed interval: if the state change lands first the reader
  // resolves on its first predicate evaluation and never exercises the wakeup
  // the test exists for, so the test passes while proving nothing. The count is
  // published before that first evaluation, so observing it means the reader is
  // inside the call and about to park.
  void awaitReaderArrival() {
    while (queue_->getConfirmationWaitersForTests() == 0) {
      std::this_thread::yield();
    }
  }

  // Same, but under flexi-raft with an explicit quorum id per peer, keyed by
  // the numeric suffix of its uuid. Peer 0 is the leader.
  //
  // The local peer must carry a quorum id of its own: TrackedPeer resolves
  // membership only when both ends declare one, and a peer that resolves to
  // "unknown" is skipped by the commit-quorum walk.
  void setupFlexiQueue(const std::map<size_t, std::string>& quorumIds) {
    FLAGS_enable_flexi_raft = true;

    std::map<size_t, std::tuple<std::string, RaftPeerPB::MemberType>> members;
    for (const auto& [index, quorumId] : quorumIds) {
      members[index] = {quorumId, RaftPeerPB::VOTER};
    }

    queue_->Close();
    testLocalPeerPb_ = fakeRaftPeerPb(kLeaderUuid);
    testLocalPeerPb_.mutable_attrs()->set_quorum_id(quorumIds.at(0));
    closeAndReopenQueue(MinimumOpId(), MinimumOpId());

    queue_->setLeaderMode(
        kMinimumOpIdIndex,
        kMinimumTerm,
        buildQuorumIdRaftConfigPbForTests(members));
    for (const auto& [index, quorumId] : quorumIds) {
      if (index == 0) {
        continue;
      }
      RaftPeerPB peer =
          makePeer(fmt::format("peer-{}", index), RaftPeerPB::VOTER);
      peer.mutable_attrs()->set_quorum_id(quorumId);
      queue_->trackPeer(peer);
    }
    appendReplicateMessagesToQueue(queue_.get(), clock_, 1, 10);
    waitForLocalPeerToAckIndex(10);
  }
};

// A peer acking an operation that cannot move the commit index still proves it
// recognizes this leader. Lease renewal is nested inside the commit-index
// advancement path, so a read-only workload never refreshes it; confirmation
// must not inherit that.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationAdvancesWithoutCommitIndex) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ASSERT_EQ(confirmation(anchor), ConfirmationResult::kPending);

  ackFrom("peer-1", MakeOpId(0, 5), anchor + MonoDelta::FromMilliseconds(1));

  ASSERT_EQ(queue_->getCommittedIndex(), 0);
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);
}

// One peer is not a majority of five. Counting the fastest responder alone
// would let a leader reachable by a single follower confirm itself.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationRequiresMajority) {
  setupQueue(5);

  const MonoTime anchor = MonoTime::Now();
  const MonoTime acked = anchor + MonoDelta::FromMilliseconds(1);

  ackFrom("peer-1", MakeOpId(0, 5), acked);
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kPending);

  ackFrom("peer-2", MakeOpId(0, 5), acked);
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);
}

// A lone voter needs nobody's agreement -- nobody can win an election without
// its vote. Waiting for a confirmation that can never arrive would hang every
// critical read on dev and single-node MTR rings.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationOnSingleVoterRing) {
  setupQueue(1);

  EXPECT_EQ(confirmation(MonoTime::Now()), ConfirmationResult::kConfirmed);
}

// Responses can be reordered, and an older send time arriving late must not
// retract proof already held.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationIsMonotonic) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ackFrom("peer-1", MakeOpId(0, 5), anchor + MonoDelta::FromMilliseconds(10));
  ASSERT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);

  ackFrom("peer-1", MakeOpId(0, 6), anchor - MonoDelta::FromMilliseconds(10));
  EXPECT_EQ(
      confirmation(anchor + MonoDelta::FromMilliseconds(5)),
      ConfirmationResult::kConfirmed);
}

// The boundary is exclusive. A confirmation stamped at the anchor itself only
// shows the peer was with us up to that instant, which is what the reader
// already assumed when it sampled the anchor; it is not evidence about the
// interval the read needs covered. Requiring a strictly later confirmation also
// keeps the proof sound if the time source is ever coarsened, where two
// distinct instants could share a reading.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationExcludesTheAnchorItself) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ackFrom("peer-1", MakeOpId(0, 5), anchor);

  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kPending);

  // The very same confirmation satisfies an anchor one nanosecond earlier, so
  // the rejection above is the boundary and not a missing confirmation.
  EXPECT_EQ(
      confirmation(anchor - MonoDelta::FromNanoseconds(1)),
      ConfirmationResult::kConfirmed);
}

// An expired wait must report that it has no proof, never invent one. It must
// also actually block for the budget: returning kPending immediately would look
// identical to the caller while turning every slice into a busy loop.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationWaitTimesOut) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  // The only confirmation on offer predates the anchor, so no arrival during
  // the wait can satisfy it.
  ackFrom("peer-1", MakeOpId(0, 5), anchor - MonoDelta::FromSeconds(1));

  const MonoTime start = MonoTime::Now();
  const auto result = queue_->waitForQuorumConfirmation(
      queue_->getLeaderReadSnapshot().currentTerm,
      anchor,
      MonoDelta::FromMilliseconds(50));
  const MonoDelta elapsed = MonoTime::Now().GetDeltaSince(start);

  EXPECT_EQ(result, ConfirmationResult::kPending);
  EXPECT_GE(elapsed.ToMilliseconds(), 40);
}

// Losing leadership during the wait is reported distinctly from running out of
// time, because the two mean different things to a client: one says re-route,
// the other says back off and retry.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationWaitReportsLeadershipLoss) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  const int64_t term = queue_->getLeaderReadSnapshot().currentTerm;
  queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));

  const MonoTime start = MonoTime::Now();
  const auto result = queue_->waitForQuorumConfirmation(
      term, anchor, MonoDelta::FromSeconds(30));
  const MonoDelta elapsed = MonoTime::Now().GetDeltaSince(start);

  EXPECT_EQ(result, ConfirmationResult::kNotLeader);
  // And it gives up at once rather than sitting out a deadline it can never
  // satisfy.
  EXPECT_LT(elapsed.ToMilliseconds(), 1000);
}

// A peer rejecting our term is the one response that definitively does not
// confirm leadership -- and its rpcStart is recorded just like any other, so
// stamping it would let the exact peer that deposed us vouch for us.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationIgnoresInvalidTerm) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  refuseWithInvalidTerm(
      "peer-1", MakeOpId(0, 5), anchor + MonoDelta::FromMilliseconds(1));
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kPending);
}

// "Peer P recognized us at time T" does not expire. A later transient error
// says nothing about the interval the read cares about, and letting it retract
// the proof would stall every critical read on a three-voter ring for as long
// as one peer is unreachable -- and make confirmation non-monotonic for a fixed
// anchor, which is exactly what stamping the maximum send time avoids.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationSurvivesLaterTransientError) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ackFrom("peer-1", MakeOpId(0, 5), anchor + MonoDelta::FromMilliseconds(1));
  ASSERT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);

  queue_->UpdatePeerStatus(
      "peer-1",
      PeerStatus::RpcLayerError,
      Status::NetworkError("connection reset"));

  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);
}

// MySQL Raft always runs the flexi-raft branch, which sizes the majority from
// the leader's own commit quorum and ignores voters outside it. Vanilla raft
// would demand three confirmations on this five-voter ring; flexi-raft needs
// two, and only from r0.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationUsesFlexiRaftCommitQuorum) {
  setupFlexiQueue({{0, "r0"}, {1, "r0"}, {2, "r0"}, {3, "r1"}, {4, "r1"}});

  const MonoTime anchor = MonoTime::Now();
  const MonoTime acked = anchor + MonoDelta::FromMilliseconds(1);

  // Peers outside the commit quorum contribute nothing, however prompt.
  ackFrom("peer-3", MakeOpId(0, 5), acked);
  ackFrom("peer-4", MakeOpId(0, 5), acked);
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kPending);

  // Inside it, the leader plus one peer is a majority of three.
  ackFrom("peer-1", MakeOpId(0, 5), acked);
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);
}

// A follower in LMP mismatch accepted our term and rejected only the
// log-matching check. It holds that status for the whole of its catch-up, so
// discarding it would leave a leader unable to confirm a quorum for seconds
// after a routine failover.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationCountsLmpMismatch) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ConsensusResponsePB response;
  refuseWithLogPropertyMismatch(&response, MakeOpId(0, 5), MinimumOpId());
  response.mutable_status()->set_last_committed_idx(MinimumOpId().index());
  respond("peer-1", &response, anchor + MonoDelta::FromMilliseconds(1));

  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);
}

// Proof gathered while leading says nothing once leadership is gone.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationResetOnLeadershipLoss) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  ackFrom("peer-1", MakeOpId(0, 5), anchor + MonoDelta::FromMilliseconds(1));
  ASSERT_EQ(confirmation(anchor), ConfirmationResult::kConfirmed);

  queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));
  EXPECT_EQ(confirmation(anchor), ConfirmationResult::kNotLeader);
}

// Demotion has to wake a reader that is *already* parked, not just
// short-circuit one that has yet to start waiting. Peer responses are the only
// other signal and they stop being recorded the moment the queue leaves LEADER
// mode, so without an explicit wake here the reader sleeps out its entire
// deadline.
TEST_F(
    ConsensusQueueConfirmationTest,
    TestConfirmationWaitWokenByLeadershipLoss) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  const int64_t term = queue_->getLeaderReadSnapshot().currentTerm;

  std::thread demoter([&]() {
    awaitReaderArrival();
    queue_->setNonLeaderMode(buildRaftConfigPbForTests(3));
  });

  const MonoTime start = MonoTime::Now();
  const auto result = queue_->waitForQuorumConfirmation(
      term, anchor, MonoDelta::FromSeconds(30));
  const MonoDelta elapsed = MonoTime::Now().GetDeltaSince(start);
  demoter.join();

  EXPECT_EQ(result, ConfirmationResult::kNotLeader);
  EXPECT_LT(elapsed.ToMilliseconds(), 5000);
}

// Close() empties the peer map but leaves mode and term alone, so a parked
// reader would otherwise keep evaluating a predicate that can never again be
// satisfied. That is not merely a stalled read: ~PeerMessageQueue calls
// Close(), so the condvar the reader is parked on can be destroyed underneath
// it.
TEST_F(ConsensusQueueConfirmationTest, TestConfirmationWaitWokenByClose) {
  setupQueue(3);

  const MonoTime anchor = MonoTime::Now();
  const int64_t term = queue_->getLeaderReadSnapshot().currentTerm;

  std::thread closer([&]() {
    awaitReaderArrival();
    queue_->Close();
  });

  const MonoTime start = MonoTime::Now();
  const auto result = queue_->waitForQuorumConfirmation(
      term, anchor, MonoDelta::FromSeconds(30));
  const MonoDelta elapsed = MonoTime::Now().GetDeltaSince(start);
  closer.join();

  EXPECT_EQ(result, ConfirmationResult::kNotLeader);
  EXPECT_LT(elapsed.ToMilliseconds(), 5000);
}

// Destroying the queue under a parked reader is deliberately not covered here,
// because it is not supported. The queue cannot defend against its own
// destruction -- see the contract on waitForQuorumConfirmation -- and the
// keepalive that makes the real path safe lives in
// RaftConsensus::waitForQuorumConfirmation, above this layer.

} // namespace consensus
} // namespace kudu
