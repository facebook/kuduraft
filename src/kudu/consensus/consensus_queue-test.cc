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
        fakeRaftPeerPb(kLeaderUuid),
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

  // Now rewrite some of the operations and wait for the log to append.
  Synchronizer synch;
  CHECK_OK(queue_->appendOperations(
      {std::make_shared<RefCountedReplicate>(
          createDummyReplicate(2, 5, clock_->now(), 0), Source::Memory)},
      synch.asStatusCallback()));

  // Wait for the operation to be in the log.
  ASSERT_OK(synch.wait());

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

  // Having appended index 6, the follower is still 4 ops behind the leader.
  ASSERT_EQ(4, queue_->metrics_.num_ops_behind_leader->value());

  // The replication watermark on a follower should not advance by virtue of
  // appending entries to the log.
  ASSERT_EQ(0, queue_->getAllReplicatedIndex());
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
} // namespace consensus
} // namespace kudu
