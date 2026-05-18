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

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <gmock/gmock.h>

#include <fmt/core.h>
#include <folly/Synchronized.h>
#include "kudu/clock/clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/thread_pool_builder.h"
#include "kudu/util/threadpool-test-util.h"
#include "kudu/util/threadpool.h"

#define TOKENPASTE(x, y) x##y
#define TOKENPASTE2(x, y) TOKENPASTE(x, y)

#define ASSERT_OPID_EQ(left, right)                                            \
  do {                                                                         \
    const consensus::OpId& TOKENPASTE2(_left, __LINE__) = (left);              \
    const consensus::OpId& TOKENPASTE2(_right, __LINE__) = (right);            \
    if (!consensus::OpIdEquals(                                                \
            TOKENPASTE2(_left, __LINE__), TOKENPASTE2(_right, __LINE__))) {    \
      FAIL() << "Expected: "                                                   \
             << pb_util::SecureShortDebugString(TOKENPASTE2(_left, __LINE__))  \
             << "\n"                                                           \
             << "Value: "                                                      \
             << pb_util::SecureShortDebugString(TOKENPASTE2(_right, __LINE__)) \
             << "\n";                                                          \
    }                                                                          \
  } while (false)

namespace kudu::consensus {

inline std::unique_ptr<ReplicateMsg> createDummyReplicate(
    int64_t term,
    int64_t index,
    const Timestamp& timestamp,
    int64_t payloadSize) {
  std::unique_ptr<ReplicateMsg> msg(new ReplicateMsg);
  OpId* id = msg->mutable_id();
  id->set_term(term);
  id->set_index(index);

  msg->set_op_type(NO_OP);
  msg->mutable_noop_request()->mutable_payload_for_tests()->resize(payloadSize);
  msg->set_timestamp(timestamp.toUint64());
  return msg;
}

// Returns RaftPeerPB with given UUID and obviously-fake hostname / port combo.
inline RaftPeerPB fakeRaftPeerPb(const std::string& uuid) {
  RaftPeerPB peerPb;
  peerPb.set_permanent_uuid(uuid);
  peerPb.set_member_type(RaftPeerPB::VOTER);
  peerPb.mutable_last_known_addr()->set_host(
      fmt::format("{}-fake-hostname", CURRENT_TEST_NAME()));
  peerPb.mutable_last_known_addr()->set_port(0);
  return peerPb;
}

// Appends 'count' messages to 'queue' with different terms and indexes.
//
// An operation will only be considered done (TestOperationStatus::IsDone()
// will become true) once at least 'n_majority' peers have called
// TestOperationStatus::AckPeer().
inline void appendReplicateMessagesToQueue(
    PeerMessageQueue* queue,
    const std::shared_ptr<clock::Clock>& clock,
    int64_t first,
    int64_t count,
    int64_t payloadSize = 0) {
  for (int64_t i = first; i < first + count; i++) {
    int64_t term = i / 7;
    int64_t index = i;
    CHECK_OK(queue->appendOperation(makeScopedRefptrReplicate(
        createDummyReplicate(term, index, clock->now(), payloadSize),
        Source::Memory)));
  }
}

// Builds a configuration of 'num' voters.
inline RaftConfigPB buildRaftConfigPbForTests(
    int numVoters,
    int numNonVoters = 0) {
  RaftConfigPB raftConfig;
  for (int i = 0; i < numVoters; i++) {
    RaftPeerPB* peerPb = raftConfig.add_peers();
    peerPb->set_member_type(RaftPeerPB::VOTER);
    peerPb->set_permanent_uuid(fmt::format("peer-{}", i));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", i));
    hp->set_port(0);
  }
  for (int i = 0; i < numNonVoters; i++) {
    RaftPeerPB* peerPb = raftConfig.add_peers();
    peerPb->set_member_type(RaftPeerPB::NON_VOTER);
    peerPb->set_permanent_uuid(fmt::format("non-voter-peer-{}", i));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("non-voter-peer-{}.fake-domain-for-tests", i));
    hp->set_port(0);
  }
  return raftConfig;
}

// Builds a Raft config with commit rule of `QuorumType::QUORUM_ID`.
// `instanceRegions` is map index -> (quorum id, member type)
inline RaftConfigPB buildQuorumIdRaftConfigPbForTests(
    std::map<size_t, std::tuple<std::string, RaftPeerPB::MemberType>>
        instanceRegions) {
  RaftConfigPB raftConfig;
  for (auto it = instanceRegions.begin(); it != instanceRegions.end(); it++) {
    auto id = it->first;
    auto [quorumId, memberType] = it->second;

    auto peerPb = raftConfig.add_peers();
    peerPb->set_permanent_uuid(fmt::format("peer-{}", id));
    peerPb->set_member_type(memberType);
    auto hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", id));
    hp->set_port(0);
    peerPb->mutable_attrs()->set_quorum_id(quorumId);
  }

  auto commitRule = raftConfig.mutable_commit_rule();
  commitRule->set_quorum_type(QuorumType::QUORUM_ID);

  return raftConfig;
}

// Builds a Raft config with commit rule of `QuorumType::REGION`.
// `instanceRegions` is map index -> (region, member type)
inline RaftConfigPB buildRegionRaftConfigPbForTests(
    std::map<size_t, std::tuple<std::string, RaftPeerPB::MemberType>>
        instanceRegions) {
  RaftConfigPB raftConfig;
  for (auto it = instanceRegions.begin(); it != instanceRegions.end(); it++) {
    auto id = it->first;
    auto [region, memberType] = it->second;

    auto peerPb = raftConfig.add_peers();
    peerPb->set_permanent_uuid(fmt::format("peer-{}", id));
    peerPb->set_member_type(memberType);
    auto hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", id));
    hp->set_port(0);
    peerPb->mutable_attrs()->set_region(region);
  }

  auto commitRule = raftConfig.mutable_commit_rule();
  commitRule->set_quorum_type(QuorumType::REGION);

  return raftConfig;
}

inline RaftConfigPB buildRaftConfigPbForRoutingProxyTests(
    std::vector<std::string> databaseRegions,
    int numLbuPerDatabase = 2) {
  RaftConfigPB raftConfig;
  for (const auto& region : databaseRegions) {
    auto peerPb = raftConfig.add_peers();
    peerPb->set_permanent_uuid(fmt::format("peer-db-{}", region));
    peerPb->mutable_attrs()->set_backing_db_present(true);
    auto hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-db-{}.fake-domain-for-tests", region));
    hp->set_port(0);
    peerPb->mutable_attrs()->set_region(region);
    for (int i = 0; i < numLbuPerDatabase; i++) {
      auto lbuPeerPb = raftConfig.add_peers();
      lbuPeerPb->set_permanent_uuid(fmt::format("peer-lbu-{}-{}", region, i));
      lbuPeerPb->mutable_attrs()->set_backing_db_present(false);
      auto lbuHp = lbuPeerPb->mutable_last_known_addr();
      lbuHp->set_host(
          fmt::format("peer-lbu-{}-{}.fake-domain-for-tests", region, i));
      lbuHp->set_port(0);
      lbuPeerPb->mutable_attrs()->set_region(region);
    }
  }

  return raftConfig;
}

inline RaftConfigPB buildTransitionalRaftConfigPbForTests(
    int numOldVoters,
    int numNewVoters,
    int numOldNonVoters = 0,
    int numNewNonVoters = 0) {
  RaftConfigPB raftConfig;
  for (int i = 0; i < numOldVoters; ++i) {
    RaftPeerPB* peerPb = raftConfig.add_peers();
    peerPb->set_member_type(RaftPeerPB::VOTER);
    peerPb->set_permanent_uuid(fmt::format("peer-{}", i));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", i));
    hp->set_port(0);
  }
  for (int i = 0; i < numOldNonVoters; ++i) {
    int peerId = i + numOldVoters;
    RaftPeerPB* peerPb = raftConfig.add_peers();
    peerPb->set_member_type(RaftPeerPB::NON_VOTER);
    peerPb->set_permanent_uuid(fmt::format("peer-{}", peerId));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", peerId));
    hp->set_port(0);
  }

  // Prepare transitional config with `next_config_peers` populated,
  // having some new peers with the same uuid as the old peers.
  for (int i = 0; i < numNewVoters; ++i) {
    RaftPeerPB* peerPb = raftConfig.add_next_config_peers();
    peerPb->set_member_type(RaftPeerPB::VOTER);
    peerPb->set_permanent_uuid(fmt::format("peer-{}", i));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", i));
    hp->set_port(0);
  }
  for (int i = 0; i < numNewNonVoters; ++i) {
    int peerId = i + numNewVoters;
    RaftPeerPB* peerPb = raftConfig.add_next_config_peers();
    peerPb->set_member_type(RaftPeerPB::NON_VOTER);
    peerPb->set_permanent_uuid(fmt::format("peer-{}", peerId));
    HostPortPB* hp = peerPb->mutable_last_known_addr();
    hp->set_host(fmt::format("peer-{}.fake-domain-for-tests", peerId));
    hp->set_port(0);
  }

  return raftConfig;
}

// Abstract base class to build PeerProxy implementations on top of for testing.
// Provides a single-threaded pool to run callbacks in and callback
// registration/running, along with an enum to identify the supported methods.
class TestPeerProxy : public PeerProxy {
 public:
  // Which PeerProxy method to invoke.
  enum Method {
    kUpdate,
    kRequestVote,
  };

  explicit TestPeerProxy(ThreadPool* pool) : pool_(pool) {}

  std::string peerName() const override {
    return "TestPeerProxy";
  }

 protected:
  // Register the RPC callback in order to call later.
  // We currently only support one request of each method being in flight at a
  // time.
  virtual void registerCallback(
      Method method,
      const rpc::ResponseCallback& callback) {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    auto [it, inserted] = callbacks_.insert({method, callback});
    CHECK(inserted);
  }

  // Answer the peer.
  virtual void respond(Method method) {
    rpc::ResponseCallback callback;
    {
      std::lock_guard<SimpleSpinlock> lock(lock_);
      auto it = callbacks_.find(method);
      CHECK(it != callbacks_.end()) << "Map key not found: " << method;
      callback = it->second;
      CHECK_EQ(1, callbacks_.erase(method));
      // Drop the lock before submitting to the pool, since the callback itself
      // may destroy this instance.
    }
    // If the peer has been closed while a response was in-flight, this can
    // return a bad Status, but that's fine.
    ignoreResult(pool_->SubmitFunc(callback));
  }

  virtual void registerCallbackAndRespond(
      Method method,
      const rpc::ResponseCallback& callback) {
    registerCallback(method, callback);
    respond(method);
  }

  mutable SimpleSpinlock lock_;
  ThreadPool* pool_;
  std::map<Method, rpc::ResponseCallback> callbacks_; // Protected by lock_.
};

template <typename ProxyType>
class DelayablePeerProxy : public TestPeerProxy {
 public:
  // Add delayability of RPC responses to the delegated impl.
  // This class takes ownership of 'proxy'.
  explicit DelayablePeerProxy(ThreadPool* pool, ProxyType* proxy)
      : TestPeerProxy(pool),
        proxy_(CHECK_NOTNULL(proxy)),
        delayResponse_(false),
        latch_(1) {}

  // Delay the answer to the next response to this remote
  // peer. The response callback will only be called on respond().
  virtual void delayResponse() {
    std::lock_guard<SimpleSpinlock> l(lock_);
    delayResponse_ = true;
    latch_.reset(1); // Reset for the next time.
  }

  virtual void respondUnlessDelayed(Method method) {
    {
      std::lock_guard<SimpleSpinlock> l(lock_);
      if (delayResponse_) {
        latch_.countDown();
        delayResponse_ = false;
        return;
      }
    }
    TestPeerProxy::respond(method);
  }

  virtual void respond(Method method) override {
    latch_.wait(); // Wait until strictly after peer would have responded.
    return TestPeerProxy::respond(method);
  }

  virtual void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    registerCallback(kUpdate, callback);
    return proxy_->updateAsync(
        request,
        response,
        controller,
        boost::bind(&DelayablePeerProxy::respondUnlessDelayed, this, kUpdate));
  }

  virtual Status startElection(
      const RunLeaderElectionRequestPB* /*request*/,
      RunLeaderElectionResponsePB* /*response*/,
      rpc::RpcController* /*controller*/) override {
    return Status::OK();
  }

  virtual void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    registerCallback(kRequestVote, callback);
    return proxy_->requestConsensusVoteAsync(
        request,
        response,
        controller,
        boost::bind(
            &DelayablePeerProxy::respondUnlessDelayed, this, kRequestVote));
  }

  ProxyType* proxy() const {
    return proxy_.get();
  }

 protected:
  std::unique_ptr<ProxyType> const proxy_;
  bool delayResponse_; // Protected by lock_.
  CountDownLatch latch_;
};

// Allows complete mocking of a peer's responses.
// You set the response, it will respond with that.
class MockedPeerProxy : public TestPeerProxy {
 public:
  explicit MockedPeerProxy(ThreadPool* pool)
      : TestPeerProxy(pool), updateCount_(0) {}

  virtual void setUpdateResponse(const ConsensusResponsePB& updateResponse) {
    CHECK(updateResponse.IsInitialized())
        << pb_util::SecureShortDebugString(updateResponse);
    {
      std::lock_guard<SimpleSpinlock> l(lock_);
      updateResponse_ = updateResponse;
    }
  }

  virtual void setVoteResponse(const VoteResponsePB& voteResponse) {
    {
      std::lock_guard<SimpleSpinlock> l(lock_);
      voteResponse_ = voteResponse;
    }
  }

  virtual void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    {
      std::lock_guard<SimpleSpinlock> l(lock_);
      updateCount_++;
      *response = updateResponse_;
    }
    return registerCallbackAndRespond(kUpdate, callback);
  }

  virtual void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    *response = voteResponse_;
    return registerCallbackAndRespond(kRequestVote, callback);
  }

  Status startElection(
      const RunLeaderElectionRequestPB* /*request*/,
      RunLeaderElectionResponsePB* /*response*/,
      rpc::RpcController* /*controller*/) override {
    return Status::OK();
  }

  // Return the number of times that updateAsync() has been called.
  int updateCount() const {
    std::lock_guard<SimpleSpinlock> l(lock_);
    return updateCount_;
  }

 protected:
  int updateCount_;

  ConsensusResponsePB updateResponse_;
  VoteResponsePB voteResponse_;
};

// Allows to test peers by emulating a noop remote endpoint that just replies
// that the messages were received/replicated/committed.
class NoOpTestPeerProxy : public TestPeerProxy {
 public:
  explicit NoOpTestPeerProxy(ThreadPool* pool, consensus::RaftPeerPB peer_pb)
      : TestPeerProxy(pool), peerPb_(std::move(peer_pb)) {
    lastReceived_.CopyFrom(MinimumOpId());
  }

  virtual void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    response->Clear();
    {
      std::lock_guard<SimpleSpinlock> lock(lock_);
      if (OpIdLessThan(lastReceived_, request->preceding_id())) {
        ConsensusErrorPB* error = response->mutable_status()->mutable_error();
        error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
        statusToPb(Status::IllegalState(""), error->mutable_status());
      } else if (request->ops_size() > 0) {
        lastReceived_.CopyFrom(request->ops(request->ops_size() - 1).id());
      }

      response->set_responder_uuid(peerPb_.permanent_uuid());
      response->set_responder_term(request->caller_term());
      response->mutable_status()->mutable_last_received()->CopyFrom(
          lastReceived_);
      response->mutable_status()
          ->mutable_last_received_current_leader()
          ->CopyFrom(lastReceived_);
      // We set the last committed index to be the same index as the last
      // received. While this is unlikely to happen in a real situation, its not
      // technically incorrect and avoids having to come up with some other
      // index that it still correct.
      response->mutable_status()->set_last_committed_idx(lastReceived_.index());
    }
    return registerCallbackAndRespond(kUpdate, callback);
  }

  virtual Status startElection(
      const RunLeaderElectionRequestPB* /*request*/,
      RunLeaderElectionResponsePB* /*response*/,
      rpc::RpcController* /*controller*/) override {
    return Status::OK();
  }

  virtual void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* /*controller*/,
      const rpc::ResponseCallback& callback) override {
    {
      std::lock_guard<SimpleSpinlock> lock(lock_);
      response->set_responder_uuid(peerPb_.permanent_uuid());
      response->set_responder_term(request->candidate_term());
      response->set_vote_granted(true);
    }
    return registerCallbackAndRespond(kRequestVote, callback);
  }

  const OpId& lastReceived() {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    return lastReceived_;
  }

 private:
  const consensus::RaftPeerPB peerPb_;
  ConsensusStatusPB lastStatus_; // Protected by lock_.
  OpId lastReceived_; // Protected by lock_.
};

class NoOpTestPeerProxyFactory : public PeerProxyFactory {
 public:
  NoOpTestPeerProxyFactory() {
    CHECK_OK(
        ThreadPoolBuilder("test-peer-pool").setMaxThreads(3).build(&pool_));
    CHECK_OK(rpc::MessengerBuilder("test").Build(&messenger_));
  }

  Status newProxy(
      const consensus::RaftPeerPB& peer_pb,
      std::shared_ptr<PeerProxy>* proxy) override {
    proxy->reset(new NoOpTestPeerProxy(pool_.get(), peer_pb));
    return Status::OK();
  }

  const std::shared_ptr<rpc::Messenger>& messenger() const override {
    return messenger_;
  }

 private:
  std::unique_ptr<ThreadPool> pool_;
  std::shared_ptr<rpc::Messenger> messenger_;
};

using TestPeerMap =
    std::unordered_map<std::string, std::shared_ptr<RaftConsensus>>;

// Thread-safe manager for list of peers being used in tests.
class TestPeerMapManager {
 public:
  explicit TestPeerMapManager(RaftConfigPB config)
      : config_(std::move(config)) {}

  void AddPeer(
      const std::string& peer_uuid,
      const std::shared_ptr<RaftConsensus>& peer) {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    auto [it, inserted] = peers_.insert({peer_uuid, peer});
    CHECK(inserted);
  }

  Status GetPeerByIdx(int idx, std::shared_ptr<RaftConsensus>* peer_out) const {
    CHECK_LT(idx, config_.peers_size());
    return GetPeerByUuid(config_.peers(idx).permanent_uuid(), peer_out);
  }

  Status GetPeerByUuid(
      const std::string& peer_uuid,
      std::shared_ptr<RaftConsensus>* peer_out) const {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    if (!findCopy(peers_, peer_uuid, peer_out)) {
      return Status::NotFound("Other consensus instance was destroyed");
    }
    return Status::OK();
  }

  void RemovePeer(const std::string& peer_uuid) {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    peers_.erase(peer_uuid);
  }

  TestPeerMap GetPeerMapCopy() const {
    std::lock_guard<SimpleSpinlock> lock(lock_);
    return peers_;
  }

  void Clear() {
    // We create a copy of the peers before we clear 'peers_' so that there's
    // still a reference to each peer. If we reduce the reference count to 0
    // under the lock we might get a deadlock as on shutdown consensus
    // indirectly destroys the test proxies which in turn reach into this class.
    TestPeerMap copy = peers_;
    {
      std::lock_guard<SimpleSpinlock> lock(lock_);
      peers_.clear();
    }
  }

 private:
  const RaftConfigPB config_;
  TestPeerMap peers_;
  mutable SimpleSpinlock lock_;
};

// Allows to test remote peers by emulating an RPC.
// Both the "remote" peer's RPC call and the caller peer's response are executed
// asynchronously in a ThreadPool.
class LocalTestPeerProxy : public TestPeerProxy {
 public:
  LocalTestPeerProxy(
      std::string peer_uuid,
      ThreadPool* pool,
      TestPeerMapManager* peers)
      : TestPeerProxy(pool),
        peer_uuid_(std::move(peer_uuid)),
        peers_(peers),
        miss_comm_(false) {}

  virtual void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override {
    registerCallback(kUpdate, callback);
    CHECK_OK(pool_->SubmitFunc(
        boost::bind(
            &LocalTestPeerProxy::SendUpdateRequest, this, request, response)));
  }

  Status startElection(
      const RunLeaderElectionRequestPB* /*request*/,
      RunLeaderElectionResponsePB* /*response*/,
      rpc::RpcController* /*controller*/) override {
    return Status::OK();
  }

  virtual void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* /*controller*/,
      const rpc::ResponseCallback& callback) override {
    registerCallback(kRequestVote, callback);
    CHECK_OK(pool_->SubmitFunc(
        boost::bind(
            &LocalTestPeerProxy::SendVoteRequest, this, request, response)));
  }

  template <class Response>
  void SetResponseError(const Status& status, Response* response) {
    ServerErrorPB* error = response->mutable_error();
    error->set_code(ServerErrorPB::UNKNOWN_ERROR);
    statusToPb(status, error->mutable_status());
  }

  template <class Request, class Response>
  void RespondOrMissResponse(
      Request* request,
      const Response& response_temp,
      Response* final_response,
      Method method) {
    bool miss_comm_copy;
    {
      std::lock_guard<SimpleSpinlock> lock(lock_);
      miss_comm_copy = miss_comm_;
      miss_comm_ = false;
    }
    if (PREDICT_FALSE(miss_comm_copy)) {
      VLOG(2) << this << ": injecting fault on "
              << pb_util::SecureShortDebugString(*request);
      SetResponseError(
          Status::IOError(
              "Artificial error caused by communication "
              "failure injection."),
          final_response);
    } else {
      final_response->CopyFrom(response_temp);
    }
    respond(method);
  }

  void SendUpdateRequest(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response) {
    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    ConsensusRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);

    // Give the other peer a clean response object to write to.
    ConsensusResponsePB other_peer_resp;
    std::shared_ptr<RaftConsensus> peer;
    Status s = peers_->GetPeerByUuid(peer_uuid_, &peer);

    if (s.ok()) {
      s = peer->update(&other_peer_req, &other_peer_resp);
      if (s.ok() && !other_peer_resp.has_error()) {
        CHECK(other_peer_resp.has_status());
        CHECK(other_peer_resp.status().IsInitialized());
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not Update replica with request: "
                   << pb_util::SecureShortDebugString(other_peer_req)
                   << " Status: " << s.ToString();
      SetResponseError(s, &other_peer_resp);
    }

    response->CopyFrom(other_peer_resp);
    RespondOrMissResponse(request, other_peer_resp, response, kUpdate);
  }

  void SendVoteRequest(const VoteRequestPB* request, VoteResponsePB* response) {
    // Copy the request and the response for the other peer so that ownership
    // remains as close to the dist. impl. as possible.
    VoteRequestPB other_peer_req;
    other_peer_req.CopyFrom(*request);
    VoteResponsePB other_peer_resp;
    other_peer_resp.CopyFrom(*response);

    std::shared_ptr<RaftConsensus> peer;
    Status s = peers_->GetPeerByUuid(peer_uuid_, &peer);

    if (s.ok()) {
      s = peer->requestVote(
          &other_peer_req,
          TabletVotingState({}),
          // anirban-fb
          // TabletVotingState({}, tablet::TABLET_DATA_READY),
          &other_peer_resp);
    }
    if (!s.ok()) {
      LOG(WARNING) << "Could not RequestVote from replica with request: "
                   << pb_util::SecureShortDebugString(other_peer_req)
                   << " Status: " << s.ToString();
      SetResponseError(s, &other_peer_resp);
    }

    response->CopyFrom(other_peer_resp);
    RespondOrMissResponse(request, other_peer_resp, response, kRequestVote);
  }

  void InjectCommFaultLeaderSide() {
    VLOG(2) << this << ": injecting fault next time";
    std::lock_guard<SimpleSpinlock> lock(lock_);
    miss_comm_ = true;
  }

  const std::string& GetTarget() const {
    return peer_uuid_;
  }

 private:
  const std::string peer_uuid_;
  TestPeerMapManager* const peers_;
  bool miss_comm_;
};

class LocalTestPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit LocalTestPeerProxyFactory(TestPeerMapManager* peers)
      : peers_(peers) {
    CHECK_OK(
        ThreadPoolBuilder("test-peer-pool").setMaxThreads(3).build(&pool_));
    CHECK_OK(rpc::MessengerBuilder("test").Build(&messenger_));
  }

  Status newProxy(
      const consensus::RaftPeerPB& peer_pb,
      std::shared_ptr<PeerProxy>* proxy) override {
    LocalTestPeerProxy* new_proxy =
        new LocalTestPeerProxy(peer_pb.permanent_uuid(), pool_.get(), peers_);
    proxy->reset(new_proxy);
    proxies_.push_back(new_proxy);
    return Status::OK();
  }

  const std::vector<LocalTestPeerProxy*>& GetProxies() {
    return proxies_;
  }

  const std::shared_ptr<rpc::Messenger>& messenger() const override {
    return messenger_;
  }

 private:
  std::unique_ptr<ThreadPool> pool_;
  std::shared_ptr<rpc::Messenger> messenger_;
  TestPeerMapManager* const peers_;
  // NOTE: There is no need to delete this on the dctor because proxies are
  // externally managed
  std::vector<LocalTestPeerProxy*> proxies_;
};

// A simple implementation of the transaction driver.
// This is usually implemented by TransactionDriver but here we
// keep the implementation to the minimally required to have consensus
// work.
class TestDriver {
 public:
  TestDriver(ThreadPool* pool, const std::shared_ptr<ConsensusRound>& round)
      : round_(round), pool_(pool) {}

  void SetRound(const std::shared_ptr<ConsensusRound>& round) {
    round_ = round;
  }

  // Does nothing but enqueue the Apply
  void ReplicationFinished(const Status& status) {
    if (status.IsAborted()) {
      Cleanup();
      return;
    }
    CHECK_OK(status);
    CHECK_OK(pool_->SubmitFunc(boost::bind(&TestDriver::Apply, this)));
  }

  // Called in all modes to delete the transaction and, transitively, the
  // consensus round.
  void Cleanup() {
    delete this;
  }

  std::shared_ptr<ConsensusRound> round_;

 private:
  // The commit message has the exact same type of the replicate message, but
  // no content.
  void Apply() {
    std::unique_ptr<CommitMsg> msg(new CommitMsg);
    msg->set_op_type(round_->replicate_msg()->op_type());
    msg->mutable_commited_op_id()->CopyFrom(round_->id());
    Cleanup();
  }

  ThreadPool* pool_;
};

// A transaction factory for tests, usually this is implemented by
// TabletReplica.
class TestTransactionFactory : public ConsensusRoundHandler {
 public:
  explicit TestTransactionFactory() : consensus_(nullptr) {
    CHECK_OK(
        ThreadPoolBuilder("test-txn-factory").setMaxThreads(1).build(&pool_));
  }

  void SetConsensus(RaftConsensus* consensus) {
    consensus_ = consensus;
  }

  Status startFollowerTransaction(
      const std::shared_ptr<ConsensusRound>& round) override {
    auto txn = new TestDriver(pool_.get(), round);
    txn->round_->SetConsensusReplicatedCallback(
        std::bind(
            &TestDriver::ReplicationFinished, txn, std::placeholders::_1));
    return Status::OK();
  }

  Status startConsensusOnlyRound(
      const std::shared_ptr<ConsensusRound>& round) override {
    return Status::OK();
  }

  void finishConsensusOnlyRound(ConsensusRound* /*round*/) override {}

  bool isLeaderEligible() const override {
    return true;
  }

  void ReplicateAsync(const std::shared_ptr<ConsensusRound>& round) {
    CHECK_OK(consensus_->replicate(round));
  }

  void WaitDone() {
    waitForPool(*pool_);
  }

  void ShutDown() {
    WaitDone();
    pool_->Shutdown();
  }

  ~TestTransactionFactory() {
    ShutDown();
  }

 private:
  std::unique_ptr<ThreadPool> pool_;
  RaftConsensus* consensus_;
};

// A stateful mock log that stores appended operations in memory
// and can retrieve them during lookup calls
class StatefulMockLog : public kudu::log::Log {
 public:
  StatefulMockLog(
      log::LogOptions logOptions,
      FsManager* fsManager,
      std::string logPath,
      std::string tabletId,
      std::shared_ptr<MetricEntity> metricEntity)
      : Log(std::move(logOptions),
            fsManager,
            std::move(logPath),
            std::move(tabletId),
            std::move(metricEntity)) {}

  Status Init() override {
    return Status::OK();
  }

  // Override asyncAppendReplicates to store operations in memory
  Status asyncAppendReplicates(
      const std::vector<ReplicateRefPtr>& replicates,
      const StatusCallback& callback) override {
    ops_.withWLock([&](auto& ops) {
      for (const auto& replicate : replicates) {
        const OpId& opId = replicate->get()->id();
        ops[opId.index()] = opId;
      }
    });

    if (!callback.is_null()) {
      callback.Run(Status::OK());
    }
    return Status::OK();
  }

  // Override lookupOpId to retrieve stored operations
  Status lookupOpId(int64_t opIndex, OpId* opId) const override {
    return ops_.withRLock([&](const auto& ops) -> Status {
      auto it = ops.find(opIndex);
      if (it != ops.end()) {
        *opId = it->second;
        return Status::OK();
      }
      return Status::NotFound("OpId not found");
    });
  }

  // Override readReplicatesInRange to return stored operations
  Status readReplicatesInRange(
      int64_t startIndex,
      int64_t endIndex,
      int64_t,
      const ReadContext&,
      std::vector<ReplicateRefPtr>* replicates) const override {
    ops_.withRLock([&](const auto& ops) {
      for (int64_t i = startIndex; i <= endIndex; i++) {
        auto it = ops.find(i);
        if (it != ops.end()) {
          // Create a ReplicateMsg with the stored OpId
          auto replicateMsg = std::make_unique<ReplicateMsg>();
          replicateMsg->mutable_id()->CopyFrom(it->second);
          replicates->push_back(
              makeScopedRefptrReplicate(std::move(replicateMsg), Source::Disk));
        }
      }
    });

    return Status::OK();
  }

  // Override truncateOpsAfter to remove operations after the given index
  Status truncateOpsAfter(int64_t index, int64_t* numTruncated) override {
    int64_t count = ops_.withWLock([&](auto& ops) {
      int64_t cnt = 0;
      auto it = ops.upper_bound(index);
      while (it != ops.end()) {
        it = ops.erase(it);
        cnt++;
      }
      return cnt;
    });

    if (numTruncated != nullptr) {
      *numTruncated = count;
    }

    return Status::OK();
  }

  // Get all stored OpIds sorted by index
  std::vector<OpId> getAllOpIds() const {
    return ops_.withRLock([](const auto& ops) {
      std::vector<OpId> result;
      result.reserve(ops.size());
      for (const auto& entry : ops) {
        result.push_back(entry.second);
      }
      return result;
    });
  }

  Status Close() override {
    ops_.wlock()->clear();
    return Status::OK();
  }

 private:
  folly::Synchronized<std::map<int64_t, OpId>> ops_;
};

} // namespace kudu::consensus
