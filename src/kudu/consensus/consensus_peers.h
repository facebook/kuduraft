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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

DECLARE_bool(raft_enforce_rpc_token);
DECLARE_int32(peer_rtt_update_interval_us);

namespace kudu {
class ThreadPoolToken;

namespace rpc {
class Messenger;
class PeriodicTimer;
} // namespace rpc

namespace consensus {
class PeerMessageQueue;
class PeerProxy;
class PeerProxyPool;

// A remote peer in consensus.
//
// Leaders use peers to update the remote replicas. Each peer
// may have at most one outstanding request at a time. If a
// request is signaled when there is already one outstanding,
// the request will be generated once the outstanding one finishes.
//
// Peers are owned by the consensus implementation and do not keep
// state aside from the most recent request and response.
//
// Peers are also responsible for sending periodic heartbeats
// to assert liveness of the leader. The peer constructs a heartbeater
// thread to trigger these heartbeats.
//
// The actual request construction is delegated to a PeerMessageQueue
// object, and performed on a thread pool (since it may do IO). When a
// response is received, the peer updates the PeerMessageQueue
// using PeerMessageQueue::ResponseFromPeer(...) on the same thread pool.
class Peer : public std::enable_shared_from_this<Peer> {
 public:
  // Initializes a peer and start sending periodic heartbeats.
  Status init();

  // Signals that this peer has a new request to replicate/store.
  // 'even_if_queue_empty' indicates whether the peer should force
  // send the request even if the queue is empty. This is used for
  // status-only requests.
  Status signalRequest(
      bool even_if_queue_empty = false,
      bool is_leader_lease_revoke = false);

  // Synchronously starts a leader election on this peer.
  // This method is ad hoc, using this instance's PeerProxy to send the
  // startElection request.
  // The startElection RPC does not count as the single outstanding request
  // that this class tracks.
  Status startElection(
      RunLeaderElectionResponsePB* resp,
      RunLeaderElectionRequestPB req = {});

  const RaftPeerPB& peerPb() const {
    return peer_pb_;
  }

  void setUpdateConsensusRpcStart(MonoTime starttime) {
    rpc_start_ = starttime;
  }

  // Stop sending requests and periodic heartbeats.
  //
  // This does not block waiting on any current outstanding requests to finish.
  // However, when they do finish, the results will be disregarded, so this
  // is safe to call at any point.
  //
  // This method must be called before the Peer's associated ThreadPoolToken
  // is destructed. Once this method returns, it is safe to destruct
  // the ThreadPoolToken.
  void close();

  ~Peer();

  // Creates a new remote peer and makes the queue track it.'
  //
  // Requests to this peer (which may end up doing IO to read non-cached
  // log entries) are assembled on 'raft_pool_token'.
  // Response handling may also involve IO related to log-entry lookups and is
  // also done on 'raft_pool_token'.
  static Status newRemotePeer(
      RaftPeerPB peerPb,
      std::string tabletId,
      std::string leaderUuid,
      PeerMessageQueue* queue,
      PeerProxyPool* peerProxyPool,
      ThreadPoolToken* raftPoolToken,
      std::shared_ptr<PeerProxy> proxy,
      std::shared_ptr<rpc::Messenger> messenger,
      std::shared_ptr<Peer>* peer);

 private:
  Peer(
      RaftPeerPB peerPb,
      std::string tabletId,
      std::string leaderUuid,
      PeerMessageQueue* queue,
      PeerProxyPool* peerProxyPool,
      ThreadPoolToken* raftPoolToken,
      std::shared_ptr<PeerProxy> proxy,
      std::shared_ptr<rpc::Messenger> messenger);

  void sendNextRequest(
      bool even_if_queue_empty,
      bool is_leader_lease_revoke = false);

  // Signals that a response was received from the peer.
  //
  // This method is called from the reactor thread and calls
  // doProcessResponse() on raft_pool_token_ to do any work that requires IO or
  // lock-taking.
  void processResponse();

  // Run on 'raft_pool_token'. Does response handling that requires IO or may
  // block.
  void doProcessResponse();

  // Signals there was an error sending the request to the peer.
  void processResponseError(const Status& status);

  std::string LogPrefixUnlocked() const;

  const std::string& tabletId() const {
    return tablet_id_;
  }

  const std::string tablet_id_;
  const std::string leader_uuid_;

  RaftPeerPB peer_pb_;

  std::shared_ptr<PeerProxy> proxy_;

  PeerMessageQueue* queue_;
  /**
   * The proxy pools for all peers.
   *
   * Note that is this owned by PeerManager and can be cleared when PeerManager
   * is closing itself and all Peers.
   */
  PeerProxyPool* peer_proxy_pool_;
  uint64_t failed_attempts_;

  // Time when the last request was sent
  MonoTime last_request_time_;

  // The latest consensus update request and response.
  ConsensusRequestPB request_;
  ConsensusResponsePB response_;

  // Reference-counted pointers to any ReplicateMsgs which are in-flight to the
  // peer. We may have loaded these messages from the LogCache, in which case we
  // are potentially sharing the same object as other peers. Since the PB
  // request_ itself can't hold reference counts, this holds them.
  std::vector<ReplicateRefPtr> replicate_msg_refs_;

  rpc::RpcController controller_;

  std::shared_ptr<rpc::Messenger> messenger_;

  // Thread pool token used to construct requests to this peer.
  //
  // RaftConsensus owns this shared token and is responsible for destroying it.
  ThreadPoolToken* raft_pool_token_;

  // Repeating timer responsible for scheduling heartbeats to this peer.
  std::shared_ptr<rpc::PeriodicTimer> heartbeater_;

  // lock that protects Peer state changes, initialization, etc.
  mutable simple_spinlock peer_lock_;
  std::atomic<bool> request_pending_;
  bool closed_ = false;
  bool has_sent_first_request_ = false;
  // Cached state of whether this peer is proxied thru another peer. This info
  // can be stale, consult the PeerMessageQueue to get the upto date info
  // -1 means we've not inited the variable, 0 means false, 1 means true
  std::atomic<int> cached_is_peer_proxied_{-1};
  // Leader Leases: captures UpdateConsensus rpc start time for each peer
  MonoTime rpc_start_;
  MonoTime last_rtt_update_{MonoTime::Min()};

  std::optional<bool> is_peer_in_local_region_;
};

// A proxy to another peer. Usually a thin wrapper around an rpc proxy but can
// be replaced for tests.
class PeerProxy {
 public:
  virtual ~PeerProxy() = default;

  // Sends a request, asynchronously, to a remote peer.
  virtual void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) = 0;

  // Sends a RequestConsensusVote to a remote peer.
  virtual void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) = 0;

  virtual Status startElection(
      const RunLeaderElectionRequestPB* request,
      RunLeaderElectionResponsePB* response,
      rpc::RpcController* controller) = 0;

  // Remote endpoint or description of the peer.
  virtual std::string peerName() const = 0;
};

// A peer proxy factory. Usually just obtains peers through the rpc
// implementation but can be replaced for tests.
class PeerProxyFactory {
 public:
  virtual Status newProxy(
      const RaftPeerPB& peerPb,
      std::shared_ptr<PeerProxy>* proxy) = 0;

  virtual ~PeerProxyFactory() = default;

  virtual const std::shared_ptr<rpc::Messenger>& messenger() const = 0;
};

// Provides access to shared PeerProxy instances based on destination server
// uuid. This class is thread-safe.
class PeerProxyPool {
 public:
  // Return the PeerProxy associated with the given uuid.
  // If 'uuid' is not found, returns a shared_ptr initialized to nullptr, which
  // is falsy.
  std::shared_ptr<PeerProxy> get(const std::string& uuid) const;

  // Add a PeerProxy to the pool, given its uuid.
  void put(const std::string& uuid, std::shared_ptr<PeerProxy> proxy);

  // Clear the pool. Does not close the PeerProxy instances.
  void clear();

 private:
  folly::ConcurrentHashMap<std::string, std::shared_ptr<PeerProxy>>
      peerProxyMap_;
};

// PeerProxy implementation that does RPC calls
class RpcPeerProxy : public PeerProxy {
 public:
  RpcPeerProxy(
      std::unique_ptr<HostPort> hostport,
      std::shared_ptr<ConsensusServiceProxy> consensus_proxy,
      std::shared_ptr<Counter> num_rpc_token_mismatches);

  void updateAsync(
      const ConsensusRequestPB* request,
      ConsensusResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override;

  void requestConsensusVoteAsync(
      const VoteRequestPB* request,
      VoteResponsePB* response,
      rpc::RpcController* controller,
      const rpc::ResponseCallback& callback) override;

  Status startElection(
      const RunLeaderElectionRequestPB* request,
      RunLeaderElectionResponsePB* response,
      rpc::RpcController* controller) override;

  std::string peerName() const override;

 private:
  std::unique_ptr<HostPort> hostport_;
  std::shared_ptr<ConsensusServiceProxy> consensus_proxy_;

  std::shared_ptr<Counter> num_rpc_token_mismatches_;
};

// PeerProxyFactory implementation that generates RPCPeerProxies
class RpcPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit RpcPeerProxyFactory(
      std::shared_ptr<rpc::Messenger> messenger,
      const std::shared_ptr<MetricEntity>& metric_entity);

  Status newProxy(const RaftPeerPB& peerPb, std::shared_ptr<PeerProxy>* proxy)
      override;

  ~RpcPeerProxyFactory();

  const std::shared_ptr<rpc::Messenger>& messenger() const override {
    return messenger_;
  }

 private:
  std::shared_ptr<rpc::Messenger> messenger_;

  std::shared_ptr<Counter> num_rpc_token_mismatches_;
};

// Query the consensus service at last known host/port that is
// specified in 'remote_peer' and set the 'permanent_uuid' field based
// on the response.
Status setPermanentUuidForRemotePeer(
    const std::shared_ptr<rpc::Messenger>& messenger,
    RaftPeerPB* remotePeer);

} // namespace consensus
} // namespace kudu
