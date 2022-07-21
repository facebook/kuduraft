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

#include "kudu/consensus/consensus_peers.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/routing.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/periodic.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#ifdef FB_DO_NOT_REMOVE
#include "kudu/tserver/tserver.pb.h"
#endif
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(consensus_rpc_timeout_ms, 30000,
             "Timeout used for all consensus internal RPC communications.");
TAG_FLAG(consensus_rpc_timeout_ms, advanced);

DEFINE_int32(raft_get_node_instance_timeout_ms, 30000,
             "Timeout for retrieving node instance data over RPC.");
TAG_FLAG(raft_get_node_instance_timeout_ms, hidden);

DEFINE_double(fault_crash_on_leader_request_fraction, 0.0,
              "Fraction of the time when the leader will crash just before sending an "
              "UpdateConsensus RPC. (For testing only!)");
TAG_FLAG(fault_crash_on_leader_request_fraction, unsafe);

DEFINE_double(fault_crash_after_leader_request_fraction, 0.0,
              "Fraction of the time when the leader will crash on getting a response for an "
              "UpdateConsensus RPC. (For testing only!)");
TAG_FLAG(fault_crash_after_leader_request_fraction, unsafe);


// Allow for disabling Tablet Copy in unit tests where we want to test
// certain scenarios without triggering bootstrap of a remote peer.
DEFINE_bool(enable_tablet_copy, true,
            "Whether Tablet Copy will be initiated by the leader when it "
            "detects that a follower is out of date or does not have a tablet "
            "replica. For testing purposes only.");
TAG_FLAG(enable_tablet_copy, unsafe);

DEFINE_int32(raft_proxy_max_hops, 16,
             "Maximum proxy routing hops allowed. In other words, the proxy routing TTL");
TAG_FLAG(raft_proxy_max_hops, advanced);

DECLARE_int32(raft_heartbeat_interval_ms);

DEFINE_int32(proxy_batch_duration_ms, 0,
             "Time (in ms) to wait before reading ops for proxy requests");

DECLARE_bool(raft_enforce_rpc_token);

METRIC_DEFINE_counter(server, raft_rpc_token_num_response_mismatches,
                           "Reponse RPC token mismatches",
                           kudu::MetricUnit::kRequests,
                           "Number of RPC responses that did not have a token "
                           "that matches this instance's");

using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::Messenger;
using kudu::rpc::PeriodicTimer;
using kudu::rpc::RpcController;
//using kudu::tserver::TabletServerErrorPB;
using std::shared_ptr;
using std::string;
using std::vector;
using std::weak_ptr;
using strings::Substitute;


namespace kudu {
namespace consensus {

Status Peer::NewRemotePeer(RaftPeerPB peer_pb,
                           string tablet_id,
                           string leader_uuid,
                           PeerMessageQueue* queue,
                           PeerProxyPool* peer_proxy_pool,
                           ThreadPoolToken* raft_pool_token,
                           shared_ptr<PeerProxy> proxy,
                           shared_ptr<Messenger> messenger,
                           shared_ptr<Peer>* peer) {

  shared_ptr<Peer> new_peer(new Peer(std::move(peer_pb),
                                     std::move(tablet_id),
                                     std::move(leader_uuid),
                                     queue,
                                     peer_proxy_pool,
                                     raft_pool_token,
                                     std::move(proxy),
                                     std::move(messenger)));
  RETURN_NOT_OK(new_peer->Init());
  *peer = std::move(new_peer);
  return Status::OK();
}

Peer::Peer(RaftPeerPB peer_pb,
           string tablet_id,
           string leader_uuid,
           PeerMessageQueue* queue,
           PeerProxyPool* peer_proxy_pool,
           ThreadPoolToken* raft_pool_token,
           shared_ptr<PeerProxy> proxy,
           shared_ptr<Messenger> messenger)
    : tablet_id_(std::move(tablet_id)),
      leader_uuid_(std::move(leader_uuid)),
      peer_pb_(std::move(peer_pb)),
      proxy_(std::move(proxy)),
      queue_(queue),
      peer_proxy_pool_(peer_proxy_pool),
      failed_attempts_(0),
      last_request_time_(MonoTime::Now()),
      messenger_(std::move(messenger)),
      raft_pool_token_(raft_pool_token) {
  request_pending_ = false;
}

Status Peer::Init() {
  {
    std::lock_guard<simple_spinlock> l(peer_lock_);
    queue_->TrackPeer(peer_pb_);
  }

  // Capture a weak_ptr reference into the functor so it can safely handle
  // outliving the peer.
  weak_ptr<Peer> w = shared_from_this();
  heartbeater_ = PeriodicTimer::Create(
      messenger_,
      [w]() {
        if (auto p = w.lock()) {
          p->SignalRequest(true, true);
        }
      },
      MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms));
  heartbeater_->Start();
  return Status::OK();
}

Status Peer::SignalRequest(bool even_if_queue_empty, bool from_heartbeater) {
  // Only allow one request at a time. No sense waking up the
  // raft thread pool if the task will just abort anyway.
  //
  // "request_pending_" is an atomic, hence no need to take peer_lock_ here.
  // This allows to return early without blocking on "peer_lock_". Note that
  // "peer_lock_" is also held during Peer::SendNextRequest(...) which could
  // take some time for a lagging peer as it involves multiple disk IO
  if (request_pending_) {
    return Status::OK();
  }

  std::lock_guard<simple_spinlock> l(peer_lock_);

  if (PREDICT_FALSE(closed_)) {
    return Status::IllegalState("Peer was closed.");
  }

  // For proxied peers we only send requests every FLAGS_proxy_batch_duration_ms
  // milliseconds
  if (!from_heartbeater && !ProxyBatchDurationHasPassed()) {
    return Status::OK();
  }

  // Capture a weak_ptr reference into the submitted functor so that we can
  // safely handle the functor outliving its peer.
  weak_ptr<Peer> w_this = shared_from_this();
  RETURN_NOT_OK(raft_pool_token_->SubmitFunc(
        [even_if_queue_empty, from_heartbeater, w_this]() {
    if (auto p = w_this.lock()) {
      p->SendNextRequest(even_if_queue_empty, from_heartbeater);
    }
  }));
  return Status::OK();
}

bool Peer::ProxyBatchDurationHasPassed() {
  if (FLAGS_proxy_batch_duration_ms == 0) {
    return true;
  }

  const bool has_duration_passed = MonoTime::Now() - last_request_time_ >=
    MonoDelta::FromMilliseconds(FLAGS_proxy_batch_duration_ms);

  // We update cached proxied status when batch duration has passed (or it's not
  // populated yet), this means we could be looking at stale info for
  // FLAGS_proxy_batch_duration_ms
  if (has_duration_passed || cached_is_peer_proxied_ == -1) {
    const std::string& uuid = peer_pb_.permanent_uuid();
    std::string next_hop_uuid;
    // TODO: The routing table is consulted again in RequestForPeer(), ideally we
    // can do it once
    queue_->GetNextRoutingHopFromLeader(uuid, &next_hop_uuid);
    cached_is_peer_proxied_ = next_hop_uuid != uuid;
  }

  return cached_is_peer_proxied_ != 1 || has_duration_passed;
}

void Peer::SendNextRequest(bool even_if_queue_empty, bool from_heartbeater) {
  std::unique_lock<simple_spinlock> l(peer_lock_);

  if (PREDICT_FALSE(closed_)) {
    return;
  }

  // Only allow one request at a time.
  if (request_pending_) {
    return;
  }

  // For the first request sent by the peer, we send it even if the queue is empty,
  // which it will always appear to be for the first request, since this is the
  // negotiation round.
  if (!has_sent_first_request_) {
    even_if_queue_empty = true;
    has_sent_first_request_ = true;
  }

  // If our last request generated an error, and this is not a normal
  // heartbeat request, then don't send the "per-op" request. Instead,
  // we'll wait for the heartbeat.
  //
  // TODO(todd): we could consider looking at the number of consecutive failed
  // attempts, and instead of ignoring the signal, ask the heartbeater
  // to "expedite" the next heartbeat in order to achieve something like
  // exponential backoff after an error. As it is implemented today, any
  // transient error will result in a latency blip as long as the heartbeat
  // period.
  if (failed_attempts_ > 0 && !even_if_queue_empty) {
    return;
  }

  if (!from_heartbeater && !ProxyBatchDurationHasPassed()) {
    return;
  }

  // The peer has no pending request nor is sending: send the request.
  bool needs_tablet_copy = false;

  // If this peer is not healthy (as indicated by failed_attempts_), then
  // degrade this peer to 'status-only' heartbeat request i.e donot read any ops
  // from log/log-cache to build the entire batch of ops to be sent to this
  // peer. This ensures that leader is not doing the expensive operation of
  // reading from log-cache to build the message for a peer that is not
  // reachable.
  bool read_ops = (failed_attempts_ <= 0);

  request_pending_ = true;

  last_request_time_ = MonoTime::Now();

  // The next hop to route to to ship messages to this peer. This could be
  // different than the peer_uuid when proxy is enabled
  string next_hop_uuid;
  int64_t commit_index_before = request_.has_committed_index() ?
      request_.committed_index() : kMinimumOpIdIndex;
  Status s = queue_->RequestForPeer(peer_pb_.permanent_uuid(), read_ops, &request_,
                                    &replicate_msg_refs_, &needs_tablet_copy,
                                    &next_hop_uuid);
  int64_t commit_index_after = request_.has_committed_index() ?
      request_.committed_index() : kMinimumOpIdIndex;

  if (PREDICT_FALSE(!s.ok())) {
    // Incrementing failed_attempts_ prevents a RequestForPeer error to continually
    // trigger an error on every actual write. The next attempt to RequestForPeer
    // will now be restricted to Heartbeats, but because this is hard failure
    // it will keep failing but only fail less often.
    // TODO -
    // Make empty heartbeats go through after a failure to send actual messages,
    // without changing the cursor at all on peer, but still maintaining authority on
    // it. Otherwise node keeps asking for votes, destabilizing cluster.
    failed_attempts_++;
    VLOG_WITH_PREFIX_UNLOCKED(1) << s.ToString();
    request_pending_ = false;
    return;
  }

#ifdef FB_DO_NOT_REMOVE
  if (PREDICT_FALSE(needs_tablet_copy)) {
    Status s = PrepareTabletCopyRequest();
    if (s.ok()) {
      controller_.Reset();
      request_pending_ = true;
      l.unlock();
      // Capture a shared_ptr reference into the RPC callback so that we're guaranteed
      // that this object outlives the RPC.
      shared_ptr<Peer> s_this = shared_from_this();
      proxy_->StartTabletCopyAsync(&tc_request_, &tc_response_, &controller_,
                                   [s_this]() {
                                     s_this->ProcessTabletCopyResponse();
                                   });
    } else {
      LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to generate Tablet Copy request for peer: "
                                        << s.ToString();
      request_pending_ = false;
    }
    return;
  }
#endif

  request_.set_tablet_id(tablet_id_);
  request_.set_caller_uuid(leader_uuid_);
  request_.set_dest_uuid(peer_pb_.permanent_uuid());

  bool req_has_ops = request_.ops_size() > 0 || (commit_index_after > commit_index_before);
  // If the queue is empty, check if we were told to send a status-only
  // message, if not just return.
  if (PREDICT_FALSE(!req_has_ops && !even_if_queue_empty)) {
    request_pending_ = false;
    return;
  }

  if (req_has_ops) {
    // If we're actually sending ops there's no need to heartbeat for a while.
    heartbeater_->Snooze();
  }

  MAYBE_FAULT(FLAGS_fault_crash_on_leader_request_fraction);


  VLOG_WITH_PREFIX_UNLOCKED(2) << "Sending to peer " << peer_pb().permanent_uuid() << ": "
      << SecureShortDebugString(request_);
  controller_.Reset();

  l.unlock();
  // Capture a shared_ptr reference into the RPC callback so that we're guaranteed
  // that this object outlives the RPC.
  shared_ptr<Peer> s_this = shared_from_this();

  // TODO: Refactor this code. Ideally all fields in 'request_' related to
  // proxying should be set inside PeerMessageQueue::RequestForPeer(). Move the
  // setting of 'proxy_hops_remaining' to PeerMessageQueue::RequestForPeer()
  if (next_hop_uuid != peer_pb().permanent_uuid()) {
    // If this is a proxy request, set the hops remaining value.
    request_.set_proxy_hops_remaining(FLAGS_raft_proxy_max_hops);
  }

  shared_ptr<PeerProxy> next_hop_proxy = peer_proxy_pool_->Get(next_hop_uuid);
  if (!next_hop_proxy) {
    LOG_WITH_PREFIX_UNLOCKED(FATAL) << "peer with uuid " << next_hop_uuid
                                    << " not found in peer proxy pool";
  }

  next_hop_proxy->UpdateAsync(&request_, &response_, &controller_,
                              [s_this]() {
                                s_this->ProcessResponse();
                              });
}

Status Peer::StartElection(RunLeaderElectionRequestPB req) {
  RunLeaderElectionResponsePB resp;
  RpcController controller;
  req.set_dest_uuid(peer_pb().permanent_uuid());
  req.set_tablet_id(tablet_id_);
  RETURN_NOT_OK(proxy_->StartElection(&req, &resp, &controller));
  RETURN_NOT_OK(controller.status());
  if (resp.has_error()) {
    return StatusFromPB(response_.error().status());
  }
  return Status::OK();
}

void Peer::ProcessResponse() {
  // Note: This method runs on the reactor thread.
  std::unique_lock<simple_spinlock> lock(peer_lock_);
  if (closed_) {
    return;
  }
  CHECK(request_pending_);

  MAYBE_FAULT(FLAGS_fault_crash_after_leader_request_fraction);

  // Process RpcController errors.
  const auto controller_status = controller_.status();
  if (!controller_status.ok()) {
    auto ps = controller_status.IsRemoteError() ?
        PeerStatus::REMOTE_ERROR : PeerStatus::RPC_LAYER_ERROR;
    queue_->UpdatePeerStatus(peer_pb_.permanent_uuid(), ps, controller_status);
    ProcessResponseError(controller_status);
    return;
  }

  // Process CANNOT_PREPARE.
  // TODO(todd): there is no integration test coverage of this code path. Likely a bug in
  // this path is responsible for KUDU-1779.
  if (response_.status().has_error() &&
      response_.status().error().code() == consensus::ConsensusErrorPB::CANNOT_PREPARE) {
    Status response_status = StatusFromPB(response_.status().error().status());
    queue_->UpdatePeerStatus(peer_pb_.permanent_uuid(), PeerStatus::CANNOT_PREPARE,
                             response_status);
    ProcessResponseError(response_status);
    return;
  }

  // Process tserver-level errors.
  if (response_.has_error()) {
    Status response_status = StatusFromPB(response_.error().status());
    PeerStatus ps;
    ps = PeerStatus::REMOTE_ERROR;

    ServerErrorPB resp_error = response_.error();
    switch (response_.error().code()) {
      // We treat WRONG_SERVER_UUID as failed.
      case ServerErrorPB::WRONG_SERVER_UUID: FALLTHROUGH_INTENDED;
#ifdef FB_DO_NOT_REMOVE
      case TabletServerErrorPB::TABLET_FAILED:
        ps = PeerStatus::TABLET_FAILED;
        break;
      case TabletServerErrorPB::TABLET_NOT_FOUND:
        ps = PeerStatus::TABLET_NOT_FOUND;
        break;
#endif
      default:
        // Unknown kind of error.
        ps = PeerStatus::REMOTE_ERROR;
    }
    queue_->UpdatePeerStatus(peer_pb_.permanent_uuid(), ps, response_status);
    ProcessResponseError(response_status);
    return;
  }

  // The queue's handling of the peer response may generate IO (reads against
  // the WAL) and SendNextRequest() may do the same thing. So we run the rest
  // of the response handling logic on our thread pool and not on the reactor
  // thread.
  //
  // Capture a weak_ptr reference into the submitted functor so that we can
  // safely handle the functor outliving its peer.
  weak_ptr<Peer> w_this = shared_from_this();
  Status s = raft_pool_token_->SubmitFunc([w_this]() {
    if (auto p = w_this.lock()) {
      p->DoProcessResponse();

    }
  });
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to process peer response: " << s.ToString()
        << ": " << SecureShortDebugString(response_);
    request_pending_ = false;
  }
}

void Peer::DoProcessResponse() {

  VLOG_WITH_PREFIX_UNLOCKED(2) << "Response from peer " << peer_pb().permanent_uuid() << ": "
      << SecureShortDebugString(response_);

  bool send_more_immediately = queue_->ResponseFromPeer(peer_pb_.permanent_uuid(), response_);

  {
    std::unique_lock<simple_spinlock> lock(peer_lock_);
    CHECK(request_pending_);
    failed_attempts_ = 0;
    request_pending_ = false;
  }
  // We're OK to read the state_ without a lock here -- if we get a race,
  // the worst thing that could happen is that we'll make one more request before
  // noticing a close.
  if (send_more_immediately) {
    SendNextRequest(true);
  }
}

#ifdef FB_DO_NOT_REMOVE
Status Peer::PrepareTabletCopyRequest() {
  if (!FLAGS_enable_tablet_copy) {
    failed_attempts_++;
    return Status::NotSupported("Tablet Copy is disabled");
  }

  RETURN_NOT_OK(queue_->GetTabletCopyRequestForPeer(peer_pb_.permanent_uuid(), &tc_request_));

  return Status::OK();
}

void Peer::ProcessTabletCopyResponse() {
  // If the peer is already closed return.
  std::unique_lock<simple_spinlock> lock(peer_lock_);
  if (closed_) {
    return;
  }
  CHECK(request_pending_);
  request_pending_ = false;

  // If the response is OK, or ALREADY_INPROGRESS, then consider the RPC successful.
  const auto controller_status = controller_.status();
  bool success =
    controller_status.ok() &&
    (!tc_response_.has_error() ||
     tc_response_.error().code() == TabletServerErrorPB::TabletServerErrorPB::ALREADY_INPROGRESS);

  if (success) {
    lock.unlock();
    queue_->UpdatePeerStatus(peer_pb_.permanent_uuid(), PeerStatus::OK, Status::OK());
  } else if (!tc_response_.has_error() ||
              tc_response_.error().code() != TabletServerErrorPB::TabletServerErrorPB::THROTTLED) {
    // THROTTLED is a common response after a tserver with many replicas fails;
    // logging it would generate a great deal of log spam.
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to start Tablet Copy on peer: "
                                      << (controller_status.ok() ?
                                          SecureShortDebugString(tc_response_) :
                                          controller_status.ToString());
  }
}
#endif

void Peer::ProcessResponseError(const Status& status) {
  string resp_err_info;

#ifdef FB_DO_NOT_REMOVE
  if (response_.has_error()) {
    resp_err_info = Substitute(" Error code: $0 ($1).",
                               TabletServerErrorPB::Code_Name(response_.error().code()),
                               response_.error().code());
  }
#endif

  request_pending_ = false;

  if (status.IsIllegalState() &&
      status.ToString().find("Previous Rotate Event with") != std::string::npos) {
    // This is expected rotation delay. Do not log error and return early
    return;
  }

  // Increment failed attempts only when this is not an expected rejection by a
  // peer due to file rotation
  failed_attempts_++;
  KLOG_EVERY_N_SECS(WARNING, 300)
      << LogPrefixUnlocked() << "Couldn't send request to peer "
      << peer_pb_.permanent_uuid() << " for tablet " << tablet_id_ << "."
      << resp_err_info
      << " Status: " << status.ToString() << "."
      << " Retrying in the next heartbeat period."
      << " Already tried " << failed_attempts_ << " times.";
}

string Peer::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1 -> Peer $2 ($3:$4): ",
                    tablet_id_, leader_uuid_, peer_pb_.permanent_uuid(),
                    peer_pb_.last_known_addr().host(), peer_pb_.last_known_addr().port());
}

void Peer::Close() {
  // If the peer is already closed return.
  {
    std::lock_guard<simple_spinlock> lock(peer_lock_);
    if (closed_) return;
    closed_ = true;
  }
  KLOG_EVERY_N(INFO, 5) << LogPrefixUnlocked() << "Closing peer [EVERY 5]: " << peer_pb_.permanent_uuid();

  queue_->UntrackPeer(peer_pb_.permanent_uuid());
}

Peer::~Peer() {
  Close();
  if (heartbeater_) {
    heartbeater_->Stop();
  }

  // We don't own the ops (the queue does).
  request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), nullptr);
}

shared_ptr<PeerProxy> PeerProxyPool::Get(const string& uuid) const {
  shared_lock<rw_spinlock> l(lock_.get_lock());
  return FindWithDefault(peer_proxy_map_, uuid, std::shared_ptr<PeerProxy>());
}

void PeerProxyPool::Put(const string& uuid, shared_ptr<PeerProxy> proxy) {
  std::lock_guard<percpu_rwlock> l(lock_);
  peer_proxy_map_[uuid] = std::move(proxy);
}

void PeerProxyPool::Clear() {
  std::lock_guard<percpu_rwlock> l(lock_);
  peer_proxy_map_.clear();
}

template <class RespType>
void CheckAndEnforceResponseToken(
    const std::string& method_name, RespType* response,
    boost::optional<std::string> rpc_token,
    const scoped_refptr<Counter>& mismatch_counter) {
  if (!rpc_token && !response->has_raft_rpc_token()) {
    // Empty on both, nothing to enforce
    return;
  }

  if (rpc_token && response->has_raft_rpc_token() &&
      *rpc_token == response->raft_rpc_token()) {
    // Tokens match
    return;
  }

  mismatch_counter->Increment();

  auto error_message = Substitute(
      "Raft RPC token mismatch on response. Request token: $0. "
      "Response token: $1",
      rpc_token ? *rpc_token : "<null>",
      response->has_raft_rpc_token() ? response->raft_rpc_token() : "<null>");

  if (!FLAGS_raft_enforce_rpc_token) {
    // Mismatch but don't enforce
    KLOG_EVERY_N_SECS(WARNING, 300)
        << method_name
        << ": Token mismatch ignored: " << std::move(error_message);
    return;
  }

  KLOG_EVERY_N_SECS(ERROR, 60)
      << method_name << ": Rejecting RPC response: " << error_message;

  // We're rejecting the response, clear everything to prevent leaks
  response->Clear();
  ServerErrorPB* error = response->mutable_error();
  StatusToPB(Status::NotAuthorized(std::move(error_message)),
             error->mutable_status());
  error->set_code(ServerErrorPB::RING_TOKEN_MISMATCH);
}

RpcPeerProxy::RpcPeerProxy(gscoped_ptr<HostPort> hostport,
                           shared_ptr<ConsensusServiceProxy> consensus_proxy,
                           scoped_refptr<Counter> num_rpc_token_mismatches)
    : hostport_(std::move(hostport)),
      consensus_proxy_(std::move(consensus_proxy)),
      num_rpc_token_mismatches_(std::move(num_rpc_token_mismatches)) {
  DCHECK(hostport_ != NULL);
  DCHECK(consensus_proxy_ != NULL);
  DCHECK(num_rpc_token_mismatches_ != nullptr);
}

void RpcPeerProxy::UpdateAsync(const ConsensusRequestPB* request,
                               ConsensusResponsePB* response,
                               rpc::RpcController* controller,
                               const rpc::ResponseCallback& callback) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));

  boost::optional<std::string> rpc_token = request->has_raft_rpc_token()
                                               ? request->raft_rpc_token()
                                               : boost::optional<std::string>();
  consensus_proxy_->UpdateConsensusAsync(
      *request, response, controller,
      [callback, response, request_token = std::move(rpc_token),
       mismatch_counter = num_rpc_token_mismatches_]() {
        // Should not need to lock here since only one request can happen at any
        // time
        CheckAndEnforceResponseToken("UpdateAsync", response, request_token,
                                     mismatch_counter);
        callback();
      });
}

Status RpcPeerProxy::StartElection(const RunLeaderElectionRequestPB* request,
                                 RunLeaderElectionResponsePB* response,
                                 rpc::RpcController* controller) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));
  return consensus_proxy_->RunLeaderElection(*request, response, controller);
}

void RpcPeerProxy::RequestConsensusVoteAsync(const VoteRequestPB* request,
                                             VoteResponsePB* response,
                                             rpc::RpcController* controller,
                                             const rpc::ResponseCallback& callback) {
  boost::optional<std::string> rpc_token = request->has_raft_rpc_token()
                                               ? request->raft_rpc_token()
                                               : boost::optional<std::string>();
  consensus_proxy_->RequestConsensusVoteAsync(
      *request, response, controller,
      [callback, response, request_token = std::move(rpc_token),
       mismatch_counter = num_rpc_token_mismatches_]() {
        CheckAndEnforceResponseToken("RequestConsensusVoteAsync", response,
                                     request_token, mismatch_counter);
        callback();
      });
}

#ifdef FB_DO_NOT_REMOVE
void RpcPeerProxy::StartTabletCopyAsync(const StartTabletCopyRequestPB* request,
                                        StartTabletCopyResponsePB* response,
                                        rpc::RpcController* controller,
                                        const rpc::ResponseCallback& callback) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));
  consensus_proxy_->StartTabletCopyAsync(*request, response, controller, callback);
}
#endif

string RpcPeerProxy::PeerName() const {
  return hostport_->ToString();
}

namespace {

Status CreateConsensusServiceProxyForHost(const shared_ptr<Messenger>& messenger,
                                          const HostPort& hostport,
                                          shared_ptr<ConsensusServiceProxy>* new_proxy) {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(hostport.ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING)<< "Peer address '" << hostport.ToString() << "' "
    << "resolves to " << addrs.size() << " different addresses. Using "
    << addrs[0].ToString();
  }
  new_proxy->reset(new ConsensusServiceProxy(messenger, addrs[0], hostport.host()));
  return Status::OK();
}

} // anonymous namespace

RpcPeerProxyFactory::RpcPeerProxyFactory(
    shared_ptr<Messenger> messenger,
    const scoped_refptr<MetricEntity>& metric_entity)
    : messenger_(std::move(messenger)),
      num_rpc_token_mismatches_(metric_entity->FindOrCreateCounter(
          &METRIC_raft_rpc_token_num_response_mismatches)) {}

Status RpcPeerProxyFactory::NewProxy(const RaftPeerPB& peer_pb,
                                     shared_ptr<PeerProxy>* proxy) {
  gscoped_ptr<HostPort> hostport(new HostPort);
  RETURN_NOT_OK(HostPortFromPB(peer_pb.last_known_addr(), hostport.get()));
  shared_ptr<ConsensusServiceProxy> new_proxy;
  RETURN_NOT_OK(CreateConsensusServiceProxyForHost(messenger_, *hostport, &new_proxy));
  proxy->reset(new RpcPeerProxy(std::move(hostport), std::move(new_proxy),
                                num_rpc_token_mismatches_));
  return Status::OK();
}

RpcPeerProxyFactory::~RpcPeerProxyFactory() {}

Status SetPermanentUuidForRemotePeer(const shared_ptr<Messenger>& messenger,
                                     RaftPeerPB* remote_peer) {
  DCHECK(!remote_peer->has_permanent_uuid());
  HostPort hostport;
  RETURN_NOT_OK(HostPortFromPB(remote_peer->last_known_addr(), &hostport));
  shared_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(CreateConsensusServiceProxyForHost(messenger, hostport, &proxy));
  GetNodeInstanceRequestPB req;
  GetNodeInstanceResponsePB resp;
  rpc::RpcController controller;

  // TODO generalize this exponential backoff algorithm, as we do the
  // same thing in catalog_manager.cc
  // (AsyncTabletRequestTask::RpcCallBack).
  MonoTime deadline = MonoTime::Now() +
      MonoDelta::FromMilliseconds(FLAGS_raft_get_node_instance_timeout_ms);
  int attempt = 1;
  while (true) {
    VLOG(2) << "Getting uuid from remote peer. Request: " << SecureShortDebugString(req);

    controller.Reset();
    Status s = proxy->GetNodeInstance(req, &resp, &controller);
    if (s.ok()) {
      if (controller.status().ok()) {
        break;
      }
      s = controller.status();
    }

    LOG(WARNING) << "Error getting permanent uuid from config peer " << hostport.ToString() << ": "
                 << s.ToString();
    MonoTime now = MonoTime::Now();
    if (now < deadline) {
      int64_t remaining_ms = (deadline - now).ToMilliseconds();
      int64_t base_delay_ms = 1LL << (attempt + 3); // 1st retry delayed 2^4 ms, 2nd 2^5, etc..
      int64_t jitter_ms = rand() % 50; // Add up to 50ms of additional random delay.
      int64_t delay_ms = std::min<int64_t>(base_delay_ms + jitter_ms, remaining_ms);
      VLOG(1) << "Sleeping " << delay_ms << " ms. before retrying to get uuid from remote peer...";
      SleepFor(MonoDelta::FromMilliseconds(delay_ms));
      LOG(INFO) << "Retrying to get permanent uuid for remote peer: "
          << SecureShortDebugString(*remote_peer) << " attempt: " << attempt++;
    } else {
      s = Status::TimedOut(Substitute("Getting permanent uuid from $0 timed out after $1 ms.",
                                      hostport.ToString(),
                                      FLAGS_raft_get_node_instance_timeout_ms),
                           s.ToString());
      return s;
    }
  }
  remote_peer->set_permanent_uuid(resp.node_instance().permanent_uuid());
  return Status::OK();
}

}  // namespace consensus
}  // namespace kudu
