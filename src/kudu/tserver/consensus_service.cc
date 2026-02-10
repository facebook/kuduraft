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

#include "kudu/tserver/consensus_service.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>

#include <folly/ScopeGuard.h>
#include <folly/stop_watch.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/clock/clock.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/server/server_base.h"
#include "kudu/tserver/simple_tablet_manager.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

DECLARE_int32(memory_limit_warn_threshold_percentage);

using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::GetNodeInstanceRequestPB;
using kudu::consensus::GetNodeInstanceResponsePB;
using kudu::consensus::LeaderElectionContextPB;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RunLeaderElectionRequestPB;
using kudu::consensus::RunLeaderElectionResponsePB;
using kudu::consensus::ServerErrorPB;
using kudu::consensus::UnsafeChangeConfigRequestPB;
using kudu::consensus::UnsafeChangeConfigResponsePB;
using kudu::consensus::VoteRequestPB;
using kudu::consensus::VoteResponsePB;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcContext;
using kudu::server::ServerBase;
using std::shared_ptr;
using std::string;

METRIC_DEFINE_counter(
    server,
    raft_rpc_token_num_request_mismatches,
    "Request RPC token mismatches",
    kudu::MetricUnit::kRequests,
    "Number of RPC request that did not have a token "
    "that matches this instance's");

namespace kudu {

namespace tserver {

static void setupErrorAndRespond(
    ServerErrorPB* error,
    const Status& s,
    ServerErrorPB::Code code,
    rpc::RpcContext* context) {
  // Generic "service unavailable" errors will cause the client to retry later.
  if ((code == ServerErrorPB::UNKNOWN_ERROR /*||
       code == TabletServerErrorPB::THROTTLED */) && s.IsServiceUnavailable()) {
    context->respondRpcFailure(rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  statusToPb(s, error->mutable_status());
  error->set_code(code);
  context->respondNoCache();
}

namespace {

template <class ReqClass, class RespClass>
bool checkUuidMatchOrRespondGeneric(
    TabletManagerIf& tabletManager,
    const char* methodName,
    const ReqClass* req,
    RespClass* resp,
    rpc::RpcContext* context) {
  const string& localUuid = tabletManager.NodeInstance().permanent_uuid();
  if (PREDICT_FALSE(!req->has_dest_uuid())) {
    // Maintain compat in release mode, but complain.
    string msg = fmt::format(
        "{}: Missing destination UUID in request from {}: {}",
        methodName,
        context->requestor_string(),
        SecureShortDebugString(*req));
#ifdef NDEBUG
    KLOG_EVERY_N(ERROR, 100) << msg;
#else
    LOG(DFATAL) << msg;
#endif
    return true;
  }
  if (PREDICT_FALSE(req->dest_uuid() != localUuid)) {
    Status s = Status::InvalidArgument(
        fmt::format(
            "{}: Wrong destination UUID requested. "
            "Local UUID: {}. Requested UUID: {}",
            methodName,
            localUuid,
            req->dest_uuid()));
    LOG(WARNING) << s.ToString() << ": from " << context->requestor_string()
                 << ": " << SecureShortDebugString(*req);
    setupErrorAndRespond(
        resp->mutable_error(), s, ServerErrorPB::WRONG_SERVER_UUID, context);
    return false;
  }
  return true;
}

template <class ReqClass, class RespClass>
bool checkUuidMatchOrRespond(
    TabletManagerIf& tabletManager,
    const char* methodName,
    const ReqClass* req,
    RespClass* resp,
    rpc::RpcContext* context) {
  return checkUuidMatchOrRespondGeneric(
      tabletManager, methodName, req, resp, context);
}

template <>
bool checkUuidMatchOrRespond(
    TabletManagerIf& tabletManager,
    const char* methodName,
    const ConsensusRequestPB* req,
    ConsensusResponsePB* resp,
    rpc::RpcContext* context) {
  const string& localUuid = tabletManager.NodeInstance().permanent_uuid();
  if (req->has_proxy_dest_uuid()) {
    if (PREDICT_FALSE(req->proxy_dest_uuid() != localUuid)) {
      Status s = Status::InvalidArgument(
          fmt::format(
              "{}: Wrong proxy UUID requested. "
              "Local UUID: {}. Requested UUID: {}",
              methodName,
              localUuid,
              req->proxy_dest_uuid()));
      LOG(WARNING) << s.ToString() << ": from " << context->requestor_string()
                   << ": " << SecureShortDebugString(*req);
      setupErrorAndRespond(
          resp->mutable_error(), s, ServerErrorPB::WRONG_SERVER_UUID, context);
      return false;
    }
    return true;
  }
  return checkUuidMatchOrRespondGeneric(
      tabletManager, methodName, req, resp, context);
}

template <class ReqClass, class RespClass>
bool getConsensusOrRespond(
    TabletManagerIf& tabletManager,
    ReqClass* req,
    RespClass* resp,
    rpc::RpcContext* context,
    shared_ptr<RaftConsensus>* consensusOut) {
  shared_ptr<RaftConsensus> tmpConsensus =
      tabletManager.shared_consensus(req->tablet_id());
  if (!tmpConsensus) {
    Status s = Status::ServiceUnavailable(
        "Raft Consensus unavailable", "Tablet replica not initialized");
    setupErrorAndRespond(
        resp->mutable_error(),
        s,
        ServerErrorPB::CONSENSUS_NOT_RUNNING,
        context);
    return false;
  }
  *consensusOut = std::move(tmpConsensus);
  return true;
}

template <class ReqType, class RespType>
bool checkRaftRpcTokenOrRespond(
    const std::string& methodName,
    const ReqType* req,
    RespType resp,
    rpc::RpcContext* context,
    const consensus::RaftConsensus& consensus,
    const std::shared_ptr<Counter>& mismatchCounter) {
  const auto& ownToken = consensus.getRaftRpcToken();
  if (!ownToken && !req->has_raft_rpc_token()) {
    // Empty on both, nothing to enforce
    return true;
  }

  if (ownToken && req->has_raft_rpc_token() &&
      *ownToken == req->raft_rpc_token()) {
    // Tokens match
    return true;
  }

  mismatchCounter->Increment();

  auto errorMessage = fmt::format(
      "Raft RPC token mismatch. Receiver token: {}. Request token: {}",
      ownToken ? *ownToken : "<null>",
      req->has_raft_rpc_token() ? req->raft_rpc_token() : "<null>");

  if (!consensus.shouldEnforceRaftRpcToken()) {
    // Mismatch but don't enforce
    KLOG_EVERY_N_SECS(WARNING, 300)
        << methodName
        << ": Token mismatch ignored: " << std::move(errorMessage);
    return true;
  }

  KLOG_EVERY_N_SECS(ERROR, 60)
      << methodName << ": Rejecting incoming RPC: " << errorMessage;
  setupErrorAndRespond(
      resp->mutable_error(),
      Status::NotAuthorized(std::move(errorMessage)),
      ServerErrorPB::RING_TOKEN_MISMATCH,
      context);
  return false;
}

template <class RespType>
void handleUnknownError(const Status& s, RespType* resp, RpcContext* context) {
  resp->Clear();
  setupErrorAndRespond(
      resp->mutable_error(), s, ServerErrorPB::UNKNOWN_ERROR, context);
}

template <class ReqType, class RespType>
void handleResponse(
    const ReqType* /* req */,
    RespType* resp,
    RpcContext* context,
    const Status& s) {
  if (PREDICT_FALSE(!s.ok())) {
    handleUnknownError(s, resp, context);
    return;
  }
  context->respondSuccess();
}

template <class ReqType, class RespType>
static StdStatusCallback
bindHandleResponse(const ReqType* req, RespType* resp, RpcContext* context) {
  return std::bind(
      &handleResponse<ReqType, RespType>,
      req,
      resp,
      context,
      std::placeholders::_1);
}

} // namespace

template <class ReqType, class RespType>
void handleErrorResponse(
    const ReqType* /* req */,
    RespType* resp,
    RpcContext* context,
    const std::optional<ServerErrorPB::Code>& errorCode,
    const Status& s) {
  resp->Clear();
  if (errorCode) {
    setupErrorAndRespond(resp->mutable_error(), s, *errorCode, context);
  } else {
    handleUnknownError(s, resp, context);
  }
}

ConsensusServiceImpl::ConsensusServiceImpl(
    ServerBase* server,
    TabletManagerIf& tabletManager)
    : ConsensusServiceIf(server->metricEntity(), server->resultTracker()),
      server_(server),
      tabletManager_(tabletManager),
      requestRpcTokenMismatches_(server->metricEntity()->FindOrCreateCounter(
          &METRIC_raft_rpc_token_num_request_mismatches)) {}

ConsensusServiceImpl::~ConsensusServiceImpl() = default;

bool ConsensusServiceImpl::AuthorizeServiceUser(
    const google::protobuf::Message* /*req*/,
    google::protobuf::Message* /*resp*/,
    rpc::RpcContext* rpc) {
  return server_->Authorize(
      rpc, ServerBase::SUPER_USER | ServerBase::SERVICE_USER);
}

void ConsensusServiceImpl::LongUpdateConsensusLoading() {
  if (shared_ptr<RaftConsensus> consensus =
          tabletManager_.shared_consensus("")) {
    consensus->PauseFailureDetector();
  }
}

void ConsensusServiceImpl::LongUpdateConsensusLoaded() {
  if (shared_ptr<RaftConsensus> consensus =
          tabletManager_.shared_consensus("")) {
    consensus->ResumeFailureDetector();
  }
}

void ConsensusServiceImpl::UpdateConsensus(
    const ConsensusRequestPB* req,
    ConsensusResponsePB* resp,
    rpc::RpcContext* context) {
  auto stopWatch = folly::stop_watch<std::chrono::microseconds>();
  auto setProcessTime =
      [](const folly::stop_watch<std::chrono::microseconds>& stopWatchUs,
         ConsensusResponsePB* response) {
        if (response) {
          response->set_server_process_time_us(stopWatchUs.elapsed().count());
        }
      };
  DVLOG(3) << "Received Consensus Update RPC: " << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(
          tabletManager_, "UpdateConsensus", req, resp, context)) {
    return;
  }

  // Submit the update directly to the TabletReplica's RaftConsensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }

  auto ownToken = consensus->getRaftRpcToken();
  if (ownToken) {
    // Stamp response token regardless of whether if it matches request so
    // sender can log and debug
    resp->set_raft_rpc_token(*ownToken);
  }

  if (!checkRaftRpcTokenOrRespond(
          "UpdateConsensus",
          req,
          resp,
          context,
          *consensus,
          requestRpcTokenMismatches_)) {
    return;
  }

  // Fast path for proxy requests.
  if (consensus->IsProxyRequest(req)) {
    consensus->HandleProxyRequest(req, resp, context);
    return;
  }

  Status s = consensus->Update(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    // Clear the response first, since a partially-filled response could
    // result in confusing a caller, or in having missing required fields
    // in embedded optional messages.
    resp->Clear();
    if (ownToken) {
      // Put the token back so real error can be logged
      resp->set_raft_rpc_token(*ownToken);
    }
    setProcessTime(stopWatch, resp);
    setupErrorAndRespond(
        resp->mutable_error(), s, ServerErrorPB::UNKNOWN_ERROR, context);
    return;
  }
  setProcessTime(stopWatch, resp);
  context->respondSuccess();
}

void ConsensusServiceImpl::RequestConsensusVote(
    const VoteRequestPB* req,
    VoteResponsePB* resp,
    rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Request Vote RPC: "
           << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(
          tabletManager_, "RequestConsensusVote", req, resp, context)) {
    return;
  }

  std::optional<OpId> lastLoggedOpId;
  // Submit the vote request directly to the consensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }

  if (auto ownToken = consensus->getRaftRpcToken()) {
    // Stamp response token regardless of whether if it matches request so
    // sender can log and debug
    resp->set_raft_rpc_token(*std::move(ownToken));
  }

  if (!checkRaftRpcTokenOrRespond(
          "RequestConsensusVote",
          req,
          resp,
          context,
          *consensus,
          requestRpcTokenMismatches_)) {
    return;
  }

  Status s = consensus->RequestVote(
      req,
      consensus::TabletVotingState(std::move(
          lastLoggedOpId) /*,
data_state*/),
      resp);
  if (PREDICT_FALSE(!s.ok())) {
    setupErrorAndRespond(
        resp->mutable_error(), s, ServerErrorPB::UNKNOWN_ERROR, context);
    return;
  }
  context->respondSuccess();
}

void ConsensusServiceImpl::ChangeConfig(
    const ChangeConfigRequestPB* req,
    ChangeConfigResponsePB* resp,
    RpcContext* context) {
  VLOG(1) << "Received ChangeConfig RPC: " << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(
          tabletManager_, "ChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }
  std::optional<ServerErrorPB::Code> errorCode;
  Status s = consensus->ChangeConfig(
      *req, bindHandleResponse(req, resp, context), &errorCode);
  if (PREDICT_FALSE(!s.ok())) {
    handleErrorResponse(req, resp, context, errorCode, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::BulkChangeConfig(
    const BulkChangeConfigRequestPB* req,
    ChangeConfigResponsePB* resp,
    RpcContext* context) {
  VLOG(1) << "Received BulkChangeConfig RPC: " << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(
          tabletManager_, "BulkChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }
  std::optional<ServerErrorPB::Code> errorCode;
  Status s = consensus->BulkChangeConfig(
      *req, bindHandleResponse(req, resp, context), &errorCode);
  if (PREDICT_FALSE(!s.ok())) {
    handleErrorResponse(req, resp, context, errorCode, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::UnsafeChangeConfig(
    const UnsafeChangeConfigRequestPB* req,
    UnsafeChangeConfigResponsePB* resp,
    RpcContext* context) {
  LOG(INFO) << "Received UnsafeChangeConfig RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!checkUuidMatchOrRespond(
          tabletManager_, "UnsafeChangeConfig", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }
  std::optional<ServerErrorPB::Code> errorCode;
  const Status s = consensus->UnsafeChangeConfig(*req, &errorCode);
  if (PREDICT_FALSE(!s.ok())) {
    handleErrorResponse(req, resp, context, errorCode, s);
    return;
  }
  context->respondSuccess();
}

void ConsensusServiceImpl::ChangeProxyTopology(
    const consensus::ChangeProxyTopologyRequestPB* req,
    consensus::ChangeProxyTopologyResponsePB* resp,
    rpc::RpcContext* context) {
  LOG(INFO) << "Received ChangeProxyTopology RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!checkUuidMatchOrRespond(
          tabletManager_, "ChangeProxyTopology", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }

  handleResponse(
      req, resp, context, consensus->ChangeProxyTopology(req->new_config()));
}

void ConsensusServiceImpl::GetNodeInstance(
    const GetNodeInstanceRequestPB* req,
    GetNodeInstanceResponsePB* resp,
    rpc::RpcContext* context) {
  VLOG(1) << "Received Get Node Instance RPC: " << SecureDebugString(*req);
  resp->mutable_node_instance()->CopyFrom(tabletManager_.NodeInstance());
  context->respondSuccess();
}

void ConsensusServiceImpl::RunLeaderElection(
    const RunLeaderElectionRequestPB* req,
    RunLeaderElectionResponsePB* resp,
    rpc::RpcContext* context) {
  LOG(INFO) << "Received Run Leader Election RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!checkUuidMatchOrRespond(
          tabletManager_, "RunLeaderElection", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }

  if (!checkRaftRpcTokenOrRespond(
          "RunLeaderElection",
          req,
          resp,
          context,
          *consensus,
          requestRpcTokenMismatches_)) {
    return;
  }

  consensus::ElectionMode mode = consensus::ELECT_EVEN_IF_LEADER_IS_ALIVE;
  std::optional<OpId> mockElectionSnapshotOpId;
  if (req->has_mock_election_snapshot_op_id()) {
    mode = consensus::MOCK_ELECTION;
    mockElectionSnapshotOpId = req->mock_election_snapshot_op_id();
  }

  std::function<void(const consensus::ElectionResult&)> callback;
  bool waitForDecision =
      (req->has_wait_for_decision() && req->wait_for_decision()) ||
      mode == consensus::MOCK_ELECTION;

  if (waitForDecision) {
    callback = std::bind(
        [resp](rpc::RpcContext* ctx, const consensus::ElectionResult& result) {
          resp->set_election_won(
              result.decision == consensus::ElectionVote::VOTE_GRANTED);
          ctx->respondSuccess();
        },
        context,
        std::placeholders::_1);
  }

  Status s;
  if (req->has_election_context()) {
    const LeaderElectionContextPB& ctx = req->election_context();
    // original_start_time in protobuf is nanoseconds since epoch
    std::chrono::system_clock::time_point requestStart(
        std::chrono::duration_cast<std::chrono::system_clock::duration>(
            std::chrono::nanoseconds(ctx.original_start_time())));
    s = consensus->startElection(
        mode,
        {consensus::ElectionReason::kExternalRequest,
         requestStart,
         std::move(mockElectionSnapshotOpId),
         ctx.original_uuid(),
         ctx.is_origin_dead_promotion()},
        callback);
  } else {
    s = consensus->startElection(
        mode,
        {consensus::ElectionReason::kExternalRequest,
         std::chrono::system_clock::now(),
         std::move(mockElectionSnapshotOpId)},
        callback);
  }

  if (PREDICT_FALSE(!s.ok())) {
    setupErrorAndRespond(
        resp->mutable_error(), s, ServerErrorPB::UNKNOWN_ERROR, context);
    return;
  }

  if (!waitForDecision) {
    context->respondSuccess();
  }
}

void ConsensusServiceImpl::LeaderStepDown(
    const LeaderStepDownRequestPB* req,
    LeaderStepDownResponsePB* resp,
    RpcContext* context) {
  LOG(INFO) << "Received LeaderStepDown RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!checkUuidMatchOrRespond(
          tabletManager_, "LeaderStepDown", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }
  Status s = consensus->StepDown(resp);
  if (PREDICT_FALSE(!s.ok())) {
    setupErrorAndRespond(
        resp->mutable_error(), s, ServerErrorPB::UNKNOWN_ERROR, context);
    return;
  }
  context->respondSuccess();
}

void ConsensusServiceImpl::GetLastOpId(
    const consensus::GetLastOpIdRequestPB* req,
    consensus::GetLastOpIdResponsePB* resp,
    rpc::RpcContext* context) {
  DVLOG(3) << "Received GetLastOpId RPC: " << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(
          tabletManager_, "GetLastOpId", req, resp, context)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!getConsensusOrRespond(tabletManager_, req, resp, context, &consensus)) {
    return;
  }
  if (PREDICT_FALSE(req->opid_type() == consensus::UNKNOWN_OPID_TYPE)) {
    handleUnknownError(
        Status::InvalidArgument("Invalid opid_type specified to GetLastOpId()"),
        resp,
        context);
    return;
  }
  std::optional<OpId> opid = consensus->GetLastOpId(req->opid_type());
  if (!opid) {
    setupErrorAndRespond(
        resp->mutable_error(),
        Status::IllegalState("Cannot fetch last OpId in WAL"),
        ServerErrorPB::CONSENSUS_NOT_RUNNING,
        context);
    return;
  }
  *resp->mutable_opid() = *opid;
  context->respondSuccess();
}

void ConsensusServiceImpl::GetConsensusState(
    const consensus::GetConsensusStateRequestPB* /* req */,
    consensus::GetConsensusStateResponsePB* /* resp */,
    rpc::RpcContext* context) {
#if 0
  DVLOG(3) << "Received GetConsensusState RPC: " << SecureDebugString(*req);
  if (!checkUuidMatchOrRespond(tabletManager_, "GetConsensusState", req, resp, context)) {
    return;
  }

  unordered_set<string> requested_ids(req->tablet_ids().begin(), req->tablet_ids().end());
  bool all_ids = requested_ids.empty();

  vector<std::shared_ptr<TabletReplica>> tablet_replicas;
  tabletManager_.GetTabletReplicas(&tablet_replicas);
  for (const std::shared_ptr<TabletReplica>& replica : tablet_replicas) {
    if (!all_ids && !requested_ids.contains(replica->tablet_id())) {
      continue;
    }

    shared_ptr<RaftConsensus> consensus(replica->shared_consensus());
    if (!consensus) {
      continue;
    }

    consensus::GetConsensusStateResponsePB_TabletConsensusInfoPB tablet_info;
    Status s = consensus->ConsensusState(tablet_info.mutable_cstate(), req->report_health());
    if (!s.ok()) {
      DCHECK(s.IsIllegalState()) << s.ToString();
      continue;
    }
    tablet_info.set_tablet_id(replica->tablet_id());
    *resp->add_tablets() = std::move(tablet_info);
  }
  const auto scheme = FLAGS_raft_prepare_replacement_before_eviction
      ? consensus::ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : consensus::ReplicaManagementInfoPB::EVICT_FIRST;
  resp->mutable_replica_management_info()->set_replacement_scheme(scheme);

#endif

  context->respondSuccess();
}

} // namespace tserver
} // namespace kudu
