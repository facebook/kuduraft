// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/vote_request_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

namespace {
const std::string kEmptyString;
} // namespace

ElectionMode fromPbElectionMode(::kudu::consensus::ElectionMode pbMode) {
  return static_cast<ElectionMode>(pbMode);
}

::kudu::consensus::ElectionMode toPbElectionMode(ElectionMode mode) {
  return static_cast<::kudu::consensus::ElectionMode>(mode);
}

// VoteRequestPbView implementation

VoteRequestPbView::VoteRequestPbView(VoteRequestPB& pb) : pb_(pb) {}

const std::string& VoteRequestPbView::destUuid() const {
  if (pb_.has_dest_uuid()) {
    return pb_.dest_uuid();
  }
  return kEmptyString;
}

bool VoteRequestPbView::hasDestUuid() const {
  return pb_.has_dest_uuid();
}

const std::string& VoteRequestPbView::tabletId() const {
  return pb_.tablet_id();
}

const std::string& VoteRequestPbView::candidateUuid() const {
  return pb_.candidate_uuid();
}

int64_t VoteRequestPbView::candidateTerm() const {
  return pb_.candidate_term();
}

std::unique_ptr<ConsensusStatusView> VoteRequestPbView::candidateStatus() {
  return std::make_unique<ConsensusStatusPbView>(
      *pb_.mutable_candidate_status());
}

bool VoteRequestPbView::hasCandidateStatus() const {
  return pb_.has_candidate_status();
}

ElectionMode VoteRequestPbView::mode() const {
  return fromPbElectionMode(pb_.mode());
}

bool VoteRequestPbView::hasMode() const {
  return pb_.has_mode();
}

std::unique_ptr<OpIdView> VoteRequestPbView::mockElectionSnapshotOpId() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_mock_election_snapshot_op_id());
}

bool VoteRequestPbView::hasMockElectionSnapshotOpId() const {
  return pb_.has_mock_election_snapshot_op_id();
}

const std::string& VoteRequestPbView::raftRpcToken() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteRequestPbView::hasRaftRpcToken() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<CandidateContextView> VoteRequestPbView::candidateContext() {
  return std::make_unique<CandidateContextPbView>(
      *pb_.mutable_candidate_context());
}

bool VoteRequestPbView::hasCandidateContext() const {
  return pb_.has_candidate_context();
}

void VoteRequestPbView::setDestUuid(const std::string& uuid) {
  pb_.set_dest_uuid(uuid);
}

void VoteRequestPbView::clearDestUuid() {
  pb_.clear_dest_uuid();
}

void VoteRequestPbView::setTabletId(const std::string& id) {
  pb_.set_tablet_id(id);
}

void VoteRequestPbView::setCandidateUuid(const std::string& uuid) {
  pb_.set_candidate_uuid(uuid);
}

void VoteRequestPbView::setCandidateTerm(int64_t term) {
  pb_.set_candidate_term(term);
}

void VoteRequestPbView::setMode(ElectionMode mode) {
  pb_.set_mode(toPbElectionMode(mode));
}

void VoteRequestPbView::clearMode() {
  pb_.clear_mode();
}

void VoteRequestPbView::setRaftRpcToken(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteRequestPbView::clearRaftRpcToken() {
  pb_.clear_raft_rpc_token();
}

std::unique_ptr<VoteRequestPb> VoteRequestPbView::toOwned() const {
  return std::make_unique<VoteRequestPb>(pb_);
}

// VoteRequestPb implementation

VoteRequestPb::VoteRequestPb() = default;

VoteRequestPb::VoteRequestPb(VoteRequestPB pb) : pb_(std::move(pb)) {}

const std::string& VoteRequestPb::destUuid() const {
  if (pb_.has_dest_uuid()) {
    return pb_.dest_uuid();
  }
  return kEmptyString;
}

bool VoteRequestPb::hasDestUuid() const {
  return pb_.has_dest_uuid();
}

const std::string& VoteRequestPb::tabletId() const {
  return pb_.tablet_id();
}

const std::string& VoteRequestPb::candidateUuid() const {
  return pb_.candidate_uuid();
}

int64_t VoteRequestPb::candidateTerm() const {
  return pb_.candidate_term();
}

std::unique_ptr<ConsensusStatusView> VoteRequestPb::candidateStatus() {
  return std::make_unique<ConsensusStatusPbView>(
      *pb_.mutable_candidate_status());
}

bool VoteRequestPb::hasCandidateStatus() const {
  return pb_.has_candidate_status();
}

ElectionMode VoteRequestPb::mode() const {
  return fromPbElectionMode(pb_.mode());
}

bool VoteRequestPb::hasMode() const {
  return pb_.has_mode();
}

std::unique_ptr<OpIdView> VoteRequestPb::mockElectionSnapshotOpId() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_mock_election_snapshot_op_id());
}

bool VoteRequestPb::hasMockElectionSnapshotOpId() const {
  return pb_.has_mock_election_snapshot_op_id();
}

const std::string& VoteRequestPb::raftRpcToken() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteRequestPb::hasRaftRpcToken() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<CandidateContextView> VoteRequestPb::candidateContext() {
  return std::make_unique<CandidateContextPbView>(
      *pb_.mutable_candidate_context());
}

bool VoteRequestPb::hasCandidateContext() const {
  return pb_.has_candidate_context();
}

void VoteRequestPb::setDestUuid(const std::string& uuid) {
  pb_.set_dest_uuid(uuid);
}

void VoteRequestPb::clearDestUuid() {
  pb_.clear_dest_uuid();
}

void VoteRequestPb::setTabletId(const std::string& id) {
  pb_.set_tablet_id(id);
}

void VoteRequestPb::setCandidateUuid(const std::string& uuid) {
  pb_.set_candidate_uuid(uuid);
}

void VoteRequestPb::setCandidateTerm(int64_t term) {
  pb_.set_candidate_term(term);
}

void VoteRequestPb::setMode(ElectionMode mode) {
  pb_.set_mode(toPbElectionMode(mode));
}

void VoteRequestPb::clearMode() {
  pb_.clear_mode();
}

void VoteRequestPb::setRaftRpcToken(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteRequestPb::clearRaftRpcToken() {
  pb_.clear_raft_rpc_token();
}

const VoteRequestPB& VoteRequestPb::pb() const {
  return pb_;
}

VoteRequestPB* VoteRequestPb::mutablePb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
