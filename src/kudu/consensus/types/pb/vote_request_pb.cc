// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/vote_request_pb.h"

namespace kudu {
namespace consensus {
namespace types {

namespace {
const std::string kEmptyString;
} // namespace

ElectionMode FromPbElectionMode(::kudu::consensus::ElectionMode pb_mode) {
  return static_cast<ElectionMode>(pb_mode);
}

::kudu::consensus::ElectionMode ToPbElectionMode(ElectionMode mode) {
  return static_cast<::kudu::consensus::ElectionMode>(mode);
}

// VoteRequestPbView implementation

VoteRequestPbView::VoteRequestPbView(VoteRequestPB& pb) : pb_(pb) {}

const std::string& VoteRequestPbView::dest_uuid() const {
  if (pb_.has_dest_uuid()) {
    return pb_.dest_uuid();
  }
  return kEmptyString;
}

bool VoteRequestPbView::has_dest_uuid() const {
  return pb_.has_dest_uuid();
}

const std::string& VoteRequestPbView::tablet_id() const {
  return pb_.tablet_id();
}

const std::string& VoteRequestPbView::candidate_uuid() const {
  return pb_.candidate_uuid();
}

int64_t VoteRequestPbView::candidate_term() const {
  return pb_.candidate_term();
}

std::unique_ptr<ConsensusStatusView> VoteRequestPbView::candidate_status() {
  return std::make_unique<ConsensusStatusPbView>(
      *pb_.mutable_candidate_status());
}

bool VoteRequestPbView::has_candidate_status() const {
  return pb_.has_candidate_status();
}

ElectionMode VoteRequestPbView::mode() const {
  return FromPbElectionMode(pb_.mode());
}

bool VoteRequestPbView::has_mode() const {
  return pb_.has_mode();
}

std::unique_ptr<OpIdView> VoteRequestPbView::mock_election_snapshot_op_id() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_mock_election_snapshot_op_id());
}

bool VoteRequestPbView::has_mock_election_snapshot_op_id() const {
  return pb_.has_mock_election_snapshot_op_id();
}

const std::string& VoteRequestPbView::raft_rpc_token() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteRequestPbView::has_raft_rpc_token() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<CandidateContextView> VoteRequestPbView::candidate_context() {
  return std::make_unique<CandidateContextPbView>(
      *pb_.mutable_candidate_context());
}

bool VoteRequestPbView::has_candidate_context() const {
  return pb_.has_candidate_context();
}

void VoteRequestPbView::set_dest_uuid(const std::string& uuid) {
  pb_.set_dest_uuid(uuid);
}

void VoteRequestPbView::clear_dest_uuid() {
  pb_.clear_dest_uuid();
}

void VoteRequestPbView::set_tablet_id(const std::string& id) {
  pb_.set_tablet_id(id);
}

void VoteRequestPbView::set_candidate_uuid(const std::string& uuid) {
  pb_.set_candidate_uuid(uuid);
}

void VoteRequestPbView::set_candidate_term(int64_t term) {
  pb_.set_candidate_term(term);
}

void VoteRequestPbView::set_mode(ElectionMode mode) {
  pb_.set_mode(ToPbElectionMode(mode));
}

void VoteRequestPbView::clear_mode() {
  pb_.clear_mode();
}

void VoteRequestPbView::set_raft_rpc_token(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteRequestPbView::clear_raft_rpc_token() {
  pb_.clear_raft_rpc_token();
}

std::unique_ptr<VoteRequestPb> VoteRequestPbView::to_owned() const {
  return std::make_unique<VoteRequestPb>(pb_);
}

// VoteRequestPb implementation

VoteRequestPb::VoteRequestPb() = default;

VoteRequestPb::VoteRequestPb(const VoteRequestPB& pb) : pb_(pb) {}

const std::string& VoteRequestPb::dest_uuid() const {
  if (pb_.has_dest_uuid()) {
    return pb_.dest_uuid();
  }
  return kEmptyString;
}

bool VoteRequestPb::has_dest_uuid() const {
  return pb_.has_dest_uuid();
}

const std::string& VoteRequestPb::tablet_id() const {
  return pb_.tablet_id();
}

const std::string& VoteRequestPb::candidate_uuid() const {
  return pb_.candidate_uuid();
}

int64_t VoteRequestPb::candidate_term() const {
  return pb_.candidate_term();
}

std::unique_ptr<ConsensusStatusView> VoteRequestPb::candidate_status() {
  return std::make_unique<ConsensusStatusPbView>(
      *pb_.mutable_candidate_status());
}

bool VoteRequestPb::has_candidate_status() const {
  return pb_.has_candidate_status();
}

ElectionMode VoteRequestPb::mode() const {
  return FromPbElectionMode(pb_.mode());
}

bool VoteRequestPb::has_mode() const {
  return pb_.has_mode();
}

std::unique_ptr<OpIdView> VoteRequestPb::mock_election_snapshot_op_id() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_mock_election_snapshot_op_id());
}

bool VoteRequestPb::has_mock_election_snapshot_op_id() const {
  return pb_.has_mock_election_snapshot_op_id();
}

const std::string& VoteRequestPb::raft_rpc_token() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteRequestPb::has_raft_rpc_token() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<CandidateContextView> VoteRequestPb::candidate_context() {
  return std::make_unique<CandidateContextPbView>(
      *pb_.mutable_candidate_context());
}

bool VoteRequestPb::has_candidate_context() const {
  return pb_.has_candidate_context();
}

void VoteRequestPb::set_dest_uuid(const std::string& uuid) {
  pb_.set_dest_uuid(uuid);
}

void VoteRequestPb::clear_dest_uuid() {
  pb_.clear_dest_uuid();
}

void VoteRequestPb::set_tablet_id(const std::string& id) {
  pb_.set_tablet_id(id);
}

void VoteRequestPb::set_candidate_uuid(const std::string& uuid) {
  pb_.set_candidate_uuid(uuid);
}

void VoteRequestPb::set_candidate_term(int64_t term) {
  pb_.set_candidate_term(term);
}

void VoteRequestPb::set_mode(ElectionMode mode) {
  pb_.set_mode(ToPbElectionMode(mode));
}

void VoteRequestPb::clear_mode() {
  pb_.clear_mode();
}

void VoteRequestPb::set_raft_rpc_token(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteRequestPb::clear_raft_rpc_token() {
  pb_.clear_raft_rpc_token();
}

const VoteRequestPB& VoteRequestPb::pb() const {
  return pb_;
}

VoteRequestPB* VoteRequestPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
