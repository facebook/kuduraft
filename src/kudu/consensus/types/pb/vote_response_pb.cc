// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/vote_response_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

namespace {
const std::string kEmptyString;
} // namespace

// VoteResponsePbView implementation

VoteResponsePbView::VoteResponsePbView(VoteResponsePB& pb) : pb_(pb) {}

const std::string& VoteResponsePbView::responderUuid() const {
  if (pb_.has_responder_uuid()) {
    return pb_.responder_uuid();
  }
  return kEmptyString;
}

bool VoteResponsePbView::hasResponderUuid() const {
  return pb_.has_responder_uuid();
}

std::optional<int64_t> VoteResponsePbView::responderTerm() const {
  if (pb_.has_responder_term()) {
    return pb_.responder_term();
  }
  return std::nullopt;
}

bool VoteResponsePbView::hasResponderTerm() const {
  return pb_.has_responder_term();
}

std::optional<bool> VoteResponsePbView::voteGranted() const {
  if (pb_.has_vote_granted()) {
    return pb_.vote_granted();
  }
  return std::nullopt;
}

bool VoteResponsePbView::hasVoteGranted() const {
  return pb_.has_vote_granted();
}

const std::string& VoteResponsePbView::raftRpcToken() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteResponsePbView::hasRaftRpcToken() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<ConsensusErrorView> VoteResponsePbView::consensus_error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_consensus_error());
}

bool VoteResponsePbView::has_consensus_error() const {
  return pb_.has_consensus_error();
}

std::unique_ptr<ServerErrorView> VoteResponsePbView::error() {
  return std::make_unique<ServerErrorPbView>(*pb_.mutable_error());
}

bool VoteResponsePbView::has_error() const {
  return pb_.has_error();
}

void VoteResponsePbView::set_responder_uuid(const std::string& uuid) {
  pb_.set_responder_uuid(uuid);
}

void VoteResponsePbView::clear_responder_uuid() {
  pb_.clear_responder_uuid();
}

void VoteResponsePbView::set_responder_term(int64_t term) {
  pb_.set_responder_term(term);
}

void VoteResponsePbView::clear_responder_term() {
  pb_.clear_responder_term();
}

void VoteResponsePbView::set_vote_granted(bool granted) {
  pb_.set_vote_granted(granted);
}

void VoteResponsePbView::clear_vote_granted() {
  pb_.clear_vote_granted();
}

void VoteResponsePbView::set_raft_rpc_token(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteResponsePbView::clear_raft_rpc_token() {
  pb_.clear_raft_rpc_token();
}

std::vector<PreviousVote> VoteResponsePbView::previousVoteHistory() const {
  std::vector<PreviousVote> result;
  result.reserve(pb_.previous_vote_history_size());
  for (const auto& pb_vote : pb_.previous_vote_history()) {
    result.emplace_back(pb_vote.candidate_uuid(), pb_vote.election_term());
  }
  return result;
}

int VoteResponsePbView::previousVoteHistorySize() const {
  return pb_.previous_vote_history_size();
}

std::optional<int64_t> VoteResponsePbView::last_pruned_term() const {
  if (pb_.has_last_pruned_term()) {
    return pb_.last_pruned_term();
  }
  return std::nullopt;
}

bool VoteResponsePbView::has_last_pruned_term() const {
  return pb_.has_last_pruned_term();
}

std::optional<LastKnownLeader> VoteResponsePbView::last_known_leader() const {
  if (pb_.has_last_known_leader()) {
    return LastKnownLeader(
        pb_.last_known_leader().uuid(),
        pb_.last_known_leader().election_term());
  }
  return std::nullopt;
}

bool VoteResponsePbView::has_last_known_leader() const {
  return pb_.has_last_known_leader();
}

std::unique_ptr<VoterContextView> VoteResponsePbView::voterContext() {
  return std::make_unique<VoterContextPbView>(*pb_.mutable_voter_context());
}

bool VoteResponsePbView::hasVoterContext() const {
  return pb_.has_voter_context();
}

void VoteResponsePbView::add_previous_vote(const PreviousVote& vote) {
  auto* pb_vote = pb_.add_previous_vote_history();
  pb_vote->set_candidate_uuid(vote.candidate_uuid());
  pb_vote->set_election_term(vote.election_term());
}

void VoteResponsePbView::clear_previous_vote_history() {
  pb_.clear_previous_vote_history();
}

void VoteResponsePbView::set_last_pruned_term(int64_t term) {
  pb_.set_last_pruned_term(term);
}

void VoteResponsePbView::clear_last_pruned_term() {
  pb_.clear_last_pruned_term();
}

void VoteResponsePbView::set_last_known_leader(const LastKnownLeader& leader) {
  pb_.mutable_last_known_leader()->set_uuid(leader.uuid());
  pb_.mutable_last_known_leader()->set_election_term(leader.electionTerm());
}

void VoteResponsePbView::clear_last_known_leader() {
  pb_.clear_last_known_leader();
}

std::unique_ptr<VoteResponsePb> VoteResponsePbView::to_owned() const {
  return std::make_unique<VoteResponsePb>(pb_);
}

// VoteResponsePb implementation

VoteResponsePb::VoteResponsePb() = default;

VoteResponsePb::VoteResponsePb(VoteResponsePB pb) : pb_(std::move(pb)) {}

const std::string& VoteResponsePb::responderUuid() const {
  if (pb_.has_responder_uuid()) {
    return pb_.responder_uuid();
  }
  return kEmptyString;
}

bool VoteResponsePb::hasResponderUuid() const {
  return pb_.has_responder_uuid();
}

std::optional<int64_t> VoteResponsePb::responderTerm() const {
  if (pb_.has_responder_term()) {
    return pb_.responder_term();
  }
  return std::nullopt;
}

bool VoteResponsePb::hasResponderTerm() const {
  return pb_.has_responder_term();
}

std::optional<bool> VoteResponsePb::voteGranted() const {
  if (pb_.has_vote_granted()) {
    return pb_.vote_granted();
  }
  return std::nullopt;
}

bool VoteResponsePb::hasVoteGranted() const {
  return pb_.has_vote_granted();
}

const std::string& VoteResponsePb::raftRpcToken() const {
  if (pb_.has_raft_rpc_token()) {
    return pb_.raft_rpc_token();
  }
  return kEmptyString;
}

bool VoteResponsePb::hasRaftRpcToken() const {
  return pb_.has_raft_rpc_token();
}

std::unique_ptr<ConsensusErrorView> VoteResponsePb::consensus_error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_consensus_error());
}

bool VoteResponsePb::has_consensus_error() const {
  return pb_.has_consensus_error();
}

std::unique_ptr<ServerErrorView> VoteResponsePb::error() {
  return std::make_unique<ServerErrorPbView>(*pb_.mutable_error());
}

bool VoteResponsePb::has_error() const {
  return pb_.has_error();
}

void VoteResponsePb::set_responder_uuid(const std::string& uuid) {
  pb_.set_responder_uuid(uuid);
}

void VoteResponsePb::clear_responder_uuid() {
  pb_.clear_responder_uuid();
}

void VoteResponsePb::set_responder_term(int64_t term) {
  pb_.set_responder_term(term);
}

void VoteResponsePb::clear_responder_term() {
  pb_.clear_responder_term();
}

void VoteResponsePb::set_vote_granted(bool granted) {
  pb_.set_vote_granted(granted);
}

void VoteResponsePb::clear_vote_granted() {
  pb_.clear_vote_granted();
}

void VoteResponsePb::set_raft_rpc_token(const std::string& token) {
  pb_.set_raft_rpc_token(token);
}

void VoteResponsePb::clear_raft_rpc_token() {
  pb_.clear_raft_rpc_token();
}

std::vector<PreviousVote> VoteResponsePb::previousVoteHistory() const {
  std::vector<PreviousVote> result;
  result.reserve(pb_.previous_vote_history_size());
  for (const auto& pb_vote : pb_.previous_vote_history()) {
    result.emplace_back(pb_vote.candidate_uuid(), pb_vote.election_term());
  }
  return result;
}

int VoteResponsePb::previousVoteHistorySize() const {
  return pb_.previous_vote_history_size();
}

std::optional<int64_t> VoteResponsePb::last_pruned_term() const {
  if (pb_.has_last_pruned_term()) {
    return pb_.last_pruned_term();
  }
  return std::nullopt;
}

bool VoteResponsePb::has_last_pruned_term() const {
  return pb_.has_last_pruned_term();
}

std::optional<LastKnownLeader> VoteResponsePb::last_known_leader() const {
  if (pb_.has_last_known_leader()) {
    return LastKnownLeader(
        pb_.last_known_leader().uuid(),
        pb_.last_known_leader().election_term());
  }
  return std::nullopt;
}

bool VoteResponsePb::has_last_known_leader() const {
  return pb_.has_last_known_leader();
}

std::unique_ptr<VoterContextView> VoteResponsePb::voterContext() {
  return std::make_unique<VoterContextPbView>(*pb_.mutable_voter_context());
}

bool VoteResponsePb::hasVoterContext() const {
  return pb_.has_voter_context();
}

void VoteResponsePb::add_previous_vote(const PreviousVote& vote) {
  auto* pb_vote = pb_.add_previous_vote_history();
  pb_vote->set_candidate_uuid(vote.candidate_uuid());
  pb_vote->set_election_term(vote.election_term());
}

void VoteResponsePb::clear_previous_vote_history() {
  pb_.clear_previous_vote_history();
}

void VoteResponsePb::set_last_pruned_term(int64_t term) {
  pb_.set_last_pruned_term(term);
}

void VoteResponsePb::clear_last_pruned_term() {
  pb_.clear_last_pruned_term();
}

void VoteResponsePb::set_last_known_leader(const LastKnownLeader& leader) {
  pb_.mutable_last_known_leader()->set_uuid(leader.uuid());
  pb_.mutable_last_known_leader()->set_election_term(leader.electionTerm());
}

void VoteResponsePb::clear_last_known_leader() {
  pb_.clear_last_known_leader();
}

const VoteResponsePB& VoteResponsePb::pb() const {
  return pb_;
}

VoteResponsePB* VoteResponsePb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
