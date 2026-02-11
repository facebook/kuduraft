// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/last_known_leader.h"
#include "kudu/consensus/types/pb/consensus_error_pb.h"
#include "kudu/consensus/types/pb/server_error_pb.h"
#include "kudu/consensus/types/pb/voter_context_pb.h"
#include "kudu/consensus/types/previous_vote.h"
#include "kudu/consensus/types/vote_response.h"
#include "kudu/consensus/types/vote_response_view.h"

namespace kudu {
namespace consensus {
namespace types {

class VoteResponsePb;

// Protobuf-backed implementation of VoteResponseView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class VoteResponsePbView : public VoteResponseView {
 public:
  explicit VoteResponsePbView(VoteResponsePB& pb);
  ~VoteResponsePbView() override = default;

  // VoteResponseView interface - getters
  const std::string& responder_uuid() const override;
  bool has_responder_uuid() const override;
  std::optional<int64_t> responder_term() const override;
  bool has_responder_term() const override;
  std::optional<bool> vote_granted() const override;
  bool has_vote_granted() const override;
  const std::string& raft_rpc_token() const override;
  bool has_raft_rpc_token() const override;
  std::vector<PreviousVote> previous_vote_history() const override;
  int previous_vote_history_size() const override;
  std::optional<int64_t> last_pruned_term() const override;
  bool has_last_pruned_term() const override;
  std::optional<LastKnownLeader> last_known_leader() const override;
  bool has_last_known_leader() const override;
  std::unique_ptr<VoterContextView> voter_context() override;
  bool has_voter_context() const override;
  std::unique_ptr<ConsensusErrorView> consensus_error() override;
  bool has_consensus_error() const override;
  std::unique_ptr<ServerErrorView> error() override;
  bool has_error() const override;

  // VoteResponseView interface - setters
  void set_responder_uuid(const std::string& uuid) override;
  void clear_responder_uuid() override;
  void set_responder_term(int64_t term) override;
  void clear_responder_term() override;
  void set_vote_granted(bool granted) override;
  void clear_vote_granted() override;
  void set_raft_rpc_token(const std::string& token) override;
  void clear_raft_rpc_token() override;
  void add_previous_vote(const PreviousVote& vote) override;
  void clear_previous_vote_history() override;
  void set_last_pruned_term(int64_t term) override;
  void clear_last_pruned_term() override;
  void set_last_known_leader(const LastKnownLeader& leader) override;
  void clear_last_known_leader() override;

  // Create an owning copy of this view.
  std::unique_ptr<VoteResponsePb> to_owned() const;

 private:
  VoteResponsePB& pb_;
};

// Protobuf-backed implementation of VoteResponse.
// Owns the underlying protobuf.
class VoteResponsePb : public VoteResponse {
 public:
  VoteResponsePb();
  explicit VoteResponsePb(VoteResponsePB pb);
  ~VoteResponsePb() override = default;

  // VoteResponseView interface - getters
  const std::string& responder_uuid() const override;
  bool has_responder_uuid() const override;
  std::optional<int64_t> responder_term() const override;
  bool has_responder_term() const override;
  std::optional<bool> vote_granted() const override;
  bool has_vote_granted() const override;
  const std::string& raft_rpc_token() const override;
  bool has_raft_rpc_token() const override;
  std::vector<PreviousVote> previous_vote_history() const override;
  int previous_vote_history_size() const override;
  std::optional<int64_t> last_pruned_term() const override;
  bool has_last_pruned_term() const override;
  std::optional<LastKnownLeader> last_known_leader() const override;
  bool has_last_known_leader() const override;
  std::unique_ptr<VoterContextView> voter_context() override;
  bool has_voter_context() const override;
  std::unique_ptr<ConsensusErrorView> consensus_error() override;
  bool has_consensus_error() const override;
  std::unique_ptr<ServerErrorView> error() override;
  bool has_error() const override;

  // VoteResponseView interface - setters
  void set_responder_uuid(const std::string& uuid) override;
  void clear_responder_uuid() override;
  void set_responder_term(int64_t term) override;
  void clear_responder_term() override;
  void set_vote_granted(bool granted) override;
  void clear_vote_granted() override;
  void set_raft_rpc_token(const std::string& token) override;
  void clear_raft_rpc_token() override;
  void add_previous_vote(const PreviousVote& vote) override;
  void clear_previous_vote_history() override;
  void set_last_pruned_term(int64_t term) override;
  void clear_last_pruned_term() override;
  void set_last_known_leader(const LastKnownLeader& leader) override;
  void clear_last_known_leader() override;

  // Access the underlying protobuf.
  const VoteResponsePB& pb() const;
  VoteResponsePB* mutable_pb();

 private:
  VoteResponsePB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
