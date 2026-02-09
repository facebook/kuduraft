// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kudu/consensus/types/consensus_error_view.h"
#include "kudu/consensus/types/last_known_leader.h"
#include "kudu/consensus/types/previous_vote.h"
#include "kudu/consensus/types/server_error_view.h"
#include "kudu/consensus/types/voter_context_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Interface for a VoteResponse.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class VoteResponseView {
 public:
  virtual ~VoteResponseView() = default;

  // Getters

  // The uuid of the node sending the reply.
  virtual const std::string& responder_uuid() const = 0;
  virtual bool has_responder_uuid() const = 0;

  // The term of the node sending the reply.
  virtual std::optional<int64_t> responder_term() const = 0;
  virtual bool has_responder_term() const = 0;

  // True if this peer voted for the caller.
  virtual std::optional<bool> vote_granted() const = 0;
  virtual bool has_vote_granted() const = 0;

  // Raft RPC token.
  virtual const std::string& raft_rpc_token() const = 0;
  virtual bool has_raft_rpc_token() const = 0;

  // Previously granted votes by this server.
  virtual std::vector<PreviousVote> previous_vote_history() const = 0;
  virtual int previous_vote_history_size() const = 0;

  // The greatest term that has been pruned from previous_vote_history.
  virtual std::optional<int64_t> last_pruned_term() const = 0;
  virtual bool has_last_pruned_term() const = 0;

  // Last known leader as per the responding voter.
  virtual std::optional<LastKnownLeader> last_known_leader() const = 0;
  virtual bool has_last_known_leader() const = 0;

  // Additional context sent back by the voter.
  virtual std::unique_ptr<VoterContextView> voter_context() = 0;
  virtual bool has_voter_context() const = 0;

  // Consensus error (if any).
  virtual std::unique_ptr<ConsensusErrorView> consensus_error() = 0;
  virtual bool has_consensus_error() const = 0;

  // Server error (if any).
  virtual std::unique_ptr<ServerErrorView> error() = 0;
  virtual bool has_error() const = 0;

  // Setters

  virtual void set_responder_uuid(const std::string& uuid) = 0;
  virtual void clear_responder_uuid() = 0;

  virtual void set_responder_term(int64_t term) = 0;
  virtual void clear_responder_term() = 0;

  virtual void set_vote_granted(bool granted) = 0;
  virtual void clear_vote_granted() = 0;

  virtual void set_raft_rpc_token(const std::string& token) = 0;
  virtual void clear_raft_rpc_token() = 0;

  // Add a previous vote to the history.
  virtual void add_previous_vote(const PreviousVote& vote) = 0;
  virtual void clear_previous_vote_history() = 0;

  virtual void set_last_pruned_term(int64_t term) = 0;
  virtual void clear_last_pruned_term() = 0;

  virtual void set_last_known_leader(const LastKnownLeader& leader) = 0;
  virtual void clear_last_known_leader() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
