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
  virtual const std::string& responderUuid() const = 0;
  virtual bool hasResponderUuid() const = 0;

  // The term of the node sending the reply.
  virtual std::optional<int64_t> responderTerm() const = 0;
  virtual bool hasResponderTerm() const = 0;

  // True if this peer voted for the caller.
  virtual std::optional<bool> voteGranted() const = 0;
  virtual bool hasVoteGranted() const = 0;

  // Raft RPC token.
  virtual const std::string& raftRpcToken() const = 0;
  virtual bool hasRaftRpcToken() const = 0;

  // Previously granted votes by this server.
  virtual std::vector<PreviousVote> previousVoteHistory() const = 0;
  virtual int previousVoteHistorySize() const = 0;

  // The greatest term that has been pruned from previous_vote_history.
  virtual std::optional<int64_t> lastPrunedTerm() const = 0;
  virtual bool hasLastPrunedTerm() const = 0;

  // Last known leader as per the responding voter.
  virtual std::optional<LastKnownLeader> lastKnownLeader() const = 0;
  virtual bool hasLastKnownLeader() const = 0;

  // Additional context sent back by the voter.
  virtual std::unique_ptr<VoterContextView> voterContext() = 0;
  virtual bool hasVoterContext() const = 0;

  // Consensus error (if any).
  virtual std::unique_ptr<ConsensusErrorView> consensusError() = 0;
  virtual bool hasConsensusError() const = 0;

  // Server error (if any).
  virtual std::unique_ptr<ServerErrorView> error() = 0;
  virtual bool hasError() const = 0;

  // Setters

  virtual void setResponderUuid(const std::string& uuid) = 0;
  virtual void clearResponderUuid() = 0;

  virtual void setResponderTerm(int64_t term) = 0;
  virtual void clearResponderTerm() = 0;

  virtual void setVoteGranted(bool granted) = 0;
  virtual void clearVoteGranted() = 0;

  virtual void setRaftRpcToken(const std::string& token) = 0;
  virtual void clearRaftRpcToken() = 0;

  // Add a previous vote to the history.
  virtual void addPreviousVote(const PreviousVote& vote) = 0;
  virtual void clearPreviousVoteHistory() = 0;

  virtual void setLastPrunedTerm(int64_t term) = 0;
  virtual void clearLastPrunedTerm() = 0;

  virtual void setLastKnownLeader(const LastKnownLeader& leader) = 0;
  virtual void clearLastKnownLeader() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
