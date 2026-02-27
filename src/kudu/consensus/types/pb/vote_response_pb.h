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
  const std::string& responderUuid() const override;
  bool hasResponderUuid() const override;
  std::optional<int64_t> responderTerm() const override;
  bool hasResponderTerm() const override;
  std::optional<bool> voteGranted() const override;
  bool hasVoteGranted() const override;
  const std::string& raftRpcToken() const override;
  bool hasRaftRpcToken() const override;
  std::vector<PreviousVote> previousVoteHistory() const override;
  int previousVoteHistorySize() const override;
  std::optional<int64_t> lastPrunedTerm() const override;
  bool hasLastPrunedTerm() const override;
  std::optional<LastKnownLeader> lastKnownLeader() const override;
  bool hasLastKnownLeader() const override;
  std::unique_ptr<VoterContextView> voterContext() override;
  bool hasVoterContext() const override;
  std::unique_ptr<ConsensusErrorView> consensusError() override;
  bool hasConsensusError() const override;
  std::unique_ptr<ServerErrorView> error() override;
  bool hasError() const override;

  // VoteResponseView interface - setters
  void setResponderUuid(const std::string& uuid) override;
  void clearResponderUuid() override;
  void setResponderTerm(int64_t term) override;
  void clearResponderTerm() override;
  void setVoteGranted(bool granted) override;
  void clearVoteGranted() override;
  void setRaftRpcToken(const std::string& token) override;
  void clearRaftRpcToken() override;
  void addPreviousVote(const PreviousVote& vote) override;
  void clearPreviousVoteHistory() override;
  void setLastPrunedTerm(int64_t term) override;
  void clearLastPrunedTerm() override;
  void setLastKnownLeader(const LastKnownLeader& leader) override;
  void clearLastKnownLeader() override;

  // Create an owning copy of this view.
  std::unique_ptr<VoteResponsePb> toOwned() const;

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
  const std::string& responderUuid() const override;
  bool hasResponderUuid() const override;
  std::optional<int64_t> responderTerm() const override;
  bool hasResponderTerm() const override;
  std::optional<bool> voteGranted() const override;
  bool hasVoteGranted() const override;
  const std::string& raftRpcToken() const override;
  bool hasRaftRpcToken() const override;
  std::vector<PreviousVote> previousVoteHistory() const override;
  int previousVoteHistorySize() const override;
  std::optional<int64_t> lastPrunedTerm() const override;
  bool hasLastPrunedTerm() const override;
  std::optional<LastKnownLeader> lastKnownLeader() const override;
  bool hasLastKnownLeader() const override;
  std::unique_ptr<VoterContextView> voterContext() override;
  bool hasVoterContext() const override;
  std::unique_ptr<ConsensusErrorView> consensusError() override;
  bool hasConsensusError() const override;
  std::unique_ptr<ServerErrorView> error() override;
  bool hasError() const override;

  // VoteResponseView interface - setters
  void setResponderUuid(const std::string& uuid) override;
  void clearResponderUuid() override;
  void setResponderTerm(int64_t term) override;
  void clearResponderTerm() override;
  void setVoteGranted(bool granted) override;
  void clearVoteGranted() override;
  void setRaftRpcToken(const std::string& token) override;
  void clearRaftRpcToken() override;
  void addPreviousVote(const PreviousVote& vote) override;
  void clearPreviousVoteHistory() override;
  void setLastPrunedTerm(int64_t term) override;
  void clearLastPrunedTerm() override;
  void setLastKnownLeader(const LastKnownLeader& leader) override;
  void clearLastKnownLeader() override;

  // Access the underlying protobuf.
  const VoteResponsePB& pb() const;
  VoteResponsePB* mutable_pb();

 private:
  VoteResponsePB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
