// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "kudu/consensus/types/candidate_context_view.h"
#include "kudu/consensus/types/consensus_status_view.h"
#include "kudu/consensus/types/opid_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Election modes.
// These mirror the protobuf ElectionMode values.
enum class ElectionMode {
  kUnknown = 0,
  kNormalElection = 1,
  kPreElection = 2,
  kElectEvenIfLeaderIsAlive = 3,
  kMockElection = 4,
};

// Interface for a VoteRequest.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class VoteRequestView {
 public:
  virtual ~VoteRequestView() = default;

  // Getters

  // UUID of server this request is addressed to.
  virtual const std::string& destUuid() const = 0;
  virtual bool hasDestUuid() const = 0;

  // The tablet id.
  virtual const std::string& tabletId() const = 0;

  // The uuid of the sending peer (candidate).
  virtual const std::string& candidateUuid() const = 0;

  // The term we are requesting a vote for.
  virtual int64_t candidateTerm() const = 0;

  // The candidate's consensus status.
  virtual std::unique_ptr<ConsensusStatusView> candidateStatus() = 0;
  virtual bool hasCandidateStatus() const = 0;

  // The election mode.
  virtual ElectionMode mode() const = 0;
  virtual bool hasMode() const = 0;

  // Mock election snapshot op id (for MOCK_ELECTION mode).
  virtual std::unique_ptr<OpIdView> mockElectionSnapshotOpId() = 0;
  virtual bool hasMockElectionSnapshotOpId() const = 0;

  // Raft RPC token.
  virtual const std::string& raftRpcToken() const = 0;
  virtual bool hasRaftRpcToken() const = 0;

  // Candidate context (candidate's peer information).
  virtual std::unique_ptr<CandidateContextView> candidateContext() = 0;
  virtual bool hasCandidateContext() const = 0;

  // Setters

  virtual void setDestUuid(const std::string& uuid) = 0;
  virtual void clearDestUuid() = 0;

  virtual void setTabletId(const std::string& id) = 0;

  virtual void setCandidateUuid(const std::string& uuid) = 0;

  virtual void setCandidateTerm(int64_t term) = 0;

  virtual void setMode(ElectionMode mode) = 0;
  virtual void clearMode() = 0;

  virtual void setRaftRpcToken(const std::string& token) = 0;
  virtual void clearRaftRpcToken() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
