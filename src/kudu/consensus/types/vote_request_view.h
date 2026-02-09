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
  virtual const std::string& dest_uuid() const = 0;
  virtual bool has_dest_uuid() const = 0;

  // The tablet id.
  virtual const std::string& tablet_id() const = 0;

  // The uuid of the sending peer (candidate).
  virtual const std::string& candidate_uuid() const = 0;

  // The term we are requesting a vote for.
  virtual int64_t candidate_term() const = 0;

  // The candidate's consensus status.
  virtual std::unique_ptr<ConsensusStatusView> candidate_status() = 0;
  virtual bool has_candidate_status() const = 0;

  // The election mode.
  virtual ElectionMode mode() const = 0;
  virtual bool has_mode() const = 0;

  // Mock election snapshot op id (for MOCK_ELECTION mode).
  virtual std::unique_ptr<OpIdView> mock_election_snapshot_op_id() = 0;
  virtual bool has_mock_election_snapshot_op_id() const = 0;

  // Raft RPC token.
  virtual const std::string& raft_rpc_token() const = 0;
  virtual bool has_raft_rpc_token() const = 0;

  // Candidate context (candidate's peer information).
  virtual std::unique_ptr<CandidateContextView> candidate_context() = 0;
  virtual bool has_candidate_context() const = 0;

  // Setters

  virtual void set_dest_uuid(const std::string& uuid) = 0;
  virtual void clear_dest_uuid() = 0;

  virtual void set_tablet_id(const std::string& id) = 0;

  virtual void set_candidate_uuid(const std::string& uuid) = 0;

  virtual void set_candidate_term(int64_t term) = 0;

  virtual void set_mode(ElectionMode mode) = 0;
  virtual void clear_mode() = 0;

  virtual void set_raft_rpc_token(const std::string& token) = 0;
  virtual void clear_raft_rpc_token() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
