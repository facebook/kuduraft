// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/pb/candidate_context_pb.h"
#include "kudu/consensus/types/pb/consensus_status_pb.h"
#include "kudu/consensus/types/pb/opid_pb.h"
#include "kudu/consensus/types/vote_request.h"
#include "kudu/consensus/types/vote_request_view.h"

namespace kudu {
namespace consensus {
namespace types {

class VoteRequestPb;

// Convert between protobuf ElectionMode and wrapper ElectionMode.
ElectionMode FromPbElectionMode(::kudu::consensus::ElectionMode pb_mode);
::kudu::consensus::ElectionMode ToPbElectionMode(ElectionMode mode);

// Protobuf-backed implementation of VoteRequestView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class VoteRequestPbView : public VoteRequestView {
 public:
  explicit VoteRequestPbView(VoteRequestPB& pb);
  ~VoteRequestPbView() override = default;

  // VoteRequestView interface - getters
  const std::string& dest_uuid() const override;
  bool has_dest_uuid() const override;
  const std::string& tablet_id() const override;
  const std::string& candidate_uuid() const override;
  int64_t candidate_term() const override;
  std::unique_ptr<ConsensusStatusView> candidate_status() override;
  bool has_candidate_status() const override;
  ElectionMode mode() const override;
  bool has_mode() const override;
  std::unique_ptr<OpIdView> mock_election_snapshot_op_id() override;
  bool has_mock_election_snapshot_op_id() const override;
  const std::string& raft_rpc_token() const override;
  bool has_raft_rpc_token() const override;
  std::unique_ptr<CandidateContextView> candidate_context() override;
  bool has_candidate_context() const override;

  // VoteRequestView interface - setters
  void set_dest_uuid(const std::string& uuid) override;
  void clear_dest_uuid() override;
  void set_tablet_id(const std::string& id) override;
  void set_candidate_uuid(const std::string& uuid) override;
  void set_candidate_term(int64_t term) override;
  void set_mode(ElectionMode mode) override;
  void clear_mode() override;
  void set_raft_rpc_token(const std::string& token) override;
  void clear_raft_rpc_token() override;

  // Create an owning copy of this view.
  std::unique_ptr<VoteRequestPb> to_owned() const;

 private:
  VoteRequestPB& pb_;
};

// Protobuf-backed implementation of VoteRequest.
// Owns the underlying protobuf.
class VoteRequestPb : public VoteRequest {
 public:
  VoteRequestPb();
  explicit VoteRequestPb(const VoteRequestPB& pb);
  ~VoteRequestPb() override = default;

  // VoteRequestView interface - getters
  const std::string& dest_uuid() const override;
  bool has_dest_uuid() const override;
  const std::string& tablet_id() const override;
  const std::string& candidate_uuid() const override;
  int64_t candidate_term() const override;
  std::unique_ptr<ConsensusStatusView> candidate_status() override;
  bool has_candidate_status() const override;
  ElectionMode mode() const override;
  bool has_mode() const override;
  std::unique_ptr<OpIdView> mock_election_snapshot_op_id() override;
  bool has_mock_election_snapshot_op_id() const override;
  const std::string& raft_rpc_token() const override;
  bool has_raft_rpc_token() const override;
  std::unique_ptr<CandidateContextView> candidate_context() override;
  bool has_candidate_context() const override;

  // VoteRequestView interface - setters
  void set_dest_uuid(const std::string& uuid) override;
  void clear_dest_uuid() override;
  void set_tablet_id(const std::string& id) override;
  void set_candidate_uuid(const std::string& uuid) override;
  void set_candidate_term(int64_t term) override;
  void set_mode(ElectionMode mode) override;
  void clear_mode() override;
  void set_raft_rpc_token(const std::string& token) override;
  void clear_raft_rpc_token() override;

  // Access the underlying protobuf.
  const VoteRequestPB& pb() const;
  VoteRequestPB* mutable_pb();

 private:
  VoteRequestPB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
