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
ElectionMode fromPbElectionMode(::kudu::consensus::ElectionMode pbMode);
::kudu::consensus::ElectionMode toPbElectionMode(ElectionMode mode);

// Protobuf-backed implementation of VoteRequestView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class VoteRequestPbView : public VoteRequestView {
 public:
  explicit VoteRequestPbView(VoteRequestPB& pb);
  ~VoteRequestPbView() override = default;

  // VoteRequestView interface - getters
  const std::string& destUuid() const override;
  bool hasDestUuid() const override;
  const std::string& tabletId() const override;
  const std::string& candidateUuid() const override;
  int64_t candidateTerm() const override;
  std::unique_ptr<ConsensusStatusView> candidateStatus() override;
  bool hasCandidateStatus() const override;
  ElectionMode mode() const override;
  bool hasMode() const override;
  std::unique_ptr<OpIdView> mockElectionSnapshotOpId() override;
  bool hasMockElectionSnapshotOpId() const override;
  const std::string& raftRpcToken() const override;
  bool hasRaftRpcToken() const override;
  std::unique_ptr<CandidateContextView> candidateContext() override;
  bool hasCandidateContext() const override;

  // VoteRequestView interface - setters
  void setDestUuid(const std::string& uuid) override;
  void clearDestUuid() override;
  void setTabletId(const std::string& id) override;
  void setCandidateUuid(const std::string& uuid) override;
  void setCandidateTerm(int64_t term) override;
  void setMode(ElectionMode mode) override;
  void clearMode() override;
  void setRaftRpcToken(const std::string& token) override;
  void clearRaftRpcToken() override;

  // Create an owning copy of this view.
  std::unique_ptr<VoteRequestPb> toOwned() const;

 private:
  VoteRequestPB& pb_;
};

// Protobuf-backed implementation of VoteRequest.
// Owns the underlying protobuf.
class VoteRequestPb : public VoteRequest {
 public:
  VoteRequestPb();
  explicit VoteRequestPb(VoteRequestPB pb);
  ~VoteRequestPb() override = default;

  // VoteRequestView interface - getters
  const std::string& destUuid() const override;
  bool hasDestUuid() const override;
  const std::string& tabletId() const override;
  const std::string& candidateUuid() const override;
  int64_t candidateTerm() const override;
  std::unique_ptr<ConsensusStatusView> candidateStatus() override;
  bool hasCandidateStatus() const override;
  ElectionMode mode() const override;
  bool hasMode() const override;
  std::unique_ptr<OpIdView> mockElectionSnapshotOpId() override;
  bool hasMockElectionSnapshotOpId() const override;
  const std::string& raftRpcToken() const override;
  bool hasRaftRpcToken() const override;
  std::unique_ptr<CandidateContextView> candidateContext() override;
  bool hasCandidateContext() const override;

  // VoteRequestView interface - setters
  void setDestUuid(const std::string& uuid) override;
  void clearDestUuid() override;
  void setTabletId(const std::string& id) override;
  void setCandidateUuid(const std::string& uuid) override;
  void setCandidateTerm(int64_t term) override;
  void setMode(ElectionMode mode) override;
  void clearMode() override;
  void setRaftRpcToken(const std::string& token) override;
  void clearRaftRpcToken() override;

  // Access the underlying protobuf.
  const VoteRequestPB& pb() const;
  VoteRequestPB* mutablePb();

 private:
  VoteRequestPB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
