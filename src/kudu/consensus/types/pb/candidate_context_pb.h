// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/candidate_context.h"
#include "kudu/consensus/types/candidate_context_view.h"

namespace kudu {
namespace consensus {
namespace types {

class CandidateContextPb;

// Protobuf-backed implementation of CandidateContextView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class CandidateContextPbView : public CandidateContextView {
 public:
  explicit CandidateContextPbView(::kudu::consensus::CandidateContext& pb);
  ~CandidateContextPbView() override = default;

  // CandidateContextView interface
  bool hasCandidatePeer() const override;

  // Access the underlying protobuf directly.
  // Use this until RaftPeerView accessor is available.
  const ::kudu::consensus::RaftPeerPB& candidatePeerPb() const;
  ::kudu::consensus::RaftPeerPB* mutableCandidatePeerPb();

  // Create an owning copy of this view.
  std::unique_ptr<CandidateContextPb> toOwned() const;

 private:
  ::kudu::consensus::CandidateContext& pb_;
};

// Protobuf-backed implementation of CandidateContext.
// Owns the underlying protobuf.
class CandidateContextPb : public CandidateContext {
 public:
  CandidateContextPb();
  explicit CandidateContextPb(::kudu::consensus::CandidateContext pb);
  ~CandidateContextPb() override = default;

  // CandidateContextView interface
  bool hasCandidatePeer() const override;

  // Access the underlying protobuf directly.
  // Use this until RaftPeerView accessor is available.
  const ::kudu::consensus::RaftPeerPB& candidatePeerPb() const;
  ::kudu::consensus::RaftPeerPB* mutableCandidatePeerPb();

  // Access the underlying protobuf.
  const ::kudu::consensus::CandidateContext& pb() const;
  ::kudu::consensus::CandidateContext* mutablePb();

 private:
  ::kudu::consensus::CandidateContext pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
