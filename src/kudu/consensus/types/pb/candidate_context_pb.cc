// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/candidate_context_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

// CandidateContextPbView implementation

CandidateContextPbView::CandidateContextPbView(
    ::kudu::consensus::CandidateContext& pb)
    : pb_(pb) {}

bool CandidateContextPbView::has_candidate_peer() const {
  return pb_.has_candidate_peer_pb();
}

const ::kudu::consensus::RaftPeerPB& CandidateContextPbView::candidate_peer_pb()
    const {
  return pb_.candidate_peer_pb();
}

::kudu::consensus::RaftPeerPB*
CandidateContextPbView::mutable_candidate_peer_pb() {
  return pb_.mutable_candidate_peer_pb();
}

std::unique_ptr<CandidateContextPb> CandidateContextPbView::to_owned() const {
  return std::make_unique<CandidateContextPb>(pb_);
}

// CandidateContextPb implementation

CandidateContextPb::CandidateContextPb() = default;

CandidateContextPb::CandidateContextPb(::kudu::consensus::CandidateContext pb)
    : pb_(std::move(pb)) {}

bool CandidateContextPb::has_candidate_peer() const {
  return pb_.has_candidate_peer_pb();
}

const ::kudu::consensus::RaftPeerPB& CandidateContextPb::candidate_peer_pb()
    const {
  return pb_.candidate_peer_pb();
}

::kudu::consensus::RaftPeerPB* CandidateContextPb::mutable_candidate_peer_pb() {
  return pb_.mutable_candidate_peer_pb();
}

const ::kudu::consensus::CandidateContext& CandidateContextPb::pb() const {
  return pb_;
}

::kudu::consensus::CandidateContext* CandidateContextPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
