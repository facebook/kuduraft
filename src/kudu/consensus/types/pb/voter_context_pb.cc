// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/voter_context_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

// VoterContextPbView implementation

VoterContextPbView::VoterContextPbView(::kudu::consensus::VoterContext& pb)
    : pb_(pb) {}

bool VoterContextPbView::isCandidateRemoved() const {
  return pb_.is_candidate_removed();
}

void VoterContextPbView::setIsCandidateRemoved(bool removed) {
  pb_.set_is_candidate_removed(removed);
}

std::unique_ptr<VoterContextPb> VoterContextPbView::toOwned() const {
  return std::make_unique<VoterContextPb>(pb_);
}

// VoterContextPb implementation

VoterContextPb::VoterContextPb() = default;

VoterContextPb::VoterContextPb(::kudu::consensus::VoterContext pb)
    : pb_(std::move(pb)) {}

bool VoterContextPb::isCandidateRemoved() const {
  return pb_.is_candidate_removed();
}

void VoterContextPb::setIsCandidateRemoved(bool removed) {
  pb_.set_is_candidate_removed(removed);
}

const ::kudu::consensus::VoterContext& VoterContextPb::pb() const {
  return pb_;
}

::kudu::consensus::VoterContext* VoterContextPb::mutablePb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
