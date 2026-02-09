// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/voter_context_pb.h"

namespace kudu {
namespace consensus {
namespace types {

// VoterContextPbView implementation

VoterContextPbView::VoterContextPbView(::kudu::consensus::VoterContext& pb)
    : pb_(pb) {}

bool VoterContextPbView::is_candidate_removed() const {
  return pb_.is_candidate_removed();
}

void VoterContextPbView::set_is_candidate_removed(bool removed) {
  pb_.set_is_candidate_removed(removed);
}

std::unique_ptr<VoterContextPb> VoterContextPbView::to_owned() const {
  return std::make_unique<VoterContextPb>(pb_);
}

// VoterContextPb implementation

VoterContextPb::VoterContextPb() = default;

VoterContextPb::VoterContextPb(const ::kudu::consensus::VoterContext& pb)
    : pb_(pb) {}

bool VoterContextPb::is_candidate_removed() const {
  return pb_.is_candidate_removed();
}

void VoterContextPb::set_is_candidate_removed(bool removed) {
  pb_.set_is_candidate_removed(removed);
}

const ::kudu::consensus::VoterContext& VoterContextPb::pb() const {
  return pb_;
}

::kudu::consensus::VoterContext* VoterContextPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
