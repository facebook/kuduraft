// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/consensus_error_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

ConsensusErrorCode FromPbCode(ConsensusErrorPB::Code pb_code) {
  return static_cast<ConsensusErrorCode>(pb_code);
}

ConsensusErrorPB::Code ToPbCode(ConsensusErrorCode code) {
  return static_cast<ConsensusErrorPB::Code>(code);
}

// ConsensusErrorPbView implementation

ConsensusErrorPbView::ConsensusErrorPbView(ConsensusErrorPB& pb) : pb_(pb) {}

ConsensusErrorCode ConsensusErrorPbView::code() const {
  return FromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ConsensusErrorPbView::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ConsensusErrorPbView::set_code(ConsensusErrorCode code) {
  pb_.set_code(ToPbCode(code));
}

std::unique_ptr<ConsensusErrorPb> ConsensusErrorPbView::to_owned() const {
  return std::make_unique<ConsensusErrorPb>(pb_);
}

// ConsensusErrorPb implementation

ConsensusErrorPb::ConsensusErrorPb() = default;

ConsensusErrorPb::ConsensusErrorPb(ConsensusErrorPB pb) : pb_(std::move(pb)) {}

ConsensusErrorCode ConsensusErrorPb::code() const {
  return FromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ConsensusErrorPb::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ConsensusErrorPb::set_code(ConsensusErrorCode code) {
  pb_.set_code(ToPbCode(code));
}

const ConsensusErrorPB& ConsensusErrorPb::pb() const {
  return pb_;
}

ConsensusErrorPB* ConsensusErrorPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
