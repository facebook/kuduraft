// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/consensus_error_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

ConsensusErrorCode fromPbCode(ConsensusErrorPB::Code pbCode) {
  return static_cast<ConsensusErrorCode>(pbCode);
}

ConsensusErrorPB::Code toPbCode(ConsensusErrorCode code) {
  return static_cast<ConsensusErrorPB::Code>(code);
}

// ConsensusErrorPbView implementation

ConsensusErrorPbView::ConsensusErrorPbView(ConsensusErrorPB& pb) : pb_(pb) {}

ConsensusErrorCode ConsensusErrorPbView::code() const {
  return fromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ConsensusErrorPbView::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ConsensusErrorPbView::setCode(ConsensusErrorCode code) {
  pb_.set_code(toPbCode(code));
}

std::unique_ptr<ConsensusErrorPb> ConsensusErrorPbView::toOwned() const {
  return std::make_unique<ConsensusErrorPb>(pb_);
}

// ConsensusErrorPb implementation

ConsensusErrorPb::ConsensusErrorPb() = default;

ConsensusErrorPb::ConsensusErrorPb(ConsensusErrorPB pb) : pb_(std::move(pb)) {}

ConsensusErrorCode ConsensusErrorPb::code() const {
  return fromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ConsensusErrorPb::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ConsensusErrorPb::setCode(ConsensusErrorCode code) {
  pb_.set_code(toPbCode(code));
}

const ConsensusErrorPB& ConsensusErrorPb::pb() const {
  return pb_;
}

ConsensusErrorPB* ConsensusErrorPb::mutablePb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
