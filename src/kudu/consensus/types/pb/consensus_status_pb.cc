// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/consensus_status_pb.h"

namespace kudu {
namespace consensus {
namespace types {

// ConsensusStatusPbView implementation

ConsensusStatusPbView::ConsensusStatusPbView(ConsensusStatusPB& pb) : pb_(pb) {}

std::unique_ptr<OpIdView> ConsensusStatusPbView::last_received() {
  return std::make_unique<OpIdPbView>(*pb_.mutable_last_received());
}

bool ConsensusStatusPbView::has_last_received() const {
  return pb_.has_last_received();
}

std::unique_ptr<OpIdView>
ConsensusStatusPbView::last_received_current_leader() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_last_received_current_leader());
}

bool ConsensusStatusPbView::has_last_received_current_leader() const {
  return pb_.has_last_received_current_leader();
}

std::optional<int64_t> ConsensusStatusPbView::last_committed_idx() const {
  if (pb_.has_last_committed_idx()) {
    return pb_.last_committed_idx();
  }
  return std::nullopt;
}

bool ConsensusStatusPbView::has_last_committed_idx() const {
  return pb_.has_last_committed_idx();
}

std::unique_ptr<ConsensusErrorView> ConsensusStatusPbView::error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_error());
}

bool ConsensusStatusPbView::has_error() const {
  return pb_.has_error();
}

void ConsensusStatusPbView::set_last_committed_idx(int64_t idx) {
  pb_.set_last_committed_idx(idx);
}

void ConsensusStatusPbView::clear_last_committed_idx() {
  pb_.clear_last_committed_idx();
}

std::unique_ptr<ConsensusStatusPb> ConsensusStatusPbView::to_owned() const {
  return std::make_unique<ConsensusStatusPb>(pb_);
}

// ConsensusStatusPb implementation

ConsensusStatusPb::ConsensusStatusPb() = default;

ConsensusStatusPb::ConsensusStatusPb(const ConsensusStatusPB& pb) : pb_(pb) {}

std::unique_ptr<OpIdView> ConsensusStatusPb::last_received() {
  return std::make_unique<OpIdPbView>(*pb_.mutable_last_received());
}

bool ConsensusStatusPb::has_last_received() const {
  return pb_.has_last_received();
}

std::unique_ptr<OpIdView> ConsensusStatusPb::last_received_current_leader() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_last_received_current_leader());
}

bool ConsensusStatusPb::has_last_received_current_leader() const {
  return pb_.has_last_received_current_leader();
}

std::optional<int64_t> ConsensusStatusPb::last_committed_idx() const {
  if (pb_.has_last_committed_idx()) {
    return pb_.last_committed_idx();
  }
  return std::nullopt;
}

bool ConsensusStatusPb::has_last_committed_idx() const {
  return pb_.has_last_committed_idx();
}

std::unique_ptr<ConsensusErrorView> ConsensusStatusPb::error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_error());
}

bool ConsensusStatusPb::has_error() const {
  return pb_.has_error();
}

void ConsensusStatusPb::set_last_committed_idx(int64_t idx) {
  pb_.set_last_committed_idx(idx);
}

void ConsensusStatusPb::clear_last_committed_idx() {
  pb_.clear_last_committed_idx();
}

const ConsensusStatusPB& ConsensusStatusPb::pb() const {
  return pb_;
}

ConsensusStatusPB* ConsensusStatusPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
