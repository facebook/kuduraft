// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/consensus_status_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

// ConsensusStatusPbView implementation

ConsensusStatusPbView::ConsensusStatusPbView(ConsensusStatusPB& pb) : pb_(pb) {}

std::unique_ptr<OpIdView> ConsensusStatusPbView::lastReceived() {
  return std::make_unique<OpIdPbView>(*pb_.mutable_last_received());
}

bool ConsensusStatusPbView::hasLastReceived() const {
  return pb_.has_last_received();
}

std::unique_ptr<OpIdView> ConsensusStatusPbView::lastReceivedCurrentLeader() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_last_received_current_leader());
}

bool ConsensusStatusPbView::hasLastReceivedCurrentLeader() const {
  return pb_.has_last_received_current_leader();
}

std::optional<int64_t> ConsensusStatusPbView::lastCommittedIdx() const {
  if (pb_.has_last_committed_idx()) {
    return pb_.last_committed_idx();
  }
  return std::nullopt;
}

bool ConsensusStatusPbView::hasLastCommittedIdx() const {
  return pb_.has_last_committed_idx();
}

std::unique_ptr<ConsensusErrorView> ConsensusStatusPbView::error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_error());
}

bool ConsensusStatusPbView::hasError() const {
  return pb_.has_error();
}

void ConsensusStatusPbView::setLastCommittedIdx(int64_t idx) {
  pb_.set_last_committed_idx(idx);
}

void ConsensusStatusPbView::clearLastCommittedIdx() {
  pb_.clear_last_committed_idx();
}

std::unique_ptr<ConsensusStatusPb> ConsensusStatusPbView::toOwned() const {
  return std::make_unique<ConsensusStatusPb>(pb_);
}

// ConsensusStatusPb implementation

ConsensusStatusPb::ConsensusStatusPb() = default;

ConsensusStatusPb::ConsensusStatusPb(ConsensusStatusPB pb)
    : pb_(std::move(pb)) {}

std::unique_ptr<OpIdView> ConsensusStatusPb::lastReceived() {
  return std::make_unique<OpIdPbView>(*pb_.mutable_last_received());
}

bool ConsensusStatusPb::hasLastReceived() const {
  return pb_.has_last_received();
}

std::unique_ptr<OpIdView> ConsensusStatusPb::lastReceivedCurrentLeader() {
  return std::make_unique<OpIdPbView>(
      *pb_.mutable_last_received_current_leader());
}

bool ConsensusStatusPb::hasLastReceivedCurrentLeader() const {
  return pb_.has_last_received_current_leader();
}

std::optional<int64_t> ConsensusStatusPb::lastCommittedIdx() const {
  if (pb_.has_last_committed_idx()) {
    return pb_.last_committed_idx();
  }
  return std::nullopt;
}

bool ConsensusStatusPb::hasLastCommittedIdx() const {
  return pb_.has_last_committed_idx();
}

std::unique_ptr<ConsensusErrorView> ConsensusStatusPb::error() {
  return std::make_unique<ConsensusErrorPbView>(*pb_.mutable_error());
}

bool ConsensusStatusPb::hasError() const {
  return pb_.has_error();
}

void ConsensusStatusPb::setLastCommittedIdx(int64_t idx) {
  pb_.set_last_committed_idx(idx);
}

void ConsensusStatusPb::clearLastCommittedIdx() {
  pb_.clear_last_committed_idx();
}

const ConsensusStatusPB& ConsensusStatusPb::pb() const {
  return pb_;
}

ConsensusStatusPB* ConsensusStatusPb::mutablePb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
