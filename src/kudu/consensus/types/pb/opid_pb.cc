// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/opid_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

// OpIdPbView implementation

OpIdPbView::OpIdPbView(::kudu::consensus::OpId& pb) : pb_(pb) {}

int64_t OpIdPbView::term() const {
  return pb_.term();
}

int64_t OpIdPbView::index() const {
  return pb_.index();
}

void OpIdPbView::set_term(int64_t term) {
  pb_.set_term(term);
}

void OpIdPbView::set_index(int64_t index) {
  pb_.set_index(index);
}

std::unique_ptr<OpIdPb> OpIdPbView::to_owned() const {
  return std::make_unique<OpIdPb>(pb_);
}

// OpIdPb implementation

OpIdPb::OpIdPb() = default;

OpIdPb::OpIdPb(::kudu::consensus::OpId pb) : pb_(std::move(pb)) {}

OpIdPb::OpIdPb(int64_t term, int64_t index) {
  pb_.set_term(term);
  pb_.set_index(index);
}

int64_t OpIdPb::term() const {
  return pb_.term();
}

int64_t OpIdPb::index() const {
  return pb_.index();
}

void OpIdPb::set_term(int64_t term) {
  pb_.set_term(term);
}

void OpIdPb::set_index(int64_t index) {
  pb_.set_index(index);
}

const ::kudu::consensus::OpId& OpIdPb::pb() const {
  return pb_;
}

::kudu::consensus::OpId* OpIdPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
