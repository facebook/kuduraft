// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/types/opid.h"
#include "kudu/consensus/types/opid_view.h"

namespace kudu {
namespace consensus {
namespace types {

class OpIdPb;

// Protobuf-backed implementation of OpIdView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
// Use this for accessing nested OpId fields within other protobufs.
class OpIdPbView : public OpIdView {
 public:
  // Note: The protobuf class is kudu::consensus::OpId (generated from
  // opid.proto)
  explicit OpIdPbView(::kudu::consensus::OpId& pb);
  ~OpIdPbView() override = default;

  // OpIdView interface
  int64_t term() const override;
  int64_t index() const override;
  void set_term(int64_t term) override;
  void set_index(int64_t index) override;

  // Create an owning copy of this view.
  std::unique_ptr<OpIdPb> to_owned() const;

 private:
  ::kudu::consensus::OpId& pb_;
};

// Protobuf-backed implementation of OpId.
// Owns the underlying protobuf.
class OpIdPb : public OpId {
 public:
  OpIdPb();
  explicit OpIdPb(::kudu::consensus::OpId pb);
  OpIdPb(int64_t term, int64_t index);
  ~OpIdPb() override = default;

  // OpIdView interface
  int64_t term() const override;
  int64_t index() const override;
  void set_term(int64_t term) override;
  void set_index(int64_t index) override;

  // Access the underlying protobuf.
  const ::kudu::consensus::OpId& pb() const;
  ::kudu::consensus::OpId* mutable_pb();

 private:
  ::kudu::consensus::OpId pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
