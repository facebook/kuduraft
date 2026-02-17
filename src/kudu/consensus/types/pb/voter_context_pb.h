// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/voter_context.h"
#include "kudu/consensus/types/voter_context_view.h"

namespace kudu {
namespace consensus {
namespace types {

class VoterContextPb;

// Protobuf-backed implementation of VoterContextView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class VoterContextPbView : public VoterContextView {
 public:
  explicit VoterContextPbView(::kudu::consensus::VoterContext& pb);
  ~VoterContextPbView() override = default;

  // VoterContextView interface
  bool isCandidateRemoved() const override;
  void setIsCandidateRemoved(bool removed) override;

  // Create an owning copy of this view.
  std::unique_ptr<VoterContextPb> toOwned() const;

 private:
  ::kudu::consensus::VoterContext& pb_;
};

// Protobuf-backed implementation of VoterContext.
// Owns the underlying protobuf.
class VoterContextPb : public VoterContext {
 public:
  VoterContextPb();
  explicit VoterContextPb(::kudu::consensus::VoterContext pb);
  ~VoterContextPb() override = default;

  // VoterContextView interface
  bool isCandidateRemoved() const override;
  void setIsCandidateRemoved(bool removed) override;

  // Access the underlying protobuf.
  const ::kudu::consensus::VoterContext& pb() const;
  ::kudu::consensus::VoterContext* mutable_pb();

 private:
  ::kudu::consensus::VoterContext pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
