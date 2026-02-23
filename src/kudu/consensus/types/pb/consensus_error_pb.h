// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/common/types/pb/app_status_pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/consensus_error.h"
#include "kudu/consensus/types/consensus_error_view.h"

namespace kudu {
namespace consensus {
namespace types {

class ConsensusErrorPb;

// Convert between protobuf Code and wrapper ConsensusErrorCode.
ConsensusErrorCode fromPbCode(ConsensusErrorPB::Code pbCode);
ConsensusErrorPB::Code toPbCode(ConsensusErrorCode code);

// Protobuf-backed implementation of ConsensusErrorView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
// Use this for accessing nested ConsensusError fields within other protobufs.
class ConsensusErrorPbView : public ConsensusErrorView {
 public:
  explicit ConsensusErrorPbView(ConsensusErrorPB& pb);
  ~ConsensusErrorPbView() override = default;

  // ConsensusErrorView interface - getters
  ConsensusErrorCode code() const override;
  std::unique_ptr<::kudu::types::AppStatusView> status() override;

  // ConsensusErrorView interface - setters
  void setCode(ConsensusErrorCode code) override;

  // Create an owning copy of this view.
  std::unique_ptr<ConsensusErrorPb> toOwned() const;

 private:
  ConsensusErrorPB& pb_;
};

// Protobuf-backed implementation of ConsensusError.
// Owns the underlying protobuf.
class ConsensusErrorPb : public ConsensusError {
 public:
  ConsensusErrorPb();
  explicit ConsensusErrorPb(ConsensusErrorPB pb);
  ~ConsensusErrorPb() override = default;

  // ConsensusErrorView interface - getters
  ConsensusErrorCode code() const override;
  std::unique_ptr<::kudu::types::AppStatusView> status() override;

  // ConsensusErrorView interface - setters
  void setCode(ConsensusErrorCode code) override;

  // Access the underlying protobuf.
  const ConsensusErrorPB& pb() const;
  ConsensusErrorPB* mutablePb();

 private:
  ConsensusErrorPB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
