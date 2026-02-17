// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/consensus_status.h"
#include "kudu/consensus/types/consensus_status_view.h"
#include "kudu/consensus/types/pb/consensus_error_pb.h"
#include "kudu/consensus/types/pb/opid_pb.h"

namespace kudu {
namespace consensus {
namespace types {

class ConsensusStatusPb;

// Protobuf-backed implementation of ConsensusStatusView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class ConsensusStatusPbView : public ConsensusStatusView {
 public:
  explicit ConsensusStatusPbView(ConsensusStatusPB& pb);
  ~ConsensusStatusPbView() override = default;

  // ConsensusStatusView interface - getters
  std::unique_ptr<OpIdView> lastReceived() override;
  bool hasLastReceived() const override;
  std::unique_ptr<OpIdView> lastReceivedCurrentLeader() override;
  bool hasLastReceivedCurrentLeader() const override;
  std::optional<int64_t> lastCommittedIdx() const override;
  bool hasLastCommittedIdx() const override;
  std::unique_ptr<ConsensusErrorView> error() override;
  bool hasError() const override;

  // ConsensusStatusView interface - setters
  void setLastCommittedIdx(int64_t idx) override;
  void clearLastCommittedIdx() override;

  // Create an owning copy of this view.
  std::unique_ptr<ConsensusStatusPb> toOwned() const;

 private:
  ConsensusStatusPB& pb_;
};

// Protobuf-backed implementation of ConsensusStatus.
// Owns the underlying protobuf.
class ConsensusStatusPb : public ConsensusStatus {
 public:
  ConsensusStatusPb();
  explicit ConsensusStatusPb(ConsensusStatusPB pb);
  ~ConsensusStatusPb() override = default;

  // ConsensusStatusView interface - getters
  std::unique_ptr<OpIdView> lastReceived() override;
  bool hasLastReceived() const override;
  std::unique_ptr<OpIdView> lastReceivedCurrentLeader() override;
  bool hasLastReceivedCurrentLeader() const override;
  std::optional<int64_t> lastCommittedIdx() const override;
  bool hasLastCommittedIdx() const override;
  std::unique_ptr<ConsensusErrorView> error() override;
  bool hasError() const override;

  // ConsensusStatusView interface - setters
  void setLastCommittedIdx(int64_t idx) override;
  void clearLastCommittedIdx() override;

  // Access the underlying protobuf.
  const ConsensusStatusPB& pb() const;
  ConsensusStatusPB* mutablePb();

 private:
  ConsensusStatusPB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
