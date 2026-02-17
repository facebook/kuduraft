// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "kudu/consensus/types/consensus_error_view.h"
#include "kudu/consensus/types/opid_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Interface for a ConsensusStatus.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class ConsensusStatusView {
 public:
  virtual ~ConsensusStatusView() = default;

  // Getters

  // The last message received (and replicated) by the peer.
  virtual std::unique_ptr<OpIdView> lastReceived() = 0;

  // Whether lastReceived is set.
  virtual bool hasLastReceived() const = 0;

  // The id of the last op replicated by the current leader.
  virtual std::unique_ptr<OpIdView> lastReceivedCurrentLeader() = 0;

  // Whether lastReceivedCurrentLeader is set.
  virtual bool hasLastReceivedCurrentLeader() const = 0;

  // The last committed index known to the peer.
  virtual std::optional<int64_t> lastCommittedIdx() const = 0;

  // Whether lastCommittedIdx is set.
  virtual bool hasLastCommittedIdx() const = 0;

  // The error, if any.
  virtual std::unique_ptr<ConsensusErrorView> error() = 0;

  // Whether an error is set.
  virtual bool hasError() const = 0;

  // Setters

  // Set lastCommittedIdx.
  virtual void setLastCommittedIdx(int64_t idx) = 0;

  // Clear lastCommittedIdx.
  virtual void clearLastCommittedIdx() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
