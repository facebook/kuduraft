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
  virtual std::unique_ptr<OpIdView> last_received() = 0;

  // Whether last_received is set.
  virtual bool has_last_received() const = 0;

  // The id of the last op replicated by the current leader.
  virtual std::unique_ptr<OpIdView> last_received_current_leader() = 0;

  // Whether last_received_current_leader is set.
  virtual bool has_last_received_current_leader() const = 0;

  // The last committed index known to the peer.
  virtual std::optional<int64_t> last_committed_idx() const = 0;

  // Whether last_committed_idx is set.
  virtual bool has_last_committed_idx() const = 0;

  // The error, if any.
  virtual std::unique_ptr<ConsensusErrorView> error() = 0;

  // Whether an error is set.
  virtual bool has_error() const = 0;

  // Setters

  // Set last_committed_idx.
  virtual void set_last_committed_idx(int64_t idx) = 0;

  // Clear last_committed_idx.
  virtual void clear_last_committed_idx() = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
