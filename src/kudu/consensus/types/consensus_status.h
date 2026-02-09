// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/consensus_status_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for a ConsensusStatus.
// Extends ConsensusStatusView to indicate that implementations own their
// underlying data.
//
// Use ConsensusStatus when you need an owned copy, ConsensusStatusView when a
// reference suffices.
class ConsensusStatus : public ConsensusStatusView {
 public:
  ~ConsensusStatus() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
