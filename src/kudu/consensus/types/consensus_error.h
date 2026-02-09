// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/consensus_error_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for a ConsensusError.
// Extends ConsensusErrorView to indicate that implementations own their
// underlying data.
//
// Use ConsensusError when you need an owned copy, ConsensusErrorView when a
// reference suffices.
class ConsensusError : public ConsensusErrorView {
 public:
  ~ConsensusError() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
