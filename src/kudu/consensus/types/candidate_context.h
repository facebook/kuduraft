// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/candidate_context_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for CandidateContext.
class CandidateContext : public CandidateContextView {
 public:
  ~CandidateContext() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
