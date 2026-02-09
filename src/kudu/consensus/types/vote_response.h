// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/vote_response_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for a VoteResponse.
// Extends VoteResponseView to indicate that implementations own their
// underlying data.
class VoteResponse : public VoteResponseView {
 public:
  ~VoteResponse() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
