// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/vote_request_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for a VoteRequest.
// Extends VoteRequestView to indicate that implementations own their
// underlying data.
class VoteRequest : public VoteRequestView {
 public:
  ~VoteRequest() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
