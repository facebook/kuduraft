// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

namespace kudu {
namespace consensus {
namespace types {

// Interface for VoterContext.
//
// VoterContext is additional context sent back by the voter in VoteResponse.
class VoterContextView {
 public:
  virtual ~VoterContextView() = default;

  // Getters

  // Whether the candidate was removed from the voter's committed config.
  virtual bool is_candidate_removed() const = 0;

  // Setters

  virtual void set_is_candidate_removed(bool removed) = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
