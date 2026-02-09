// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

namespace kudu {
namespace consensus {
namespace types {

// Interface for CandidateContext.
//
// CandidateContext is additional context passed by the candidate in
// VoteRequest. Contains the candidate's peer information.
//
// Note: candidate_peer() accessor requires RaftPeerView which is defined in
// the nested types. Use has_candidate_peer() for presence check.
class CandidateContextView {
 public:
  virtual ~CandidateContextView() = default;

  // Check if candidate peer info is present.
  virtual bool has_candidate_peer() const = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
