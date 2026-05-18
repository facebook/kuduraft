// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <string>

namespace kudu {
namespace consensus {
namespace types {

// Simple value type for previous vote information.
// Used to track voting history in VoteResponse.
class PreviousVote {
 public:
  PreviousVote() = default;

  PreviousVote(std::string candidateUuid, int64_t electionTerm)
      : candidateUuid_(std::move(candidateUuid)), electionTerm_(electionTerm) {}

  const std::string& candidateUuid() const {
    return candidateUuid_;
  }

  void setCandidateUuid(const std::string& uuid) {
    candidateUuid_ = uuid;
  }

  int64_t electionTerm() const {
    return electionTerm_;
  }

  void setElectionTerm(int64_t term) {
    electionTerm_ = term;
  }

 private:
  std::string candidateUuid_;
  int64_t electionTerm_ = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
