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

  PreviousVote(std::string candidate_uuid, int64_t election_term)
      : candidate_uuid_(std::move(candidate_uuid)),
        election_term_(election_term) {}

  const std::string& candidate_uuid() const {
    return candidate_uuid_;
  }

  void set_candidate_uuid(const std::string& uuid) {
    candidate_uuid_ = uuid;
  }

  int64_t election_term() const {
    return election_term_;
  }

  void set_election_term(int64_t term) {
    election_term_ = term;
  }

 private:
  std::string candidate_uuid_;
  int64_t election_term_ = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
