// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <string>

namespace kudu {
namespace consensus {
namespace types {

// Simple value type for last known leader information.
// Used to track leader history in VoteResponse.
class LastKnownLeader {
 public:
  LastKnownLeader() = default;

  LastKnownLeader(std::string uuid, int64_t electionTerm)
      : uuid_(std::move(uuid)), electionTerm_(electionTerm) {}

  const std::string& uuid() const {
    return uuid_;
  }

  void setUuid(const std::string& uuid) {
    uuid_ = uuid;
  }

  int64_t electionTerm() const {
    return electionTerm_;
  }

  void setElectionTerm(int64_t term) {
    electionTerm_ = term;
  }

 private:
  std::string uuid_;
  int64_t electionTerm_ = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
