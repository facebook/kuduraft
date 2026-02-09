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

  LastKnownLeader(std::string uuid, int64_t election_term)
      : uuid_(std::move(uuid)), election_term_(election_term) {}

  const std::string& uuid() const {
    return uuid_;
  }

  void set_uuid(const std::string& uuid) {
    uuid_ = uuid;
  }

  int64_t election_term() const {
    return election_term_;
  }

  void set_election_term(int64_t term) {
    election_term_ = term;
  }

 private:
  std::string uuid_;
  int64_t election_term_ = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
