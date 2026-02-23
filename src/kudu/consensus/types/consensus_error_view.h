// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <memory>

#include "kudu/common/types/app_status_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Error codes for ConsensusError.
// These mirror the protobuf ConsensusErrorPB::Code values.
enum class ConsensusErrorCode {
  kUnknown = 0,
  kInvalidTerm = 2,
  kLastOpIdTooOld = 3,
  kAlreadyVoted = 4,
  kNotInQuorum = 5,
  kPrecedingEntryDidntMatch = 6,
  kLeaderIsAlive = 7,
  kConsensusBusy = 8,
  kCannotPrepare = 9,
};

// Interface for a ConsensusError.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class ConsensusErrorView {
 public:
  virtual ~ConsensusErrorView() = default;

  // Getters

  // The error code.
  virtual ConsensusErrorCode code() const = 0;

  // The status (contains message and details).
  // Returns a mutable view into the nested AppStatusPB.
  virtual std::unique_ptr<::kudu::types::AppStatusView> status() = 0;

  // Setters

  // Set the error code.
  virtual void setCode(ConsensusErrorCode code) = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
