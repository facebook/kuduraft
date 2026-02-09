// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <memory>

#include "kudu/common/types/app_status_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Error codes for ServerError.
// These mirror the protobuf ServerErrorPB::Code values.
enum class ServerErrorCode {
  kUnknownError = 1,
  kInvalidConfig = 9,
  kConsensusNotRunning = 12,
  kNotTheLeader = 15,
  kWrongServerUuid = 16,
  kCasFailed = 17,
  kAlreadyInprogress = 18,
  kRingTokenMismatch = 19,
  kInvalidClientRequest = 20,
  kServiceUnavailable = 21,
  kNotVoter = 22,
  kProxyMissingLogEntries = 23,
};

// Interface for a ServerError.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class ServerErrorView {
 public:
  virtual ~ServerErrorView() = default;

  // Getters

  // The error code.
  virtual ServerErrorCode code() const = 0;

  // The status (contains message and details).
  // Returns a mutable view into the nested AppStatusPB.
  virtual std::unique_ptr<::kudu::types::AppStatusView> status() = 0;

  // Setters

  // Set the error code.
  virtual void set_code(ServerErrorCode code) = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
