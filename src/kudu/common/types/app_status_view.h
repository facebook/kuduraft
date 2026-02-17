// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace kudu {
namespace types {

// Error codes for AppStatus.
// These mirror the protobuf AppStatusPB::ErrorCode values.
enum class AppStatusCode {
  kUnknownError = 999,
  kOk = 0,
  kNotFound = 1,
  kCorruption = 2,
  kNotSupported = 3,
  kInvalidArgument = 4,
  kIoError = 5,
  kAlreadyPresent = 6,
  kRuntimeError = 7,
  kNetworkError = 8,
  kIllegalState = 9,
  kNotAuthorized = 10,
  kAborted = 11,
  kRemoteError = 12,
  kServiceUnavailable = 13,
  kTimedOut = 14,
  kUninitialized = 15,
  kConfigurationError = 16,
  kIncomplete = 17,
  kEndOfFile = 18,
  kCancelled = 19,
  kCompressionDictMismatch = 20,
  kContinue = 21,
};

// Interface for an AppStatus (application status/error).
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class AppStatusView {
 public:
  virtual ~AppStatusView() = default;

  // Getters

  // The error code.
  virtual AppStatusCode code() const = 0;

  // The error message (empty if not set).
  virtual const std::string& message() const = 0;

  // Whether a message is set.
  virtual bool hasMessage() const = 0;

  // The POSIX error code (nullopt if not set).
  virtual std::optional<int32_t> posixCode() const = 0;

  // Whether a POSIX code is set.
  virtual bool hasPosixCode() const = 0;

  // Setters

  // Set the error code.
  virtual void setCode(AppStatusCode code) = 0;

  // Set the error message.
  virtual void setMessage(const std::string& message) = 0;

  // Clear the error message.
  virtual void clearMessage() = 0;

  // Set the POSIX error code.
  virtual void setPosixCode(int32_t posixCode) = 0;

  // Clear the POSIX error code.
  virtual void clearPosixCode() = 0;
};

} // namespace types
} // namespace kudu
