// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/common/types/pb/app_status_pb.h"

#include <utility>

namespace kudu {
namespace types {

namespace {
const std::string kEmptyString;
} // namespace

AppStatusCode fromPbErrorCode(AppStatusPB::ErrorCode pbCode) {
  return static_cast<AppStatusCode>(pbCode);
}

AppStatusPB::ErrorCode toPbErrorCode(AppStatusCode code) {
  return static_cast<AppStatusPB::ErrorCode>(code);
}

// AppStatusPbView implementation

AppStatusPbView::AppStatusPbView(AppStatusPB& pb) : pb_(pb) {}

AppStatusCode AppStatusPbView::code() const {
  return fromPbErrorCode(pb_.code());
}

const std::string& AppStatusPbView::message() const {
  if (pb_.has_message()) {
    return pb_.message();
  }
  return kEmptyString;
}

bool AppStatusPbView::hasMessage() const {
  return pb_.has_message();
}

std::optional<int32_t> AppStatusPbView::posixCode() const {
  if (pb_.has_posix_code()) {
    return pb_.posix_code();
  }
  return std::nullopt;
}

bool AppStatusPbView::hasPosixCode() const {
  return pb_.has_posix_code();
}

void AppStatusPbView::setCode(AppStatusCode code) {
  pb_.set_code(toPbErrorCode(code));
}

void AppStatusPbView::setMessage(const std::string& message) {
  pb_.set_message(message);
}

void AppStatusPbView::clearMessage() {
  pb_.clear_message();
}

void AppStatusPbView::setPosixCode(int32_t posixCode) {
  pb_.set_posix_code(posixCode);
}

void AppStatusPbView::clearPosixCode() {
  pb_.clear_posix_code();
}

std::unique_ptr<AppStatusPb> AppStatusPbView::toOwned() const {
  return std::make_unique<AppStatusPb>(pb_);
}

// AppStatusPb implementation

AppStatusPb::AppStatusPb() = default;

AppStatusPb::AppStatusPb(AppStatusPB pb) : pb_(std::move(pb)) {}

AppStatusCode AppStatusPb::code() const {
  return fromPbErrorCode(pb_.code());
}

const std::string& AppStatusPb::message() const {
  if (pb_.has_message()) {
    return pb_.message();
  }
  return kEmptyString;
}

bool AppStatusPb::hasMessage() const {
  return pb_.has_message();
}

std::optional<int32_t> AppStatusPb::posixCode() const {
  if (pb_.has_posix_code()) {
    return pb_.posix_code();
  }
  return std::nullopt;
}

bool AppStatusPb::hasPosixCode() const {
  return pb_.has_posix_code();
}

void AppStatusPb::setCode(AppStatusCode code) {
  pb_.set_code(toPbErrorCode(code));
}

void AppStatusPb::setMessage(const std::string& message) {
  pb_.set_message(message);
}

void AppStatusPb::clearMessage() {
  pb_.clear_message();
}

void AppStatusPb::setPosixCode(int32_t posixCode) {
  pb_.set_posix_code(posixCode);
}

void AppStatusPb::clearPosixCode() {
  pb_.clear_posix_code();
}

const AppStatusPB& AppStatusPb::pb() const {
  return pb_;
}

AppStatusPB* AppStatusPb::mutablePb() {
  return &pb_;
}

} // namespace types
} // namespace kudu
