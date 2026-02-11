// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/common/types/pb/app_status_pb.h"

#include <utility>

namespace kudu {
namespace types {

namespace {
const std::string kEmptyString;
} // namespace

AppStatusCode FromPbErrorCode(AppStatusPB::ErrorCode pb_code) {
  return static_cast<AppStatusCode>(pb_code);
}

AppStatusPB::ErrorCode ToPbErrorCode(AppStatusCode code) {
  return static_cast<AppStatusPB::ErrorCode>(code);
}

// AppStatusPbView implementation

AppStatusPbView::AppStatusPbView(AppStatusPB& pb) : pb_(pb) {}

AppStatusCode AppStatusPbView::code() const {
  return FromPbErrorCode(pb_.code());
}

const std::string& AppStatusPbView::message() const {
  if (pb_.has_message()) {
    return pb_.message();
  }
  return kEmptyString;
}

bool AppStatusPbView::has_message() const {
  return pb_.has_message();
}

std::optional<int32_t> AppStatusPbView::posix_code() const {
  if (pb_.has_posix_code()) {
    return pb_.posix_code();
  }
  return std::nullopt;
}

bool AppStatusPbView::has_posix_code() const {
  return pb_.has_posix_code();
}

void AppStatusPbView::set_code(AppStatusCode code) {
  pb_.set_code(ToPbErrorCode(code));
}

void AppStatusPbView::set_message(const std::string& message) {
  pb_.set_message(message);
}

void AppStatusPbView::clear_message() {
  pb_.clear_message();
}

void AppStatusPbView::set_posix_code(int32_t posix_code) {
  pb_.set_posix_code(posix_code);
}

void AppStatusPbView::clear_posix_code() {
  pb_.clear_posix_code();
}

std::unique_ptr<AppStatusPb> AppStatusPbView::to_owned() const {
  return std::make_unique<AppStatusPb>(pb_);
}

// AppStatusPb implementation

AppStatusPb::AppStatusPb() = default;

AppStatusPb::AppStatusPb(AppStatusPB pb) : pb_(std::move(pb)) {}

AppStatusCode AppStatusPb::code() const {
  return FromPbErrorCode(pb_.code());
}

const std::string& AppStatusPb::message() const {
  if (pb_.has_message()) {
    return pb_.message();
  }
  return kEmptyString;
}

bool AppStatusPb::has_message() const {
  return pb_.has_message();
}

std::optional<int32_t> AppStatusPb::posix_code() const {
  if (pb_.has_posix_code()) {
    return pb_.posix_code();
  }
  return std::nullopt;
}

bool AppStatusPb::has_posix_code() const {
  return pb_.has_posix_code();
}

void AppStatusPb::set_code(AppStatusCode code) {
  pb_.set_code(ToPbErrorCode(code));
}

void AppStatusPb::set_message(const std::string& message) {
  pb_.set_message(message);
}

void AppStatusPb::clear_message() {
  pb_.clear_message();
}

void AppStatusPb::set_posix_code(int32_t posix_code) {
  pb_.set_posix_code(posix_code);
}

void AppStatusPb::clear_posix_code() {
  pb_.clear_posix_code();
}

const AppStatusPB& AppStatusPb::pb() const {
  return pb_;
}

AppStatusPB* AppStatusPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace kudu
