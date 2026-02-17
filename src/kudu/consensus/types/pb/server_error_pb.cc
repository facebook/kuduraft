// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/types/pb/server_error_pb.h"

#include <utility>

namespace kudu {
namespace consensus {
namespace types {

ServerErrorCode fromPbCode(ServerErrorPB::Code pbCode) {
  return static_cast<ServerErrorCode>(pbCode);
}

ServerErrorPB::Code toPbCode(ServerErrorCode code) {
  return static_cast<ServerErrorPB::Code>(code);
}

// ServerErrorPbView implementation

ServerErrorPbView::ServerErrorPbView(ServerErrorPB& pb) : pb_(pb) {}

ServerErrorCode ServerErrorPbView::code() const {
  return fromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ServerErrorPbView::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ServerErrorPbView::set_code(ServerErrorCode code) {
  pb_.set_code(toPbCode(code));
}

std::unique_ptr<ServerErrorPb> ServerErrorPbView::to_owned() const {
  return std::make_unique<ServerErrorPb>(pb_);
}

// ServerErrorPb implementation

ServerErrorPb::ServerErrorPb() = default;

ServerErrorPb::ServerErrorPb(ServerErrorPB pb) : pb_(std::move(pb)) {}

ServerErrorCode ServerErrorPb::code() const {
  return fromPbCode(pb_.code());
}

std::unique_ptr<::kudu::types::AppStatusView> ServerErrorPb::status() {
  return std::make_unique<::kudu::types::AppStatusPbView>(
      *pb_.mutable_status());
}

void ServerErrorPb::set_code(ServerErrorCode code) {
  pb_.set_code(toPbCode(code));
}

const ServerErrorPB& ServerErrorPb::pb() const {
  return pb_;
}

ServerErrorPB* ServerErrorPb::mutable_pb() {
  return &pb_;
}

} // namespace types
} // namespace consensus
} // namespace kudu
