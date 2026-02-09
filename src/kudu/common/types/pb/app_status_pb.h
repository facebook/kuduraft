// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/common/types/app_status.h"
#include "kudu/common/types/app_status_view.h"
#include "kudu/common/wire_protocol.pb.h"

namespace kudu {
namespace types {

class AppStatusPb;

// Convert between protobuf ErrorCode and wrapper AppStatusCode.
AppStatusCode FromPbErrorCode(AppStatusPB::ErrorCode pb_code);
AppStatusPB::ErrorCode ToPbErrorCode(AppStatusCode code);

// Protobuf-backed implementation of AppStatusView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
// Use this for accessing nested AppStatus fields within other protobufs.
class AppStatusPbView : public AppStatusView {
 public:
  explicit AppStatusPbView(AppStatusPB& pb);
  ~AppStatusPbView() override = default;

  // AppStatusView interface - getters
  AppStatusCode code() const override;
  const std::string& message() const override;
  bool has_message() const override;
  std::optional<int32_t> posix_code() const override;
  bool has_posix_code() const override;

  // AppStatusView interface - setters
  void set_code(AppStatusCode code) override;
  void set_message(const std::string& message) override;
  void clear_message() override;
  void set_posix_code(int32_t posix_code) override;
  void clear_posix_code() override;

  // Create an owning copy of this view.
  std::unique_ptr<AppStatusPb> to_owned() const;

 private:
  AppStatusPB& pb_;
};

// Protobuf-backed implementation of AppStatus.
// Owns the underlying protobuf.
class AppStatusPb : public AppStatus {
 public:
  AppStatusPb();
  explicit AppStatusPb(const AppStatusPB& pb);
  ~AppStatusPb() override = default;

  // AppStatusView interface - getters
  AppStatusCode code() const override;
  const std::string& message() const override;
  bool has_message() const override;
  std::optional<int32_t> posix_code() const override;
  bool has_posix_code() const override;

  // AppStatusView interface - setters
  void set_code(AppStatusCode code) override;
  void set_message(const std::string& message) override;
  void clear_message() override;
  void set_posix_code(int32_t posix_code) override;
  void clear_posix_code() override;

  // Access the underlying protobuf.
  const AppStatusPB& pb() const;
  AppStatusPB* mutable_pb();

 private:
  AppStatusPB pb_;
};

} // namespace types
} // namespace kudu
