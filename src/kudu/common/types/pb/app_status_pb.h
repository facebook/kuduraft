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
AppStatusCode fromPbErrorCode(AppStatusPB::ErrorCode pbCode);
AppStatusPB::ErrorCode toPbErrorCode(AppStatusCode code);

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
  bool hasMessage() const override;
  std::optional<int32_t> posixCode() const override;
  bool hasPosixCode() const override;

  // AppStatusView interface - setters
  void setCode(AppStatusCode code) override;
  void setMessage(const std::string& message) override;
  void clearMessage() override;
  void setPosixCode(int32_t posixCode) override;
  void clearPosixCode() override;

  // Create an owning copy of this view.
  std::unique_ptr<AppStatusPb> toOwned() const;

 private:
  AppStatusPB& pb_;
};

// Protobuf-backed implementation of AppStatus.
// Owns the underlying protobuf.
class AppStatusPb : public AppStatus {
 public:
  AppStatusPb();
  explicit AppStatusPb(AppStatusPB pb);
  ~AppStatusPb() override = default;

  // AppStatusView interface - getters
  AppStatusCode code() const override;
  const std::string& message() const override;
  bool hasMessage() const override;
  std::optional<int32_t> posixCode() const override;
  bool hasPosixCode() const override;

  // AppStatusView interface - setters
  void setCode(AppStatusCode code) override;
  void setMessage(const std::string& message) override;
  void clearMessage() override;
  void setPosixCode(int32_t posixCode) override;
  void clearPosixCode() override;

  // Access the underlying protobuf.
  const AppStatusPB& pb() const;
  AppStatusPB* mutablePb();

 private:
  AppStatusPB pb_;
};

} // namespace types
} // namespace kudu
