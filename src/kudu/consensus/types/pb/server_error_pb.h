// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <memory>

#include "kudu/common/types/pb/app_status_pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/types/server_error.h"
#include "kudu/consensus/types/server_error_view.h"

namespace kudu {
namespace consensus {
namespace types {

class ServerErrorPb;

// Convert between protobuf Code and wrapper ServerErrorCode.
ServerErrorCode fromPbCode(ServerErrorPB::Code pbCode);
ServerErrorPB::Code toPbCode(ServerErrorCode code);

// Protobuf-backed implementation of ServerErrorView.
// Does NOT own the underlying protobuf - holds a mutable reference to it.
class ServerErrorPbView : public ServerErrorView {
 public:
  explicit ServerErrorPbView(ServerErrorPB& pb);
  ~ServerErrorPbView() override = default;

  // ServerErrorView interface - getters
  ServerErrorCode code() const override;
  std::unique_ptr<::kudu::types::AppStatusView> status() override;

  // ServerErrorView interface - setters
  void set_code(ServerErrorCode code) override;

  // Create an owning copy of this view.
  std::unique_ptr<ServerErrorPb> to_owned() const;

 private:
  ServerErrorPB& pb_;
};

// Protobuf-backed implementation of ServerError.
// Owns the underlying protobuf.
class ServerErrorPb : public ServerError {
 public:
  ServerErrorPb();
  explicit ServerErrorPb(ServerErrorPB pb);
  ~ServerErrorPb() override = default;

  // ServerErrorView interface - getters
  ServerErrorCode code() const override;
  std::unique_ptr<::kudu::types::AppStatusView> status() override;

  // ServerErrorView interface - setters
  void set_code(ServerErrorCode code) override;

  // Access the underlying protobuf.
  const ServerErrorPB& pb() const;
  ServerErrorPB* mutable_pb();

 private:
  ServerErrorPB pb_;
};

} // namespace types
} // namespace consensus
} // namespace kudu
