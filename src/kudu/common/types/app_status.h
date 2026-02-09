// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/common/types/app_status_view.h"

namespace kudu {
namespace types {

// Owning interface for an AppStatus (application status/error).
// Extends AppStatusView to indicate that implementations own their underlying
// data.
//
// Use AppStatus when you need an owned copy, AppStatusView when a reference
// suffices.
class AppStatus : public AppStatusView {
 public:
  ~AppStatus() override = default;
};

} // namespace types
} // namespace kudu
