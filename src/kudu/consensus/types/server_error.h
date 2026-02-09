// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/server_error_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for a ServerError.
// Extends ServerErrorView to indicate that implementations own their
// underlying data.
//
// Use ServerError when you need an owned copy, ServerErrorView when a
// reference suffices.
class ServerError : public ServerErrorView {
 public:
  ~ServerError() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
