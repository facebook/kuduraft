// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "kudu/consensus/types/opid_view.h"

namespace kudu {
namespace consensus {
namespace types {

// Owning interface for an OpId (operation identifier).
// Extends OpIdView to indicate that implementations own their underlying data.
//
// Use OpId when you need an owned copy, OpIdView when a reference suffices.
class OpId : public OpIdView {
 public:
  ~OpId() override = default;
};

} // namespace types
} // namespace consensus
} // namespace kudu
