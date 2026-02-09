// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>

namespace kudu {
namespace consensus {
namespace types {

// Interface for an OpId (operation identifier).
// An OpId uniquely identifies an operation in the Raft log, composed of
// the leader's term and the index within that term.
//
// "View" indicates this may be a non-owning reference to underlying data.
// Subclasses indicate ownership semantics.
class OpIdView {
 public:
  virtual ~OpIdView() = default;

  // The term of the operation (leader's sequence id).
  virtual int64_t term() const = 0;

  // The index of the operation within the term.
  virtual int64_t index() const = 0;

  // Set the term of the operation.
  virtual void set_term(int64_t term) = 0;

  // Set the index of the operation.
  virtual void set_index(int64_t index) = 0;
};

} // namespace types
} // namespace consensus
} // namespace kudu
