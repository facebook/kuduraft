// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/rpc/rpc_sidecar.h"

#include <cstdint>
#include <memory>
#include <utility>

#include <fmt/core.h>
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/status.h"

using std::unique_ptr;

namespace kudu {
namespace rpc {

// Sidecar that simply wraps a Slice. The data associated with the slice is
// therefore not owned by this class, and it's the caller's responsibility to
// ensure it has a lifetime at least as long as this sidecar.
class SliceSidecar : public RpcSidecar {
 public:
  explicit SliceSidecar(Slice slice) : slice_(slice) {}
  Slice AsSlice() const override {
    return slice_;
  }

 private:
  const Slice slice_;
};

class FaststringSidecar : public RpcSidecar {
 public:
  explicit FaststringSidecar(unique_ptr<faststring> data)
      : data_(std::move(data)) {}
  Slice AsSlice() const override {
    return *data_;
  }

 private:
  const unique_ptr<faststring> data_;
};

unique_ptr<RpcSidecar> RpcSidecar::FromFaststring(unique_ptr<faststring> data) {
  return unique_ptr<RpcSidecar>(new FaststringSidecar(std::move(data)));
}

unique_ptr<RpcSidecar> RpcSidecar::FromSlice(Slice slice) {
  return unique_ptr<RpcSidecar>(new SliceSidecar(slice));
}

Status RpcSidecar::ParseSidecars(
    const ::google::protobuf::RepeatedField<::google::protobuf::uint32>&
        offsets,
    Slice buffer,
    Slice* sidecars) {
  if (offsets.size() == 0) {
    return Status::OK();
  }

  int last = offsets.size() - 1;
  if (last >= TransferLimits::kMaxSidecars) {
    return Status::Corruption(
        fmt::format(
            "Received {} additional payload slices, expected at most {}",
            last,
            TransferLimits::kMaxSidecars));
  }

  if (buffer.size() > TransferLimits::kMaxTotalSidecarBytes) {
    return Status::Corruption(
        fmt::format(
            "Received {} payload bytes, expected at most {}",
            buffer.size(),
            TransferLimits::kMaxTotalSidecarBytes));
  }

  for (int i = 0; i < last; ++i) {
    int64_t cur_offset = offsets.Get(i);
    int64_t next_offset = offsets.Get(i + 1);
    if (next_offset > buffer.size()) {
      return Status::Corruption(
          fmt::format(
              "Invalid sidecar offsets; sidecar {} apparently starts at {},"
              " has length {}, but the entire message has length {}",
              i,
              cur_offset,
              (next_offset - cur_offset),
              buffer.size()));
    }
    if (next_offset < cur_offset) {
      return Status::Corruption(
          fmt::format(
              "Invalid sidecar offsets; sidecar {} apparently starts at {},"
              " but ends before that at offset {}.",
              i,
              cur_offset,
              next_offset));
    }

    sidecars[i] = Slice(buffer.data() + cur_offset, next_offset - cur_offset);
  }

  int64_t cur_offset = offsets.Get(last);
  if (cur_offset > buffer.size()) {
    return Status::Corruption(
        fmt::format(
            "Invalid sidecar offsets: sidecar {} "
            "starts at offset {}after message ends (message length {}).",
            last,
            cur_offset,
            buffer.size()));
  }
  sidecars[last] =
      Slice(buffer.data() + cur_offset, buffer.size() - cur_offset);

  return Status::OK();
}

} // namespace rpc
} // namespace kudu
