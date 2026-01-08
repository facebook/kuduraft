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

#include "kudu/rpc/request_tracker.h"

#include <mutex>
#include <string>
#include <utility>

namespace kudu {
namespace rpc {

const RequestTracker::SequenceNumber RequestTracker::kNoSeqNo = -1;

RequestTracker::RequestTracker(std::string client_id)
    : client_id_(std::move(client_id)), next_(0) {}

Status RequestTracker::NewSeqNo(SequenceNumber* seq_no) {
  // Atomically fetch the next sequence number and increment it.
  // This operation is lock-free and reduces contention.
  *seq_no = next_.fetch_add(1, std::memory_order_relaxed);

  // Still need the lock to insert into the set.
  std::lock_guard<simple_spinlock> l(lock_);
  auto [it, inserted] = incomplete_rpcs_.insert(*seq_no);
  CHECK(inserted) << "Sequence number " << *seq_no << " already exists";
  return Status::OK();
}

RequestTracker::SequenceNumber RequestTracker::FirstIncomplete() {
  std::lock_guard<simple_spinlock> l(lock_);
  if (incomplete_rpcs_.empty()) {
    return kNoSeqNo;
  }
  return *incomplete_rpcs_.begin();
}

void RequestTracker::RpcCompleted(const SequenceNumber& seq_no) {
  std::lock_guard<simple_spinlock> l(lock_);
  incomplete_rpcs_.erase(seq_no);
}

} // namespace rpc
} // namespace kudu
