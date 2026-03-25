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

RequestTracker::RequestTracker(std::string clientId)
    : clientId_(std::move(clientId)), next_(0) {}

Status RequestTracker::newSeqNo(SequenceNumber* seqNo) {
  // Atomically fetch the next sequence number and increment it.
  // This operation is lock-free and reduces contention.
  *seqNo = next_.fetch_add(1, std::memory_order_relaxed);

  // Still need the lock to insert into the set.
  std::lock_guard<SimpleSpinlock> l(lock_);
  auto [it, inserted] = incompleteRpcs_.insert(*seqNo);
  CHECK(inserted) << "Sequence number " << *seqNo << " already exists";
  return Status::OK();
}

RequestTracker::SequenceNumber RequestTracker::firstIncomplete() {
  std::lock_guard<SimpleSpinlock> l(lock_);
  if (incompleteRpcs_.empty()) {
    return kNoSeqNo;
  }
  return *incompleteRpcs_.begin();
}

void RequestTracker::rpcCompleted(const SequenceNumber& seqNo) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  incompleteRpcs_.erase(seqNo);
}

} // namespace rpc
} // namespace kudu
