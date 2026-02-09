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
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

// RequestTracker implementation, inspired by:
// "Implementing Linearizability at Large Scale and Low Latency" by Colin Lee et
// al.
//
// This generates sequence numbers for retriable RPCs and tracks the ongoing
// ones. The main point of this is to enable exactly-once semantics, i.e. making
// sure that an RPC is only executed once, by uniquely identifying each RPC that
// is sent to the server.
//
// Note that the sequence numbers here are differet from RPC 'call ids'. A call
// id uniquely identifies a call _to a server_. All calls have a call id that is
// assigned incrementally. Sequence numbers, on the other hand, uniquely
// identify the RPC operation itself. That is, if an RPC is retried on another
// server it will have a different call id, but the same sequence number.
//
// By keeping track of the RPCs that are in-flight and which ones are completed
// we can determine the first incomplete RPC. When this information is sent
// to the server it can use it to garbage collect RPC results that it might be
// saving for future retries, since it now knows there won't be any.
//
// This class is thread safe.
class RequestTracker {
 public:
  using SequenceNumber = int64_t;
  static const RequestTracker::SequenceNumber kNoSeqNo;
  explicit RequestTracker(std::string clientId);

  // Creates a new, unique, sequence number.
  // Sequence numbers are assigned in increasing integer order.
  // Returns Status::OK() and sets 'seqNo' if it was able to generate a
  // sequence number or returns Status::ServiceUnavailable() if too many RPCs
  // are in-flight, in which case the caller should try again later.
  Status NewSeqNo(SequenceNumber* seqNo);

  // Returns the sequence number of the first incomplete RPC.
  // If there is no incomplete RPC returns kNoSeqNo.
  SequenceNumber FirstIncomplete();

  // Marks the rpc with 'seqNo' as completed.
  void RpcCompleted(const SequenceNumber& seqNo);

  // Returns the client id for this request tracker.
  const std::string& clientId() {
    return clientId_;
  }

 private:
  // The client id for this request tracker.
  const std::string clientId_;

  // Lock that protects incompleteRpcs_. next_ is now atomic and doesn't
  // require locking.
  simple_spinlock lock_;

  // The next sequence number. Atomically incremented.
  std::atomic<SequenceNumber> next_;

  // The (ordered) set of incomplete RPCs.
  std::set<SequenceNumber> incompleteRpcs_;
};

} // namespace rpc
} // namespace kudu
