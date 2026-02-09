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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/rpc/request_tracker.h"
#include "kudu/util/test_macros.h"

using std::vector;

namespace kudu {
namespace rpc {

TEST(RequestTrackerTest, TestSequenceNumberGeneration) {
  const int kMax = 10;

  std::shared_ptr<RequestTracker> tracker(new RequestTracker("test_client"));

  // A new tracker should have no incomplete RPCs
  RequestTracker::SequenceNumber seqNo = tracker->FirstIncomplete();
  ASSERT_EQ(seqNo, RequestTracker::kNoSeqNo);

  vector<RequestTracker::SequenceNumber> generatedSeqNos;

  // Generate kMax in flight RPCs, making sure they are correctly returned.
  for (int i = 0; i < kMax; i++) {
    ASSERT_OK(tracker->NewSeqNo(&seqNo));
    generatedSeqNos.push_back(seqNo);
  }

  // Now we should get a first incomplete.
  ASSERT_EQ(generatedSeqNos[0], tracker->FirstIncomplete());

  // Marking 'first_incomplete' as done, should advance the first incomplete.
  tracker->RpcCompleted(tracker->FirstIncomplete());

  ASSERT_EQ(generatedSeqNos[1], tracker->FirstIncomplete());

  // Marking a 'middle' rpc, should not advance 'first_incomplete'.
  tracker->RpcCompleted(generatedSeqNos[5]);
  ASSERT_EQ(generatedSeqNos[1], tracker->FirstIncomplete());

  // Marking half the rpc as complete should advance FirstIncomplete.
  // Note that this also tests that RequestTracker::RpcCompleted() is
  // idempotent, i.e. that marking the same sequence number as complete twice is
  // a no-op.
  for (int i = 0; i < kMax / 2; i++) {
    tracker->RpcCompleted(generatedSeqNos[i]);
  }

  ASSERT_EQ(generatedSeqNos[6], tracker->FirstIncomplete());

  for (int i = kMax / 2; i <= kMax; i++) {
    ASSERT_OK(tracker->NewSeqNo(&seqNo));
    generatedSeqNos.push_back(seqNo);
  }

  // Marking them all as completed should cause
  // RequestTracker::FirstIncomplete() to return Status::NotFound() again.
  for (auto seqNo : generatedSeqNos) {
    tracker->RpcCompleted(seqNo);
  }

  ASSERT_EQ(tracker->FirstIncomplete(), RequestTracker::kNoSeqNo);
}

} // namespace rpc
} // namespace kudu
