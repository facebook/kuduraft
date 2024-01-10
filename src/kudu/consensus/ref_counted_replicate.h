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

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu::consensus {

// A simple ref-counted wrapper around ReplicateMsg.
class RefCountedReplicate : public RefCountedThreadSafe<RefCountedReplicate> {
 public:
  explicit RefCountedReplicate(ReplicateMsg* msg) : msg_(msg) {}

  ReplicateMsg* get() {
    return msg_.get();
  }

 private:
  std::unique_ptr<ReplicateMsg> msg_;
};

using ReplicateRefPtr = scoped_refptr<RefCountedReplicate>;

inline ReplicateRefPtr make_scoped_refptr_replicate(ReplicateMsg* replicate) {
  return ReplicateRefPtr(new RefCountedReplicate(replicate));
}

} // namespace kudu::consensus
