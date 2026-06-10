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

#include "kudu/consensus/opid_util.h"

#include <limits>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/port.h"

namespace kudu::consensus {

const int64_t kMinimumTerm = 0;
const int64_t kMinimumOpIdIndex = 0;
const int64_t kInvalidOpIdIndex = -1;

int opIdCompare(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  if (PREDICT_TRUE(left.term() == right.term())) {
    return left.index() < right.index() ? -1
        : left.index() == right.index() ? 0
                                        : 1;
  }
  return left.term() < right.term() ? -1 : 1;
}

bool OpIdEquals(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  return left.term() == right.term() && left.index() == right.index();
}

bool OpIdLessThan(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  if (left.term() < right.term()) {
    return true;
  }
  if (left.term() > right.term()) {
    return false;
  }
  return left.index() < right.index();
}

bool opIdBiggerThan(const OpId& left, const OpId& right) {
  DCHECK(left.IsInitialized());
  DCHECK(right.IsInitialized());
  if (left.term() > right.term()) {
    return true;
  }
  if (left.term() < right.term()) {
    return false;
  }
  return left.index() > right.index();
}

bool copyIfOpIdLessThan(
    const consensus::OpId& toCompare,
    consensus::OpId* target) {
  if (toCompare.IsInitialized() &&
      (!target->IsInitialized() || OpIdLessThan(toCompare, *target))) {
    target->CopyFrom(toCompare);
    return true;
  }
  return false;
}

const OpId& minOpId(const OpId& left, const OpId& right) {
  return OpIdLessThan(left, right) ? left : right;
}

size_t OpIdHashFunctor::operator()(const OpId& id) const {
  return (id.term() + 31) ^ id.index();
}

bool OpIdEqualsFunctor::operator()(const OpId& left, const OpId& right) const {
  return OpIdEquals(left, right);
}

bool OpIdLessThanPtrFunctor::operator()(const OpId* left, const OpId* right)
    const {
  return OpIdLessThan(*left, *right);
}

bool OpIdIndexLessThanPtrFunctor::operator()(
    const OpId* left,
    const OpId* right) const {
  return left->index() < right->index();
}

bool OpIdCompareFunctor::operator()(const OpId& left, const OpId& right) const {
  return OpIdLessThan(left, right);
}

bool OpIdBiggerThanFunctor::operator()(const OpId& left, const OpId& right)
    const {
  return opIdBiggerThan(left, right);
}

OpId MinimumOpId() {
  OpId opId;
  opId.set_term(0);
  opId.set_index(0);
  return opId;
}

OpId maximumOpId() {
  OpId opId;
  opId.set_term(std::numeric_limits<int64_t>::max());
  opId.set_index(std::numeric_limits<int64_t>::max());
  return opId;
}

// helper hash functor for delta store ids
struct DeltaIdHashFunction {
  size_t operator()(const std::pair<int64_t, int64_t>& id) const {
    return (id.first + 31) ^ id.second;
  }
};

// helper equals functor for delta store ids
struct DeltaIdEqualsTo {
  bool operator()(
      const std::pair<int64_t, int64_t>& left,
      const std::pair<int64_t, int64_t>& right) const {
    return left.first == right.first && left.second == right.second;
  }
};

std::ostream& operator<<(std::ostream& os, const consensus::OpId& opId) {
  os << OpIdToString(opId);
  return os;
}

std::string OpIdToString(const OpId& id) {
  if (!id.IsInitialized()) {
    return "<uninitialized op>";
  }
  return fmt::format("{}.{}", id.term(), id.index());
}

std::string opsRangeString(const ConsensusRequestPB& req) {
  std::string ret;
  ret.reserve(100);
  ret.push_back('[');
  if (req.ops_size() > 0) {
    const OpId& firstOp = req.ops(0).id();
    const OpId& lastOp = req.ops(req.ops_size() - 1).id();
    ret += fmt::format(
        "{}.{}-{}.{}",
        firstOp.term(),
        firstOp.index(),
        lastOp.term(),
        lastOp.index());
  }
  ret.push_back(']');
  return ret;
}

OpId MakeOpId(int64_t term, int64_t index) {
  CHECK_GE(term, 0);
  CHECK_GE(index, 0);
  OpId ret;
  ret.set_index(index);
  ret.set_term(term);
  return ret;
}

} // namespace kudu::consensus
