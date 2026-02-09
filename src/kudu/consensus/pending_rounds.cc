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

#include "kudu/consensus/pending_rounds.h"

#include <ostream>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"

using kudu::pb_util::SecureShortDebugString;
using std::string;

namespace kudu::consensus {

//------------------------------------------------------------
// PendingRounds
//------------------------------------------------------------

PendingRounds::PendingRounds(
    string logPrefix,
    std::shared_ptr<ITimeManager> timeManager)
    : logPrefix_(std::move(logPrefix)),
      lastCommittedOpId_(MinimumOpId()),
      timeManager_(std::move(timeManager)) {}

PendingRounds::~PendingRounds() = default;

Status PendingRounds::cancelPendingTransactions() {
  ThreadRestrictions::assertWaitAllowed();
  if (pendingTxns_.empty()) {
    return Status::OK();
  }

  LOG_WITH_PREFIX(INFO) << "Trying to abort " << pendingTxns_.size()
                        << " pending transactions.";
  for (const auto& txn : pendingTxns_) {
    const std::shared_ptr<ConsensusRound>& round = txn.second;
    // We cancel only transactions whose applies have not yet been triggered.
    LOG_WITH_PREFIX(INFO) << "Aborting transaction as it isn't in flight: "
                          << SecureShortDebugString(
                                 *txn.second->replicate_msg());
    round->NotifyReplicationFinished(Status::Aborted("Transaction aborted"));
  }
  return Status::OK();
}

void PendingRounds::abortOpsAfter(int64_t index) {
  LOG_WITH_PREFIX(INFO)
      << "Aborting all transactions after (but not including) " << index;

  DCHECK_GE(index, 0);
  OpId newPreceding;

  auto iter = pendingTxns_.lower_bound(index);

  // Either the new preceding id is in the pendings set or it must be equal to
  // the committed index since we can't truncate already committed operations.
  if (iter != pendingTxns_.end() && (*iter).first == index) {
    newPreceding = (*iter).second->replicate_msg()->id();
    ++iter;
  } else {
    CHECK_EQ(index, lastCommittedOpId_.index());
    newPreceding = lastCommittedOpId_;
  }

  for (; iter != pendingTxns_.end();) {
    const std::shared_ptr<ConsensusRound>& round = (*iter).second;
    auto opType = round->replicate_msg()->op_type();
    LOG_WITH_PREFIX(INFO) << "Aborting uncommitted "
                          << OperationType_Name(opType)
                          << " operation due to leader change: "
                          << round->replicate_msg()->id();

    round->NotifyReplicationFinished(
        Status::Aborted("Transaction aborted by new leader"));
    // Erase the entry from pendings.
    pendingTxns_.erase(iter++);
  }
}

Status PendingRounds::addPendingOperation(
    const std::shared_ptr<ConsensusRound>& round) {
  auto [it, inserted] =
      pendingTxns_.insert({round->replicate_msg()->id().index(), round});
  CHECK(inserted) << "Key already exists: "
                  << round->replicate_msg()->id().index();
  return Status::OK();
}

std::shared_ptr<ConsensusRound> PendingRounds::getPendingOpByIndexOrNull(
    int64_t index) {
  auto it = pendingTxns_.find(index);
  return (it != pendingTxns_.end()) ? it->second : nullptr;
}

bool PendingRounds::isOpCommittedOrPending(
    const OpId& opId,
    bool* termMismatch) {
  *termMismatch = false;

  if (opId.index() <= getCommittedIndex()) {
    return true;
  }

  std::shared_ptr<ConsensusRound> round =
      getPendingOpByIndexOrNull(opId.index());
  if (!round) {
    return false;
  }

  if (round->id().term() != opId.term()) {
    *termMismatch = true;
    return false;
  }
  return true;
}

OpId PendingRounds::getLastPendingTransactionOpId() const {
  return pendingTxns_.empty() ? MinimumOpId()
                              : (--pendingTxns_.end())->second->id();
}

Status PendingRounds::advanceCommittedIndex(int64_t committedIndex) {
  // If we already committed up to (or past) 'id' return.
  // This can happen in the case that multiple UpdateConsensus() calls end
  // up in the RPC queue at the same time, and then might get interleaved out
  // of order.
  if (lastCommittedOpId_.index() >= committedIndex) {
    VLOG_WITH_PREFIX(1) << "Already marked ops through " << lastCommittedOpId_
                        << " as committed. "
                        << "Now trying to mark " << committedIndex
                        << " which would be a no-op.";
    return Status::OK();
  }

  if (pendingTxns_.empty()) {
    LOG(ERROR) << "Advancing commit index to " << committedIndex << " from "
               << lastCommittedOpId_ << " we have no pending txns"
               << GetStackTrace();
    VLOG_WITH_PREFIX(1) << "No transactions to mark as committed up to: "
                        << committedIndex;
    return Status::OK();
  }

  // Start at the operation after the last committed one.
  auto iter = pendingTxns_.upper_bound(lastCommittedOpId_.index());
  // Stop at the operation after the last one we must commit.
  auto endIter = pendingTxns_.upper_bound(committedIndex);
  CHECK(iter != pendingTxns_.end());

  VLOG_WITH_PREFIX(1) << "Last triggered apply was: " << lastCommittedOpId_
                      << " Starting to apply from log index: " << (*iter).first;

  while (iter != endIter) {
    std::shared_ptr<ConsensusRound> round = (*iter).second; // Make a copy.
    DCHECK(round);
    const OpId& currentId = round->id();

    if (PREDICT_TRUE(!OpIdEquals(lastCommittedOpId_, MinimumOpId()))) {
      CHECK_OK(checkOpInSequence(lastCommittedOpId_, currentId));
    }

    pendingTxns_.erase(iter++);
    lastCommittedOpId_ = round->id();
    timeManager_->AdvanceSafeTimeWithMessage(*round->replicate_msg());
    round->NotifyReplicationFinished(Status::OK());
  }

  return Status::OK();
}

Status PendingRounds::setInitialCommittedOpId(const OpId& committedOp) {
  CHECK_EQ(lastCommittedOpId_.index(), 0);
  if (!pendingTxns_.empty()) {
    int64_t firstPendingIndex = pendingTxns_.begin()->first;
    if (committedOp.index() < firstPendingIndex) {
      if (committedOp.index() != firstPendingIndex - 1) {
        return Status::Corruption(
            fmt::format(
                "pending operations should start at first operation "
                "after the committed operation (committed={}, first pending={})",
                OpIdToString(committedOp),
                firstPendingIndex));
      }
      lastCommittedOpId_ = committedOp;
    }

    RETURN_NOT_OK(advanceCommittedIndex(committedOp.index()));
    CHECK_EQ(
        SecureShortDebugString(lastCommittedOpId_),
        SecureShortDebugString(committedOp));

  } else {
    lastCommittedOpId_ = committedOp;
  }
  return Status::OK();
}

Status PendingRounds::checkOpInSequence(
    const OpId& previous,
    const OpId& current) {
  if (current.term() < previous.term()) {
    return Status::Corruption(
        fmt::format(
            "New operation's term is not >= than the previous "
            "op's term. Current: {}. Previous: {}",
            OpIdToString(current),
            OpIdToString(previous)));
  }
  if (current.index() != previous.index() + 1) {
    return Status::Corruption(
        fmt::format(
            "New operation's index does not follow the previous"
            " op's index. Current: {}. Previous: {}",
            OpIdToString(current),
            OpIdToString(previous)));
  }
  return Status::OK();
}

int64_t PendingRounds::getCommittedIndex() const {
  return lastCommittedOpId_.index();
}

int64_t PendingRounds::getTermWithLastCommittedOp() const {
  return lastCommittedOpId_.term();
}

int PendingRounds::getNumPendingTxns() const {
  return pendingTxns_.size();
}

} // namespace kudu::consensus
