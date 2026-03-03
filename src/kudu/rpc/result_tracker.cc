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

#include "kudu/rpc/result_tracker.h"

#include <algorithm>
#include <mutex>
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
// IWYU pragma: no_include <deque>

DEFINE_int64(
    remember_clients_ttl_ms,
    3600 * 1000 /* 1 hour */,
    "Maximum amount of time, in milliseconds, the server \"remembers\" a client for the "
    "purpose of caching its responses. After this period without hearing from it, the "
    "client is no longer remembered and the memory occupied by its responses is reclaimed. "
    "Retries of requests older than 'remember_clients_ttl_ms' are treated as new "
    "ones.");
TAG_FLAG(remember_clients_ttl_ms, advanced);

DEFINE_int64(
    remember_responses_ttl_ms,
    600 * 1000 /* 10 mins */,
    "Maximum amount of time, in milliseconds, the server \"remembers\" a response to a "
    "specific request for a client. After this period has elapsed, the response may have "
    "been garbage collected and the client might get a response indicating the request is "
    "STALE.");
TAG_FLAG(remember_responses_ttl_ms, advanced);

DEFINE_int64(
    result_tracker_gc_interval_ms,
    1000,
    "Interval at which the result tracker will look for entries to GC.");
TAG_FLAG(result_tracker_gc_interval_ms, hidden);

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using kudu::MemTracker;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::lock_guard;
using std::make_pair;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

// This tracks the size changes of anything that has a memoryFootprint()
// method. It must be instantiated before the updates, and it makes sure that
// the MemTracker is updated on scope exit.
template <class T>
struct ScopedMemTrackerUpdater {
  ScopedMemTrackerUpdater(MemTracker* tracker, const T* tracked)
      : tracker(tracker),
        tracked(tracked),
        memoryBefore(tracked->memoryFootprint()),
        cancelled(false) {}

  ~ScopedMemTrackerUpdater() {
    if (cancelled) {
      return;
    }
    tracker->Release(memoryBefore - tracked->memoryFootprint());
  }

  void cancel() {
    cancelled = true;
  }

  MemTracker* tracker;
  const T* tracked;
  int64_t memoryBefore;
  bool cancelled;
};

ResultTracker::ResultTracker(shared_ptr<MemTracker> memTracker)
    : memTracker_(std::move(memTracker)),
      clients_(
          ClientStateMap::key_compare(),
          ClientStateMapAllocator(memTracker_)),
      gcThreadStopLatch_(1) {}

ResultTracker::~ResultTracker() {
  if (gcThread_) {
    gcThreadStopLatch_.countDown();
    gcThread_->Join();
  }

  lock_guard<simple_spinlock> l(lock_);
  // Release all the memory for the stuff we'll delete on destruction.
  for (auto& clientState : clients_) {
    clientState.second->gcCompletionRecords(
        memTracker_, [](SequenceNumber, CompletionRecord*) { return true; });
    memTracker_->Release(clientState.second->memoryFootprint());
  }
}

ResultTracker::RpcState ResultTracker::trackRpc(
    const RequestIdPB& requestId,
    Message* response,
    RpcContext* context) {
  lock_guard<simple_spinlock> l(lock_);
  return trackRpcUnlocked(requestId, response, context);
}

ResultTracker::RpcState ResultTracker::trackRpcUnlocked(
    const RequestIdPB& requestId,
    Message* response,
    RpcContext* context) {
  auto clientIt = clients_.find(requestId.client_id());
  if (clientIt == clients_.end()) {
    unique_ptr<ClientState> newClientState(new ClientState(memTracker_));
    memTracker_->Consume(newClientState->memoryFootprint());
    newClientState->staleBeforeSeqNo = requestId.first_incomplete_seq_no();
    clientIt =
        clients_.emplace(requestId.client_id(), std::move(newClientState))
            .first;
  }
  ClientState* clientState = clientIt->second.get();

  clientState->lastHeardFrom = MonoTime::Now();

  // If the arriving request is older than our per-client GC watermark, report
  // its staleness to the client.
  if (PREDICT_FALSE(requestId.seq_no() < clientState->staleBeforeSeqNo)) {
    if (context) {
      context->call_->respondFailure(
          ErrorStatusPB::ERROR_REQUEST_STALE,
          Status::Incomplete(
              fmt::format(
                  "Request with id {{ {} }} is stale.",
                  SecureShortDebugString(requestId))));
      delete context;
    }
    return RpcState::kStale;
  }

  // GC records according to the client's first incomplete watermark.
  clientState->gcCompletionRecords(
      memTracker_,
      [&](SequenceNumber seqNo, CompletionRecord* completionRecord) {
        return completionRecord->state != RpcState::kInProgress &&
            seqNo < requestId.first_incomplete_seq_no();
      });

  auto compIt = clientState->completionRecords.find(requestId.seq_no());
  bool wasAbsent = (compIt == clientState->completionRecords.end());
  if (wasAbsent) {
    unique_ptr<CompletionRecord> newCompletionRecord(
        new CompletionRecord(RpcState::kInProgress, requestId.attempt_no()));
    memTracker_->Consume(newCompletionRecord->memoryFootprint());
    compIt = clientState->completionRecords
                 .emplace(requestId.seq_no(), std::move(newCompletionRecord))
                 .first;
  }
  auto result = std::make_pair(compIt, wasAbsent);

  CompletionRecord* completionRecord = result.first->second.get();
  ScopedMemTrackerUpdater<CompletionRecord> crUpdater(
      memTracker_.get(), completionRecord);

  if (PREDICT_TRUE(result.second)) {
    // When a follower is applying an operation it doesn't have a response yet,
    // and it won't have a context, so only set them if they exist.
    if (response != nullptr) {
      completionRecord->ongoingRpcs.push_back(
          {response, DCHECK_NOTNULL(context), requestId.attempt_no()});
    }
    return RpcState::kNew;
  }

  completionRecord->lastUpdated = MonoTime::Now();
  switch (completionRecord->state) {
    case RpcState::kCompleted: {
      // If the RPC is COMPLETED and the request originates from a client
      // (context, response are non-null) copy the response and reply
      // immediately. If there is no context/response do nothing.
      if (context != nullptr) {
        DCHECK_NOTNULL(response)->CopyFrom(*completionRecord->response);
        context->call_->respondSuccess(*response);
        delete context;
      }
      return RpcState::kCompleted;
    }
    case RpcState::kInProgress: {
      // If the RPC is IN_PROGRESS check if there is a context and, if so,
      // attach it so that the rpc gets the same response when the original one
      // completes.
      if (context != nullptr) {
        completionRecord->ongoingRpcs.push_back(
            {DCHECK_NOTNULL(response), context, kNoHandler});
      }
      return RpcState::kInProgress;
    }
    default:
      LOG(FATAL) << "Wrong state: " << completionRecord->state;
      // dummy return to avoid warnings
      return RpcState::kStale;
  }
}

ResultTracker::RpcState ResultTracker::trackRpcOrChangeDriver(
    const RequestIdPB& requestId) {
  lock_guard<simple_spinlock> l(lock_);
  RpcState state = trackRpcUnlocked(requestId, nullptr, nullptr);

  if (state != RpcState::kInProgress) {
    return state;
  }

  CompletionRecord* completionRecord =
      findCompletionRecordOrDieUnlocked(requestId);
  ScopedMemTrackerUpdater<CompletionRecord> updater(
      memTracker_.get(), completionRecord);

  // ... if we did find a CompletionRecord change the driver and return true.
  completionRecord->driverAttemptNo = requestId.attempt_no();
  completionRecord->ongoingRpcs.push_back(
      {nullptr, nullptr, requestId.attempt_no()});

  // Since we changed the driver of the RPC, return kNew, so that the caller
  // knows to store the result.
  return RpcState::kNew;
}

bool ResultTracker::isCurrentDriver(const RequestIdPB& requestId) {
  lock_guard<simple_spinlock> l(lock_);
  CompletionRecord* completionRecord =
      findCompletionRecordOrNullUnlocked(requestId);

  // If we couldn't find the CompletionRecord, someone might have called
  // failAndRespond() so just return false.
  if (completionRecord == nullptr) {
    return false;
  }

  // ... if we did find a CompletionRecord return true if we're the driver or
  // false otherwise.
  return completionRecord->driverAttemptNo == requestId.attempt_no();
}

void ResultTracker::logAndTraceAndRespondSuccess(
    RpcContext* context,
    const Message& msg) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().serviceName()
          << ": Sending RPC success "
             "response for "
          << call->toString() << ":" << std::endl
          << SecureDebugString(msg);
  TRACE_EVENT_ASYNC_END2(
      "rpc_call",
      "RPC",
      this,
      "response",
      pb_util::PbTracer::TracePb(msg),
      "trace",
      context->trace()->dumpToString());
  call->respondSuccess(msg);
  delete context;
}

void ResultTracker::logAndTraceFailure(
    RpcContext* context,
    const Message& msg) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().serviceName()
          << ": Sending RPC failure "
             "response for "
          << call->toString() << ": " << SecureDebugString(msg);
  TRACE_EVENT_ASYNC_END2(
      "rpc_call",
      "RPC",
      this,
      "response",
      pb_util::PbTracer::TracePb(msg),
      "trace",
      context->trace()->dumpToString());
}

void ResultTracker::logAndTraceFailure(
    RpcContext* context,
    ErrorStatusPB_RpcErrorCodePB /* err */,
    const Status& status) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().serviceName()
          << ": Sending RPC failure "
             "response for "
          << call->toString() << ": " << status.ToString();
  TRACE_EVENT_ASYNC_END2(
      "rpc_call",
      "RPC",
      this,
      "status",
      status.ToString(),
      "trace",
      context->trace()->dumpToString());
}

ResultTracker::CompletionRecord*
ResultTracker::findCompletionRecordOrDieUnlocked(const RequestIdPB& requestId) {
  auto clientIt = clients_.find(requestId.client_id());
  ClientState* clientState = DCHECK_NOTNULL(
      clientIt != clients_.end() ? clientIt->second.get() : nullptr);
  auto compIt = clientState->completionRecords.find(requestId.seq_no());
  return DCHECK_NOTNULL(
      compIt != clientState->completionRecords.end() ? compIt->second.get()
                                                     : nullptr);
}

pair<ResultTracker::ClientState*, ResultTracker::CompletionRecord*>
ResultTracker::findClientStateAndCompletionRecordOrNullUnlocked(
    const RequestIdPB& requestId) {
  auto clientIt = clients_.find(requestId.client_id());
  ClientState* clientState =
      clientIt != clients_.end() ? clientIt->second.get() : nullptr;
  CompletionRecord* completionRecord = nullptr;
  if (clientState != nullptr) {
    auto compIt = clientState->completionRecords.find(requestId.seq_no());
    completionRecord = compIt != clientState->completionRecords.end()
        ? compIt->second.get()
        : nullptr;
  }
  return make_pair(clientState, completionRecord);
}

ResultTracker::CompletionRecord*
ResultTracker::findCompletionRecordOrNullUnlocked(
    const RequestIdPB& requestId) {
  return findClientStateAndCompletionRecordOrNullUnlocked(requestId).second;
}

void ResultTracker::recordCompletionAndRespond(
    const RequestIdPB& requestId,
    const Message* response) {
  vector<OngoingRpcInfo> toRespond;
  {
    lock_guard<simple_spinlock> l(lock_);

    CompletionRecord* completionRecord =
        findCompletionRecordOrDieUnlocked(requestId);
    ScopedMemTrackerUpdater<CompletionRecord> updater(
        memTracker_.get(), completionRecord);

    CHECK_EQ(completionRecord->driverAttemptNo, requestId.attempt_no())
        << "Called recordCompletionAndRespond() from an executor identified with an "
        << "attempt number that was not marked as the driver for the RPC. RequestId: "
        << SecureShortDebugString(requestId) << "\nTracker state:\n "
        << toStringUnlocked();
    DCHECK_EQ(completionRecord->state, RpcState::kInProgress);
    completionRecord->response.reset(DCHECK_NOTNULL(response)->New());
    completionRecord->response->CopyFrom(*response);
    completionRecord->state = RpcState::kCompleted;
    completionRecord->lastUpdated = MonoTime::Now();

    CHECK_EQ(completionRecord->driverAttemptNo, requestId.attempt_no());

    int64_t handlerAttemptNo = requestId.attempt_no();

    // Go through the ongoing RPCs and reply to each one.
    for (auto orpcIter = completionRecord->ongoingRpcs.rbegin();
         orpcIter != completionRecord->ongoingRpcs.rend();) {
      const OngoingRpcInfo& ongoingRpc = *orpcIter;
      if (mustHandleRpc(handlerAttemptNo, completionRecord, ongoingRpc)) {
        if (ongoingRpc.context != nullptr) {
          toRespond.push_back(ongoingRpc);
        }
        ++orpcIter;
        orpcIter = std::vector<OngoingRpcInfo>::reverse_iterator(
            completionRecord->ongoingRpcs.erase(orpcIter.base()));
      } else {
        ++orpcIter;
      }
    }
  }

  // Respond outside of holding the lock. This reduces lock contention and also
  // means that we will have fully updated our memory tracking before
  // responding, which makes testing easier.
  for (auto& ongoingRpc : toRespond) {
    if (PREDICT_FALSE(ongoingRpc.response != response)) {
      ongoingRpc.response->CopyFrom(*response);
    }
    logAndTraceAndRespondSuccess(ongoingRpc.context, *ongoingRpc.response);
  }
}

void ResultTracker::failAndRespondInternal(
    const RequestIdPB& requestId,
    const HandleOngoingRpcFunc& func) {
  vector<OngoingRpcInfo> toHandle;
  {
    lock_guard<simple_spinlock> l(lock_);
    auto stateAndRecord =
        findClientStateAndCompletionRecordOrNullUnlocked(requestId);
    if (PREDICT_FALSE(stateAndRecord.first == nullptr)) {
      LOG(FATAL) << "Couldn't find ClientState for request: "
                 << SecureShortDebugString(requestId) << ". \nTracker state:\n"
                 << toStringUnlocked();
    }

    CompletionRecord* completionRecord = stateAndRecord.second;

    // It is possible for this method to be called for an RPC that was never
    // actually tracked (though recordCompletionAndRespond() can't). One such
    // case is when a follower transaction fails on the TransactionManager, for
    // some reason, before it was tracked. The CompletionCallback still calls
    // this method. In this case, do nothing.
    if (completionRecord == nullptr) {
      return;
    }

    ScopedMemTrackerUpdater<CompletionRecord> crUpdater(
        memTracker_.get(), completionRecord);
    completionRecord->lastUpdated = MonoTime::Now();

    int64_t seqNo = requestId.seq_no();
    int64_t handlerAttemptNo = requestId.attempt_no();

    // If we're copying from a client originated response we need to take care
    // to reply to that call last, otherwise we'll lose 'response', before we go
    // through all the CompletionRecords.
    for (auto orpcIter = completionRecord->ongoingRpcs.rbegin();
         orpcIter != completionRecord->ongoingRpcs.rend();) {
      const OngoingRpcInfo& ongoingRpc = *orpcIter;
      if (mustHandleRpc(handlerAttemptNo, completionRecord, ongoingRpc)) {
        toHandle.push_back(ongoingRpc);
        ++orpcIter;
        orpcIter = std::vector<OngoingRpcInfo>::reverse_iterator(
            completionRecord->ongoingRpcs.erase(orpcIter.base()));
      } else {
        ++orpcIter;
      }
    }

    // If we're the last ones trying this and the state is not completed,
    // delete the completion record.
    if (completionRecord->ongoingRpcs.size() == 0 &&
        completionRecord->state != RpcState::kCompleted) {
      crUpdater.cancel();
      auto compIt = stateAndRecord.first->completionRecords.find(seqNo);
      unique_ptr<CompletionRecord> erasedCompletionRecord =
          std::move(compIt->second);
      stateAndRecord.first->completionRecords.erase(compIt);
      memTracker_->Release(erasedCompletionRecord->memoryFootprint());
    }
  }

  // Wait until outside the lock to do the heavy-weight work.
  for (auto& ongoingRpc : toHandle) {
    if (ongoingRpc.context != nullptr) {
      func(ongoingRpc);
      delete ongoingRpc.context;
    }
  }
}

void ResultTracker::failAndRespond(
    const RequestIdPB& requestId,
    Message* response) {
  auto func = [&](const OngoingRpcInfo& ongoingRpc) {
    // In the common case RPCs are just executed once so, in that case, avoid an
    // extra copy of the response.
    if (PREDICT_FALSE(ongoingRpc.response != response)) {
      ongoingRpc.response->CopyFrom(*response);
    }
    logAndTraceFailure(ongoingRpc.context, *response);
    ongoingRpc.context->call_->respondSuccess(*response);
  };
  failAndRespondInternal(requestId, func);
}

void ResultTracker::failAndRespond(
    const RequestIdPB& requestId,
    ErrorStatusPB_RpcErrorCodePB err,
    const Status& status) {
  auto func = [&](const OngoingRpcInfo& ongoingRpc) {
    logAndTraceFailure(ongoingRpc.context, err, status);
    ongoingRpc.context->call_->respondFailure(err, status);
  };
  failAndRespondInternal(requestId, func);
}

void ResultTracker::failAndRespond(
    const RequestIdPB& requestId,
    int errorExtId,
    const string& message,
    const Message& appErrorPb) {
  auto func = [&](const OngoingRpcInfo& ongoingRpc) {
    logAndTraceFailure(ongoingRpc.context, appErrorPb);
    ongoingRpc.context->call_->respondApplicationError(
        errorExtId, message, appErrorPb);
  };
  failAndRespondInternal(requestId, func);
}

void ResultTracker::startGcThread() {
  CHECK(!gcThread_);
  CHECK_OK(
      Thread::Create(
          "server",
          "result-tracker",
          &ResultTracker::runGcThread,
          this,
          &gcThread_));
}

void ResultTracker::runGcThread() {
  while (!gcThreadStopLatch_.waitFor(
      MonoDelta::FromMilliseconds(FLAGS_result_tracker_gc_interval_ms))) {
    gcResults();
  }
}

void ResultTracker::gcResults() {
  lock_guard<simple_spinlock> l(lock_);
  MonoTime now = MonoTime::Now();
  // Calculate the instants before which we'll start GCing ClientStates and
  // CompletionRecords.
  MonoTime timeToGcClientsFrom = now;
  timeToGcClientsFrom.AddDelta(
      MonoDelta::FromMilliseconds(-FLAGS_remember_clients_ttl_ms));
  MonoTime timeToGcResponsesFrom = now;
  timeToGcResponsesFrom.AddDelta(
      MonoDelta::FromMilliseconds(-FLAGS_remember_responses_ttl_ms));

  // Now go through the ClientStates. If we haven't heard from a client in a
  // while GC it and all its completion records (making sure there isn't
  // actually one in progress first). If we've heard from a client recently, but
  // some of its responses are old, GC those responses.
  for (auto iter = clients_.begin(); iter != clients_.end();) {
    auto& clientState = iter->second;
    if (clientState->lastHeardFrom < timeToGcClientsFrom) {
      // Client should be GCed.
      bool ongoingRequest = false;
      clientState->gcCompletionRecords(
          memTracker_, [&](SequenceNumber, CompletionRecord* completionRecord) {
            if (PREDICT_FALSE(
                    completionRecord->state == RpcState::kInProgress)) {
              ongoingRequest = true;
              return false;
            }
            return true;
          });
      // Don't delete the client state if there is still a request in execution.
      if (PREDICT_FALSE(ongoingRequest)) {
        ++iter;
        continue;
      }
      memTracker_->Release(clientState->memoryFootprint());
      iter = clients_.erase(iter);
    } else {
      // Client can't be GCed, but its calls might be GCable.
      iter->second->gcCompletionRecords(
          memTracker_, [&](SequenceNumber, CompletionRecord* completionRecord) {
            return completionRecord->state != RpcState::kInProgress &&
                completionRecord->lastUpdated < timeToGcResponsesFrom;
          });
      ++iter;
    }
  }
}

string ResultTracker::toString() {
  lock_guard<simple_spinlock> l(lock_);
  return toStringUnlocked();
}

string ResultTracker::toStringUnlocked() const {
  string result = fmt::format(
      "ResultTracker[this: {}, Num. Client States: {}, Client States:\n",
      fmt::ptr(this),
      clients_.size());
  for (auto& cs : clients_) {
    result +=
        fmt::format("\n\tClient: {}, {}", cs.first, cs.second->toString());
  }
  result.append("]");
  return result;
}

template <class MustGcRecordFunc>
void ResultTracker::ClientState::gcCompletionRecords(
    const shared_ptr<kudu::MemTracker>& memTracker,
    MustGcRecordFunc mustGcRecordFunc) {
  ScopedMemTrackerUpdater<ClientState> updater(memTracker.get(), this);
  for (auto iter = completionRecords.begin();
       iter != completionRecords.end();) {
    if (mustGcRecordFunc(iter->first, iter->second.get())) {
      memTracker->Release(iter->second->memoryFootprint());
      SequenceNumber deletedSeqNo = iter->first;
      iter = completionRecords.erase(iter);
      // Each time we GC a response, update 'staleBeforeSeqNo'.
      // This will allow to answer clients that their responses are stale if we
      // get a request with a sequence number lower than or equal to this one.
      staleBeforeSeqNo = std::max(deletedSeqNo + 1, staleBeforeSeqNo);
      continue;
    }
    // Since we store completion records in order, if we found one that
    // shouldn't be GCed, don't GC anything after it.
    return;
  }
}

string ResultTracker::ClientState::toString() const {
  auto sinceLastHeard = MonoTime::Now().GetDeltaSince(lastHeardFrom);
  string result = fmt::format(
      "Client State[Last heard from: {}s ago, "
      "{} CompletionRecords:",
      sinceLastHeard.ToString(),
      completionRecords.size());
  for (auto& completionRecord : completionRecords) {
    result += fmt::format(
        "\n\tCompletion Record: {}, {}",
        completionRecord.first,
        completionRecord.second->toString());
  }
  result.append("\t]");
  return result;
}

string ResultTracker::CompletionRecord::toString() const {
  string result = fmt::format(
      "Completion Record[State: {}, Driver: {}, "
      "Cached response: {}, {} OngoingRpcs:",
      state,
      driverAttemptNo,
      response ? SecureShortDebugString(*response) : "None",
      ongoingRpcs.size());
  for (auto& orpc : ongoingRpcs) {
    result += fmt::format("\n\t{}", orpc.toString());
  }
  result.append("\t\t]");
  return result;
}

string ResultTracker::OngoingRpcInfo::toString() const {
  return fmt::format(
      "OngoingRpc[Handler: {}, Context: {}, Response: {}]",
      handlerAttemptNo,
      fmt::ptr(context),
      response ? SecureShortDebugString(*response) : "NULL");
}

} // namespace rpc
} // namespace kudu
