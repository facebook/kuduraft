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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/message.h>

#include <fmt/core.h>
#include "kudu/gutil/port.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/net/sockaddr.h"

// 100M cycles should be about 50ms on a 2Ghz box. This should be high
// enough that involuntary context switches don't trigger it, but low enough
// that any serious blocking behavior on the reactor would.
DEFINE_int64(
    rpc_callback_max_cycles,
    100 * 1000 * 1000,
    "The maximum number of cycles for which an RPC callback "
    "should be allowed to run without emitting a warning."
    " (Advanced debugging option)");
TAG_FLAG(rpc_callback_max_cycles, advanced);
TAG_FLAG(rpc_callback_max_cycles, runtime);

// Flag used in debug build for injecting cancellation at different code paths.
DEFINE_int32(
    rpc_inject_cancellation_state,
    -1,
    "If this flag is not -1, it is the state in which a cancellation request "
    "will be injected. Should use values in OutboundCall::State only");
TAG_FLAG(rpc_inject_cancellation_state, unsafe);

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

using google::protobuf::Message;

static const double kMicrosPerSecond = 1000000.0;

///
/// OutboundCall
///

OutboundCall::OutboundCall(
    const ConnectionId& connId,
    const RemoteMethod& remoteMethod,
    google::protobuf::Message* responseStorage,
    RpcController* controller,
    ResponseCallback callback)
    : state_(kReady),
      remoteMethod_(remoteMethod),
      connId_(connId),
      callback_(std::move(callback)),
      controller_(DCHECK_NOTNULL(controller)),
      response_(DCHECK_NOTNULL(responseStorage)),
      cancellationRequested_(false) {
  DVLOG(4) << "OutboundCall " << this << " constructed with state_: "
           << stateName(state_.load(std::memory_order_relaxed))
           << " and RPC timeout: "
           << (controller->timeout().Initialized()
                   ? controller->timeout().ToString()
                   : "none");
  header_.set_call_id(kInvalidCallId);
  remoteMethod.toPb(header_.mutable_remote_method());
  startTime_ = MonoTime::Now();

  if (!controller_->requiredServerFeatures().empty()) {
    requiredRpcFeatures_.insert(RpcFeatureFlag::APPLICATION_FEATURE_FLAGS);
  }

  if (controller_->request_id_) {
    header_.set_allocated_request_id(controller_->request_id_.release());
  }
}

OutboundCall::~OutboundCall() {
  DCHECK(isFinished());
  DVLOG(4) << "OutboundCall " << this << " destroyed with state_: "
           << stateName(state_.load(std::memory_order_relaxed));
}

size_t OutboundCall::serializeTo(TransferPayload* slices) {
  DCHECK_LT(0, requestBuf_.size())
      << "Must call setRequestPayload() before serializeTo()";

  const MonoDelta& timeout = controller_->timeout();
  if (timeout.Initialized()) {
    header_.set_timeout_millis(timeout.ToMilliseconds());
  }

  for (uint32_t feature : controller_->requiredServerFeatures()) {
    header_.add_required_feature_flags(feature);
  }

  DCHECK_LE(0, sidecarByteSize_);
  serialization::serializeHeader(
      header_, sidecarByteSize_ + requestBuf_.size(), &headerBuf_);

  size_t nSlices = 2 + sidecars_.size();
  DCHECK_LE(nSlices, slices->size());
  auto sliceIter = slices->begin();
  *sliceIter++ = Slice(headerBuf_);
  *sliceIter++ = Slice(requestBuf_);
  for (auto& sidecar : sidecars_) {
    *sliceIter++ = sidecar->asSlice();
  }
  DCHECK_EQ(sliceIter - slices->begin(), nSlices);
  return nSlices;
}

void OutboundCall::setRequestPayload(
    const Message& req,
    vector<unique_ptr<RpcSidecar>>&& sidecars) {
  DCHECK_EQ(-1, sidecarByteSize_);

  sidecars_ = std::move(sidecars);
  DCHECK_LE(sidecars_.size(), TransferLimits::kMaxSidecars);

  // Compute total size of sidecar payload so that extra space can be reserved
  // as part of the request body.
  uint32_t messageSize = req.ByteSize();
  sidecarByteSize_ = 0;
  for (const unique_ptr<RpcSidecar>& car : sidecars_) {
    header_.add_sidecar_offsets(sidecarByteSize_ + messageSize);
    int32_t sidecarBytes = car->asSlice().size();
    DCHECK_LE(
        sidecarByteSize_, TransferLimits::kMaxTotalSidecarBytes - sidecarBytes);
    sidecarByteSize_ += sidecarBytes;
  }

  serialization::serializeMessage(req, &requestBuf_, sidecarByteSize_, true);
}

Status OutboundCall::status() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return status_;
}

const ErrorStatusPB* OutboundCall::errorPb() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return errorPb_.get();
}

string OutboundCall::stateName(State state) {
  switch (state) {
    case kReady:
      return "READY";
    case kOnOutboundQueue:
      return "ON_OUTBOUND_QUEUE";
    case kSending:
      return "SENDING";
    case kSent:
      return "SENT";
    case kNegotiationTimedOut:
      return "NEGOTIATION_TIMED_OUT";
    case kTimedOut:
      return "TIMED_OUT";
    case kCancelled:
      return "CANCELLED";
    case kFinishedNegotiationError:
      return "FINISHED_NEGOTIATION_ERROR";
    case kFinishedError:
      return "FINISHED_ERROR";
    case kFinishedSuccess:
      return "FINISHED_SUCCESS";
    default:
      LOG(DFATAL) << "Unknown state in OutboundCall: " << state;
      return fmt::format("UNKNOWN({})", state);
  }
}

void OutboundCall::setState(State newState) {
  setStateUnlocked(newState);
}

OutboundCall::State OutboundCall::state() const {
  return state_.load(std::memory_order_acquire);
}

void OutboundCall::setStateUnlocked(State newState) {
  State oldState = state_.load(std::memory_order_relaxed);

  // Use compare-and-exchange loop to ensure atomicity of state transition
  // validation and update.
  do {
    // Sanity check state transitions.
    DVLOG(3) << "OutboundCall " << this << " (" << toString()
             << ") switching from " << stateName(oldState) << " to "
             << stateName(newState);
    switch (newState) {
      case kOnOutboundQueue:
        DCHECK_EQ(oldState, kReady);
        break;
      case kSending:
        // Allow kSending to be set idempotently so we don't have to
        // specifically check whether the state is transitioning in the RPC
        // code.
        DCHECK(oldState == kOnOutboundQueue || oldState == kSending);
        break;
      case kSent:
        DCHECK_EQ(oldState, kSending);
        break;
      case kNegotiationTimedOut:
        DCHECK(oldState == kOnOutboundQueue);
        break;
      case kTimedOut:
        DCHECK(
            oldState == kSent || oldState == kOnOutboundQueue ||
            oldState == kSending);
        break;
      case kCancelled:
        DCHECK(
            oldState == kReady || oldState == kOnOutboundQueue ||
            oldState == kSent);
        break;
      case kFinishedSuccess:
        DCHECK_EQ(oldState, kSent);
        break;
      default:
        // No sanity checks for others.
        break;
    }
    // Attempt to atomically update state from oldState to newState.
    // If another thread changed the state, oldState will be updated with
    // the current value and we'll retry the validation and CAS.
  } while (!state_.compare_exchange_weak(
      oldState,
      newState,
      std::memory_order_release,
      std::memory_order_relaxed));
}

void OutboundCall::cancel() {
  cancellationRequested_ = true;
  switch (state_.load(std::memory_order_acquire)) {
    case kReady:
    case kOnOutboundQueue:
    case kSent: {
      setCancelled();
      break;
    }
    case kSending:
    case kNegotiationTimedOut:
    case kTimedOut:
    case kCancelled:
    case kFinishedNegotiationError:
    case kFinishedError:
    case kFinishedSuccess:
      break;
  }
}

void OutboundCall::callCallback() {
  // Clear references to outbound sidecars before invoking callback.
  sidecars_.clear();

  int64_t startCycles = kudu::CycleClock::now();
  {
    SCOPED_WATCH_STACK(0);
    callback_();
    // Clear the callback, since it may be holding onto reference counts
    // via bound parameters. We do this inside the timer because it's possible
    // the user has naughty destructors that block, and we want to account for
    // that time here if they happen to run on this thread.
    callback_ = NULL;
  }
  int64_t endCycles = kudu::CycleClock::now();
  int64_t waitCycles = endCycles - startCycles;
  if (PREDICT_FALSE(waitCycles > FLAGS_rpc_callback_max_cycles)) {
    double micros = static_cast<double>(waitCycles) / base::cyclesPerSecond() *
        kMicrosPerSecond;

    LOG(WARNING) << "RPC callback for " << toString()
                 << " blocked reactor thread for " << micros << "us";
  }
}

void OutboundCall::setResponse(unique_ptr<CallResponse> resp) {
  callResponse_ = std::move(resp);
  Slice r(callResponse_->serialized_response());

  if (callResponse_->isSuccess()) {
    // TODO: here we're deserializing the call response within the reactor
    // thread, which isn't great, since it would block processing of other RPCs
    // in parallel. Should look into a way to avoid this.
    if (!response_->ParseFromArray(r.data(), r.size())) {
      setFailed(
          Status::IOError(
              "invalid RPC response, missing fields",
              response_->InitializationErrorString()));
      return;
    }
    setState(kFinishedSuccess);
    callCallback();
  } else {
    // Error
    unique_ptr<ErrorStatusPB> err(new ErrorStatusPB());
    if (!err->ParseFromArray(r.data(), r.size())) {
      setFailed(
          Status::IOError(
              "Was an RPC error but could not parse error response",
              err->InitializationErrorString()));
      return;
    }
    Status s = Status::RemoteError(err->message());
    setFailed(std::move(s), Phase::RemoteCall, std::move(err));
  }
}

void OutboundCall::setQueued() {
  setState(kOnOutboundQueue);
}

void OutboundCall::setSending() {
  setState(kSending);
}

void OutboundCall::setSent() {
  setState(kSent);

  // This method is called in the reactor thread, so free the header buf,
  // which was also allocated from this thread. tcmalloc's thread caching
  // behavior is a lot more efficient if memory is freed from the same thread
  // which allocated it -- this lets it keep to thread-local operations instead
  // of taking a mutex to put memory back on the global freelist.
  delete[] headerBuf_.release();

  // requestBuf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.

  // If cancellation was requested, it's now a good time to do the actual
  // cancellation.
  if (cancellationRequested()) {
    setCancelled();
  }
}

void OutboundCall::setFailed(
    Status status,
    Phase phase,
    unique_ptr<ErrorStatusPB> errPb) {
  DCHECK(!status.ok());
  DCHECK(phase == Phase::ConnectionNegotiation || phase == Phase::RemoteCall);
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = std::move(status);
    errorPb_ = std::move(errPb);
    setStateUnlocked(
        phase == Phase::ConnectionNegotiation ? kFinishedNegotiationError
                                              : kFinishedError);
  }
  callCallback();
}

void OutboundCall::setTimedOut(Phase phase) {
  DCHECK(phase == Phase::ConnectionNegotiation || phase == Phase::RemoteCall);

  // We have to fetch timeout outside the lock to avoid a lock
  // order inversion between this class and RpcController.
  const MonoDelta timeout = controller_->timeout();
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (phase == Phase::RemoteCall) {
      status_ = Status::TimedOut(
          fmt::format(
              "{} RPC to {} timed out after {} ({})",
              remoteMethod_.methodName(),
              connId_.remote().ToString(),
              timeout.ToString(),
              stateName(state_)));
    } else {
      status_ = Status::TimedOut(
          fmt::format(
              "connection negotiation to {} for RPC {} timed out after {} ({})",
              connId_.remote().ToString(),
              remoteMethod_.methodName(),
              timeout.ToString(),
              stateName(state_.load(std::memory_order_relaxed))));
    }
    setStateUnlocked(
        (phase == Phase::RemoteCall) ? kTimedOut : kNegotiationTimedOut);
  }
  callCallback();
}

void OutboundCall::setCancelled() {
  DCHECK(!isFinished());
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = Status::Aborted(
        fmt::format(
            "{} RPC to {} is cancelled in state {}",
            remoteMethod_.methodName(),
            connId_.remote().ToString(),
            stateName(state_.load(std::memory_order_relaxed))));
    setStateUnlocked(kCancelled);
  }
  callCallback();
}

bool OutboundCall::isTimedOut() const {
  switch (state_.load(std::memory_order_acquire)) {
    case kNegotiationTimedOut: // fall-through
    case kTimedOut:
      return true;
    default:
      return false;
  }
}

bool OutboundCall::isCancelled() const {
  return state_.load(std::memory_order_acquire) == kCancelled;
}

bool OutboundCall::isNegotiationError() const {
  switch (state_.load(std::memory_order_acquire)) {
    case kFinishedNegotiationError: // fall-through
    case kNegotiationTimedOut:
      return true;
    default:
      return false;
  }
}

bool OutboundCall::isFinished() const {
  State currentState = state_.load(std::memory_order_acquire);
  switch (currentState) {
    case kReady:
    case kSending:
    case kOnOutboundQueue:
    case kSent:
      return false;
    case kNegotiationTimedOut:
    case kTimedOut:
    case kCancelled:
    case kFinishedNegotiationError:
    case kFinishedError:
    case kFinishedSuccess:
      return true;
    default:
      LOG(FATAL) << "Unknown call state: " << currentState;
  }
}

string OutboundCall::toString() const {
  return fmt::format(
      "RPC call {} -> {}", remoteMethod_.toString(), connId_.ToString());
}

void OutboundCall::dumpPb(
    const DumpRunningRpcsRequestPB& /* req */,
    RpcCallInProgressPB* resp) {
  resp->mutable_header()->CopyFrom(header_);
  resp->set_micros_elapsed((MonoTime::Now() - startTime_).ToMicroseconds());

  switch (state_.load(std::memory_order_acquire)) {
    case kReady:
      // Don't bother setting a state for "kReady" since we don't expose a call
      // until it's at least on the queue of a connection.
      break;
    case kOnOutboundQueue:
      resp->set_state(RpcCallInProgressPB::ON_OUTBOUND_QUEUE);
      break;
    case kSending:
      resp->set_state(RpcCallInProgressPB::SENDING);
      break;
    case kSent:
      resp->set_state(RpcCallInProgressPB::SENT);
      break;
    case kNegotiationTimedOut:
      resp->set_state(RpcCallInProgressPB::NEGOTIATION_TIMED_OUT);
      break;
    case kTimedOut:
      resp->set_state(RpcCallInProgressPB::TIMED_OUT);
      break;
    case kCancelled:
      resp->set_state(RpcCallInProgressPB::CANCELLED);
      break;
    case kFinishedNegotiationError:
      resp->set_state(RpcCallInProgressPB::FINISHED_NEGOTIATION_ERROR);
      break;
    case kFinishedError:
      resp->set_state(RpcCallInProgressPB::FINISHED_ERROR);
      break;
    case kFinishedSuccess:
      resp->set_state(RpcCallInProgressPB::FINISHED_SUCCESS);
      break;
  }
}

///
/// CallResponse
///

CallResponse::CallResponse() : parsed_(false) {}

Status CallResponse::GetSidecar(int idx, Slice* sidecar) const {
  DCHECK(parsed_);
  if (idx < 0 || idx >= header_.sidecar_offsets_size()) {
    return Status::InvalidArgument(
        fmt::format("Index {} does not reference a valid sidecar", idx));
  }
  *sidecar = sidecar_slices_[idx];
  return Status::OK();
}

Status CallResponse::parseFrom(unique_ptr<InboundTransfer> transfer) {
  CHECK(!parsed_);
  RETURN_NOT_OK(
      serialization::parseMessage(
          transfer->data(), &header_, &serialized_response_));

  // Use information from header to extract the payload slices.
  RETURN_NOT_OK(
      RpcSidecar::parseSidecars(
          header_.sidecar_offsets(), serialized_response_, sidecar_slices_));

  if (header_.sidecar_offsets_size() > 0) {
    serialized_response_ =
        Slice(serialized_response_.data(), header_.sidecar_offsets(0));
  }

  transfer_.swap(transfer);
  parsed_ = true;
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
