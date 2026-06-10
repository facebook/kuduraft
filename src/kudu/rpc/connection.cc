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

#include "kudu/rpc/connection.h"

#include <algorithm>
#include <cerrno>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <type_traits>

#include <boost/intrusive/detail/list_iterator.hpp>
#include <boost/intrusive/list.hpp>
#include <ev.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/Stats.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::includes;
using std::set;
using std::shared_ptr;
using std::unique_ptr;

METRIC_DEFINE_counter(
    server,
    timeout_connection_kill,
    "Timeout Connection Kill",
    kudu::MetricUnit::kOperations,
    "Number of a times a connection was killed after exceeding max number of "
    "timeouts");

DEFINE_int32(
    client_max_timeouts_before_connection_kill,
    5,
    "Number of timeouts incurred on outbound requests before we destroy the "
    "outbound connection. A value 0 or less will disable this feature.");
TAG_FLAG(client_max_timeouts_before_connection_kill, advanced);
TAG_FLAG(client_max_timeouts_before_connection_kill, runtime);

namespace kudu {
namespace rpc {

using Phase = OutboundCall::Phase;

///
/// Connection
///
Connection::Connection(
    ReactorThread* reactorThread,
    Sockaddr remote,
    unique_ptr<Socket> socket,
    ConnectionDirection direction,
    CredentialsPolicy policy,
    std::shared_ptr<MetricEntity> metricEntity)
    : reactorThread_(reactorThread),
      remote_(remote),
      socket_(std::move(socket)),
      direction_(direction),
      lastActivityTime_(MonoTime::Now()),
      isEpollRegistered_(false),
      nextCallId_(1),
      credentialsPolicy_(policy),
      negotiationComplete_(false),
      isConfidential_(false),
      scheduledForShutdown_(false),
      clientConsecutiveTimeouts_(0) {
  if (metricEntity) {
    timeoutConnectionKillCounter_ =
        METRIC_timeout_connection_kill.instantiate(metricEntity);
  }
}

Status Connection::setNonBlocking(bool enabled) {
  return socket_->setNonBlocking(enabled);
}

void Connection::epollRegister(ev::loop_ref& loop) {
  DCHECK(reactorThread_->isCurrentThread());
  DVLOG(4) << "Registering connection for epoll: " << toString();
  writeIo_.set(loop);
  writeIo_.set(socket_->getFd(), ev::WRITE);
  writeIo_.set<Connection, &Connection::writeHandler>(this);
  if (direction_ == ConnectionDirection::kClient && negotiationComplete_) {
    writeIo_.start();
  }
  readIo_.set(loop);
  readIo_.set(socket_->getFd(), ev::READ);
  readIo_.set<Connection, &Connection::readHandler>(this);
  readIo_.start();
  isEpollRegistered_ = true;
}

Connection::~Connection() {
  // Must clear the outboundTransfers_ list before deleting.
  CHECK(outboundTransfers_.begin() == outboundTransfers_.end())
      << "Pending outbound transfers on connection destruction: " << toString();

  // It's crucial that the connection is Shutdown first -- otherwise
  // our destructor will end up calling readIo_.stop() and writeIo_.stop()
  // from a possibly non-reactor thread context. This can then make all
  // hell break loose with libev.
  CHECK(!isEpollRegistered_)
      << "Event base attached during connection destruction. Connection was not shut down properly: "
      << toString();
}

bool Connection::idle() const {
  DCHECK(reactorThread_->isCurrentThread());
  // check if we're in the middle of receiving something
  InboundTransfer* transfer = inbound_.get();
  if (transfer && (transfer->transferStarted())) {
    return false;
  }
  // check if we still need to send something
  if (!outboundTransfers_.empty()) {
    return false;
  }
  // can't kill a connection if calls are waiting response
  if (!awaitingResponse_.empty()) {
    return false;
  }

  if (!callsBeingHandled_.empty()) {
    return false;
  }

  // We are not idle if we are in the middle of connection negotiation.
  if (!negotiationComplete_) {
    return false;
  }

  return true;
}

void Connection::shutdown(
    const Status& status,
    unique_ptr<ErrorStatusPB> rpcError) {
  DCHECK(reactorThread_->isCurrentThread());
  shutdownStatus_ = status.cloneAndPrepend("RPC connection failed");

  if (inbound_ && inbound_->transferStarted()) {
    double secsSinceActive =
        (reactorThread_->curTime() - lastActivityTime_).ToSeconds();
    LOG(WARNING) << "Shutting down " << toString()
                 << " with pending inbound data (" << inbound_->statusAsString()
                 << ", last active "
                 << HumanReadableElapsedTime::toShortString(secsSinceActive)
                 << " ago, status=" << status.ToString() << ")";
  }

  // Clear any calls which have been sent and were awaiting a response.
  for (const CarMap::value_type& v : awaitingResponse_) {
    CallAwaitingResponse* c = v.second;
    if (c->call) {
      // Make sure every awaiting call receives the error info, if any.
      unique_ptr<ErrorStatusPB> error;
      if (rpcError) {
        error.reset(new ErrorStatusPB(*rpcError));
      }
      c->call->setFailed(
          status,
          negotiationComplete_ ? Phase::RemoteCall
                               : Phase::ConnectionNegotiation,
          std::move(error));
    }
    // And we must return the CallAwaitingResponse to the pool
    carPool_.destroy(c);
  }
  awaitingResponse_.clear();
  clientConsecutiveTimeouts_ = 0;

  // Clear any outbound transfers.
  while (!outboundTransfers_.empty()) {
    OutboundTransfer* t = &outboundTransfers_.front();
    outboundTransfers_.pop_front();
    delete t;
  }

  readIo_.stop();
  writeIo_.stop();
  isEpollRegistered_ = false;
  if (socket_) {
    Status scStatus = socket_->close();
    if (PREDICT_FALSE(!scStatus.ok())) {
      VLOG(2) << "Error closing socket: " << scStatus.ToString();
    }
  }
}

void Connection::queueOutbound(unique_ptr<OutboundTransfer> transfer) {
  DCHECK(reactorThread_->isCurrentThread());

  if (!shutdownStatus_.ok()) {
    // If we've already shut down, then we just need to abort the
    // transfer rather than bothering to queue it.
    transfer->abort(shutdownStatus_);
    return;
  }

  DVLOG(3) << "Queueing transfer: " << transfer->hexDump();

  outboundTransfers_.push_back(*transfer.release());

  if (negotiationComplete_ && !writeIo_.is_active()) {
    // Optimistically assume that the socket is writable if we didn't already
    // have something queued.
    if (processOutboundTransfers() == kMoreToSend) {
      writeIo_.start();
    }
  }
}

Connection::CallAwaitingResponse::~CallAwaitingResponse() {
  DCHECK(conn->reactorThread_->isCurrentThread());
}

void Connection::CallAwaitingResponse::handleTimeout(
    ev::timer& watcher,
    int /* revents */) {
  if (remainingTimeout > 0) {
    if (watcher.remaining() < -1.0) {
      LOG(WARNING)
          << "RPC call timeout handler was delayed by " << -watcher.remaining()
          << "s! This may be due to a process-wide "
          << "pause such as swapping, logging-related delays, or allocator lock "
          << "contention. Will allow an additional " << remainingTimeout
          << "s for a response.";
    }

    watcher.set(remainingTimeout, 0);
    watcher.start();
    remainingTimeout = 0;
    return;
  }

  conn->handleOutboundCallTimeout(this);
}

void Connection::handleOutboundCallTimeout(CallAwaitingResponse* car) {
  DCHECK(reactorThread_->isCurrentThread());
  DCHECK(car->call);
  // The timeout timer is stopped by the car destructor exiting
  // Connection::handleCallResponse()
  DCHECK(!car->call->isFinished());

  // Mark the call object as failed.
  car->call->setTimedOut(
      negotiationComplete_ ? Phase::RemoteCall : Phase::ConnectionNegotiation);

  // Test cancellation when 'car->call' is in 'TIMED_OUT' state
  maybeInjectCancellation(car->call);

  // Drop the reference to the call. If the original caller has moved on after
  // seeing the timeout, we no longer need to hold onto the allocated memory
  // from the request.
  car->call.reset();

  // We still leave the CallAwaitingResponse in the map -- this is because we
  // may still receive a response from the server, and we don't want a spurious
  // log message when we do finally receive the response. The fact that
  // CallAwaitingResponse::call is a NULL pointer indicates to the response
  // processing code that the call already timed out.

  // If timeouts exceed X limit, destroy connection.
  int32_t maxTimeouts = FLAGS_client_max_timeouts_before_connection_kill;
  if (maxTimeouts > 0 && ++clientConsecutiveTimeouts_ > maxTimeouts) {
    LOG(WARNING) << "Shutting down connection "
                 << this->outboundConnectionId().ToString()
                 << " because we have incurred " << clientConsecutiveTimeouts_
                 << " consecutive timeouts which exceeds our max of "
                 << maxTimeouts;
    if (timeoutConnectionKillCounter_) {
      timeoutConnectionKillCounter_->increment();
      STATS_timeoutConnectionKill.add(1, KUDU_STATS_TAG);
    }
    setScheduledForShutdown();
  }
}

void Connection::cancelOutboundCall(const shared_ptr<OutboundCall>& call) {
  auto it = awaitingResponse_.find(call->callId());
  CallAwaitingResponse* car =
      (it != awaitingResponse_.end()) ? it->second : nullptr;
  if (car != nullptr) {
    // car->call may be NULL if the call has timed out already.
    DCHECK(!car->call || car->call.get() == call.get());
    car->call.reset();
  }
}

// Inject a cancellation when 'call' is in state
// 'FLAGS_rpc_inject_cancellation_state'.
void inline Connection::maybeInjectCancellation(
    const shared_ptr<OutboundCall>& call) {
  if (PREDICT_FALSE(call->shouldInjectCancellation())) {
    reactorThread_->reactor()->messenger()->queueCancellation(call);
  }
}

// Callbacks after sending a call on the wire.
// This notifies the OutboundCall object to change its state to SENT once it
// has been fully transmitted.
struct CallTransferCallbacks : public TransferCallbacks {
 public:
  explicit CallTransferCallbacks(
      shared_ptr<OutboundCall> call,
      Connection* conn)
      : call_(std::move(call)), conn_(conn) {}

  virtual void notifyTransferFinished() override {
    // TODO: would be better to cancel the transfer while it is still on the
    // queue if we timed out before the transfer started, but there is still a
    // race in the case of a partial send that we have to handle here
    if (call_->isFinished()) {
      DCHECK(call_->isTimedOut() || call_->isCancelled());
    } else {
      call_->setSent();
      // Test cancellation when 'call_' is in 'SENT' state.
      conn_->maybeInjectCancellation(call_);
    }
    delete this;
  }

  virtual void notifyTransferAborted(const Status& status) override {
    VLOG(1) << "Transfer of RPC call " << call_->toString()
            << " aborted: " << status.ToString();
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
  Connection* conn_;
};

void Connection::queueOutboundCall(shared_ptr<OutboundCall> call) {
  DCHECK(call);
  DCHECK_EQ(direction_, ConnectionDirection::kClient);
  DCHECK(reactorThread_->isCurrentThread());

  if (PREDICT_FALSE(!shutdownStatus_.ok())) {
    // Already shutdown
    call->setFailed(
        shutdownStatus_,
        negotiationComplete_ ? Phase::RemoteCall
                             : Phase::ConnectionNegotiation);
    return;
  }

  // At this point the call has a serialized request, but no call header, since
  // we haven't yet assigned a call ID.
  DCHECK(!call->callIdAssigned());

  // We shouldn't reach this point if 'call' was requested to be cancelled.
  DCHECK(!call->cancellationRequested());

  // Assign the call ID.
  int32_t callId = getNextCallId();
  call->setCallId(callId);

  // Serialize the actual bytes to be put on the wire.
  TransferPayload tmpSlices;
  size_t nSlices = call->serializeTo(&tmpSlices);

  call->setQueued();

  // Test cancellation when 'call_' is in 'ON_OUTBOUND_QUEUE' state.
  maybeInjectCancellation(call);

  ScopedCar car(carPool_.makeScopedPtr(carPool_.construct()));
  car->conn = this;
  car->call = call;

  // Set up the timeout timer.
  const MonoDelta& timeout = call->controller()->timeout();
  if (timeout.Initialized()) {
    reactorThread_->registerTimeout(&car->timeoutTimer);
    car->timeoutTimer.set<
        CallAwaitingResponse, // NOLINT(*)
        &CallAwaitingResponse::handleTimeout>(car.get());

    // For calls with a timeout of at least 500ms, we actually run the timeout
    // handler in two stages. The first timeout fires with a timeout 10% less
    // than the user-specified one. It then schedules a second timeout for the
    // remaining amount of time.
    //
    // The purpose of this two-stage timeout is to be more robust when the
    // client has some process-wide pause, such as lock contention in tcmalloc,
    // or a reactor callback that blocks in glog. Consider the following case:
    //
    // T = 0s        user issues an RPC with 5 second timeout
    // T = 0.5s - 6s   process is blocked
    // T = 6s        process unblocks, and the timeout fires (1s late)
    //
    // Without the two-stage timeout, we would determine that the call had timed
    // out, even though it's likely that the response is waiting on our TCP
    // socket. With the two-stage timeout, we'll end up with:
    //
    // T = 0s           user issues an RPC with 5 second timeout
    // T = 0.5s - 6s    process is blocked
    // T = 6s           process unblocks, and the first-stage timeout fires
    // (1.5s late) T = 6s - 6.200s  time for the client to read the response
    // which is waiting T = 6.200s       if the response was not actually
    // available, we'll time out here
    //
    // We don't bother with this logic for calls with very short timeouts -
    // assumedly a user setting such a short RPC timeout is well equipped to
    // handle one.
    double time = timeout.ToSeconds();
    if (time >= 0.5) {
      car->remainingTimeout = time * 0.1;
      time -= car->remainingTimeout;
    } else {
      car->remainingTimeout = 0;
    }

    car->timeoutTimer.set(time, 0);
    car->timeoutTimer.start();
  }

  TransferCallbacks* cb = new CallTransferCallbacks(std::move(call), this);
  awaitingResponse_[callId] = car.release();
  queueOutbound(
      unique_ptr<OutboundTransfer>(OutboundTransfer::createForCallRequest(
          callId, tmpSlices, nSlices, cb)));
}

// Callbacks for sending an RPC call response from the server.
// This takes ownership of the InboundCall object so that, once it has
// been responded to, we can free up all of the associated memory.
struct ResponseTransferCallbacks : public TransferCallbacks {
 public:
  ResponseTransferCallbacks(unique_ptr<InboundCall> call, Connection* conn)
      : call_(std::move(call)), conn_(conn) {}

  ~ResponseTransferCallbacks() {
    // Remove the call from the map.
    auto it = conn_->callsBeingHandled_.find(call_->callId());
    InboundCall* callFromMap =
        (it != conn_->callsBeingHandled_.end()) ? it->second : nullptr;
    if (it != conn_->callsBeingHandled_.end()) {
      conn_->callsBeingHandled_.erase(it);
    }
    DCHECK_EQ(callFromMap, call_.get());
  }

  virtual void notifyTransferFinished() override {
    delete this;
  }

  virtual void notifyTransferAborted(const Status& /* status */) override {
    LOG(WARNING) << "Connection torn down before " << call_->toString()
                 << " could send its response";
    delete this;
  }

 private:
  unique_ptr<InboundCall> call_;
  Connection* conn_;
};

// Reactor task which puts a transfer on the outbound transfer queue.
class QueueTransferTask : public ReactorTask {
 public:
  QueueTransferTask(unique_ptr<OutboundTransfer> transfer, Connection* conn)
      : transfer_(std::move(transfer)), conn_(conn) {}

  void run(ReactorThread* /* thr */) override {
    conn_->queueOutbound(std::move(transfer_));
    delete this;
  }

  void abort(const Status& status) override {
    transfer_->abort(status);
    delete this;
  }

 private:
  unique_ptr<OutboundTransfer> transfer_;
  Connection* conn_;
};

void Connection::queueResponseForCall(unique_ptr<InboundCall> call) {
  // This is usually called by the IPC worker thread when the response
  // is set, but in some circumstances may also be called by the
  // reactor thread (e.g. if the service has shut down)

  DCHECK_EQ(direction_, ConnectionDirection::kServer);

  // If the connection is torn down, then the queueOutbound() call that
  // eventually runs in the reactor thread will take care of calling
  // ResponseTransferCallbacks::notifyTransferAborted.

  TransferPayload tmpSlices;
  size_t nSlices = call->serializeResponseTo(&tmpSlices);

  TransferCallbacks* cb = new ResponseTransferCallbacks(std::move(call), this);
  // After the response is sent, can delete the InboundCall object.
  // We set a dummy call ID and required feature set, since these are not needed
  // when sending responses.
  unique_ptr<OutboundTransfer> t(
      OutboundTransfer::createForCallResponse(tmpSlices, nSlices, cb));

  QueueTransferTask* task = new QueueTransferTask(std::move(t), this);
  reactorThread_->reactor()->scheduleReactorTask(task);
}

void Connection::setConfidential(bool isConfidential) {
  isConfidential_ = isConfidential;
}

bool Connection::satisfiesCredentialsPolicy(CredentialsPolicy policy) const {
  DCHECK_EQ(direction_, ConnectionDirection::kClient);
  return (policy == CredentialsPolicy::AnyCredentials) ||
      (policy == credentialsPolicy_);
}

RpczStore* Connection::rpczStore() {
  return reactorThread_->reactor()->messenger()->rpczStore();
}

void Connection::readHandler(ev::io& /* watcher */, int revents) {
  DCHECK(reactorThread_->isCurrentThread());

  DVLOG(3) << toString() << " ReadHandler(revents=" << revents << ")";
  if (revents & EV_ERROR) {
    reactorThread_->destroyConnection(
        this,
        Status::NetworkError(
            toString() + ": ReadHandler encountered an error"));
    return;
  }
  lastActivityTime_ = reactorThread_->curTime();

  while (true) {
    if (!inbound_) {
      inbound_.reset(new InboundTransfer());
    }
    Status status = inbound_->receiveBuffer(*socket_);
    if (PREDICT_FALSE(!status.ok())) {
      if (status.posixCode() == ESHUTDOWN) {
        VLOG(1) << toString() << " shut down by remote end.";
      } else {
        KLOG_EVERY_N_SECS(WARNING, 300)
            << toString()
            << " recv error [EVERY 300 seconds]: " << status.ToString();
      }
      reactorThread_->destroyConnection(this, status);
      return;
    }
    if (!inbound_->transferFinished()) {
      if (shouldHandleLongCall()) {
        handleLongIncomingCall();
      }
      DVLOG(3) << toString() << ": read is not yet finished yet.";
      return;
    }
    DVLOG(3) << toString() << ": finished reading " << inbound_->data().size()
             << " bytes";

    inbound_->callAndClearLongTransferCallback();
    if (direction_ == ConnectionDirection::kClient) {
      handleCallResponse(std::move(inbound_));
    } else if (direction_ == ConnectionDirection::kServer) {
      handleIncomingCall(std::move(inbound_));
    } else {
      LOG(FATAL) << "Invalid direction: " << direction_;
    }

    // TODO: it would seem that it would be good to loop around and see if
    // there is more data on the socket by trying another recv(), but it turns
    // out that it really hurts throughput to do so. A better approach
    // might be for each InboundTransfer to actually try to read an extra byte,
    // and if it succeeds, then we'd copy that byte into a new InboundTransfer
    // and loop around, since it's likely the next call also arrived at the
    // same time.
    break;
  }
}

bool Connection::shouldHandleLongCall() const {
  if (!inbound_) {
    return false;
  }
  return direction_ == ConnectionDirection::kServer &&
      inbound_->isLongTransfer() && !inbound_->hasLongTransferCallback();
}

void Connection::handleLongIncomingCall() {
  if (!inbound_) {
    return;
  }

  uint32_t totalSize;
  RequestHeader header;
  if (serialization::tryParseRpcHeader(inbound_->data(), &totalSize, &header)
          .ok()) {
    inbound_->setLongTransferCallback(
        reactorThread_->reactor()->messenger()->signalLongInboundCall(
            header.remote_method().service_name(),
            header.remote_method().method_name()));
  }
}

void Connection::handleIncomingCall(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactorThread_->isCurrentThread());

  unique_ptr<InboundCall> call(new InboundCall(shared_from_this()));
  Status s = call->parseFrom(std::move(transfer));
  if (!s.ok()) {
    LOG(WARNING) << toString() << ": received bad data: " << s.ToString();
    // TODO: shutdown? probably, since any future stuff on this socket will be
    // "unsynchronized"
    return;
  }

  auto result = callsBeingHandled_.insert({call->callId(), call.get()});
  if (!result.second) {
    LOG(WARNING) << toString() << ": received call ID " << call->callId()
                 << " but was already processing this ID! Ignoring";
    reactorThread_->destroyConnection(
        this,
        Status::RuntimeError(
            "Received duplicate call id", fmt::format("{}", call->callId())));
    return;
  }

  reactorThread_->reactor()->messenger()->queueInboundCall(std::move(call));
}

void Connection::handleCallResponse(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactorThread_->isCurrentThread());
  unique_ptr<CallResponse> resp(new CallResponse);
  CHECK_OK(resp->parseFrom(std::move(transfer)));

  auto it = awaitingResponse_.find(resp->callId());
  CallAwaitingResponse* carPtr =
      (it != awaitingResponse_.end()) ? it->second : nullptr;
  if (it != awaitingResponse_.end()) {
    awaitingResponse_.erase(it);
  }
  if (PREDICT_FALSE(carPtr == nullptr)) {
    LOG(WARNING) << toString() << ": Got a response for call id "
                 << resp->callId() << " which "
                 << "was not pending! Ignoring.";
    return;
  }

  // The car->timeout_timer ev::timer will be stopped automatically by its
  // destructor.
  ScopedCar car(carPool_.makeScopedPtr(carPtr));

  if (PREDICT_FALSE(!car->call)) {
    // The call already failed due to a timeout.
    VLOG(1) << "Got response to call id " << resp->callId() << " after client "
            << "already timed out or cancelled";
    return;
  }

  clientConsecutiveTimeouts_ = 0;

  car->call->setResponse(std::move(resp));

  // Test cancellation when 'car->call' is in 'FINISHED_SUCCESS' or
  // 'FINISHED_ERROR' state.
  maybeInjectCancellation(car->call);
}

void Connection::writeHandler(ev::io& /* watcher */, int revents) {
  DCHECK(reactorThread_->isCurrentThread());

  if (revents & EV_ERROR) {
    reactorThread_->destroyConnection(
        this,
        Status::NetworkError(
            toString() + ": writeHandler encountered an error"));
    return;
  }
  DVLOG(3) << toString() << ": writeHandler: revents = " << revents;

  if (outboundTransfers_.empty()) {
    LOG(WARNING) << toString()
                 << " got a ready-to-write callback, but there is "
                    "nothing to write.";
    writeIo_.stop();
    return;
  }
  if (processOutboundTransfers() == kNoMoreToSend) {
    writeIo_.stop();
  }
}

Connection::ProcessOutboundTransfersResult
Connection::processOutboundTransfers() {
  while (!outboundTransfers_.empty()) {
    OutboundTransfer* transfer = &(outboundTransfers_.front());
    transfer = &(outboundTransfers_.front());

    if (!transfer->transferStarted()) {
      if (transfer->isForOutboundCall()) {
        auto it = awaitingResponse_.find(transfer->callId());
        CHECK(it != awaitingResponse_.end())
            << "Map key not found: " << transfer->callId();
        CallAwaitingResponse* car = it->second;
        if (!car->call) {
          // If the call has already timed out or has already been cancelled,
          // the 'call' field would be set to NULL. In that case, don't bother
          // sending it.
          outboundTransfers_.pop_front();
          transfer->abort(Status::Aborted("already timed out or cancelled"));
          delete transfer;
          continue;
        }

        // If this is the start of the transfer, then check if the server has
        // the required RPC flags. We have to wait until just before the
        // transfer in order to ensure that the negotiation has taken place, so
        // that the flags are available.
        const set<RpcFeatureFlag>& requiredFeatures =
            car->call->requiredRpcFeatures();
        if (!includes(
                remoteFeatures_.begin(),
                remoteFeatures_.end(),
                requiredFeatures.begin(),
                requiredFeatures.end())) {
          outboundTransfers_.pop_front();
          Status s = Status::NotSupported(
              "server does not support the required RPC features");
          transfer->abort(s);
          Phase phase = negotiationComplete_ ? Phase::RemoteCall
                                             : Phase::ConnectionNegotiation;
          car->call->setFailed(std::move(s), phase);
          // Test cancellation when 'call_' is in 'FINISHED_ERROR' state.
          maybeInjectCancellation(car->call);
          car->call.reset();
          delete transfer;
          continue;
        }

        car->call->setSending();

        // Test cancellation when 'call_' is in 'SENDING' state.
        maybeInjectCancellation(car->call);
      }
    }

    lastActivityTime_ = reactorThread_->curTime();
    Status status = transfer->sendBuffer(*socket_);
    if (PREDICT_FALSE(!status.ok())) {
      KLOG_EVERY_N_SECS(WARNING, 300)
          << toString()
          << " send error [EVERY 300 seconds]: " << status.ToString();
      reactorThread_->destroyConnection(this, status);
      return kConnectionDestroyed;
    }

    if (!transfer->transferFinished()) {
      DVLOG(3) << toString() << ": writeHandler: xfer not finished.";
      return kMoreToSend;
    }

    outboundTransfers_.pop_front();
    delete transfer;
  }
  return kNoMoreToSend;
}

std::string Connection::toString() const {
  // This may be called from other threads, so we cannot
  // include anything in the output about the current state,
  // which might concurrently change from another thread.
  return fmt::format(
      "{} {}",
      direction_ == ConnectionDirection::kServer ? "server connection from"
                                                 : "client connection to",
      remote_.ToString());
}

// Reactor task that transitions this Connection from connection negotiation to
// regular RPC handling. Destroys Connection on negotiation error.
class NegotiationCompletedTask : public ReactorTask {
 public:
  NegotiationCompletedTask(
      std::shared_ptr<Connection> conn,
      Status negotiationStatus,
      std::unique_ptr<ErrorStatusPB> rpcError)
      : conn_(std::move(conn)),
        negotiationStatus_(std::move(negotiationStatus)),
        rpcError_(std::move(rpcError)) {}

  void run(ReactorThread* rthread) override {
    rthread->completeConnectionNegotiation(
        conn_, negotiationStatus_, std::move(rpcError_));
    delete this;
  }

  void abort(const Status& status) override {
    DCHECK(conn_->reactorThread()->reactor()->closing());
    VLOG(1) << "Failed connection negotiation due to shut down reactor thread: "
            << status.ToString();
    delete this;
  }

 private:
  std::shared_ptr<Connection> conn_;
  const Status negotiationStatus_;
  std::unique_ptr<ErrorStatusPB> rpcError_;
};

void Connection::completeNegotiation(
    Status negotiationStatus,
    unique_ptr<ErrorStatusPB> rpcError) {
  auto task = new NegotiationCompletedTask(
      shared_from_this(), std::move(negotiationStatus), std::move(rpcError));
  reactorThread_->reactor()->scheduleReactorTask(task);
}

void Connection::markNegotiationStarted() {
  negotiationRunning_ = true;
}

void Connection::markNegotiationComplete() {
  DCHECK(reactorThread_->isCurrentThread());
  negotiationRunning_ = false;
  negotiationComplete_ = true;
}

Status Connection::dumpPb(
    const DumpRunningRpcsRequestPB& req,
    RpcConnectionPB* resp) {
  DCHECK(reactorThread_->isCurrentThread());
  resp->set_remote_ip(remote_.ToString());
  if (negotiationComplete_) {
    resp->set_state(RpcConnectionPB::OPEN);
  } else {
    resp->set_state(RpcConnectionPB::NEGOTIATING);
  }

  if (direction_ == ConnectionDirection::kClient) {
    for (const CarMap::value_type& entry : awaitingResponse_) {
      CallAwaitingResponse* c = entry.second;
      if (c->call) {
        c->call->dumpPb(req, resp->add_calls_in_flight());
      }
    }

    resp->set_outbound_queue_size(numQueuedOutboundTransfers());
  } else if (direction_ == ConnectionDirection::kServer) {
    if (negotiationComplete_) {
      // It's racy to dump credentials while negotiating, since the Connection
      // object is owned by the negotiation thread at that point.
      resp->set_remote_user_credentials(remoteUser_.toString());
    }
    for (const InboundCallMap::value_type& entry : callsBeingHandled_) {
      InboundCall* c = entry.second;
      c->dumpPb(req, resp->add_calls_in_flight());
    }
  } else {
    LOG(FATAL);
  }
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
