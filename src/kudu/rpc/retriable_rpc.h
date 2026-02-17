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

#include <memory>
#include <string>

#include <fmt/core.h>
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace rpc {

namespace internal {
typedef rpc::RequestTracker::SequenceNumber SequenceNumber;
}

// A base class for retriable RPCs that handles replica picking and retry logic.
//
// The 'Server' template parameter refers to the type of the server that will be
// looked up and passed to the derived classes on tryRpc(). For instance in the
// case of WriteRpc it's RemoteTabletServer.
//
// TODO(unknown): merge RpcRetrier into this class? Can't be done right now as
// the retrier is used independently elsewhere, but likely possible when all
// replicated RPCs have a ReplicaPicker.
//
// TODO(unknown): allow to target replicas other than the leader, if needed.
//
// TODO(unknown): once we have retry handling on all the RPCs merge this with
// rpc::Rpc.
template <class Server, class RequestPB, class ResponsePB>
class RetriableRpc : public Rpc {
 public:
  RetriableRpc(
      const std::shared_ptr<ServerPicker<Server>>& serverPicker,
      const std::shared_ptr<RequestTracker>& requestTracker,
      const MonoTime& deadline,
      std::shared_ptr<Messenger> messenger)
      : Rpc(deadline, std::move(messenger)),
        serverPicker_(serverPicker),
        requestTracker_(requestTracker),
        sequenceNumber_(RequestTracker::kNoSeqNo),
        numAttempts_(0) {}

  virtual ~RetriableRpc() {
    DCHECK_EQ(sequenceNumber_, RequestTracker::kNoSeqNo);
  }

  // Performs server lookup/initialization.
  // If/when the server is looked up and initialized successfully RetriableRpc
  // will call tryRpc() to actually send the request.
  void sendRpc() override;

  // The callback to call upon retrieving (of failing to retrieve) a new authn
  // token. This is the callback that subclasses should call in their custom
  // implementation of the getNewAuthnTokenAndRetry() method.
  void getNewAuthnTokenAndRetryCb(const Status& status);

 protected:
  // Subclasses implement this method to actually try the RPC.
  // The server been looked up and is ready to be used.
  virtual void tryRpc(Server* replica, const ResponseCallback& callback) = 0;

  // Subclasses implement this method to analyze 'status', the controller status
  // or the response and return a RetriableRpcStatus which will then be used to
  // decide how to proceed (retry or give up).
  virtual RetriableRpcStatus analyzeResponse(const Status& status) = 0;

  // Subclasses implement this method to perform cleanup and/or final steps.
  // After this is called the RPC will be no longer retried.
  virtual void finish(const Status& status) = 0;

  // Returns 'true' if the RPC is to scheduled for retry with a new authn token,
  // 'false' otherwise. For RPCs performed in the context of providing token
  // for authentication it's necessary to implement this method. The default
  // implementation returns 'false' meaning the calls returning
  // INVALID_AUTHENTICATION_TOKEN RPC status are not retried.
  virtual bool getNewAuthnTokenAndRetry() {
    return false;
  }

  // Request body.
  RequestPB req_;

  // Response body.
  ResponsePB resp_;

 private:
  friend class CalculatorServiceRpc;

  // Decides whether to retry the RPC, based on the result of analyzeResponse()
  // and retries if that is the case.
  // Returns true if the RPC was retried or false otherwise.
  bool retryIfNeeded(const RetriableRpcStatus& result, Server* server);

  // Called when the replica has been looked up.
  void replicaFoundCb(const Status& status, Server* server);

  // Called after the RPC was performed.
  void sendRpcCb(const Status& status) override;

  // Performs final cleanup, after the RPC is done (independently of success).
  void finishInternal();

  std::shared_ptr<ServerPicker<Server>> serverPicker_;
  std::shared_ptr<RequestTracker> requestTracker_;
  std::shared_ptr<Messenger> messenger_;

  // The sequence number for this RPC.
  internal::SequenceNumber sequenceNumber_;

  // The number of times this RPC has been attempted
  int32_t numAttempts_;

  // Keeps track of the replica the RPCs were sent to.
  // TODO Remove this and pass the used replica around. For now we need to keep
  // this as the retrier calls the sendRpcCb directly and doesn't know the
  // replica that was being written to.
  Server* current_;
};

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::sendRpc() {
  if (sequenceNumber_ == RequestTracker::kNoSeqNo) {
    CHECK_OK(requestTracker_->NewSeqNo(&sequenceNumber_));
  }
  serverPicker_->pickLeader(
      Bind(&RetriableRpc::replicaFoundCb, Unretained(this)),
      retrier().deadline());
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::getNewAuthnTokenAndRetryCb(
    const Status& status) {
  if (status.ok()) {
    // Perform the RPC call with the newly fetched authn token.
    mutableRetrier()->mutableController()->Reset();
    sendRpc();
  } else {
    // Back to the retry sequence, hoping for better conditions after some time.
    VLOG(1) << "Failed to get new authn token: " << status.ToString();
    mutableRetrier()->delayedRetry(this, status);
  }
}

template <class Server, class RequestPB, class ResponsePB>
bool RetriableRpc<Server, RequestPB, ResponsePB>::retryIfNeeded(
    const RetriableRpcStatus& result,
    Server* server) {
  // Handle the cases where we retry.
  switch (result.result) {
    case RetriableRpcStatus::kServiceUnavailable:
      // For writes, always retry the request on the same server in case of the
      // SERVICE_UNAVAILABLE error.
      break;

    case RetriableRpcStatus::kServerNotAccessible:
      // TODO(KUDU-1745): not checking for null here results in a crash, since
      // in the case of a failed master lookup we have no tablet server
      // corresponding to the error.
      //
      // But, with the null check, we end up with a relatively tight retry loop
      // in this scenario whereas we should be backing off. Need to improve
      // test coverage here to understand why the back-off is not taking effect.
      if (server != nullptr) {
        VLOG(1) << "Failing " << ToString()
                << " to a new target: " << result.status.ToString();
        // Mark the server as failed. As for details on the only existing
        // implementation of ServerPicker::markServerFailed(), see the note on
        // the MetaCacheServerPicker::markServerFailed() method.
        serverPicker_->markServerFailed(server, result.status);
      }
      break;

    case RetriableRpcStatus::kResourceNotFound:
      // The TabletServer was not part of the config serving the tablet.
      // We mark our tablet cache as stale, forcing a master lookup on the
      // next attempt.
      //
      // TODO(KUDU-1314): Don't backoff the first time we hit this error.
      serverPicker_->markResourceNotFound(server);
      break;

    case RetriableRpcStatus::kReplicaNotLeader:
      // The TabletServer was not the leader of the quorum.
      serverPicker_->markReplicaNotLeader(server);
      break;

    case RetriableRpcStatus::kInvalidAuthenticationToken: {
      // This is a special case for retry: first it's necessary to get a new
      // authn token and then retry the operation with the new token.
      if (getNewAuthnTokenAndRetry()) {
        // The RPC will be retried.
        resp_.Clear();
        return true;
      }
      // Do not retry.
      return false;
    }

    case RetriableRpcStatus::kNonRetriableError:
      if (server != nullptr && result.status.IsTimedOut()) {
        // For the NON_RETRIABLE_ERROR result in case of TimedOut status,
        // mark the server as failed. As for details on the only existing
        // implementation of ServerPicker::markServerFailed(), see the note on
        // the MetaCacheServerPicker::markServerFailed() method.
        VLOG(1) << "Failing " << ToString()
                << " to a new target: " << result.status.ToString();
        serverPicker_->markServerFailed(server, result.status);
      }
      // Do not retry in the case of non-retriable error.
      return false;

    default:
      // For the OK case we should not retry.
      DCHECK(result.result == RetriableRpcStatus::kOk);
      return false;
  }
  resp_.Clear();
  current_ = nullptr;
  mutableRetrier()->delayedRetry(this, result.status);
  return true;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::finishInternal() {
  // Mark the RPC as completed and set the sequence number to kNoSeqNo to make
  // sure we're in the appropriate state before destruction.
  requestTracker_->RpcCompleted(sequenceNumber_);
  sequenceNumber_ = RequestTracker::kNoSeqNo;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::replicaFoundCb(
    const Status& status,
    Server* server) {
  // NOTE: 'server' here may be nullptr in the case that status is not OK!
  RetriableRpcStatus result = analyzeResponse(status);
  if (retryIfNeeded(result, server))
    return;

  if (result.result == RetriableRpcStatus::kNonRetriableError) {
    finishInternal();
    finish(result.status);
    return;
  }

  // We successfully found a replica, so prepare the RequestIdPB before we send
  // out the call.
  std::unique_ptr<RequestIdPB> requestId(new RequestIdPB());
  requestId->set_client_id(requestTracker_->clientId());
  requestId->set_seq_no(sequenceNumber_);
  requestId->set_first_incomplete_seq_no(requestTracker_->FirstIncomplete());
  requestId->set_attempt_no(numAttempts_++);

  mutableRetrier()->mutableController()->SetRequestIdPB(std::move(requestId));

  DCHECK_EQ(result.result, RetriableRpcStatus::kOk);
  current_ = server;
  tryRpc(server, boost::bind(&RetriableRpc::sendRpcCb, this, Status::OK()));
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::sendRpcCb(
    const Status& status) {
  RetriableRpcStatus result = analyzeResponse(status);
  if (retryIfNeeded(result, current_))
    return;

  finishInternal();

  // From here on out the RPC has either succeeded of suffered a non-retriable
  // failure.
  Status finalStatus = result.status;
  if (!finalStatus.ok()) {
    std::string errorString;
    if (current_) {
      errorString =
          fmt::format("Failed to write to server: {}", current_->ToString());
    } else {
      errorString = "Failed to write to server: (no server available)";
    }
    finalStatus = finalStatus.CloneAndPrepend(errorString);
  }
  finish(finalStatus);
}

} // namespace rpc
} // namespace kudu
