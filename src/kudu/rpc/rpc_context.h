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
#ifndef KUDU_RPC_RPC_CONTEXT_H
#define KUDU_RPC_RPC_CONTEXT_H

#include <stddef.h>
#include <memory>
#include <string>

#include <glog/logging.h>

#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class Slice;
class Sockaddr;
class Trace;

namespace rpc {

class InboundCall;
class RemoteUser;
class ResultTracker;
class RpcSidecar;

#define PANIC_RPC(rpc_context, message)                    \
  do {                                                     \
    if (rpc_context) {                                     \
      (rpc_context)->panic(__FILE__, __LINE__, (message)); \
    } else {                                               \
      LOG(FATAL) << (message);                             \
    }                                                      \
  } while (0)

// The context provided to a generated ServiceIf. This provides
// methods to respond to the RPC. In the future, this will also
// include methods to access information about the caller: e.g
// authentication info, tracing info, and cancellation status.
//
// This is the server-side analogue to the RpcController class.
class RpcContext {
 public:
  // Create an RpcContext. This is called only from generated code
  // and is not a public API.
  RpcContext(
      InboundCall* call,
      const google::protobuf::Message* request_pb,
      google::protobuf::Message* response_pb);

  ~RpcContext();

  // Initialize a result tracker for the RPC.
  //
  // This is delayed until after the constructor in order to allow for RPCs to
  // be validated and used prior to initializing the tracking (primarily for
  // authorization).
  void setResultTracker(std::shared_ptr<ResultTracker> result_tracker);

  // Return the trace buffer for this call.
  std::shared_ptr<Trace> trace();

  // Send a response to the call. The service may call this method
  // before or after returning from the original handler method,
  // and it may call this method from a different thread.
  //
  // The response should be prepared already in the response PB pointer
  // which was passed to the handler method.
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void respondSuccess();

  // Like the above, but doesn't store the results of the service call, if
  // results are being tracked. Used in cases where a call specific error was
  // set on the response protobuf, the call should be considered failed, thus
  // results shouldn't be cached.
  void respondNoCache();

  // Respond with an error to the client. This sends back an error with the code
  // ERROR_APPLICATION. Because there is no more specific error code passed back
  // to the client, most applications should create a custom error PB extension
  // and use RespondApplicationError(...) below. This method should only be used
  // for unexpected errors where the server doesn't expect the client to do any
  // more advanced handling.
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void respondFailure(const Status& status);

  // Respond with an RPC-level error. This typically manifests to the client as
  // a remote error, one whose handling is agnostic to the particulars of the
  // sent RPC. For example, both ERROR_SERVER_TOO_BUSY and ERROR_UNAVAILABLE
  // usually cause the client to retry the RPC at a later time.
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void respondRpcFailure(
      ErrorStatusPB_RpcErrorCodePB err,
      const Status& status);

  // Respond with an application-level error. This causes the caller to get a
  // RemoteError status with the provided string message. Additionally, a
  // service-specific error extension is passed back to the client. The
  // extension must be registered with the ErrorStatusPB protobuf. For
  // example:
  //
  //   message MyServiceError {
  //     extend kudu.rpc.ErrorStatusPB {
  //       optional MyServiceError my_service_error_ext = 101;
  //     }
  //     // Add any extra fields or status codes you want to pass back to
  //     // the client here.
  //     required string extra_error_data = 1;
  //   }
  //
  // NOTE: the numeric '101' above must be an integer greater than 101
  // and must be unique across your code base.
  //
  // Given the above definition in your service protobuf file, you would
  // use this method like:
  //
  //   MyServiceError err;
  //   err.set_extra_error_data("foo bar");
  //   ctx->respondApplicationError(MyServiceError::my_service_error_ext.number(),
  //                                "Some error occurred", err);
  //
  // The client side may then retreieve the error by calling:
  //   const MyServiceError& err_details =
  //     controller->error_response()->GetExtension(MyServiceError::my_service_error_ext);
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void respondApplicationError(
      int error_ext_id,
      const std::string& message,
      const google::protobuf::Message& app_error_pb);

  // Adds an RpcSidecar to the response. This is the preferred method for
  // transferring large amounts of binary data, because this avoids additional
  // copies made by serializing the protobuf.
  //
  // Assumes no changes to the sidecar's data are made after insertion.
  //
  // Upon success, writes the index of the sidecar (necessary to be retrieved
  // later) to 'idx'. Call may fail if all sidecars have already been used
  // by the RPC response.
  Status addOutboundSidecar(std::unique_ptr<RpcSidecar> car, int* idx);

  // Fills 'sidecar' with a sidecar sent by the client. Returns an error if
  // 'idx' is out of bounds.
  Status getInboundSidecar(int idx, Slice* slice) const;

  // Return the identity of remote user who made this call.
  const RemoteUser& remoteUser() const;

  // Whether it's OK to pass confidential information between the client and the
  // server in the context of the RPC call being handled.  In real world, this
  // translates into properties of the connection between the client and the
  // server. For example, this methods returns 'true' for a call over an
  // encrypted connection.
  bool isConfidential() const;

  // Discards the memory associated with the inbound call's payload. All
  // previously obtained sidecar slices will be invalidated by this call. It is
  // an error to call getInboundSidecar() after this method. requestPb()
  // remains valid. This is useful in the case where the server wishes to delay
  // responding to an RPC (perhaps to control the rate of RPC requests), but
  // knows that the RPC payload itself won't be processed any further.
  void discardTransfer();

  // Return the remote IP address and port which sent the current RPC call.
  const Sockaddr& remoteAddress() const;

  // A string identifying the requestor -- both the user info and the IP
  // address. Suitable for use in log messages.
  std::string requestorString() const;

  // Return the name of the RPC service method being called.
  std::string methodName() const;

  // Return the name of the RPC service being called.
  std::string serviceName() const;

  const google::protobuf::Message* requestPb() const {
    return request_pb_.get();
  }
  google::protobuf::Message* responsePb() const {
    return response_pb_.get();
  }

  // Return an upper bound on the client timeout deadline. This does not
  // account for transmission delays between the client and the server.
  // If the client did not specify a deadline, returns MonoTime::Max().
  MonoTime getClientDeadline() const;

  // Return the time when the inbound call was received.
  MonoTime getTimeReceived() const;

  // Whether the results of this RPC are tracked with a ResultTracker.
  // If this returns true, both result_tracker() and request_id() should return
  // non-null results.
  bool areResultsTracked() const {
    return result_tracker_.get() != nullptr;
  }

  // Returns this call's result tracker, if it is set.
  const std::shared_ptr<ResultTracker>& resultTracker() const {
    return result_tracker_;
  }

  // Returns this call's request id, if it is set.
  const rpc::RequestIdPB* requestId() const;

  // Returns the size of the transfer buffer that backs 'call_'. If the
  // transfer buffer no longer exists (e.g. getTransferSize() is called after
  // discardTransfer()), returns 0.
  size_t getTransferSize() const;

  // Panic the server. This logs a fatal error with the given message, and
  // also includes the current RPC request, requestor, trace information, etc,
  // to make it easier to debug.
  //
  // Call this via the PANIC_RPC() macro.
  void panic(const char* filepath, int lineNumber, const std::string& message)
      __attribute__((noreturn));

 private:
  friend class ResultTracker;
  InboundCall* const call_;
  const std::unique_ptr<const google::protobuf::Message> request_pb_;
  const std::unique_ptr<google::protobuf::Message> response_pb_;
  std::shared_ptr<ResultTracker> result_tracker_;
};

} // namespace rpc
} // namespace kudu
#endif
