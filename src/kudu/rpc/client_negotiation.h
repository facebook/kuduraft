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
#include <set>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <optional>

#include "kudu/gutil/port.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {

class Slice;
class faststring;

namespace security {
class TlsContext;
}

namespace rpc {

// Class for doing KRPC negotiation with a remote server over a bidirectional
// socket. Operations on this class are NOT thread-safe.
class ClientNegotiation {
 public:
  // Creates a new client negotiation instance, taking ownership of the
  // provided socket. After completing the negotiation process by setting the
  // desired options and calling Negotiate(), the socket can be retrieved with
  // 'release_socket'.
  //
  // The provided TlsContext must outlive this negotiation instance.
  ClientNegotiation(
      std::unique_ptr<Socket> socket,
      const security::TlsContext* tlsContext,
      std::optional<security::SignedTokenPB> authnToken,
      RpcEncryption encryption);

  // Returns the negotiated authentication type for the connection.
  // Must be called after Negotiate().
  AuthenticationType negotiatedAuthn() const {
    DCHECK_NE(negotiatedAuthn_, AuthenticationType::Invalid);
    return negotiatedAuthn_;
  }

  // Returns true if TLS was negotiated.
  // Must be called after Negotiate().
  bool tlsNegotiated() const {
    return tlsNegotiated_;
  }

  // Returns true if normal TLS was negotiated.
  // Must be called after Negotiate().
  bool normalTlsNegotiated() const {
    return normalTlsNegotiated_;
  }

  // Returns the set of RPC system features supported by the remote server.
  // Must be called before Negotiate().
  std::set<RpcFeatureFlag> serverFeatures() const {
    return serverFeatures_;
  }

  // Returns the set of RPC system features supported by the remote server.
  // Must be called after Negotiate().
  // Subsequent calls to this method or serverFeatures() will return an empty
  // set.
  std::set<RpcFeatureFlag> takeServerFeatures() {
    return std::move(serverFeatures_);
  }

  // Set deadline for connection negotiation.
  void setDeadline(const MonoTime& deadline);

  Socket* socket() {
    return socket_.get();
  }

  // Takes and returns the socket owned by this client negotiation. The caller
  // will own the socket after this call, and the negotiation instance should no
  // longer be used. Must be called after Negotiate(). Subsequent calls to this
  // method or socket() will return a null pointer.
  std::unique_ptr<Socket> releaseSocket() {
    return std::move(socket_);
  }

  // Negotiate with the remote server. Should only be called once per
  // ClientNegotiation and socket instance, after all options have been set.
  //
  // Returns OK on success, otherwise may return NotAuthorized, NotSupported, or
  // another non-OK status.
  Status negotiate(std::unique_ptr<ErrorStatusPB>* rpcError = nullptr);

  // Perform normal TLS handshake
  Status handleTls() WARN_UNUSED_RESULT;

 private:
  // Encode and send the specified negotiate request message to the server.
  Status sendNegotiatePb(const NegotiatePB& msg) WARN_UNUSED_RESULT;

  // Receive a negotiate response message from the server, deserializing it into
  // 'msg'. Validates that the response is not an error.
  Status recvNegotiatePb(
      NegotiatePB* msg,
      faststring* buffer,
      std::unique_ptr<ErrorStatusPB>* rpcError) WARN_UNUSED_RESULT;

  // Parse error status message from raw bytes of an ErrorStatusPB.
  Status parseError(
      const Slice& errData,
      std::unique_ptr<ErrorStatusPB>* rpcError) WARN_UNUSED_RESULT;

  Status sendConnectionHeader() WARN_UNUSED_RESULT;

  // Send a NEGOTIATE step message to the server.
  Status sendNegotiate() WARN_UNUSED_RESULT;

  // Handle NEGOTIATE step response from the server.
  Status handleNegotiate(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Send a TLS_HANDSHAKE request message to the server with the provided token.
  Status sendTlsHandshake(std::string tlsToken) WARN_UNUSED_RESULT;

  // Handle a TLS_HANDSHAKE response message from the server.
  Status handleTlsHandshake(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Authenticate to the server using a token.
  // 'recvBuf' allows a receive buffer to be reused.
  Status authenticateByToken(
      faststring* recvBuf,
      std::unique_ptr<ErrorStatusPB>* rpcError) WARN_UNUSED_RESULT;

  Status sendConnectionContext() WARN_UNUSED_RESULT;

  // The socket to the remote server.
  std::unique_ptr<Socket> socket_;

  // TLS state.
  const security::TlsContext* tlsContext_;
  security::TlsHandshake tlsHandshake_;
  const RpcEncryption encryption_;
  bool tlsNegotiated_;
  bool normalTlsNegotiated_;

  // TSK state.
  std::optional<security::SignedTokenPB> authnToken_;

  // The set of features advertised by the client. Filled in when we send
  // the first message. This is not necessarily constant since some features
  // may be dynamically enabled.
  std::set<RpcFeatureFlag> clientFeatures_;

  // The set of features supported by the server. Filled in during negotiation.
  std::set<RpcFeatureFlag> serverFeatures_;

  // The authentication type. Filled in during negotiation.
  AuthenticationType negotiatedAuthn_;

  // Negotiation timeout deadline.
  MonoTime deadline_;
};

} // namespace rpc
} // namespace kudu
