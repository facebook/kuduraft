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
      const security::TlsContext* tls_context,
      std::optional<security::SignedTokenPB> authn_token,
      RpcEncryption encryption);

  // Returns the negotiated authentication type for the connection.
  // Must be called after Negotiate().
  AuthenticationType negotiated_authn() const {
    DCHECK_NE(negotiated_authn_, AuthenticationType::INVALID);
    return negotiated_authn_;
  }

  // Returns true if TLS was negotiated.
  // Must be called after Negotiate().
  bool tls_negotiated() const {
    return tls_negotiated_;
  }

  // Returns true if normal TLS was negotiated.
  // Must be called after Negotiate().
  bool normal_tls_negotiated() const {
    return normal_tls_negotiated_;
  }

  // Returns the set of RPC system features supported by the remote server.
  // Must be called before Negotiate().
  std::set<RpcFeatureFlag> server_features() const {
    return server_features_;
  }

  // Returns the set of RPC system features supported by the remote server.
  // Must be called after Negotiate().
  // Subsequent calls to this method or server_features() will return an empty
  // set.
  std::set<RpcFeatureFlag> take_server_features() {
    return std::move(server_features_);
  }

  // Set deadline for connection negotiation.
  void set_deadline(const MonoTime& deadline);

  Socket* socket() {
    return socket_.get();
  }

  // Takes and returns the socket owned by this client negotiation. The caller
  // will own the socket after this call, and the negotiation instance should no
  // longer be used. Must be called after Negotiate(). Subsequent calls to this
  // method or socket() will return a null pointer.
  std::unique_ptr<Socket> release_socket() {
    return std::move(socket_);
  }

  // Negotiate with the remote server. Should only be called once per
  // ClientNegotiation and socket instance, after all options have been set.
  //
  // Returns OK on success, otherwise may return NotAuthorized, NotSupported, or
  // another non-OK status.
  Status Negotiate(std::unique_ptr<ErrorStatusPB>* rpc_error = nullptr);

  // Perform normal TLS handshake
  Status HandleTLS() WARN_UNUSED_RESULT;

 private:
  // Encode and send the specified negotiate request message to the server.
  Status SendNegotiatePB(const NegotiatePB& msg) WARN_UNUSED_RESULT;

  // Receive a negotiate response message from the server, deserializing it into
  // 'msg'. Validates that the response is not an error.
  Status RecvNegotiatePB(
      NegotiatePB* msg,
      faststring* buffer,
      std::unique_ptr<ErrorStatusPB>* rpc_error) WARN_UNUSED_RESULT;

  // Parse error status message from raw bytes of an ErrorStatusPB.
  Status ParseError(
      const Slice& err_data,
      std::unique_ptr<ErrorStatusPB>* rpc_error) WARN_UNUSED_RESULT;

  Status SendConnectionHeader() WARN_UNUSED_RESULT;

  // Send a NEGOTIATE step message to the server.
  Status SendNegotiate() WARN_UNUSED_RESULT;

  // Handle NEGOTIATE step response from the server.
  Status HandleNegotiate(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Send a TLS_HANDSHAKE request message to the server with the provided token.
  Status SendTlsHandshake(std::string tls_token) WARN_UNUSED_RESULT;

  // Handle a TLS_HANDSHAKE response message from the server.
  Status HandleTlsHandshake(const NegotiatePB& response) WARN_UNUSED_RESULT;

  // Authenticate to the server using a token.
  // 'recv_buf' allows a receive buffer to be reused.
  Status AuthenticateByToken(
      faststring* recv_buf,
      std::unique_ptr<ErrorStatusPB>* rpc_error) WARN_UNUSED_RESULT;

  Status SendConnectionContext() WARN_UNUSED_RESULT;

  // The socket to the remote server.
  std::unique_ptr<Socket> socket_;

  // TLS state.
  const security::TlsContext* tls_context_;
  security::TlsHandshake tls_handshake_;
  const RpcEncryption encryption_;
  bool tls_negotiated_;
  bool normal_tls_negotiated_;

  // TSK state.
  std::optional<security::SignedTokenPB> authn_token_;

  // The set of features advertised by the client. Filled in when we send
  // the first message. This is not necessarily constant since some features
  // may be dynamically enabled.
  std::set<RpcFeatureFlag> client_features_;

  // The set of features supported by the server. Filled in during negotiation.
  std::set<RpcFeatureFlag> server_features_;

  // The authentication type. Filled in during negotiation.
  AuthenticationType negotiated_authn_;

  // Negotiation timeout deadline.
  MonoTime deadline_;
};

} // namespace rpc
} // namespace kudu
