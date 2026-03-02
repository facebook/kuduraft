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

#include "kudu/rpc/client_negotiation.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <set>
#include <string>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/map-util.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/serialization.h"
#include "kudu/security/cert.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/trace.h"

using std::set;
using std::string;
using std::unique_ptr;

DECLARE_bool(rpc_encrypt_loopback_connections);

// Advertise certificate as auth mechanism even when the certificates are
// externally signed (not by internal CA)
DECLARE_bool(rpc_allow_external_cert_authentication);

namespace kudu {
namespace rpc {

// Return an appropriately-typed Status object based on an ErrorStatusPB
// returned from an Error RPC. In case there is no relevant Status type, return
// a RuntimeError.
static Status statusFromRpcError(const ErrorStatusPB& error) {
  DCHECK(error.IsInitialized()) << "Error status PB must be initialized";
  if (PREDICT_FALSE(!error.has_code())) {
    return Status::RuntimeError(error.message());
  }
  const string codeName = ErrorStatusPB::RpcErrorCodePB_Name(error.code());
  switch (error.code()) {
    case ErrorStatusPB_RpcErrorCodePB_FATAL_UNAUTHORIZED: // fall-through
    case ErrorStatusPB_RpcErrorCodePB_FATAL_INVALID_AUTHENTICATION_TOKEN:
      return Status::NotAuthorized(codeName, error.message());
    case ErrorStatusPB_RpcErrorCodePB_ERROR_UNAVAILABLE:
      return Status::ServiceUnavailable(codeName, error.message());
    default:
      return Status::RuntimeError(codeName, error.message());
  }
}

ClientNegotiation::ClientNegotiation(
    unique_ptr<Socket> socket,
    const security::TlsContext* tlsContext,
    std::optional<security::SignedTokenPB> authnToken,
    RpcEncryption encryption)
    : socket_(std::move(socket)),
      tlsContext_(tlsContext),
      encryption_(encryption),
      tlsNegotiated_(false),
      normalTlsNegotiated_(false),
      authnToken_(std::move(authnToken)),
      negotiatedAuthn_(AuthenticationType::Invalid),
      deadline_(MonoTime::Max()) {
  DCHECK(socket_);
  DCHECK(tlsContext_);
}

void ClientNegotiation::setDeadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ClientNegotiation::negotiate(unique_ptr<ErrorStatusPB>* rpcError) {
  TRACE("Beginning negotiation");

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(checkInBlockingMode(socket_.get()));

  // Perform normal TLS handshake
  RETURN_NOT_OK(handleTls());
  // Send connection context.
  RETURN_NOT_OK(sendConnectionContext());

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ClientNegotiation::handleTls() {
  if (encryption_ == RpcEncryption::DISABLED) {
    return Status::NotSupported("RPC encryption is disabled.");
  }

  if (!tlsContext_->hasSignedCert()) {
    if (FLAGS_skip_verify_tls_cert) {
      tlsHandshake_.setVerificationMode(
          security::TlsVerificationMode::VerifyNone);
    } else {
      return Status::NotSupported("A signed certificate is not available.");
    }
  }

  clientFeatures_ = kSupportedClientRpcFeatureFlags;
  clientFeatures_.insert(TLS);
  serverFeatures_ = kSupportedServerRpcFeatureFlags;
  serverFeatures_.insert(TLS);
  negotiatedAuthn_ = AuthenticationType::Certificate;

  RETURN_NOT_OK(tlsContext_->CreateSSL(&tlsHandshake_));

  RETURN_NOT_OK(tlsHandshake_.sslHandshake(&socket_, false));

  // Verify whether alpn is negotiated
  RETURN_NOT_OK(
      tlsContext_->checkAlpnSupported(tlsHandshake_.getSelectedAlpn()));

  tlsNegotiated_ = true;
  normalTlsNegotiated_ = true;

  return Status::OK();
}

Status ClientNegotiation::sendNegotiatePb(const NegotiatePB& msg) {
  RequestHeader header;
  header.set_call_id(kNegotiateCallId);

  DCHECK(socket_);
  DCHECK(msg.IsInitialized()) << "message must be initialized";
  DCHECK(msg.has_step()) << "message must have a step";

  TRACE(
      "Sending $0 NegotiatePB request",
      NegotiatePB::NegotiateStep_Name(msg.step()));
  return sendFramedMessageBlocking(socket(), header, msg, deadline_);
}

Status ClientNegotiation::recvNegotiatePb(
    NegotiatePB* msg,
    faststring* buffer,
    unique_ptr<ErrorStatusPB>* rpcError) {
  ResponseHeader header;
  Slice paramBuf;
  RETURN_NOT_OK(receiveFramedMessageBlocking(
      socket(), buffer, &header, &paramBuf, deadline_));
  if (header.is_error()) {
    return parseError(paramBuf, rpcError);
  }

  TRACE(
      "Received $0 NegotiatePB response",
      NegotiatePB::NegotiateStep_Name(msg->step()));
  return Status::OK();
}

Status ClientNegotiation::parseError(
    const Slice& errData,
    unique_ptr<ErrorStatusPB>* rpcError) {
  unique_ptr<ErrorStatusPB> error(new ErrorStatusPB);
  if (!error->ParseFromArray(errData.data(), errData.size())) {
    return Status::IOError(
        "invalid error response, missing fields",
        error->InitializationErrorString());
  }
  Status s = statusFromRpcError(*error);
  TRACE("Received error response from server: $0", s.ToString());

  if (rpcError) {
    rpcError->swap(error);
  }
  return s;
}

Status ClientNegotiation::sendConnectionHeader() {
  const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[buflen];
  serialization::SerializeConnHeader(buf);
  size_t nsent;
  return socket()->BlockingWrite(buf, buflen, &nsent, deadline_);
}

Status ClientNegotiation::sendNegotiate() {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  clientFeatures_ = kSupportedClientRpcFeatureFlags;

  if (encryption_ != RpcEncryption::DISABLED) {
    clientFeatures_.insert(TLS);
    // If the remote peer is local, then we allow using TLS for authentication
    // without encryption or integrity.
    if (socket_->IsLoopbackConnection() &&
        !FLAGS_rpc_encrypt_loopback_connections) {
      clientFeatures_.insert(TLS_AUTHENTICATION_ONLY);
    }
  }

  for (RpcFeatureFlag feature : clientFeatures_) {
    msg.add_supported_features(feature);
  }

  // We only provide authenticated TLS if the certificates are generated
  // by the internal CA.
  // However for mysql raft, in order to support pure TLS based authentication,
  // we add a backdoor to override this kudu limitation.
  if (tlsContext_->hasSignedCert() &&
      (FLAGS_rpc_allow_external_cert_authentication ||
       !tlsContext_->isExternalCert())) {
    msg.add_authn_types()->mutable_certificate();
  }
  if (authnToken_ && tlsContext_->hasTrustedCert()) {
    // TODO(KUDU-1924): check that the authn token is not expired. Can this be
    // done reliably on clients?
    msg.add_authn_types()->mutable_token();
  }

  if (PREDICT_FALSE(msg.authn_types().empty())) {
    return Status::NotAuthorized(
        "client is not configured with an authentication type");
  }

  RETURN_NOT_OK(sendNegotiatePb(msg));
  return Status::OK();
}

Status ClientNegotiation::handleNegotiate(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::NEGOTIATE)) {
    return Status::NotAuthorized(
        "expected NEGOTIATE step",
        NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received NEGOTIATE response from server");

  // Fill in the set of features supported by the server.
  for (int flag : response.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag featureFlag = RpcFeatureFlag_IsValid(flag)
        ? static_cast<RpcFeatureFlag>(flag)
        : UNKNOWN;
    if (featureFlag != UNKNOWN) {
      serverFeatures_.insert(featureFlag);
    }
  }

  if (encryption_ == RpcEncryption::REQUIRED &&
      !serverFeatures_.contains(RpcFeatureFlag::TLS)) {
    return Status::NotAuthorized(
        "server does not support required TLS encryption");
  }

  // Get the authentication type which the server would like to use.
  DCHECK_LE(response.authn_types().size(), 1);
  if (response.authn_types().empty()) {
    return Status::RuntimeError("server doesn't set authentication type");
  } else {
    const auto& authnType = response.authn_types(0);
    switch (authnType.type_case()) {
      case AuthenticationTypePB::kToken:
        // TODO(todd): we should also be checking
        // tlsContext_->has_trusted_cert() here to match the original logic we
        // used to advertise TOKEN support, or perhaps just check explicitly
        // whether we advertised TOKEN.
        if (!authnToken_) {
          return Status::RuntimeError(
              "server chose token authentication, but client has no token");
        }
        negotiatedAuthn_ = AuthenticationType::Token;
        return Status::OK();
      case AuthenticationTypePB::kCertificate:
        if (!tlsContext_->hasSignedCert()) {
          return Status::RuntimeError(
              "server chose certificate authentication, but client has no certificate");
        }
        negotiatedAuthn_ = AuthenticationType::Certificate;
        return Status::OK();
      case AuthenticationTypePB::TYPE_NOT_SET:
        return Status::RuntimeError(
            "server chose an unknown authentication type");
    }
  }
}

Status ClientNegotiation::sendTlsHandshake(string tlsToken) {
  TRACE("Sending TLS_HANDSHAKE message to server");
  NegotiatePB msg;
  msg.set_step(NegotiatePB::TLS_HANDSHAKE);
  msg.mutable_tls_handshake()->swap(tlsToken);
  return sendNegotiatePb(msg);
}

Status ClientNegotiation::handleTlsHandshake(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::TLS_HANDSHAKE)) {
    return Status::NotAuthorized(
        "expected TLS_HANDSHAKE step",
        NegotiatePB::NegotiateStep_Name(response.step()));
  }
  if (!response.tls_handshake().empty()) {
    TRACE("Received TLS_HANDSHAKE response from server");
  }

  if (PREDICT_FALSE(!response.has_tls_handshake())) {
    return Status::NotAuthorized(
        "No TLS handshake token in TLS_HANDSHAKE response from server");
  }

  string token;
  Status s = tlsHandshake_.continueHandshake(response.tls_handshake(), &token);
  if (s.IsIncomplete()) {
    // Another roundtrip is required to complete the handshake.
    RETURN_NOT_OK(sendTlsHandshake(std::move(token)));
  }

  // Check that the handshake step didn't produce an error. Will also propagate
  // an Incomplete status.
  RETURN_NOT_OK(s);

  // TLS handshake is finished.
  if (serverFeatures_.contains(TLS_AUTHENTICATION_ONLY) &&
      clientFeatures_.contains(TLS_AUTHENTICATION_ONLY)) {
    TRACE(
        "Negotiated auth-only $0 with cipher $1",
        tlsHandshake_.getProtocol(),
        tlsHandshake_.getCipherDescription());
    return tlsHandshake_.finishNoWrap(*socket_);
  }

  TRACE(
      "Negotiated $0 with cipher $1",
      tlsHandshake_.getProtocol(),
      tlsHandshake_.getCipherDescription());
  return tlsHandshake_.finish(&socket_);
}

Status ClientNegotiation::authenticateByToken(
    faststring* recvBuf,
    unique_ptr<ErrorStatusPB>* rpcError) {
  // Sanity check that TLS has been negotiated. Sending the token on an
  // unencrypted channel is a big no-no.
  CHECK(tlsNegotiated_);

  // Send the token to the server.
  NegotiatePB pb;
  pb.set_step(NegotiatePB::TOKEN_EXCHANGE);
  *pb.mutable_authn_token() = std::move(*authnToken_);
  RETURN_NOT_OK(sendNegotiatePb(pb));
  pb.Clear();

  // Check that the server responds with a non-error TOKEN_EXCHANGE message.
  RETURN_NOT_OK(recvNegotiatePb(&pb, recvBuf, rpcError));
  if (pb.step() != NegotiatePB::TOKEN_EXCHANGE) {
    return Status::NotAuthorized(
        "expected TOKEN_EXCHANGE step",
        NegotiatePB::NegotiateStep_Name(pb.step()));
  }

  return Status::OK();
}

Status ClientNegotiation::sendConnectionContext() {
  TRACE("Sending connection context");
  RequestHeader header;
  header.set_call_id(kConnectionContextCallId);

  ConnectionContextPB conn_context;
  // This field is deprecated, use a default value for backward compatibility.
  conn_context.mutable_deprecated_user_info()->set_real_user("cpp-client");

  return sendFramedMessageBlocking(socket(), header, conn_context, deadline_);
}

} // namespace rpc
} // namespace kudu
