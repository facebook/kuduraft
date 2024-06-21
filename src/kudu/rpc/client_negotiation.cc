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
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
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

using strings::Substitute;

DECLARE_bool(rpc_encrypt_loopback_connections);

// Advertise certificate as auth mechanism even when the certificates are
// externally signed (not by internal CA)
DECLARE_bool(rpc_allow_external_cert_authentication);

namespace kudu {
namespace rpc {

// Return an appropriately-typed Status object based on an ErrorStatusPB
// returned from an Error RPC. In case there is no relevant Status type, return
// a RuntimeError.
static Status StatusFromRpcError(const ErrorStatusPB& error) {
  DCHECK(error.IsInitialized()) << "Error status PB must be initialized";
  if (PREDICT_FALSE(!error.has_code())) {
    return Status::RuntimeError(error.message());
  }
  const string code_name = ErrorStatusPB::RpcErrorCodePB_Name(error.code());
  switch (error.code()) {
    case ErrorStatusPB_RpcErrorCodePB_FATAL_UNAUTHORIZED: // fall-through
    case ErrorStatusPB_RpcErrorCodePB_FATAL_INVALID_AUTHENTICATION_TOKEN:
      return Status::NotAuthorized(code_name, error.message());
    case ErrorStatusPB_RpcErrorCodePB_ERROR_UNAVAILABLE:
      return Status::ServiceUnavailable(code_name, error.message());
    default:
      return Status::RuntimeError(code_name, error.message());
  }
}

ClientNegotiation::ClientNegotiation(
    unique_ptr<Socket> socket,
    const security::TlsContext* tls_context,
    std::optional<security::SignedTokenPB> authn_token,
    RpcEncryption encryption)
    : socket_(std::move(socket)),
      tls_context_(tls_context),
      encryption_(encryption),
      tls_negotiated_(false),
      normal_tls_negotiated_(false),
      authn_token_(std::move(authn_token)),
      negotiated_authn_(AuthenticationType::INVALID),
      deadline_(MonoTime::Max()) {
  DCHECK(socket_);
  DCHECK(tls_context_);
}

void ClientNegotiation::set_deadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ClientNegotiation::Negotiate(unique_ptr<ErrorStatusPB>* rpc_error) {
  TRACE("Beginning negotiation");

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(CheckInBlockingMode(socket_.get()));

  // Perform normal TLS handshake
  RETURN_NOT_OK(HandleTLS());
  // Send connection context.
  RETURN_NOT_OK(SendConnectionContext());

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ClientNegotiation::HandleTLS() {
  if (encryption_ == RpcEncryption::DISABLED) {
    return Status::NotSupported("RPC encryption is disabled.");
  }

  if (!tls_context_->has_signed_cert()) {
    if (FLAGS_skip_verify_tls_cert) {
      tls_handshake_.set_verification_mode(
          security::TlsVerificationMode::VERIFY_NONE);
    } else {
      return Status::NotSupported("A signed certificate is not available.");
    }
  }

  client_features_ = kSupportedClientRpcFeatureFlags;
  client_features_.insert(TLS);
  server_features_ = kSupportedServerRpcFeatureFlags;
  server_features_.insert(TLS);
  negotiated_authn_ = AuthenticationType::CERTIFICATE;

  RETURN_NOT_OK(
      ((security::TlsContext*)tls_context_)->SetSupportedAlpns(kAlpns, false));

  RETURN_NOT_OK(tls_context_->CreateSSL(&tls_handshake_));

  RETURN_NOT_OK(tls_handshake_.SSLHandshake(&socket_, false));

  // Verify whether alpn is negotiated
  auto selected_alpn = tls_handshake_.GetSelectedAlpn();
  if (selected_alpn.empty()) {
    return Status::RuntimeError("ALPN not negotiated");
  }
  if (std::find(std::begin(kAlpns), std::end(kAlpns), selected_alpn) ==
      std::end(kAlpns)) {
    return Status::RuntimeError("ALPN incorrectly negotiated", selected_alpn);
  }

  tls_negotiated_ = true;
  normal_tls_negotiated_ = true;

  return Status::OK();
}

Status ClientNegotiation::SendNegotiatePB(const NegotiatePB& msg) {
  RequestHeader header;
  header.set_call_id(kNegotiateCallId);

  DCHECK(socket_);
  DCHECK(msg.IsInitialized()) << "message must be initialized";
  DCHECK(msg.has_step()) << "message must have a step";

  TRACE(
      "Sending $0 NegotiatePB request",
      NegotiatePB::NegotiateStep_Name(msg.step()));
  return SendFramedMessageBlocking(socket(), header, msg, deadline_);
}

Status ClientNegotiation::RecvNegotiatePB(
    NegotiatePB* msg,
    faststring* buffer,
    unique_ptr<ErrorStatusPB>* rpc_error) {
  ResponseHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(
      socket(), buffer, &header, &param_buf, deadline_));
  if (header.is_error()) {
    return ParseError(param_buf, rpc_error);
  }

  TRACE(
      "Received $0 NegotiatePB response",
      NegotiatePB::NegotiateStep_Name(msg->step()));
  return Status::OK();
}

Status ClientNegotiation::ParseError(
    const Slice& err_data,
    unique_ptr<ErrorStatusPB>* rpc_error) {
  unique_ptr<ErrorStatusPB> error(new ErrorStatusPB);
  if (!error->ParseFromArray(err_data.data(), err_data.size())) {
    return Status::IOError(
        "invalid error response, missing fields",
        error->InitializationErrorString());
  }
  Status s = StatusFromRpcError(*error);
  TRACE("Received error response from server: $0", s.ToString());

  if (rpc_error) {
    rpc_error->swap(error);
  }
  return s;
}

Status ClientNegotiation::SendConnectionHeader() {
  const uint8_t buflen = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[buflen];
  serialization::SerializeConnHeader(buf);
  size_t nsent;
  return socket()->BlockingWrite(buf, buflen, &nsent, deadline_);
}

Status ClientNegotiation::SendNegotiate() {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::NEGOTIATE);

  // Advertise our supported features.
  client_features_ = kSupportedClientRpcFeatureFlags;

  if (encryption_ != RpcEncryption::DISABLED) {
    client_features_.insert(TLS);
    // If the remote peer is local, then we allow using TLS for authentication
    // without encryption or integrity.
    if (socket_->IsLoopbackConnection() &&
        !FLAGS_rpc_encrypt_loopback_connections) {
      client_features_.insert(TLS_AUTHENTICATION_ONLY);
    }
  }

  for (RpcFeatureFlag feature : client_features_) {
    msg.add_supported_features(feature);
  }

  // We only provide authenticated TLS if the certificates are generated
  // by the internal CA.
  // However for mysql raft, in order to support pure TLS based authentication,
  // we add a backdoor to override this kudu limitation.
  if (tls_context_->has_signed_cert() &&
      (FLAGS_rpc_allow_external_cert_authentication ||
       !tls_context_->is_external_cert())) {
    msg.add_authn_types()->mutable_certificate();
  }
  if (authn_token_ && tls_context_->has_trusted_cert()) {
    // TODO(KUDU-1924): check that the authn token is not expired. Can this be
    // done reliably on clients?
    msg.add_authn_types()->mutable_token();
  }

  if (PREDICT_FALSE(msg.authn_types().empty())) {
    return Status::NotAuthorized(
        "client is not configured with an authentication type");
  }

  RETURN_NOT_OK(SendNegotiatePB(msg));
  return Status::OK();
}

Status ClientNegotiation::HandleNegotiate(const NegotiatePB& response) {
  if (PREDICT_FALSE(response.step() != NegotiatePB::NEGOTIATE)) {
    return Status::NotAuthorized(
        "expected NEGOTIATE step",
        NegotiatePB::NegotiateStep_Name(response.step()));
  }
  TRACE("Received NEGOTIATE response from server");

  // Fill in the set of features supported by the server.
  for (int flag : response.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag feature_flag = RpcFeatureFlag_IsValid(flag)
        ? static_cast<RpcFeatureFlag>(flag)
        : UNKNOWN;
    if (feature_flag != UNKNOWN) {
      server_features_.insert(feature_flag);
    }
  }

  if (encryption_ == RpcEncryption::REQUIRED &&
      !ContainsKey(server_features_, RpcFeatureFlag::TLS)) {
    return Status::NotAuthorized(
        "server does not support required TLS encryption");
  }

  // Get the authentication type which the server would like to use.
  DCHECK_LE(response.authn_types().size(), 1);
  if (response.authn_types().empty()) {
    return Status::RuntimeError("server doesn't set authentication type");
  } else {
    const auto& authn_type = response.authn_types(0);
    switch (authn_type.type_case()) {
      case AuthenticationTypePB::kToken:
        // TODO(todd): we should also be checking
        // tls_context_->has_trusted_cert() here to match the original logic we
        // used to advertise TOKEN support, or perhaps just check explicitly
        // whether we advertised TOKEN.
        if (!authn_token_) {
          return Status::RuntimeError(
              "server chose token authentication, but client has no token");
        }
        negotiated_authn_ = AuthenticationType::TOKEN;
        return Status::OK();
      case AuthenticationTypePB::kCertificate:
        if (!tls_context_->has_signed_cert()) {
          return Status::RuntimeError(
              "server chose certificate authentication, but client has no certificate");
        }
        negotiated_authn_ = AuthenticationType::CERTIFICATE;
        return Status::OK();
      case AuthenticationTypePB::TYPE_NOT_SET:
        return Status::RuntimeError(
            "server chose an unknown authentication type");
    }
  }
}

Status ClientNegotiation::SendTlsHandshake(string tls_token) {
  TRACE("Sending TLS_HANDSHAKE message to server");
  NegotiatePB msg;
  msg.set_step(NegotiatePB::TLS_HANDSHAKE);
  msg.mutable_tls_handshake()->swap(tls_token);
  return SendNegotiatePB(msg);
}

Status ClientNegotiation::HandleTlsHandshake(const NegotiatePB& response) {
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
  Status s = tls_handshake_.Continue(response.tls_handshake(), &token);
  if (s.IsIncomplete()) {
    // Another roundtrip is required to complete the handshake.
    RETURN_NOT_OK(SendTlsHandshake(std::move(token)));
  }

  // Check that the handshake step didn't produce an error. Will also propagate
  // an Incomplete status.
  RETURN_NOT_OK(s);

  // TLS handshake is finished.
  if (ContainsKey(server_features_, TLS_AUTHENTICATION_ONLY) &&
      ContainsKey(client_features_, TLS_AUTHENTICATION_ONLY)) {
    TRACE(
        "Negotiated auth-only $0 with cipher $1",
        tls_handshake_.GetProtocol(),
        tls_handshake_.GetCipherDescription());
    return tls_handshake_.FinishNoWrap(*socket_);
  }

  TRACE(
      "Negotiated $0 with cipher $1",
      tls_handshake_.GetProtocol(),
      tls_handshake_.GetCipherDescription());
  return tls_handshake_.Finish(&socket_);
}

Status ClientNegotiation::AuthenticateByToken(
    faststring* recv_buf,
    unique_ptr<ErrorStatusPB>* rpc_error) {
  // Sanity check that TLS has been negotiated. Sending the token on an
  // unencrypted channel is a big no-no.
  CHECK(tls_negotiated_);

  // Send the token to the server.
  NegotiatePB pb;
  pb.set_step(NegotiatePB::TOKEN_EXCHANGE);
  *pb.mutable_authn_token() = std::move(*authn_token_);
  RETURN_NOT_OK(SendNegotiatePB(pb));
  pb.Clear();

  // Check that the server responds with a non-error TOKEN_EXCHANGE message.
  RETURN_NOT_OK(RecvNegotiatePB(&pb, recv_buf, rpc_error));
  if (pb.step() != NegotiatePB::TOKEN_EXCHANGE) {
    return Status::NotAuthorized(
        "expected TOKEN_EXCHANGE step",
        NegotiatePB::NegotiateStep_Name(pb.step()));
  }

  return Status::OK();
}

Status ClientNegotiation::SendConnectionContext() {
  TRACE("Sending connection context");
  RequestHeader header;
  header.set_call_id(kConnectionContextCallId);

  ConnectionContextPB conn_context;
  // This field is deprecated, use a default value for backward compatibility.
  conn_context.mutable_deprecated_user_info()->set_real_user("cpp-client");

  return SendFramedMessageBlocking(socket(), header, conn_context, deadline_);
}

} // namespace rpc
} // namespace kudu
