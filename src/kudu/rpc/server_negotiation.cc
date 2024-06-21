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

#include "kudu/rpc/server_negotiation.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <optional>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/blocking_ops.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/serialization.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/init.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/trace.h"

using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

// Fault injection flags.
DEFINE_double(
    rpc_inject_invalid_authn_token_ratio,
    0,
    "If set higher than 0, AuthenticateByToken() randomly injects "
    "errors replying with FATAL_INVALID_AUTHENTICATION_TOKEN code. "
    "The flag's value corresponds to the probability of the fault "
    "injection event. Used for only for tests.");
TAG_FLAG(rpc_inject_invalid_authn_token_ratio, runtime);
TAG_FLAG(rpc_inject_invalid_authn_token_ratio, unsafe);

DECLARE_bool(rpc_encrypt_loopback_connections);

// Allow using certificates signed by external CAs for full authentication.
DECLARE_bool(rpc_allow_external_cert_authentication);

// Trusting everything for now.
DEFINE_string(
    trusted_subnets,
    "::/0",
    "A trusted subnet whitelist. If set explicitly, all unauthenticated "
    "or unencrypted connections are prohibited except the ones from the "
    "specified address blocks. Otherwise, private network (::1/128, etc.) "
    "and local subnets of all local network interfaces will be used. Set it "
    "to '::/0' to allow unauthenticated/unencrypted connections from all "
    "remote IP addresses. However, if network access is not otherwise restricted "
    "by a firewall, malicious users may be able to gain unauthorized access.");
TAG_FLAG(trusted_subnets, advanced);
TAG_FLAG(trusted_subnets, evolving);

DEFINE_string(
    trusted_CNs,
    "",
    "A allow list of Common Names in Subject line. Goes hand in hand with "
    "authenticate_via_CN flag. The allowed CN's should be comma separated.");
TAG_FLAG(trusted_CNs, advanced);
TAG_FLAG(trusted_CNs, evolving);

DEFINE_bool(
    authenticate_via_CN,
    false,
    "Whether the server should use the CN"
    " field in the Subject to validate the client or should it look "
    "for the userId and kerberos principal fields. Setting it to true"
    " will validate CN, false will validate userId.");
TAG_FLAG(authenticate_via_CN, advanced);
TAG_FLAG(authenticate_via_CN, evolving);

static bool ValidateTrustedSubnets(
    const char* /*flagname*/,
    const string& value) {
  if (value.empty()) {
    return true;
  }

  for (const auto& t : strings::Split(value, ",", strings::SkipEmpty())) {
    kudu::Network network;
    kudu::Status s = network.ParseCIDRString(t.ToString());
    if (!s.ok()) {
      LOG(ERROR) << "Invalid subnet address: " << t
                 << ". Subnet must be specified in CIDR notation.";
      return false;
    }
  }

  return true;
}

DEFINE_validator(trusted_subnets, &ValidateTrustedSubnets);

namespace kudu {
namespace rpc {

namespace {
vector<Network>* g_trusted_subnets = nullptr;

bool ValidateTrustedCN(const std::string& value_list, const std::string& CN) {
  std::vector<string> result =
      strings::Split(value_list, ",", strings::SkipEmpty());
  auto itr = std::find(result.begin(), result.end(), CN);
  return (itr != result.end());
}

} // anonymous namespace

ServerNegotiation::ServerNegotiation(
    unique_ptr<Socket> socket,
    const security::TlsContext* tls_context,
    const security::TokenVerifier* token_verifier,
    RpcEncryption encryption)
    : socket_(std::move(socket)),
      tls_context_(tls_context),
      encryption_(encryption),
      tls_negotiated_(false),
      normal_tls_negotiated_(false),
      token_verifier_(token_verifier),
      negotiated_authn_(AuthenticationType::INVALID),
      deadline_(MonoTime::Max()) {}

void ServerNegotiation::set_deadline(const MonoTime& deadline) {
  deadline_ = deadline;
}

Status ServerNegotiation::Negotiate() {
  TRACE("Beginning negotiation");

  // Wait until starting negotiation to check that the socket, tls_context, and
  // token_verifier are not null, since they do not need to be set for
  // PreflightCheckGSSAPI.
  DCHECK(socket_);
  DCHECK(tls_context_);
  DCHECK(token_verifier_);

  // Ensure we can use blocking calls on the socket during negotiation.
  RETURN_NOT_OK(CheckInBlockingMode(socket_.get()));

  faststring recv_buf;

  // Step 0: Detect TLS client hello packet and perform normal TLS handshake
  if (LooksLikeTLS()) {
    RETURN_NOT_OK(HandleTLS());
    RETURN_NOT_OK(AuthenticateByCertificate(
        FLAGS_authenticate_via_CN
            ? CertValidationCheck::CERT_VALIDATION_COMMON_NAME
            : CertValidationCheck::CERT_VALIDATION_USERID));
    // Receive connection context.
    RETURN_NOT_OK(RecvConnectionContext(&recv_buf));

    TRACE("Negotiation successful");
    return Status::OK();
  }

  // Step 1: Read the connection header.
  RETURN_NOT_OK(ValidateConnectionHeader(&recv_buf));

  { // Step 2: Receive and respond to the NEGOTIATE step message.
    NegotiatePB request;
    RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
    RETURN_NOT_OK(HandleNegotiate(request));
    TRACE("Negotiated authn=$0", AuthenticationTypeToString(negotiated_authn_));
  }

  // Step 3: if both ends support TLS, do a TLS handshake.
  if (encryption_ != RpcEncryption::DISABLED && tls_context_->has_cert() &&
      ContainsKey(client_features_, TLS)) {
    RETURN_NOT_OK(tls_context_->InitiateHandshake(
        security::TlsHandshakeType::SERVER, &tls_handshake_));

    if (negotiated_authn_ != AuthenticationType::CERTIFICATE) {
      // The server does not need to verify the client's certificate unless it's
      // being used for authentication.
      tls_handshake_.set_verification_mode(
          security::TlsVerificationMode::VERIFY_NONE);
    }

    while (true) {
      NegotiatePB request;
      RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));
      Status s = HandleTlsHandshake(request);
      if (s.ok())
        break;
      if (!s.IsIncomplete())
        return s;
    }
    tls_negotiated_ = true;
  }

  // Rejects any connection from public routable IPs if encryption
  // is disabled. See KUDU-1875.
  if (!tls_negotiated_) {
    Sockaddr addr;
    RETURN_NOT_OK(socket_->GetPeerAddress(&addr));

    if (!IsTrustedConnection(addr)) {
      // Receives client response before sending error
      // message, even though the response is never used,
      // to avoid risk condition that connection gets
      // closed before client receives server's error
      // message.
      NegotiatePB request;
      RETURN_NOT_OK(RecvNegotiatePB(&request, &recv_buf));

      Status s = Status::NotAuthorized(
          "unencrypted connections from publicly routable "
          "IPs are prohibited. See --trusted_subnets flag "
          "for more information.",
          addr.ToString());
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  // Step 4: Authentication
  switch (negotiated_authn_) {
    case AuthenticationType::TOKEN:
      RETURN_NOT_OK(AuthenticateByToken(&recv_buf));
      break;
    case AuthenticationType::CERTIFICATE:
      RETURN_NOT_OK(AuthenticateByCertificate(
          FLAGS_authenticate_via_CN
              ? CertValidationCheck::CERT_VALIDATION_COMMON_NAME
              : CertValidationCheck::CERT_VALIDATION_USERID));
      break;
    case AuthenticationType::INVALID:
      LOG(FATAL) << "unreachable";
  }

  // Step 5: Receive connection context.
  RETURN_NOT_OK(RecvConnectionContext(&recv_buf));

  TRACE("Negotiation successful");
  return Status::OK();
}

Status ServerNegotiation::HandleTLS() {
  if (encryption_ == RpcEncryption::DISABLED) {
    return Status::NotSupported("RPC encryption is disabled.");
  }

  if (!tls_context_->has_signed_cert()) {
    if (FLAGS_skip_verify_tls_cert) {
      // As the server we still need the client cert to find the user. Using
      // VERIFY_NONE skips requesting the client cert.
      tls_handshake_.set_verification_mode(
          security::TlsVerificationMode::VERIFY_CERT_PRESENT_ONLY);
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
      ((security::TlsContext*)tls_context_)->SetSupportedAlpns(kAlpns, true));

  RETURN_NOT_OK(tls_context_->CreateSSL(&tls_handshake_));

  RETURN_NOT_OK(tls_handshake_.SSLHandshake(&socket_, true));

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

Status ServerNegotiation::RecvNegotiatePB(
    NegotiatePB* msg,
    faststring* recv_buf) {
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(
      socket(), recv_buf, &header, &param_buf, deadline_));
  TRACE(
      "Received $0 NegotiatePB request",
      NegotiatePB::NegotiateStep_Name(msg->step()));
  return Status::OK();
}

Status ServerNegotiation::SendNegotiatePB(const NegotiatePB& msg) {
  ResponseHeader header;
  header.set_call_id(kNegotiateCallId);

  DCHECK(socket_);
  DCHECK(msg.IsInitialized()) << "message must be initialized";
  DCHECK(msg.has_step()) << "message must have a step";

  TRACE(
      "Sending $0 NegotiatePB response",
      NegotiatePB::NegotiateStep_Name(msg.step()));
  return SendFramedMessageBlocking(socket(), header, msg, deadline_);
}

Status ServerNegotiation::SendError(
    ErrorStatusPB::RpcErrorCodePB code,
    const Status& err) {
  DCHECK(!err.ok());

  // Create header with negotiation-specific callId
  ResponseHeader header;
  header.set_call_id(kNegotiateCallId);
  header.set_is_error(true);

  // Get RPC error code from Status object
  ErrorStatusPB msg;
  msg.set_code(code);
  msg.set_message(err.ToString());

  TRACE(
      "Sending RPC error: $0: $1",
      ErrorStatusPB::RpcErrorCodePB_Name(code),
      err.ToString());
  RETURN_NOT_OK(SendFramedMessageBlocking(socket(), header, msg, deadline_));

  return Status::OK();
}

bool ServerNegotiation::LooksLikeTLS() {
  faststring recv_buf;
  size_t num_read = 0;
  recv_buf.resize(kTLSPeekCount);
  uint8_t* bytes = recv_buf.data();
  auto ret = socket_->Peek(bytes, kTLSPeekCount, &num_read, deadline_);
  if (!ret.ok()) {
    return false;
  }
  DCHECK_EQ(kTLSPeekCount, num_read);
  // TLS starts with
  // 0: 0x16 - handshake magic
  // 1: 0x03 - SSL major version
  // 2: 0x00 to 0x03 - minor version
  // 3-4: Length
  // 5: 0x01 - Handshake type (Client Hello)
  if (bytes[0] != 0x16 || bytes[1] != 0x03 || bytes[5] != 0x01) {
    return false;
  }
  return true;
}

Status ServerNegotiation::ValidateConnectionHeader(faststring* recv_buf) {
  TRACE("Waiting for connection header");
  size_t num_read;
  const size_t conn_header_len = kMagicNumberLength + kHeaderFlagsLength;
  recv_buf->resize(conn_header_len);
  RETURN_NOT_OK(socket_->BlockingRecv(
      recv_buf->data(), conn_header_len, &num_read, deadline_));
  DCHECK_EQ(conn_header_len, num_read);

  RETURN_NOT_OK(serialization::ValidateConnHeader(*recv_buf));
  TRACE("Connection header received");
  return Status::OK();
}

Status ServerNegotiation::HandleNegotiate(const NegotiatePB& request) {
  if (request.step() != NegotiatePB::NEGOTIATE) {
    Status s = Status::NotAuthorized(
        "expected NEGOTIATE step",
        NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  TRACE("Received NEGOTIATE request from client");

  // Fill in the set of features supported by the client.
  for (int flag : request.supported_features()) {
    // We only add the features that our local build knows about.
    RpcFeatureFlag feature_flag = RpcFeatureFlag_IsValid(flag)
        ? static_cast<RpcFeatureFlag>(flag)
        : UNKNOWN;
    if (feature_flag != UNKNOWN) {
      client_features_.insert(feature_flag);
    }
  }

  if (encryption_ == RpcEncryption::REQUIRED &&
      !ContainsKey(client_features_, RpcFeatureFlag::TLS)) {
    Status s = Status::NotAuthorized(
        "client does not support required TLS encryption");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  // Find the set of mutually supported authentication types.
  set<AuthenticationType> authn_types;
  if (request.authn_types().empty()) {
    authn_types.insert(AuthenticationType::CERTIFICATE);
  } else {
    for (const auto& type : request.authn_types()) {
      switch (type.type_case()) {
        case AuthenticationTypePB::kToken:
          authn_types.insert(AuthenticationType::TOKEN);
          break;
        case AuthenticationTypePB::kCertificate:
          // We only provide authenticated TLS if the certificates are generated
          // by the internal CA.
          // However for MySQL Raft we provide a backdoor to bypass this kudu
          // restriction FLAGS_rpc_allow_external_cert_authentication = true,
          // bypasses this limitation
          if (FLAGS_rpc_allow_external_cert_authentication ||
              !tls_context_->is_external_cert()) {
            authn_types.insert(AuthenticationType::CERTIFICATE);
          }
          break;
        case AuthenticationTypePB::TYPE_NOT_SET: {
          Sockaddr addr;
          RETURN_NOT_OK(socket_->GetPeerAddress(&addr));
          KLOG_EVERY_N_SECS(WARNING, 60)
              << "client supports unknown authentication type, consider updating server, address [EVERY 60 seconds]: "
              << addr.ToString();
          break;
        }
      }
    }

    if (authn_types.empty()) {
      Status s =
          Status::NotSupported("no mutually supported authentication types");
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  if (encryption_ != RpcEncryption::DISABLED &&
      ContainsKey(authn_types, AuthenticationType::CERTIFICATE) &&
      tls_context_->has_signed_cert()) {
    // If the client supports it and we are locally configured with TLS and have
    // a CA-signed cert, choose cert authn.
    // TODO(KUDU-1924): consider adding the fingerprint of the CA cert which
    // signed the client's cert to the authentication message.
    negotiated_authn_ = AuthenticationType::CERTIFICATE;
  } else if (
      ContainsKey(authn_types, AuthenticationType::TOKEN) &&
      token_verifier_->GetMaxKnownKeySequenceNumber() >= 0 &&
      encryption_ != RpcEncryption::DISABLED &&
      tls_context_->has_signed_cert()) {
    // If the client supports it, we have a TSK to verify the client's token,
    // and we have a signed-cert so the client can verify us, choose token
    // authn.
    // TODO(KUDU-1924): consider adding the TSK sequence number to the
    // authentication message.
    negotiated_authn_ = AuthenticationType::TOKEN;
  } else {
    negotiated_authn_ = AuthenticationType::CERTIFICATE;
  }

  // Fill in the NEGOTIATE step response for the client.
  NegotiatePB response;
  response.set_step(NegotiatePB::NEGOTIATE);

  // Tell the client which features we support.
  server_features_ = kSupportedServerRpcFeatureFlags;
  if (tls_context_->has_cert() && encryption_ != RpcEncryption::DISABLED) {
    server_features_.insert(TLS);
    // If the remote peer is local, then we allow using TLS for authentication
    // without encryption or integrity.
    if (socket_->IsLoopbackConnection() &&
        !FLAGS_rpc_encrypt_loopback_connections) {
      server_features_.insert(TLS_AUTHENTICATION_ONLY);
    }
  }

  for (RpcFeatureFlag feature : server_features_) {
    response.add_supported_features(feature);
  }

  switch (negotiated_authn_) {
    case AuthenticationType::CERTIFICATE:
      response.add_authn_types()->mutable_certificate();
      break;
    case AuthenticationType::TOKEN:
      response.add_authn_types()->mutable_token();
      break;
    case AuthenticationType::INVALID:
      LOG(FATAL) << "unreachable";
  }

  return SendNegotiatePB(response);
}

Status ServerNegotiation::HandleTlsHandshake(const NegotiatePB& request) {
  if (PREDICT_FALSE(request.step() != NegotiatePB::TLS_HANDSHAKE)) {
    Status s = Status::NotAuthorized(
        "expected TLS_HANDSHAKE step",
        NegotiatePB::NegotiateStep_Name(request.step()));
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  if (PREDICT_FALSE(!request.has_tls_handshake())) {
    Status s = Status::NotAuthorized(
        "No TLS handshake token in TLS_HANDSHAKE request from client");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  string token;
  Status s = tls_handshake_.Continue(request.tls_handshake(), &token);

  if (PREDICT_FALSE(!s.IsIncomplete() && !s.ok())) {
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }

  // Regardless of whether this is the final handshake roundtrip (in which case
  // Continue would have returned OK), we still need to return a response.
  RETURN_NOT_OK(SendTlsHandshake(std::move(token)));
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

Status ServerNegotiation::SendTlsHandshake(string tls_token) {
  NegotiatePB msg;
  msg.set_step(NegotiatePB::TLS_HANDSHAKE);
  msg.mutable_tls_handshake()->swap(tls_token);
  return SendNegotiatePB(msg);
}

Status ServerNegotiation::AuthenticateByToken(faststring* recv_buf) {
  // Sanity check that TLS has been negotiated. Receiving the token on an
  // unencrypted channel is a big no-no.
  CHECK(tls_negotiated_);

  // Receive the token from the client.
  NegotiatePB pb;
  RETURN_NOT_OK(RecvNegotiatePB(&pb, recv_buf));

  if (pb.step() != NegotiatePB::TOKEN_EXCHANGE) {
    Status s = Status::NotAuthorized(
        "expected TOKEN_EXCHANGE step",
        NegotiatePB::NegotiateStep_Name(pb.step()));
  }
  if (!pb.has_authn_token()) {
    Status s = Status::NotAuthorized(
        "TOKEN_EXCHANGE message must include an authentication token");
  }

  // TODO(KUDU-1924): propagate the specific token verification failure back to
  // the client, so it knows how to intelligently retry.
  security::TokenPB token;
  auto verification_result =
      token_verifier_->VerifyTokenSignature(pb.authn_token(), &token);
  switch (verification_result) {
    case security::VerificationResult::VALID:
      break;

    case security::VerificationResult::INVALID_TOKEN:
    case security::VerificationResult::INVALID_SIGNATURE:
    case security::VerificationResult::EXPIRED_TOKEN:
    case security::VerificationResult::EXPIRED_SIGNING_KEY: {
      // These errors indicate the client should get a new token and try again.
      Status s = Status::NotAuthorized(
          VerificationResultToString(verification_result));
      RETURN_NOT_OK(
          SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }

    case security::VerificationResult::UNKNOWN_SIGNING_KEY: {
      // The server doesn't recognize the signing key. This indicates that the
      // server has not been updated with the most recent TSKs, so tell the
      // client to try again later.
      Status s = Status::NotAuthorized(
          VerificationResultToString(verification_result));
      RETURN_NOT_OK(SendError(ErrorStatusPB::ERROR_UNAVAILABLE, s));
      return s;
    }
    case security::VerificationResult::INCOMPATIBLE_FEATURE: {
      Status s = Status::NotAuthorized(
          VerificationResultToString(verification_result));
      // These error types aren't recoverable by having the client get a new
      // token.
      RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
      return s;
    }
  }

  if (!token.has_authn()) {
    Status s = Status::NotAuthorized(
        "non-authentication token presented for authentication");
    RETURN_NOT_OK(SendError(ErrorStatusPB::FATAL_UNAUTHORIZED, s));
    return s;
  }
  if (!token.authn().has_username()) {
    // This is a runtime error because there should be no way a client could
    // get a signed authn token without a subject.
    Status s = Status::RuntimeError("authentication token has no username");
    RETURN_NOT_OK(
        SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
    return s;
  }

  if (PREDICT_FALSE(FLAGS_rpc_inject_invalid_authn_token_ratio > 0)) {
    security::VerificationResult res;
    int sel = rand() % 4;
    switch (sel) {
      case 0:
        res = security::VerificationResult::INVALID_TOKEN;
        break;
      case 1:
        res = security::VerificationResult::INVALID_SIGNATURE;
        break;
      case 2:
        res = security::VerificationResult::EXPIRED_TOKEN;
        break;
      case 3:
        res = security::VerificationResult::EXPIRED_SIGNING_KEY;
        break;
    }
    if (kudu::fault_injection::MaybeTrue(
            FLAGS_rpc_inject_invalid_authn_token_ratio)) {
      Status s = Status::NotAuthorized(VerificationResultToString(res));
      RETURN_NOT_OK(
          SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }
  }

  authenticated_user_.SetAuthenticatedByToken(token.authn().username());

  // Respond with success message.
  pb.Clear();
  pb.set_step(NegotiatePB::TOKEN_EXCHANGE);
  return SendNegotiatePB(pb);
}

Status ServerNegotiation::AuthenticateByCertificate(CertValidationCheck mode) {
  // Sanity check that TLS has been negotiated. Cert-based authentication is
  // only possible with TLS.
  CHECK(tls_negotiated_);

  // Grab the subject from the client's cert.
  security::Cert cert;
  RETURN_NOT_OK(tls_handshake_.GetRemoteCert(&cert));

  if (mode == CertValidationCheck::CERT_VALIDATION_USERID) {
    std::optional<string> user_id = cert.UserId();
    std::optional<string> principal = cert.KuduKerberosPrincipal();
    if (!user_id) {
      Status s = Status::NotAuthorized(
          "did not find expected X509 userId extension in cert");
      RETURN_NOT_OK(
          SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }

    TRACE("Authenticated by Certificate User Id: $0", *user_id);
    authenticated_user_.SetAuthenticatedByClientCert(
        *user_id, std::move(principal));
  } else if (mode == CertValidationCheck::CERT_VALIDATION_COMMON_NAME) {
    // This mode is what is used in production of MySQL Raft
    std::optional<string> common_name = cert.CommonName();
    if (!common_name) {
      Status s =
          Status::NotAuthorized("did not find expected X509 CN in subject");
      RETURN_NOT_OK(
          SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }

    if (!ValidateTrustedCN(FLAGS_trusted_CNs, *common_name)) {
      Status s = Status::NotAuthorized(
          "did not find expected X509 CN in subject of certificate");
      RETURN_NOT_OK(
          SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
      return s;
    }

    TRACE("Authenticated by Certificate Common Name: $0", *common_name);
    authenticated_user_.SetAuthenticatedByClientCert(*common_name, {});
  } else {
    Status s = Status::NotAuthorized("Invalid mode for X509 cert validation");
    RETURN_NOT_OK(
        SendError(ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN, s));
    return s;
  }

  return Status::OK();
}

Status ServerNegotiation::RecvConnectionContext(faststring* recv_buf) {
  TRACE("Waiting for connection context");
  RequestHeader header;
  Slice param_buf;
  RETURN_NOT_OK(ReceiveFramedMessageBlocking(
      socket(), recv_buf, &header, &param_buf, deadline_));
  DCHECK(header.IsInitialized());

  if (header.call_id() != kConnectionContextCallId) {
    return Status::NotAuthorized(
        "expected ConnectionContext callid, received",
        std::to_string(header.call_id()));
  }

  ConnectionContextPB conn_context;
  if (!conn_context.ParseFromArray(param_buf.data(), param_buf.size())) {
    return Status::NotAuthorized(
        "invalid ConnectionContextPB message, missing fields",
        conn_context.InitializationErrorString());
  }

  return Status::OK();
}

bool ServerNegotiation::IsTrustedConnection(const Sockaddr& addr) {
  static std::once_flag once;
  std::call_once(once, [] {
    g_trusted_subnets = new vector<Network>();
    CHECK_OK(
        Network::ParseCIDRStrings(FLAGS_trusted_subnets, g_trusted_subnets));

    // If --trusted_subnets is not set explicitly, local subnets of all local
    // network interfaces as well as the default private subnets will be used.
    if (gflags::GetCommandLineFlagInfoOrDie("trusted_subnets").is_default) {
      std::vector<Network> local_networks;
      WARN_NOT_OK(
          GetLocalNetworks(&local_networks), "Unable to get local networks.");

      g_trusted_subnets->insert(
          g_trusted_subnets->end(),
          local_networks.begin(),
          local_networks.end());
    }
  });

  return std::any_of(
      g_trusted_subnets->begin(),
      g_trusted_subnets->end(),
      [&](const Network& t) { return t.WithinNetwork(addr); });
}

} // namespace rpc
} // namespace kudu
