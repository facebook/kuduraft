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

#include "kudu/rpc/negotiation.h"

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <ctime>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

DEFINE_bool(
    rpc_trace_negotiation,
    false,
    "If enabled, dump traces of all RPC negotiations to the log");
TAG_FLAG(rpc_trace_negotiation, runtime);
TAG_FLAG(rpc_trace_negotiation, advanced);
TAG_FLAG(rpc_trace_negotiation, experimental);

DEFINE_int32(
    rpc_negotiation_inject_delay_ms,
    0,
    "If enabled, injects the given number of milliseconds delay into "
    "the RPC negotiation process on the server side.");
TAG_FLAG(rpc_negotiation_inject_delay_ms, unsafe);

DEFINE_bool(
    rpc_encrypt_loopback_connections,
    false,
    "Whether to encrypt data transfer on RPC connections that stay within "
    "a single host. Encryption here is likely to offer no additional "
    "security benefit since only a local 'root' user could intercept the "
    "traffic, and wire encryption does not suitably protect against such "
    "an attacker.");
TAG_FLAG(rpc_encrypt_loopback_connections, advanced);

DEFINE_bool(
    rpc_allow_external_cert_authentication,
    false,
    "Allow the use of externally signed certificates for TLS authentication"
    " bypassing the kudu restriction to do this only with internal CA");
TAG_FLAG(rpc_allow_external_cert_authentication, advanced);

DEFINE_bool(
    rpc_load_cert_files_each_negotiation,
    false,
    "reload the cert files in the SSL context ech time to make sure they have not expired");
TAG_FLAG(rpc_load_cert_files_each_negotiation, advanced);

DEFINE_string(
    rpc_certificate_file,
    "",
    "Path to a PEM encoded X509 certificate to use for securing RPC "
    "connections with SSL/TLS. If set, '--rpc_private_key_file' and "
    "'--rpc_ca_certificate_file' must be set as well.");
DEFINE_string(
    rpc_private_key_file,
    "",
    "Path to a PEM encoded private key paired with the certificate "
    "from '--rpc_certificate_file'");
DEFINE_string(
    rpc_ca_certificate_file,
    "",
    "Path to the PEM encoded X509 certificate of the trusted external "
    "certificate authority. The provided certificate should be the root "
    "issuer of the certificate passed in '--rpc_certificate_file'.");
DEFINE_string(
    rpc_private_key_password_cmd,
    "",
    "A Unix command whose output "
    "returns the password used to decrypt the RPC server's private key "
    "file specified in --rpc_private_key_file. If the .PEM key file is "
    "not password-protected, this flag does not need to be set. "
    "Trailing whitespace will be trimmed before it is used to decrypt "
    "the private key.");
DEFINE_bool(
    skip_verify_tls_cert,
    false,
    "Skip verifying the TLS certificates during a normal TLS handshake, "
    "allowing for the use of self-signed certificates. This should only be used "
    "for testing environments.");

// Setting TLS certs and keys via CLI flags is only necessary for external
// PKI-based security, which is not yet production ready. Instead, see
// internal PKI (ipki) and Kerberos-based authentication.
TAG_FLAG(rpc_certificate_file, experimental);
TAG_FLAG(rpc_private_key_file, experimental);
TAG_FLAG(rpc_ca_certificate_file, experimental);

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

const char* AuthenticationTypeToString(AuthenticationType t) {
  switch (t) {
    case AuthenticationType::INVALID:
      return "INVALID";
      break;
    case AuthenticationType::SASL:
      return "SASL";
      break;
    case AuthenticationType::TOKEN:
      return "TOKEN";
      break;
    case AuthenticationType::CERTIFICATE:
      return "CERTIFICATE";
      break;
  }
  return "<cannot reach here>";
}

std::ostream& operator<<(
    std::ostream& o,
    AuthenticationType authentication_type) {
  return o << AuthenticationTypeToString(authentication_type);
}

// Wait for the client connection to be established and become ready for
// writing.
static Status WaitForClientConnect(Socket* socket, const MonoTime& deadline) {
  TRACE("Waiting for socket to connect");
  int fd = socket->GetFd();
  struct pollfd poll_fd;
  poll_fd.fd = fd;
  poll_fd.events = POLLOUT;
  poll_fd.revents = 0;

  MonoTime now;
  MonoDelta remaining;
  while (true) {
    now = MonoTime::Now();
    remaining = deadline - now;
    DVLOG(4)
        << "Client waiting to connect for negotiation, time remaining until timeout deadline: "
        << remaining.ToString();
    if (PREDICT_FALSE(remaining.ToNanoseconds() <= 0)) {
      return Status::TimedOut("Timeout exceeded waiting to connect");
    }
#if defined(__linux__)
    struct timespec ts;
    remaining.ToTimeSpec(&ts);
    int ready = ppoll(&poll_fd, 1, &ts, NULL);
#else
    int ready = poll(&poll_fd, 1, remaining.ToMilliseconds());
#endif
    if (ready == -1) {
      int err = errno;
      if (err == EINTR) {
        // We were interrupted by a signal, let's go again.
        continue;
      } else {
        return Status::NetworkError(
            "Error from ppoll() while waiting to connect",
            ErrnoToString(err),
            err);
      }
    } else if (ready == 0) {
      // Timeout exceeded. Loop back to the top to our impending doom.
      continue;
    } else {
      // Success.
      break;
    }
  }

  // Connect finished, but this doesn't mean that we connected successfully.
  // Check the socket for an error.
  int so_error = 0;
  socklen_t socklen = sizeof(so_error);
  int rc = getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &socklen);
  if (rc != 0) {
    return Status::NetworkError(
        "Unable to check connected socket for errors",
        ErrnoToString(errno),
        errno);
  }
  if (so_error != 0) {
    return Status::NetworkError("connect", ErrnoToString(so_error), so_error);
  }

  return Status::OK();
}

// Disable / reset socket timeouts.
static Status DisableSocketTimeouts(Socket* socket) {
  RETURN_NOT_OK(socket->SetSendTimeout(MonoDelta::FromNanoseconds(0L)));
  RETURN_NOT_OK(socket->SetRecvTimeout(MonoDelta::FromNanoseconds(0L)));
  return Status::OK();
}

// Perform client negotiation. We don't LOG() anything, we leave that to our
// caller.
static Status DoClientNegotiation(
    Connection* conn,
    RpcAuthentication authentication,
    RpcEncryption encryption,
    MonoTime deadline,
    unique_ptr<ErrorStatusPB>* rpc_error) {
  const auto* messenger = conn->reactor_thread()->reactor()->messenger();
  // Prefer secondary credentials (such as authn token) if permitted by policy.
  const auto authn_token =
      (conn->credentials_policy() == CredentialsPolicy::PRIMARY_CREDENTIALS)
      ? std::nullopt
      : messenger->authn_token();
  ClientNegotiation client_negotiation(
      conn->release_socket(),
      &messenger->tls_context(),
      authn_token,
      encryption,
      messenger->sasl_proto_name());

  client_negotiation.set_server_fqdn(conn->outbound_connection_id().hostname());

  if (authentication != RpcAuthentication::DISABLED) {
    Status s = client_negotiation.EnableGSSAPI();
    if (!s.ok()) {
      // If we can't enable GSSAPI, it's likely the client is just missing the
      // appropriate SASL plugin. We don't want to require it to be installed
      // if the user doesn't care about connecting to servers using Kerberos
      // authentication. So, we'll just VLOG this here. If we try to connect
      // to a server which requires Kerberos, we'll get a negotiation error
      // at that point.
      if (VLOG_IS_ON(1)) {
        KLOG_FIRST_N(INFO, 1)
            << "Couldn't enable GSSAPI (Kerberos) SASL plugin: "
            << s.message().ToString()
            << ". This process will be unable to connect to "
            << "servers requiring Kerberos authentication.";
      }

      if (authentication == RpcAuthentication::REQUIRED && !authn_token &&
          !messenger->tls_context().has_signed_cert()) {
        return Status::InvalidArgument(
            "Kerberos, token, or PKI certificate credentials must be provided in order to "
            "require authentication for a client");
      }
    }
  }

  client_negotiation.set_deadline(deadline);

  RETURN_NOT_OK(WaitForClientConnect(client_negotiation.socket(), deadline));
  RETURN_NOT_OK(client_negotiation.socket()->SetNonBlocking(false));
  RETURN_NOT_OK(client_negotiation.Negotiate(rpc_error));
  RETURN_NOT_OK(DisableSocketTimeouts(client_negotiation.socket()));

  // increment normal tls counter
  if (client_negotiation.normal_tls_negotiated()) {
    conn->reactor_thread()->IncrementNormalTLSConnections(false);
  }

  // Transfer the negotiated socket and state back to the connection.
  conn->adopt_socket(client_negotiation.release_socket());
  conn->set_remote_features(client_negotiation.take_server_features());
  conn->set_confidential(
      client_negotiation.tls_negotiated() ||
      (conn->socket()->IsLoopbackConnection() &&
       !FLAGS_rpc_encrypt_loopback_connections));

  // Sanity check: if no authn token was supplied as user credentials,
  // the negotiated authentication type cannot be AuthenticationType::TOKEN.
  DCHECK(
      authn_token.has_value() ||
      client_negotiation.negotiated_authn() != AuthenticationType::TOKEN);

  return Status::OK();
}

// Perform server negotiation. We don't LOG() anything, we leave that to our
// caller.
static Status DoServerNegotiation(
    Connection* conn,
    RpcAuthentication authentication,
    RpcEncryption encryption,
    const MonoTime& deadline) {
  const auto* messenger = conn->reactor_thread()->reactor()->messenger();
  if (authentication == RpcAuthentication::REQUIRED &&
      !messenger->tls_context().is_external_cert()) {
    return Status::InvalidArgument(
        "RPC authentication (--rpc_authentication) may not be "
        "required unless external PKI "
        "(--rpc_certificate_file et al) are configured");
  }

  if (FLAGS_rpc_negotiation_inject_delay_ms > 0) {
    LOG(WARNING) << "Injecting " << FLAGS_rpc_negotiation_inject_delay_ms
                 << "ms delay in negotiation";
    SleepFor(
        MonoDelta::FromMilliseconds(FLAGS_rpc_negotiation_inject_delay_ms));
  }

  // Create a new ServerNegotiation to handle the synchronous negotiation.
  ServerNegotiation server_negotiation(
      conn->release_socket(),
      &messenger->tls_context(),
      &messenger->token_verifier(),
      encryption,
      messenger->sasl_proto_name());

  server_negotiation.set_deadline(deadline);

  RETURN_NOT_OK(server_negotiation.socket()->SetNonBlocking(false));

  RETURN_NOT_OK(server_negotiation.Negotiate());
  RETURN_NOT_OK(DisableSocketTimeouts(server_negotiation.socket()));

  // increment normal tls counter
  if (server_negotiation.normal_tls_negotiated()) {
    conn->reactor_thread()->IncrementNormalTLSConnections(true);
  }

  // Transfer the negotiated socket and state back to the connection.
  conn->adopt_socket(server_negotiation.release_socket());
  conn->set_remote_features(server_negotiation.take_client_features());
  conn->set_remote_user(server_negotiation.take_authenticated_user());
  conn->set_confidential(
      server_negotiation.tls_negotiated() ||
      (conn->socket()->IsLoopbackConnection() &&
       !FLAGS_rpc_encrypt_loopback_connections));

  return Status::OK();
}

void Negotiation::RunNegotiation(
    const scoped_refptr<Connection>& conn,
    RpcAuthentication authentication,
    RpcEncryption encryption,
    MonoTime deadline) {
  Messenger* messenger = conn->reactor_thread()->reactor()->messenger();
  // In case certificate files should be rechecked each negotiation
  if (FLAGS_rpc_load_cert_files_each_negotiation &&
      // All 3 certificate files are properly populated
      !FLAGS_rpc_certificate_file.empty() &&
      !FLAGS_rpc_private_key_file.empty() &&
      !FLAGS_rpc_ca_certificate_file.empty()) {
    auto* tls_context = messenger->mutable_tls_context();
    Status reload_status = tls_context->LoadCertFiles(
        FLAGS_rpc_ca_certificate_file,
        FLAGS_rpc_certificate_file,
        FLAGS_rpc_private_key_file,
        FLAGS_create_new_x509_store_each_time);
    TRACE(
        "SSL context certificate store refreshed : $0",
        reload_status.ToString());
  }

  Status s;
  unique_ptr<ErrorStatusPB> rpc_error;
  if (conn->direction() == ConnectionDirection::SERVER) {
    s = DoServerNegotiation(conn.get(), authentication, encryption, deadline);
  } else {
    s = DoClientNegotiation(
        conn.get(), authentication, encryption, deadline, &rpc_error);
  }

  if (PREDICT_FALSE(!s.ok())) {
    string msg = Substitute(
        "$0 connection negotiation failed: $1",
        conn->direction() == ConnectionDirection::SERVER ? "Server" : "Client",
        conn->ToString());
    s = s.CloneAndPrepend(msg);
  }
  TRACE("Negotiation complete: $0", s.ToString());

  bool is_bad = !s.ok() &&
      !((s.IsNetworkError() && s.posix_code() == ECONNREFUSED) ||
        s.IsNotAuthorized());

  if (is_bad || FLAGS_rpc_trace_negotiation) {
    std::string msg;
    if (FLAGS_rpc_trace_negotiation) {
      msg = Trace::CurrentTrace()->DumpToString();
    } else {
      msg = Substitute(
          "$0 connection : $1",
          conn->direction() == ConnectionDirection::SERVER ? "Server"
                                                           : "Client",
          conn->ToString());
    }
    if (is_bad) {
      KLOG_EVERY_N_SECS(WARNING, 300)
          << "Failed RPC negotiation. Details [EVERY 300 seconds]: " << msg;
    } else {
      LOG(INFO) << "RPC negotiation tracing enabled. Trace:\n" << msg;
    }
  }

  if (conn->direction() == ConnectionDirection::SERVER && s.IsNotAuthorized()) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Unauthorized connection attempt [EVERY 300 seconds]: "
        << s.message().ToString();
  }
  conn->CompleteNegotiation(std::move(s), std::move(rpc_error));
}

} // namespace rpc
} // namespace kudu
