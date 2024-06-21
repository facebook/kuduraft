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

#include "kudu/rpc/rpc-test-base.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_socket.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"
#include "kudu/util/user.h"

DECLARE_bool(rpc_encrypt_loopback_connections);
DECLARE_bool(rpc_trace_negotiation);

using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

using kudu::security::Cert;
using kudu::security::PkiConfig;
using kudu::security::PrivateKey;
using kudu::security::SignedTokenPB;
using kudu::security::TlsContext;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKey;
using kudu::security::TokenVerifier;

namespace kudu {
namespace rpc {

// The negotiation configuration for a client or server endpoint.
struct EndpointConfig {
  // The PKI configuration.
  PkiConfig pki;
  // For the client, whether the client has the token.
  // For the server, whether the server has the TSK.
  bool token;
  RpcEncryption encryption;
};
std::ostream& operator<<(std::ostream& o, EndpointConfig config) {
  auto bool_string = [](bool b) { return b ? "true" : "false"; };
  o << "{pki: " << config.pki << ", token: " << bool_string(config.token)
    << ", encryption: ";

  switch (config.encryption) {
    case RpcEncryption::DISABLED:
      o << "DISABLED";
      break;
    case RpcEncryption::OPTIONAL:
      o << "OPTIONAL";
      break;
    case RpcEncryption::REQUIRED:
      o << "REQUIRED";
      break;
  }

  o << "}";
  return o;
}

// A description of a negotiation sequence, including client and server
// configuration, as well as expected results.
struct NegotiationDescriptor {
  EndpointConfig client;
  EndpointConfig server;

  bool use_test_socket;

  bool rpc_encrypt_loopback;

  // The expected client status from negotiating.
  Status client_status;
  // The expected server status from negotiating.
  Status server_status;

  // The expected negotiated authentication type.
  AuthenticationType negotiated_authn;

  // Whether the negotiation is expected to perform a TLS handshake.
  bool tls_negotiated;
};
std::ostream& operator<<(std::ostream& o, NegotiationDescriptor c) {
  auto bool_string = [](bool b) { return b ? "true" : "false"; };
  o << "{client: " << c.client << ", server: " << c.server
    << "}, rpc-encrypt-loopback: " << bool_string(c.rpc_encrypt_loopback);
  return o;
}

class NegotiationTestSocket : public Socket {
 public:
  // Return an arbitrary public IP
  Status GetPeerAddress(Sockaddr* cur_addr) const override {
    return cur_addr->ParseString("8.8.8.8:12345", 0);
  }
};

class TestNegotiation
    : public RpcTestBase,
      public ::testing::WithParamInterface<NegotiationDescriptor> {
 public:
  void SetUp() override {
    RpcTestBase::SetUp();
  }
};

TEST_P(TestNegotiation, TestNegotiation) {
  NegotiationDescriptor desc = GetParam();
  // FLAGS_skip_verify_tls_cert = false;

  // Generate a trusted root certificate.
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Create and configure a TLS context for each endpoint.
  TlsContext client_tls_context;
  TlsContext server_tls_context;
  ASSERT_OK(client_tls_context.Init());
  ASSERT_OK(server_tls_context.Init());
  ASSERT_OK(ConfigureTlsContext(
      desc.client.pki, ca_cert, ca_key, &client_tls_context));
  ASSERT_OK(ConfigureTlsContext(
      desc.server.pki, ca_cert, ca_key, &server_tls_context));

  FLAGS_rpc_encrypt_loopback_connections = desc.rpc_encrypt_loopback;

  // Generate an optional client token and server token verifier.
  TokenSigner token_signer(60, 20, std::make_shared<TokenVerifier>());
  {
    unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(token_signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(token_signer.AddKey(std::move(key)));
  }
  TokenVerifier token_verifier;
  std::optional<SignedTokenPB> authn_token;
  if (desc.client.token) {
    authn_token = SignedTokenPB();
    security::TokenPB token;
    token.set_expire_unix_epoch_seconds(WallTime_Now() + 60);
    token.mutable_authn()->set_username("client-token");
    ASSERT_TRUE(token.SerializeToString(authn_token->mutable_token_data()));
    ASSERT_OK(token_signer.SignToken(&*authn_token));
  }
  if (desc.server.token) {
    ASSERT_OK(token_verifier.ImportKeys(token_signer.verifier().ExportKeys()));
  }

  // Create the listening socket, client socket, and server socket.
  Socket listening_socket;
  ASSERT_OK(listening_socket.Init(0));
  ASSERT_OK(listening_socket.BindAndListen(Sockaddr(), 1));
  Sockaddr server_addr;
  ASSERT_OK(listening_socket.GetSocketAddress(&server_addr));

  unique_ptr<Socket> client_socket(new Socket());
  ASSERT_OK(client_socket->Init(0));
  client_socket->Connect(server_addr);

  unique_ptr<Socket> server_socket(
      desc.use_test_socket ? new NegotiationTestSocket() : new Socket());

  Sockaddr client_addr;
  CHECK_OK(listening_socket.Accept(server_socket.get(), &client_addr, 0));

  // Create and configure the client and server negotiation instances.
  ClientNegotiation client_negotiation(
      std::move(client_socket),
      &client_tls_context,
      authn_token,
      desc.client.encryption);
  ServerNegotiation server_negotiation(
      std::move(server_socket),
      &server_tls_context,
      &token_verifier,
      desc.server.encryption);

  // Run the client/server negotiation. Because negotiation is blocking, it
  // has to be done on separate threads.
  Status client_status;
  Status server_status;
  thread client_thread([&]() {
    scoped_refptr<Trace> t(new Trace());
    ADOPT_TRACE(t.get());
    client_status = client_negotiation.Negotiate();
    // Close the socket so that the server will not block forever on error.
    client_negotiation.socket()->Close();

    if (FLAGS_rpc_trace_negotiation || !client_status.ok()) {
      string msg = Trace::CurrentTrace()->DumpToString();
      if (!client_status.ok()) {
        LOG(WARNING) << "Failed client RPC negotiation. Client trace:\n" << msg;
      } else {
        LOG(INFO) << "RPC negotiation tracing enabled. Client trace:\n" << msg;
      }
    }
  });
  thread server_thread([&]() {
    scoped_refptr<Trace> t(new Trace());
    ADOPT_TRACE(t.get());
    server_status = server_negotiation.Negotiate();
    // Close the socket so that the client will not block forever on error.
    server_negotiation.socket()->Close();

    if (FLAGS_rpc_trace_negotiation || !server_status.ok()) {
      string msg = Trace::CurrentTrace()->DumpToString();
      if (!server_status.ok()) {
        LOG(WARNING) << "Failed server RPC negotiation. Server trace:\n" << msg;
      } else {
        LOG(INFO) << "RPC negotiation tracing enabled. Server trace:\n" << msg;
      }
    }
  });
  client_thread.join();
  server_thread.join();

  // Check the negotiation outcome against the expected outcome.
  EXPECT_EQ(desc.client_status.CodeAsString(), client_status.CodeAsString());
  EXPECT_EQ(desc.server_status.CodeAsString(), server_status.CodeAsString());
  ASSERT_STR_MATCHES(client_status.ToString(), desc.client_status.ToString());
  ASSERT_STR_MATCHES(server_status.ToString(), desc.server_status.ToString());

  if (client_status.ok()) {
    EXPECT_TRUE(server_status.ok());

    // Make sure the negotiations agree with the expected values.
    EXPECT_EQ(desc.negotiated_authn, client_negotiation.negotiated_authn());
    EXPECT_EQ(desc.negotiated_authn, server_negotiation.negotiated_authn());
    EXPECT_EQ(desc.tls_negotiated, server_negotiation.tls_negotiated());
    EXPECT_EQ(desc.tls_negotiated, server_negotiation.tls_negotiated());

    bool client_tls_socket =
        dynamic_cast<security::TlsSocket*>(client_negotiation.socket());
    bool server_tls_socket =
        dynamic_cast<security::TlsSocket*>(server_negotiation.socket());
    EXPECT_EQ(desc.rpc_encrypt_loopback, client_tls_socket);
    EXPECT_EQ(desc.rpc_encrypt_loopback, server_tls_socket);

    // Check that the expected user subject is authenticated.
    RemoteUser remote_user = server_negotiation.take_authenticated_user();
    switch (server_negotiation.negotiated_authn()) {
      case AuthenticationType::CERTIFICATE: {
        // We expect the cert to be using the local username, because it hasn't
        // logged in from any Keytab.
        string expected;
        CHECK_OK(GetLoggedInUser(&expected));
        EXPECT_EQ(expected, remote_user.username());
        EXPECT_FALSE(remote_user.principal());
        break;
      }
      case AuthenticationType::TOKEN:
        EXPECT_EQ("client-token", remote_user.username());
        break;
      case AuthenticationType::INVALID:
        LOG(FATAL) << "invalid authentication negotiated";
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    NegotiationCombinations,
    TestNegotiation,
    ::testing::Values(

        // 0
        // client: no authn
        // server: no authn
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::NONE,
                false,
                RpcEncryption::OPTIONAL,
            },
            EndpointConfig{
                PkiConfig::NONE,
                false,
                RpcEncryption::OPTIONAL,
            },
            false,
            false,
            Status::NetworkError(""),
            Status::NetworkError(""),
            AuthenticationType::INVALID,
            false,
        },

        // 1
        // client: signed-cert
        // server: signed-cert
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                false,
                RpcEncryption::OPTIONAL,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                false,
                RpcEncryption::OPTIONAL,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        },

        // 2
        // client: PLAIN, GSSAPI, signed-cert, token
        // server: PLAIN, GSSAPI, signed-cert, token
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::OPTIONAL,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::OPTIONAL,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        },

        // 3
        // client:               signed-cert, normal TLS
        // server: token, PLAIN, signed-cert, normal TLS
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                false,
                RpcEncryption::REQUIRED,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::REQUIRED,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        },

        // 4
        // client:               signed-cert
        // server: token, PLAIN, signed-cert, normal TLS
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                false,
                RpcEncryption::REQUIRED,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::REQUIRED,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        },

        // 5
        // client:        PLAIN, signed-cert
        // server: token, PLAIN, signed-cert, normal TLS
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                false,
                RpcEncryption::REQUIRED,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::REQUIRED,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        },

        // 6
        // client: token, PLAIN, signed-cert
        // server: token, PLAIN, signed-cert, normal TLS
        NegotiationDescriptor{
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::REQUIRED,
            },
            EndpointConfig{
                PkiConfig::SIGNED,
                true,
                RpcEncryption::REQUIRED,
            },
            false,
            true,
            Status::OK(),
            Status::OK(),
            AuthenticationType::CERTIFICATE,
            true,
        }));

// A "Callable" that takes a socket for use with starting a thread.
// Can be used for ServerNegotiation or ClientNegotiation threads.
typedef std::function<void(unique_ptr<Socket>)> SocketCallable;

// Call Accept() on the socket, then pass the connection to the server runner
static void RunAcceptingDelegator(
    Socket* acceptor,
    const SocketCallable& server_runner) {
  unique_ptr<Socket> conn(new Socket());
  Sockaddr remote;
  CHECK_OK(acceptor->Accept(conn.get(), &remote, 0));
  server_runner(std::move(conn));
}

// Set up a socket and run a negotiation sequence.
static void RunNegotiationTest(
    const SocketCallable& server_runner,
    const SocketCallable& client_runner) {
  Socket server_sock;
  CHECK_OK(server_sock.Init(0));
  ASSERT_OK(server_sock.BindAndListen(Sockaddr(), 1));
  Sockaddr server_bind_addr;
  ASSERT_OK(server_sock.GetSocketAddress(&server_bind_addr));
  thread server(RunAcceptingDelegator, &server_sock, server_runner);

  unique_ptr<Socket> client_sock(new Socket());
  CHECK_OK(client_sock->Init(0));
  ASSERT_OK(client_sock->Connect(server_bind_addr));
  thread client(client_runner, std::move(client_sock));

  LOG(INFO) << "Waiting for test threads to terminate...";
  client.join();
  LOG(INFO) << "Client thread terminated.";

  server.join();
  LOG(INFO) << "Server thread terminated.";
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutExpectingServer(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  TokenVerifier token_verifier;
  ServerNegotiation server_negotiation(
      std::move(socket),
      &tls_context,
      &token_verifier,
      RpcEncryption::OPTIONAL);
  Status s = server_negotiation.Negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected client to time out and close the connection. Got: "
      << s.ToString();
}

static void RunTimeoutNegotiationClient(unique_ptr<Socket> sock) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  ClientNegotiation client_negotiation(
      std::move(sock), &tls_context, {}, RpcEncryption::OPTIONAL);
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  client_negotiation.set_deadline(deadline);
  Status s = client_negotiation.Negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected NetworkError! Got: " << s.ToString();
  CHECK_OK(client_negotiation.socket()->Close());
}

// Ensure that the client times out.
TEST_F(TestNegotiation, TestClientConnectError) {
  RunNegotiationTest(RunTimeoutExpectingServer, RunTimeoutNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutNegotiationServer(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  TokenVerifier token_verifier;
  ServerNegotiation server_negotiation(
      std::move(socket),
      &tls_context,
      &token_verifier,
      RpcEncryption::OPTIONAL);
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  server_negotiation.set_deadline(deadline);
  Status s = server_negotiation.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(server_negotiation.socket()->Close());
}

static void RunTimeoutExpectingClient(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  ClientNegotiation client_negotiation(
      std::move(socket), &tls_context, {}, RpcEncryption::OPTIONAL);
  Status s = client_negotiation.Negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected server to time out and close the connection. Got: "
      << s.ToString();
}

// Ensure that the server times out.
TEST_F(TestNegotiation, TestServerTimeout) {
  RunNegotiationTest(RunTimeoutNegotiationServer, RunTimeoutExpectingClient);
}

} // namespace rpc
} // namespace kudu
