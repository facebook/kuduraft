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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_socket.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
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
  auto boolString = [](bool b) { return b ? "true" : "false"; };
  o << "{pki: " << config.pki << ", token: " << boolString(config.token)
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

  bool useTestSocket;

  bool rpcEncryptLoopback;

  // The expected client status from negotiating.
  Status clientStatus;
  // The expected server status from negotiating.
  Status serverStatus;

  // The expected negotiated authentication type.
  AuthenticationType negotiatedAuthn;

  // Whether the negotiation is expected to perform a TLS handshake.
  bool tlsNegotiated;
};
std::ostream& operator<<(std::ostream& o, NegotiationDescriptor c) {
  auto boolString = [](bool b) { return b ? "true" : "false"; };
  o << "{client: " << c.client << ", server: " << c.server
    << "}, rpc-encrypt-loopback: " << boolString(c.rpcEncryptLoopback);
  return o;
}

class NegotiationTestSocket : public Socket {
 public:
  // Return an arbitrary public IP
  Status GetPeerAddress(Sockaddr* curAddr) const override {
    return curAddr->ParseString("8.8.8.8:12345", 0);
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
  PrivateKey caKey;
  Cert caCert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));

  // Create and configure a TLS context for each endpoint.
  TlsContext clientTlsContext;
  TlsContext serverTlsContext;
  ASSERT_OK(clientTlsContext.init());
  ASSERT_OK(serverTlsContext.init());
  ASSERT_OK(
      ConfigureTlsContext(desc.client.pki, caCert, caKey, &clientTlsContext));
  ASSERT_OK(
      ConfigureTlsContext(desc.server.pki, caCert, caKey, &serverTlsContext));

  FLAGS_rpc_encrypt_loopback_connections = desc.rpcEncryptLoopback;

  // Generate an optional client token and server token verifier.
  TokenSigner tokenSigner(60, 20, std::make_shared<TokenVerifier>());
  {
    unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(tokenSigner.checkNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(tokenSigner.addKey(std::move(key)));
  }
  TokenVerifier tokenVerifier;
  std::optional<SignedTokenPB> authnToken;
  if (desc.client.token) {
    authnToken = SignedTokenPB();
    security::TokenPB token;
    token.set_expire_unix_epoch_seconds(wallTimeNow() + 60);
    token.mutable_authn()->set_username("client-token");
    ASSERT_TRUE(token.SerializeToString(authnToken->mutable_token_data()));
    ASSERT_OK(tokenSigner.signToken(&*authnToken));
  }
  if (desc.server.token) {
    ASSERT_OK(tokenVerifier.importKeys(tokenSigner.verifier().exportKeys()));
  }

  // Create the listening socket, client socket, and server socket.
  Socket listeningSocket;
  ASSERT_OK(listeningSocket.Init(0));
  ASSERT_OK(listeningSocket.BindAndListen(Sockaddr(), 1));
  Sockaddr serverAddr;
  ASSERT_OK(listeningSocket.GetSocketAddress(&serverAddr));

  unique_ptr<Socket> clientSocket(new Socket());
  ASSERT_OK(clientSocket->Init(0));
  clientSocket->Connect(serverAddr);

  unique_ptr<Socket> serverSocket(
      desc.useTestSocket ? new NegotiationTestSocket() : new Socket());

  Sockaddr clientAddr;
  CHECK_OK(listeningSocket.Accept(serverSocket.get(), &clientAddr, 0));

  // Create and configure the client and server negotiation instances.
  ClientNegotiation clientNegotiation(
      std::move(clientSocket),
      &clientTlsContext,
      authnToken,
      desc.client.encryption);
  ServerNegotiation serverNegotiation(
      std::move(serverSocket),
      &serverTlsContext,
      &tokenVerifier,
      desc.server.encryption);

  // Run the client/server negotiation. Because negotiation is blocking, it
  // has to be done on separate threads.
  Status clientStatus;
  Status serverStatus;
  thread clientThread([&]() {
    std::shared_ptr<Trace> t = std::make_shared<Trace>();
    ADOPT_TRACE(t);
    clientStatus = clientNegotiation.negotiate();
    // Close the socket so that the server will not block forever on error.
    clientNegotiation.socket()->Close();

    if (FLAGS_rpc_trace_negotiation || !clientStatus.ok()) {
      string msg = Trace::currentTrace()->dumpToString();
      if (!clientStatus.ok()) {
        LOG(WARNING) << "Failed client RPC negotiation. Client trace:\n" << msg;
      } else {
        LOG(INFO) << "RPC negotiation tracing enabled. Client trace:\n" << msg;
      }
    }
  });
  thread serverThread([&]() {
    std::shared_ptr<Trace> t = std::make_shared<Trace>();
    ADOPT_TRACE(t);
    serverStatus = serverNegotiation.negotiate();
    // Close the socket so that the client will not block forever on error.
    serverNegotiation.socket()->Close();

    if (FLAGS_rpc_trace_negotiation || !serverStatus.ok()) {
      string msg = Trace::currentTrace()->dumpToString();
      if (!serverStatus.ok()) {
        LOG(WARNING) << "Failed server RPC negotiation. Server trace:\n" << msg;
      } else {
        LOG(INFO) << "RPC negotiation tracing enabled. Server trace:\n" << msg;
      }
    }
  });
  clientThread.join();
  serverThread.join();

  // Check the negotiation outcome against the expected outcome.
  EXPECT_EQ(desc.clientStatus.CodeAsString(), clientStatus.CodeAsString());
  EXPECT_EQ(desc.serverStatus.CodeAsString(), serverStatus.CodeAsString());
  ASSERT_STR_MATCHES(clientStatus.ToString(), desc.clientStatus.ToString());
  ASSERT_STR_MATCHES(serverStatus.ToString(), desc.serverStatus.ToString());

  if (clientStatus.ok()) {
    EXPECT_TRUE(serverStatus.ok());

    // Make sure the negotiations agree with the expected values.
    EXPECT_EQ(desc.negotiatedAuthn, clientNegotiation.negotiatedAuthn());
    EXPECT_EQ(desc.negotiatedAuthn, serverNegotiation.negotiatedAuthn());
    EXPECT_EQ(desc.tlsNegotiated, serverNegotiation.tlsNegotiated());
    EXPECT_EQ(desc.tlsNegotiated, serverNegotiation.tlsNegotiated());

    bool clientTlsSocket =
        dynamic_cast<security::TlsSocket*>(clientNegotiation.socket());
    bool serverTlsSocket =
        dynamic_cast<security::TlsSocket*>(serverNegotiation.socket());
    EXPECT_EQ(desc.rpcEncryptLoopback, clientTlsSocket);
    EXPECT_EQ(desc.rpcEncryptLoopback, serverTlsSocket);

    // Check that the expected user subject is authenticated.
    RemoteUser remoteUser = serverNegotiation.takeAuthenticatedUser();
    switch (serverNegotiation.negotiatedAuthn()) {
      case AuthenticationType::Certificate: {
        // We expect the cert to be using the local username, because it hasn't
        // logged in from any Keytab.
        string expected;
        CHECK_OK(getLoggedInUser(&expected));
        EXPECT_EQ(expected, remoteUser.username());
        EXPECT_FALSE(remoteUser.principal());
        break;
      }
      case AuthenticationType::Token:
        EXPECT_EQ("client-token", remoteUser.username());
        break;
      case AuthenticationType::Invalid:
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
            AuthenticationType::Invalid,
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
            AuthenticationType::Certificate,
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
            AuthenticationType::Certificate,
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
            AuthenticationType::Certificate,
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
            AuthenticationType::Certificate,
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
            AuthenticationType::Certificate,
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
            AuthenticationType::Certificate,
            true,
        }));

// A "Callable" that takes a socket for use with starting a thread.
// Can be used for ServerNegotiation or ClientNegotiation threads.
using SocketCallable = std::function<void(unique_ptr<Socket>)>;

// Call Accept() on the socket, then pass the connection to the server runner
static void runAcceptingDelegator(
    Socket* acceptor,
    const SocketCallable& serverRunner) {
  unique_ptr<Socket> conn(new Socket());
  Sockaddr remote;
  CHECK_OK(acceptor->Accept(conn.get(), &remote, 0));
  serverRunner(std::move(conn));
}

// Set up a socket and run a negotiation sequence.
static void runNegotiationTest(
    const SocketCallable& serverRunner,
    const SocketCallable& clientRunner) {
  Socket serverSock;
  CHECK_OK(serverSock.Init(0));
  ASSERT_OK(serverSock.BindAndListen(Sockaddr(), 1));
  Sockaddr serverBindAddr;
  ASSERT_OK(serverSock.GetSocketAddress(&serverBindAddr));
  thread server(runAcceptingDelegator, &serverSock, serverRunner);

  unique_ptr<Socket> clientSock(new Socket());
  CHECK_OK(clientSock->Init(0));
  ASSERT_OK(clientSock->Connect(serverBindAddr));
  thread client(clientRunner, std::move(clientSock));

  LOG(INFO) << "Waiting for test threads to terminate...";
  client.join();
  LOG(INFO) << "Client thread terminated.";

  server.join();
  LOG(INFO) << "Server thread terminated.";
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

static void runTimeoutExpectingServer(unique_ptr<Socket> socket) {
  TlsContext tlsContext;
  CHECK_OK(tlsContext.init());
  TokenVerifier tokenVerifier;
  ServerNegotiation serverNegotiation(
      std::move(socket), &tlsContext, &tokenVerifier, RpcEncryption::OPTIONAL);
  Status s = serverNegotiation.negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected client to time out and close the connection. Got: "
      << s.ToString();
}

static void runTimeoutNegotiationClient(unique_ptr<Socket> sock) {
  TlsContext tlsContext;
  CHECK_OK(tlsContext.init());
  ClientNegotiation clientNegotiation(
      std::move(sock), &tlsContext, {}, RpcEncryption::OPTIONAL);
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  clientNegotiation.setDeadline(deadline);
  Status s = clientNegotiation.negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected NetworkError! Got: " << s.ToString();
  CHECK_OK(clientNegotiation.socket()->Close());
}

// Ensure that the client times out.
TEST_F(TestNegotiation, TestClientConnectError) {
  runNegotiationTest(runTimeoutExpectingServer, runTimeoutNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void runTimeoutNegotiationServer(unique_ptr<Socket> socket) {
  TlsContext tlsContext;
  CHECK_OK(tlsContext.init());
  TokenVerifier tokenVerifier;
  ServerNegotiation serverNegotiation(
      std::move(socket), &tlsContext, &tokenVerifier, RpcEncryption::OPTIONAL);
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  serverNegotiation.setDeadline(deadline);
  Status s = serverNegotiation.negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(serverNegotiation.socket()->Close());
}

static void runTimeoutExpectingClient(unique_ptr<Socket> socket) {
  TlsContext tlsContext;
  CHECK_OK(tlsContext.init());
  ClientNegotiation clientNegotiation(
      std::move(socket), &tlsContext, {}, RpcEncryption::OPTIONAL);
  Status s = clientNegotiation.negotiate();
  ASSERT_TRUE(s.IsNetworkError())
      << "Expected server to time out and close the connection. Got: "
      << s.ToString();
}

// Ensure that the server times out.
TEST_F(TestNegotiation, TestServerTimeout) {
  runNegotiationTest(runTimeoutNegotiationServer, runTimeoutExpectingClient);
}

} // namespace rpc
} // namespace kudu
