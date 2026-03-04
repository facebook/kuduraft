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

#include "kudu/security/tls_handshake.h"

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/ScopeGuard.h>
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {
namespace security {

using ca::CertSigner;

struct Case {
  PkiConfig clientPki;
  TlsVerificationMode clientVerification;
  PkiConfig serverPki;
  TlsVerificationMode serverVerification;
  Status expectedStatus;
};

// Beautifies CLI test output.
std::ostream& operator<<(std::ostream& o, Case c) {
  auto verificationModeName = [](const TlsVerificationMode& verificationMode) {
    switch (verificationMode) {
      case TlsVerificationMode::VerifyNone:
        return "NONE";
      case TlsVerificationMode::VerifyRemoteCertAndHost:
        return "REMOTE_CERT_AND_HOST";
    }
    return "unreachable";
  };

  o << "{client-pki: " << c.clientPki << ", "
    << "client-verification: " << verificationModeName(c.clientVerification)
    << ", " << "server-pki: " << c.serverPki << ", "
    << "server-verification: " << verificationModeName(c.serverVerification)
    << ", " << "expected-status: " << c.expectedStatus.ToString() << "}";

  return o;
}

class TestTlsHandshakeBase : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ASSERT_OK(clientTls_.init());
    ASSERT_OK(serverTls_.init());
  }

 protected:
  // Run a handshake using 'clientTls_' and 'serverTls_'. The client and
  // server verification modes are set to 'clientVerify' and 'serverVerify'
  // respectively.
  Status RunHandshake(
      TlsVerificationMode clientVerify,
      TlsVerificationMode serverVerify) {
    TlsHandshake client, server;
    RETURN_NOT_OK(
        clientTls_.InitiateHandshake(TlsHandshakeType::Client, &client));
    RETURN_NOT_OK(
        serverTls_.InitiateHandshake(TlsHandshakeType::Server, &server));

    client.setVerificationMode(clientVerify);
    server.setVerificationMode(serverVerify);

    bool clientDone = false, serverDone = false;
    string toClient;
    string toServer;
    while (!clientDone || !serverDone) {
      if (!clientDone) {
        Status s = client.continueHandshake(toClient, &toServer);
        VLOG(1) << "client->server: " << toServer.size() << " bytes";
        if (s.ok()) {
          clientDone = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("client error");
        }
      }
      if (!serverDone) {
        CHECK(!clientDone);
        Status s = server.continueHandshake(toServer, &toClient);
        VLOG(1) << "server->client: " << toClient.size() << " bytes";
        if (s.ok()) {
          serverDone = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("server error");
        }
      }
    }
    return Status::OK();
  }

  TlsContext clientTls_;
  TlsContext serverTls_;
};

class TestTlsHandshake : public TestTlsHandshakeBase,
                         public ::testing::WithParamInterface<Case> {};

class TestTlsHandshakeConcurrent : public TestTlsHandshakeBase,
                                   public ::testing::WithParamInterface<int> {};

// Test concurrently running handshakes while changing the certificates on the
// TLS context. We parameterize across different numbers of threads, because
// surprisingly, fewer threads seems to trigger issues more easily in some
// cases.
INSTANTIATE_TEST_CASE_P(
    NumThreads,
    TestTlsHandshakeConcurrent,
    ::testing::Values(1, 2, 4, 8));
TEST_P(TestTlsHandshakeConcurrent, TestConcurrentAdoptCert) {
  const int kNumThreads = GetParam();

  ASSERT_OK(serverTls_.generateSelfSignedCertAndKey());
  std::atomic<bool> done(false);
  vector<std::thread> handshakeThreads;
  for (int i = 0; i < kNumThreads; i++) {
    handshakeThreads.emplace_back([&]() {
      while (!done) {
        RunHandshake(
            TlsVerificationMode::VerifyNone, TlsVerificationMode::VerifyNone);
      }
    });
  }
  auto c = folly::makeGuard([&]() {
    done = true;
    for (std::thread& t : handshakeThreads) {
      t.join();
    }
  });

  SleepFor(MonoDelta::FromMilliseconds(10));
  {
    PrivateKey caKey;
    Cert caCert;
    ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));
    Cert cert;
    ASSERT_OK(CertSigner(&caCert, &caKey)
                  .sign(*serverTls_.getCsrIfNecessary(), &cert));
    ASSERT_OK(serverTls_.addTrustedCertificate(caCert));
    ASSERT_OK(serverTls_.adoptSignedCert(cert));
  }
  SleepFor(MonoDelta::FromMilliseconds(10));
}

TEST_F(TestTlsHandshake, TestHandshakeSequence) {
  PrivateKey caKey;
  Cert caCert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));

  // Both client and server have certs and CA.
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, caCert, caKey, &clientTls_));
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, caCert, caKey, &serverTls_));

  TlsHandshake server;
  TlsHandshake client;
  ASSERT_OK(clientTls_.InitiateHandshake(TlsHandshakeType::Server, &server));
  ASSERT_OK(serverTls_.InitiateHandshake(TlsHandshakeType::Client, &client));

  string buf1;
  string buf2;

  // Client sends Hello
  ASSERT_TRUE(client.continueHandshake(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Hello, and sends server Hello
  ASSERT_TRUE(server.continueHandshake(buf2, &buf1).IsIncomplete());
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Hello and sends client Finished
  ASSERT_TRUE(client.continueHandshake(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Finished and sends server Finished
  ASSERT_OK(server.continueHandshake(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Finished
  ASSERT_OK(client.continueHandshake(buf1, &buf2));
  ASSERT_EQ(buf2.size(), 0);
}

// Tests that the TlsContext can transition from self signed cert to signed
// cert, and that it rejects invalid certs along the way. We are testing this
// here instead of in a dedicated TlsContext test because it requires completing
// handshakes to fully validate.
TEST_F(TestTlsHandshake, TestTlsContextCertTransition) {
  ASSERT_FALSE(serverTls_.hasCert());
  ASSERT_FALSE(serverTls_.hasSignedCert());
  ASSERT_EQ({}, serverTls_.getCsrIfNecessary());

  ASSERT_OK(serverTls_.generateSelfSignedCertAndKey());
  ASSERT_TRUE(serverTls_.hasCert());
  ASSERT_FALSE(serverTls_.hasSignedCert());
  ASSERT_NE({}, serverTls_.getCsrIfNecessary());
  ASSERT_OK(RunHandshake(
      TlsVerificationMode::VerifyNone, TlsVerificationMode::VerifyNone));
  ASSERT_STR_MATCHES(
      RunHandshake(
          TlsVerificationMode::VerifyRemoteCertAndHost,
          TlsVerificationMode::VerifyNone)
          .ToString(),
      "client error:.*certificate verify failed");

  PrivateKey caKey;
  Cert caCert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));

  Cert cert;
  ASSERT_OK(
      CertSigner(&caCert, &caKey).sign(*serverTls_.getCsrIfNecessary(), &cert));

  // Try to adopt the cert without first trusting the CA.
  ASSERT_STR_MATCHES(
      serverTls_.adoptSignedCert(cert).ToString(),
      "could not verify certificate chain");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(serverTls_.hasCert());
  ASSERT_FALSE(serverTls_.hasSignedCert());
  ASSERT_OK(RunHandshake(
      TlsVerificationMode::VerifyNone, TlsVerificationMode::VerifyNone));

  // Trust the root cert.
  ASSERT_OK(serverTls_.addTrustedCertificate(caCert));

  // Generate a bogus cert and attempt to adopt it.
  Cert bogusCert;
  {
    TlsContext bogusTls;
    ASSERT_OK(bogusTls.init());
    ASSERT_OK(bogusTls.generateSelfSignedCertAndKey());
    ASSERT_OK(CertSigner(&caCert, &caKey)
                  .sign(*bogusTls.getCsrIfNecessary(), &bogusCert));
  }
  ASSERT_STR_MATCHES(
      serverTls_.adoptSignedCert(bogusCert).ToString(),
      "certificate public key does not match the CSR public key");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(serverTls_.hasCert());
  ASSERT_FALSE(serverTls_.hasSignedCert());
  ASSERT_OK(RunHandshake(
      TlsVerificationMode::VerifyNone, TlsVerificationMode::VerifyNone));

  // Adopt the legitimate signed cert.
  ASSERT_OK(serverTls_.adoptSignedCert(cert));

  // Check that we can do verified handshakes.
  ASSERT_TRUE(serverTls_.hasCert());
  ASSERT_TRUE(serverTls_.hasSignedCert());
  ASSERT_OK(RunHandshake(
      TlsVerificationMode::VerifyNone, TlsVerificationMode::VerifyNone));
  ASSERT_OK(clientTls_.addTrustedCertificate(caCert));
  ASSERT_OK(RunHandshake(
      TlsVerificationMode::VerifyRemoteCertAndHost,
      TlsVerificationMode::VerifyNone));
}

TEST_P(TestTlsHandshake, TestHandshake) {
  Case testCase = GetParam();

  PrivateKey caKey;
  Cert caCert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));

  ASSERT_OK(
      ConfigureTlsContext(testCase.clientPki, caCert, caKey, &clientTls_));
  ASSERT_OK(
      ConfigureTlsContext(testCase.serverPki, caCert, caKey, &serverTls_));

  Status s =
      RunHandshake(testCase.clientVerification, testCase.serverVerification);

  EXPECT_EQ(testCase.expectedStatus.CodeAsString(), s.CodeAsString());
  ASSERT_STR_MATCHES(
      s.ToString(), testCase.expectedStatus.message().ToString());
}

INSTANTIATE_TEST_CASE_P(
    CertCombinations,
    TestTlsHandshake,
    ::testing::Values(

        // We don't test any cases where the server has no cert or the client
        // has a self-signed cert, since we don't expect those to occur in
        // practice.

        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::RuntimeError("client error:.*certificate verify failed")},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError(
                "server error:.*peer did not return a certificate")},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError("client error:.*certificate verify failed")},

        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::RuntimeError("client error:.*certificate verify failed")},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError(
                "server error:.*peer did not return a certificate")},
        Case{
            PkiConfig::NONE,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError("client error:.*certificate verify failed")},

        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::RuntimeError("client error:.*certificate verify failed")},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError(
                "server error:.*peer did not return a certificate")},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError("client error:.*certificate verify failed")},

        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError(
                "server error:.*peer did not return a certificate")},
        Case{
            PkiConfig::TRUSTED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError(
                "server error:.*peer did not return a certificate")},

        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::RuntimeError("client error:.*certificate verify failed")},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            // OpenSSL 1.0.0 returns "no certificate returned" for this case,
            // which appears to be a bug.
            Status::RuntimeError(
                "server error:.*(certificate verify failed|"
                "no certificate returned)")},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SELF_SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::RuntimeError("client error:.*certificate verify failed")},

        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            Status::OK()},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyNone,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::OK()},
        Case{
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            PkiConfig::SIGNED,
            TlsVerificationMode::VerifyRemoteCertAndHost,
            Status::OK()}));

} // namespace security
} // namespace kudu
