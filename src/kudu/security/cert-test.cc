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

#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "kudu/gutil/strings/strip.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/barrier.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::pair;
using std::string;
using std::thread;
using std::vector;

namespace kudu {
namespace security {

// Test for various certificate-related functionality in the security library.
// These do not cover CA certificate mananagement part; check
// cert_management-test.cc for those.
class CertTest : public KuduTest {
 public:
  void SetUp() override {
    ASSERT_OK(caCert_.FromString(kCaCert, DataFormat::PEM));
    ASSERT_OK(caPrivateKey_.FromString(kCaPrivateKey, DataFormat::PEM));
    ASSERT_OK(caPublicKey_.FromString(kCaPublicKey, DataFormat::PEM));
    ASSERT_OK(caExpCert_.FromString(kCaExpiredCert, DataFormat::PEM));
    ASSERT_OK(
        caExpPrivateKey_.FromString(kCaExpiredPrivateKey, DataFormat::PEM));
    // Sanity checks.
    ASSERT_OK(caCert_.CheckKeyMatch(caPrivateKey_));
    ASSERT_OK(caExpCert_.CheckKeyMatch(caExpPrivateKey_));
  }

 protected:
  Cert caCert_;
  PrivateKey caPrivateKey_;
  PublicKey caPublicKey_;

  Cert caExpCert_;
  PrivateKey caExpPrivateKey_;
};

// Regression test to make sure that GetKuduKerberosPrincipalOidNid is thread
// safe. OpenSSL 1.0.0's OBJ_create method is not thread safe.
TEST_F(CertTest, GetKuduKerberosPrincipalOidNidConcurrent) {
  int kConcurrency = 16;
  Barrier barrier(kConcurrency);

  vector<thread> threads;
  for (int i = 0; i < kConcurrency; i++) {
    threads.emplace_back([&]() {
      barrier.Wait();
      CHECK_NE(NID_undef, GetKuduKerberosPrincipalOidNid());
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

// Check input/output of the X509 certificates in PEM format.
TEST_F(CertTest, CertInputOutputPEM) {
  const Cert& cert = caCert_;
  string certStr;
  ASSERT_OK(cert.ToString(&certStr, DataFormat::PEM));
  RemoveExtraWhitespace(&certStr);

  string caInputCert(kCaCert);
  RemoveExtraWhitespace(&caInputCert);
  EXPECT_EQ(caInputCert, certStr);
}

// Check that Cert behaves in a predictable way if given invalid PEM data.
TEST_F(CertTest, CertInvalidInput) {
  // Providing files which guaranteed to exists, but do not contain valid data.
  // This is to make sure the init handles that situation correctly and
  // does not choke on the wrong input data.
  Cert c;
  ASSERT_FALSE(c.FromFile("/bin/sh", DataFormat::PEM).ok());
}

// Check X509 certificate/private key matching: match cases.
TEST_F(CertTest, CertMatchesRsaPrivateKey) {
  const pair<const Cert*, const PrivateKey*> cases[] = {
      {&caCert_, &caPrivateKey_},
      {&caExpCert_, &caExpPrivateKey_},
  };
  for (const auto& e : cases) {
    EXPECT_OK(e.first->CheckKeyMatch(*e.second));
  }
}

// Check X509 certificate/private key matching: mismatch cases.
TEST_F(CertTest, CertMismatchesRsaPrivateKey) {
  const pair<const Cert*, const PrivateKey*> cases[] = {
      {&caCert_, &caExpPrivateKey_},
      {&caExpCert_, &caPrivateKey_},
  };
  for (const auto& e : cases) {
    const Status s = e.first->CheckKeyMatch(*e.second);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "certificate does not match private key");
  }
}

TEST_F(CertTest, TestGetKuduSpecificFieldsWhenMissing) {
  EXPECT_EQ({}, caCert_.UserId());
  EXPECT_EQ({}, caCert_.KuduKerberosPrincipal());
}

TEST_F(CertTest, DnsHostnameInSanField) {
  const string hostnameFooBar = "foo.bar.com";
  const string hostnameMegaGiga = "mega.giga.io";
  const string hostnameTooLong =
      "toooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo."
      "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
      "ng.hostname.io";

  Cert cert;
  ASSERT_OK(cert.FromString(kCertDnsHostnamesInSan, DataFormat::PEM));

  EXPECT_EQ(
      "C = US, ST = CA, O = MyCompany, CN = MyName, emailAddress = my@email.com",
      cert.IssuerName());
  vector<string> hostnames = cert.Hostnames();
  ASSERT_EQ(3, hostnames.size());
  EXPECT_EQ(hostnameMegaGiga, hostnames[0]);
  EXPECT_EQ(hostnameFooBar, hostnames[1]);
  EXPECT_EQ(hostnameTooLong, hostnames[2]);
}

} // namespace security
} // namespace kudu
