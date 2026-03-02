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

#include "kudu/security/ca/cert_management.h"

#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {
namespace security {
namespace ca {

class CertManagementTest : public KuduTest {
 public:
  void SetUp() override {
    ASSERT_OK(caCert_.FromString(kCaCert, DataFormat::PEM));
    ASSERT_OK(caPrivateKey_.FromString(kCaPrivateKey, DataFormat::PEM));
    ASSERT_OK(caPublicKey_.FromString(kCaPublicKey, DataFormat::PEM));
    ASSERT_OK(caExpCert_.FromString(kCaExpiredCert, DataFormat::PEM));
    ASSERT_OK(
        caExpPrivateKey_.FromString(kCaExpiredPrivateKey, DataFormat::PEM));
    // Sanity checks.
    ASSERT_OK(caCert_.checkKeyMatch(caPrivateKey_));
    ASSERT_OK(caExpCert_.checkKeyMatch(caExpPrivateKey_));
  }

 protected:
  CertRequestGenerator::Config prepareConfig(
      const string& hostname = "localhost.localdomain") {
    return {hostname};
  }

  CaCertRequestGenerator::Config prepareCaConfig(const string& cn) {
    return {cn};
  }

  // Create a new private key in 'key' and return a CSR associated with that
  // key.
  template <class CsrGen = CertRequestGenerator>
  CertSignRequest prepareTestCsr(
      typename CsrGen::Config config,
      PrivateKey* key) {
    CHECK_OK(GeneratePrivateKey(512, key));
    CsrGen gen(std::move(config));
    CHECK_OK(gen.init());
    CertSignRequest req;
    CHECK_OK(gen.generateRequest(*key, &req));
    return req;
  }

  Cert caCert_;
  PrivateKey caPrivateKey_;
  PublicKey caPublicKey_;

  Cert caExpCert_;
  PrivateKey caExpPrivateKey_;
};

// Check for basic constraints while initializing CertRequestGenerator objects.
TEST_F(CertManagementTest, RequestGeneratorConstraints) {
  const CertRequestGenerator::Config genConfig = prepareConfig("");
  CertRequestGenerator gen(genConfig);
  const Status s = gen.init();
  const string errMsg = s.ToString();
  ASSERT_TRUE(s.IsInvalidArgument()) << errMsg;
  ASSERT_STR_CONTAINS(errMsg, "hostname must not be empty");
}

// Check for the basic functionality of the CertRequestGenerator class:
// check it's able to generate keys of expected number of bits and that it
// reports an error if trying to generate a key of unsupported number of bits.
TEST_F(CertManagementTest, RequestGeneratorBasics) {
  const CertRequestGenerator::Config genConfig = prepareConfig();

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(genConfig);
  ASSERT_OK(gen.init());
  string keyStr;
  ASSERT_OK(key.ToString(&keyStr, DataFormat::PEM));
  // Check for non-supported number of bits for the key.
  Status s = GeneratePrivateKey(7, &key);
  ASSERT_TRUE(s.IsRuntimeError());
}

// Check that CertSigner behaves in a predictable way if given non-matching
// CA private key and certificate.
TEST_F(CertManagementTest, SignerInitWithMismatchedCertAndKey) {
  PrivateKey key;
  const auto& csr = prepareTestCsr(prepareConfig(), &key);
  {
    Cert cert;
    Status s = CertSigner(&caCert_, &caExpPrivateKey_).sign(csr, &cert);

    const string errMsg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << errMsg;
    ASSERT_STR_CONTAINS(errMsg, "certificate does not match private key");
  }
  {
    Cert cert;
    Status s = CertSigner(&caExpCert_, &caPrivateKey_).sign(csr, &cert);
    const string errMsg = s.ToString();
    ASSERT_TRUE(s.IsRuntimeError()) << errMsg;
    ASSERT_STR_CONTAINS(errMsg, "certificate does not match private key");
  }
}

// Check how CertSigner behaves if given expired CA certificate
// and corresponding private key.
TEST_F(CertManagementTest, SignerInitWithExpiredCert) {
  const CertRequestGenerator::Config genConfig = prepareConfig();
  PrivateKey key;
  CertSignRequest req = prepareTestCsr(genConfig, &key);

  // Signer works fine even with expired CA certificate.
  Cert cert;
  ASSERT_OK(CertSigner(&caExpCert_, &caExpPrivateKey_).sign(req, &cert));
  ASSERT_OK(cert.checkKeyMatch(key));
}

// Generate X509 CSR and issue corresponding certificate putting the specified
// hostname into the SAN X509v3 extension field. The fix for KUDU-1981 addresses
// the issue of enabling Kudu server components on systems with FQDN longer than
// 64 characters. This test is a regression for KUDU-1981, so let's verify that
// CSRs and the result X509 cerificates with long hostnames in SAN are handled
// properly.
TEST_F(CertManagementTest, SignCertLongHostnameInSan) {
  for (auto const& hostname : {
           "foo.bar.com",

           "222222222222222222222222222222222222222222222222222222222222222."
           "555555555555555555555555555555555555555555555555555555555555555."
           "555555555555555555555555555555555555555555555555555555555555555."
           "chaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaars",
       }) {
    CertRequestGenerator::Config genConfig;
    genConfig.hostname = hostname;
    genConfig.userId = "test-uid";
    PrivateKey key;
    const auto& csr = prepareTestCsr(genConfig, &key);
    Cert cert;
    ASSERT_OK(CertSigner(&caCert_, &caPrivateKey_).sign(csr, &cert));
    ASSERT_OK(cert.checkKeyMatch(key));

    EXPECT_EQ(
        "C = US, ST = CA, O = MyCompany, CN = MyName, emailAddress = my@email.com",
        cert.issuerName());
    EXPECT_EQ("UID = test-uid", cert.subjectName());
    vector<string> hostnames = cert.hostnames();
    ASSERT_EQ(1, hostnames.size());
    EXPECT_EQ(hostname, hostnames[0]);
  }
}

// Generate X509 CSR and issues corresponding certificate.
TEST_F(CertManagementTest, SignCert) {
  CertRequestGenerator::Config genConfig;
  genConfig.hostname = "foo.bar.com";
  genConfig.userId = "test-uid";
  genConfig.kerberosPrincipal = "kudu/foo.bar.com@bar.com";
  PrivateKey key;
  const auto& csr = prepareTestCsr(genConfig, &key);
  Cert cert;
  ASSERT_OK(CertSigner(&caCert_, &caPrivateKey_).sign(csr, &cert));
  ASSERT_OK(cert.checkKeyMatch(key));

  EXPECT_EQ(
      "C = US, ST = CA, O = MyCompany, CN = MyName, emailAddress = my@email.com",
      cert.issuerName());
  EXPECT_EQ("UID = test-uid", cert.subjectName());
  EXPECT_EQ(genConfig.userId, *cert.userId());
  EXPECT_EQ(genConfig.kerberosPrincipal, *cert.kuduKerberosPrincipal());
  vector<string> hostnames = cert.hostnames();
  ASSERT_EQ(1, hostnames.size());
  EXPECT_EQ("foo.bar.com", hostnames[0]);
}

// Generate X509 CA CSR and sign the result certificate.
TEST_F(CertManagementTest, SignCaCert) {
  const CaCertRequestGenerator::Config genConfig(prepareCaConfig("self-ca"));
  PrivateKey key;
  const auto& csr = prepareTestCsr<CaCertRequestGenerator>(genConfig, &key);
  Cert cert;
  ASSERT_OK(CertSigner(&caCert_, &caPrivateKey_).sign(csr, &cert));
  ASSERT_OK(cert.checkKeyMatch(key));
}

// Test the creation and use of a CA which uses a self-signed CA cert
// generated on the fly.
TEST_F(CertManagementTest, TestSelfSignedCA) {
  PrivateKey caKey;
  Cert caCert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&caKey, &caCert));

  // Create a key and CSR for the tablet server.
  const auto& config = prepareConfig();
  PrivateKey tsKey;
  CertSignRequest tsCsr = prepareTestCsr(config, &tsKey);

  // Sign it using the self-signed CA.
  Cert tsCert;
  ASSERT_OK(CertSigner(&caCert, &caKey).sign(tsCsr, &tsCert));
  ASSERT_OK(tsCert.checkKeyMatch(tsKey));
}

// Check the transformation chains for X509 CSRs:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_F(CertManagementTest, X509CsrFromAndToString) {
  static const DataFormat kFormats[] = {DataFormat::PEM, DataFormat::DER};

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(prepareConfig());
  ASSERT_OK(gen.init());
  CertSignRequest reqRef;
  ASSERT_OK(gen.generateRequest(key, &reqRef));

  for (auto format : kFormats) {
    SCOPED_TRACE(
        fmt::format("X509 CSR format: {}", DataFormatToString(format)));
    string strReqRef;
    ASSERT_OK(reqRef.ToString(&strReqRef, format));
    CertSignRequest req;
    ASSERT_OK(req.FromString(strReqRef, format));
    string strReq;
    ASSERT_OK(req.ToString(&strReq, format));
    ASSERT_EQ(strReqRef, strReq);
  }
}

// Check the transformation chains for X509 certs:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_F(CertManagementTest, X509FromAndToString) {
  static const DataFormat kFormats[] = {DataFormat::PEM, DataFormat::DER};

  PrivateKey key;
  ASSERT_OK(GeneratePrivateKey(1024, &key));
  CertRequestGenerator gen(prepareConfig());
  ASSERT_OK(gen.init());
  CertSignRequest req;
  ASSERT_OK(gen.generateRequest(key, &req));

  Cert certRef;
  ASSERT_OK(CertSigner(&caCert_, &caPrivateKey_).sign(req, &certRef));

  for (auto format : kFormats) {
    SCOPED_TRACE(fmt::format("X509 format: {}", DataFormatToString(format)));
    string strCertRef;
    ASSERT_OK(certRef.ToString(&strCertRef, format));
    Cert cert;
    ASSERT_OK(cert.FromString(strCertRef, format));
    string strCert;
    ASSERT_OK(cert.ToString(&strCert, format));
    ASSERT_EQ(strCertRef, strCert);
  }
}

} // namespace ca
} // namespace security
} // namespace kudu
