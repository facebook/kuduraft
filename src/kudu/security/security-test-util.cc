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

#include "kudu/security/security-test-util.h"

#include <cstdint>
#include <string>

#include <optional>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace security {

using ca::CaCertRequestGenerator;
using ca::CertSigner;

Status generateSelfSignedCaForTests(PrivateKey* caKey, Cert* caCert) {
  static const int64_t kRootCaCertExpirationSeconds = 24 * 60 * 60;
  // Create a key for the self-signed CA.
  //
  // OpenSSL has a concept of "security levels" which, amongst other things,
  // place certain restrictions on key strength. OpenSSL 1.0 defaults to level
  // 0 (no restrictions) while 1.1 defaults to level 1, which requires RSA keys
  // to have at least 1024 bits. For simplicity, we'll just use 1024 bits here,
  // even though shorter keys would decrease test running time.
  //
  // See
  // https://www.openssl.org/docs/man1.1.0/ssl/SSL_CTX_get_security_level.html
  // for more details.
  RETURN_NOT_OK(generatePrivateKey(1024, caKey));

  CaCertRequestGenerator::Config config = {"test-ca-cn"};
  RETURN_NOT_OK(
      CertSigner::selfSignCa(
          *caKey, config, kRootCaCertExpirationSeconds, caCert));
  return Status::OK();
}

std::ostream& operator<<(std::ostream& o, PkiConfig c) {
  switch (c) {
    case PkiConfig::None:
      o << "None";
      break;
    case PkiConfig::SelfSigned:
      o << "SelfSigned";
      break;
    case PkiConfig::Trusted:
      o << "Trusted";
      break;
    case PkiConfig::Signed:
      o << "Signed";
      break;
    case PkiConfig::ExternallySigned:
      o << "ExternallySigned";
      break;
  }
  return o;
}

Status configureTlsContext(
    PkiConfig config,
    const Cert& caCert,
    const PrivateKey& caKey,
    TlsContext* tlsContext) {
  switch (config) {
    case PkiConfig::None:
      break;
    case PkiConfig::SelfSigned:
      RETURN_NOT_OK(tlsContext->generateSelfSignedCertAndKey());
      break;
    case PkiConfig::Trusted:
      RETURN_NOT_OK(tlsContext->addTrustedCertificate(caCert));
      break;
    case PkiConfig::Signed: {
      RETURN_NOT_OK(tlsContext->addTrustedCertificate(caCert));
      RETURN_NOT_OK(tlsContext->generateSelfSignedCertAndKey());
      Cert cert;
      RETURN_NOT_OK(CertSigner(&caCert, &caKey)
                        .sign(*tlsContext->getCsrIfNecessary(), &cert));
      RETURN_NOT_OK(tlsContext->adoptSignedCert(cert));
      break;
    };
    case PkiConfig::ExternallySigned: {
      std::string certPath, keyPath;
      // Write certificate and private key to file.
      RETURN_NOT_OK(createTestSslCertWithPlainKey(
          GetTestDataDirectory(), &certPath, &keyPath));
      RETURN_NOT_OK(tlsContext->loadCertificateAndKey(certPath, keyPath));
      RETURN_NOT_OK(tlsContext->loadCertificateAuthority(certPath));
    };
  }
  return Status::OK();
}

} // namespace security
} // namespace kudu
