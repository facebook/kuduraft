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

#include <algorithm>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>

#include <glog/logging.h>
#include <openssl/conf.h>
#ifndef OPENSSL_NO_ENGINE
#include <openssl/engine.h>
#endif
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/status.h"

using std::lock_guard;
using std::string;

namespace kudu {
namespace security {

template <>
struct SslTypeTraits<ASN1_INTEGER> {
  static constexpr auto kFreeFunc = &ASN1_INTEGER_free;
};
template <>
struct SslTypeTraits<BIGNUM> {
  static constexpr auto kFreeFunc = &BN_free;
};

namespace ca {

namespace {

Status setSubjectNameField(
    X509_NAME* name,
    const char* fieldCode,
    const string& fieldValue) {
  CHECK(name);
  CHECK(fieldCode);
  OPENSSL_RET_NOT_OK(
      X509_NAME_add_entry_by_txt(
          name,
          fieldCode,
          MBSTRING_ASC,
          reinterpret_cast<const unsigned char*>(fieldValue.c_str()),
          -1,
          -1,
          0),
      fmt::format("error setting subject field {}", fieldCode));
  return Status::OK();
}

} // anonymous namespace

CertRequestGenerator::~CertRequestGenerator() {
  sk_X509_EXTENSION_pop_free(extensions_, X509_EXTENSION_free);
}

Status CertRequestGeneratorBase::GenerateRequest(
    const PrivateKey& key,
    CertSignRequest* ret) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ret);
  CHECK(Initialized());
  auto req = ssl_make_unique(X509_REQ_new());
  OPENSSL_RET_NOT_OK(
      X509_REQ_set_pubkey(req.get(), key.GetRawData()),
      "error setting X509 public key");

  // Populate the subject field of the request.
  RETURN_NOT_OK(SetSubject(req.get()));

  // Set necessary extensions into the request.
  RETURN_NOT_OK(SetExtensions(req.get()));

  // And finally sign the result.
  OPENSSL_RET_NOT_OK(
      X509_REQ_sign(req.get(), key.GetRawData(), EVP_sha256()),
      "error signing X509 request");
  ret->AdoptRawData(req.release());

  return Status::OK();
}

Status CertRequestGeneratorBase::PushExtension(
    stack_st_X509_EXTENSION* st,
    int32_t nid,
    StringPiece value) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  auto ex = ssl_make_unique(X509V3_EXT_conf_nid(
      nullptr, nullptr, nid, const_cast<char*>(value.data())));
  OPENSSL_RET_IF_NULL(ex, "error configuring extension");
  OPENSSL_RET_NOT_OK(
      sk_X509_EXTENSION_push(st, ex.release()),
      "error pushing extension into the stack");
  return Status::OK();
}

CertRequestGenerator::CertRequestGenerator(Config config)
    : CertRequestGeneratorBase(), config_(std::move(config)) {}

Status CertRequestGenerator::Init() {
  InitializeOpenSSL();
  SCOPED_OPENSSL_NO_PENDING_ERRORS;

  CHECK(!isInitialized_);

  // Build the SAN field using the specified hostname. In general, it might be
  // multiple DNS hostnames in the field, but in our use-cases it's always one.
  if (config_.hostname.empty()) {
    return Status::InvalidArgument("hostname must not be empty");
  }
  const string sanHosts = fmt::format("DNS.0:{}", config_.hostname);

  extensions_ = sk_X509_EXTENSION_new_null();

  // Permitted usages for the generated keys is set via X509 V3
  // standard/extended key usage attributes.
  // See https://www.openssl.org/docs/man1.0.1/apps/x509v3_config.html
  // for details.

  // The generated certificates are for using as TLS certificates for
  // both client and server.
  string usage = "critical,digitalSignature,keyEncipherment";
  if (forSelfSigning_) {
    // If we are generating a CSR for self-signing, then we need to
    // add this keyUsage attribute. See https://s.apache.org/BFHk
    usage += ",keyCertSign";
  }

  RETURN_NOT_OK(PushExtension(extensions_, NID_key_usage, usage));
  // The generated certificates should be good for authentication
  // of a server to a client and vice versa: the intended users of the
  // certificates are tablet servers which are going to talk to master
  // and other tablet servers via TLS channels.
  RETURN_NOT_OK(PushExtension(
      extensions_, NID_ext_key_usage, "critical,serverAuth,clientAuth"));

  // The generated certificates are not intended to be used as CA certificates
  // (i.e. they cannot be used to sign/issue certificates).
  RETURN_NOT_OK(
      PushExtension(extensions_, NID_basic_constraints, "critical,CA:FALSE"));

  if (config_.kerberosPrincipal) {
    int nid = GetKuduKerberosPrincipalOidNid();
    RETURN_NOT_OK(PushExtension(
        extensions_,
        nid,
        fmt::format("ASN1:UTF8:{}", *config_.kerberosPrincipal)));
  }
  RETURN_NOT_OK(PushExtension(extensions_, NID_subject_alt_name, sanHosts));

  isInitialized_ = true;

  return Status::OK();
}

bool CertRequestGenerator::Initialized() const {
  return isInitialized_;
}

Status CertRequestGenerator::SetSubject(X509_REQ* req) const {
  if (config_.userId) {
    RETURN_NOT_OK(setSubjectNameField(
        X509_REQ_get_subject_name(req), "UID", *config_.userId));
  }
  return Status::OK();
}

Status CertRequestGenerator::SetExtensions(X509_REQ* req) const {
  OPENSSL_RET_NOT_OK(
      X509_REQ_add_extensions(req, extensions_),
      "error setting X509 request extensions");
  return Status::OK();
}

CaCertRequestGenerator::CaCertRequestGenerator(Config config)
    : config_(std::move(config)), extensions_(nullptr), isInitialized_(false) {}

CaCertRequestGenerator::~CaCertRequestGenerator() {
  sk_X509_EXTENSION_pop_free(extensions_, X509_EXTENSION_free);
}

Status CaCertRequestGenerator::Init() {
  InitializeOpenSSL();
  SCOPED_OPENSSL_NO_PENDING_ERRORS;

  lock_guard<simple_spinlock> guard(lock_);
  if (isInitialized_) {
    return Status::OK();
  }
  if (config_.cn.empty()) {
    return Status::InvalidArgument("missing CA service UUID/name");
  }

  extensions_ = sk_X509_EXTENSION_new_null();

  // Permitted usages for the generated keys is set via X509 V3
  // standard/extended key usage attributes.
  // See https://www.openssl.org/docs/man1.0.1/apps/x509v3_config.html
  // for details.

  // The target ceritifcate is a CA certificate: it's for signing X509 certs.
  RETURN_NOT_OK(
      PushExtension(extensions_, NID_key_usage, "critical,keyCertSign"));
  // The generated certificates are for the private CA service.
  RETURN_NOT_OK(
      PushExtension(extensions_, NID_basic_constraints, "critical,CA:TRUE"));
  isInitialized_ = true;

  return Status::OK();
}

bool CaCertRequestGenerator::Initialized() const {
  lock_guard<simple_spinlock> guard(lock_);
  return isInitialized_;
}

Status CaCertRequestGenerator::SetSubject(X509_REQ* req) const {
  return setSubjectNameField(X509_REQ_get_subject_name(req), "CN", config_.cn);
}

Status CaCertRequestGenerator::SetExtensions(X509_REQ* req) const {
  OPENSSL_RET_NOT_OK(
      X509_REQ_add_extensions(req, extensions_),
      "error setting X509 request extensions");
  return Status::OK();
}

Status CertSigner::SelfSignCA(
    const PrivateKey& key,
    CaCertRequestGenerator::Config config,
    int64_t certExpirationSeconds,
    Cert* cert) {
  // Generate a CSR for the CA.
  CertSignRequest caCsr;
  {
    CaCertRequestGenerator gen(std::move(config));
    RETURN_NOT_OK(gen.Init());
    RETURN_NOT_OK(gen.GenerateRequest(key, &caCsr));
  }

  // Self-sign the CA's CSR.
  return CertSigner(nullptr, &key)
      .setExpirationInterval(MonoDelta::FromSeconds(certExpirationSeconds))
      .Sign(caCsr, cert);
}

Status CertSigner::SelfSignCert(
    const PrivateKey& key,
    CertRequestGenerator::Config config,
    Cert* cert) {
  // Generate a CSR.
  CertSignRequest csr;
  {
    CertRequestGenerator gen(std::move(config));
    gen.enableSelfSigning();
    RETURN_NOT_OK(gen.Init());
    RETURN_NOT_OK(gen.GenerateRequest(key, &csr));
  }

  // Self-sign the CSR with the key.
  return CertSigner(nullptr, &key).Sign(csr, cert);
}

CertSigner::CertSigner(const Cert* ca_cert, const PrivateKey* ca_private_key)
    : caCert_(ca_cert), caPrivateKey_(ca_private_key) {
  // Private key is required.
  CHECK(caPrivateKey_ && caPrivateKey_->GetRawData());
  // The cert is optional, but if we have it, it should be initialized.
  CHECK(!caCert_ || caCert_->GetRawData());
}

Status CertSigner::Sign(const CertSignRequest& req, Cert* ret) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  InitializeOpenSSL();
  CHECK(ret);

  // If we are not self-signing, then make sure that the provided CA
  // cert and key match each other. Technically this would be programmer
  // error since we're always using internally-generated CA certs, but
  // this isn't a hot path so we'll keep the extra safety.
  if (caCert_) {
    RETURN_NOT_OK(caCert_->CheckKeyMatch(*caPrivateKey_));
  }
  auto x509 = ssl_make_unique(X509_new());
  RETURN_NOT_OK(FillCertTemplateFromRequest(req.GetRawData(), x509.get()));
  RETURN_NOT_OK(DoSign(EVP_sha256(), expIntervalSec_, x509.get()));
  ret->AdoptX509(x509.release());

  return Status::OK();
}

// This is modeled after code in copy_extensions() function from
// $OPENSSL_ROOT/apps/apps.c with OpenSSL 1.0.2.
Status CertSigner::CopyExtensions(X509_REQ* req, X509* x) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(req);
  CHECK(x);
  STACK_OF(X509_EXTENSION)* exts = X509_REQ_get_extensions(req);
  SCOPE_EXIT {
    sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
  };
  for (size_t i = 0; i < sk_X509_EXTENSION_num(exts); ++i) {
    X509_EXTENSION* ext = sk_X509_EXTENSION_value(exts, i);
    ASN1_OBJECT* obj = X509_EXTENSION_get_object(ext);
    int32_t idx = X509_get_ext_by_OBJ(x, obj, -1);
    if (idx != -1) {
      // If extension exits, delete all extensions of same type.
      do {
        auto tmpext = ssl_make_unique(X509_get_ext(x, idx));
        X509_delete_ext(x, idx);
        idx = X509_get_ext_by_OBJ(x, obj, -1);
      } while (idx != -1);
    }
    OPENSSL_RET_NOT_OK(X509_add_ext(x, ext, -1), "error adding extension");
  }

  return Status::OK();
}

Status CertSigner::FillCertTemplateFromRequest(X509_REQ* req, X509* tmpl) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(req);

  // As of OpenSSL 1.1, req's internals are hidden.
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "OpenSSL < 1.1.0 - need to update"
#endif
  auto pubKey = ssl_make_unique(X509_REQ_get_pubkey(req));
  OPENSSL_RET_IF_NULL(pubKey, "error unpacking public key from CSR");
  const int rc = X509_REQ_verify(req, pubKey.get());
  if (rc < 0) {
    return Status::RuntimeError(
        "CSR signature verification error", GetOpenSSLErrors());
  }
  if (rc == 0) {
    return Status::RuntimeError("CSR signature mismatch", GetOpenSSLErrors());
  }
  OPENSSL_RET_NOT_OK(
      X509_set_subject_name(tmpl, X509_REQ_get_subject_name(req)),
      "error setting cert subject name");
  RETURN_NOT_OK(CopyExtensions(req, tmpl));
  OPENSSL_RET_NOT_OK(
      X509_set_pubkey(tmpl, pubKey.get()), "error setting cert public key");
  return Status::OK();
}

Status CertSigner::DigestSign(const EVP_MD* md, EVP_PKEY* pkey, X509* x) {
  OPENSSL_RET_NOT_OK(X509_sign(x, pkey, md), "error signing certificate");
  return Status::OK();
}

Status CertSigner::GenerateSerial(c_unique_ptr<ASN1_INTEGER>* ret) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  auto btmp = ssl_make_unique(BN_new());
  OPENSSL_RET_NOT_OK(
      BN_pseudo_rand(btmp.get(), 64, 0, 0), "error generating random number");
  auto serial = ssl_make_unique(ASN1_INTEGER_new());
  OPENSSL_RET_IF_NULL(
      BN_to_ASN1_INTEGER(btmp.get(), serial.get()),
      "error converting number into ASN1 representation");
  if (ret) {
    ret->swap(serial);
  }
  return Status::OK();
}

Status CertSigner::DoSign(const EVP_MD* digest, int32_t expSeconds, X509* ret)
    const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ret);

  // Version 3 (v3) of X509 certificates. The integer value is one less
  // than the version it represents. This is not a typo. :)
  static const int kX509V3 = 2;

  // If we have a CA cert, then the CA is the issuer.
  // Otherwise, we are self-signing so the target cert is also the issuer.
  X509* issuerCert = caCert_ ? caCert_->GetTopOfChainX509() : ret;
  X509_NAME* issuerName = X509_get_subject_name(issuerCert);
  OPENSSL_RET_NOT_OK(
      X509_set_issuer_name(ret, issuerName), "error setting issuer name");
  c_unique_ptr<ASN1_INTEGER> serial;
  RETURN_NOT_OK(GenerateSerial(&serial));
  // set version to v3
  OPENSSL_RET_NOT_OK(
      X509_set_version(ret, kX509V3), "error setting cert version");
  OPENSSL_RET_NOT_OK(
      X509_set_serialNumber(ret, serial.get()), "error setting cert serial");
  OPENSSL_RET_IF_NULL(
      X509_gmtime_adj(X509_get_notBefore(ret), 0L),
      "error setting cert validity time");
  OPENSSL_RET_IF_NULL(
      X509_gmtime_adj(X509_get_notAfter(ret), expSeconds),
      "error setting cert expiration time");
  RETURN_NOT_OK(DigestSign(digest, caPrivateKey_->GetRawData(), ret));

  return Status::OK();
}

} // namespace ca
} // namespace security
} // namespace kudu
