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

#include "kudu/security/cert.h"

#include <memory>
#include <mutex>
#include <ostream>
#include <string>

#include <glog/logging.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "kudu/gutil/macros.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/openssl_util_bio.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;

namespace kudu {
namespace security {

template <>
struct SslTypeTraits<GENERAL_NAMES> {
  static constexpr auto kFreeFunc = &GENERAL_NAMES_free;
};

// This OID is generated via the UUID method.
static const char* kKuduKerberosPrincipalOidStr =
    "2.25.243346677289068076843480765133256509912";

string x509NameToString(X509_NAME* name) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(name);
  auto bio = sslMakeUnique(BIO_new(BIO_s_mem()));
  OPENSSL_CHECK_OK(X509_NAME_print_ex(bio.get(), name, 0, XN_FLAG_ONELINE));

  BUF_MEM* membuf;
  OPENSSL_CHECK_OK(BIO_get_mem_ptr(bio.get(), &membuf));
  return string(membuf->data, membuf->length);
}

int getKuduKerberosPrincipalOidNid() {
  initializeOpenSsl();
  static std::once_flag flag;
  static int nid;
  std::call_once(flag, [&]() {
    nid = OBJ_create(
        kKuduKerberosPrincipalOidStr, "kuduPrinc", "kuduKerberosPrincipal");
    CHECK_NE(nid, NID_undef)
        << "failed to create kuduPrinc oid: " << getOpenSslErrors();
  });
  return nid;
}

X509* Cert::getTopOfChainX509() const {
  CHECK_GT(chainLen(), 0);
  return sk_X509_value(data_.get(), 0);
}

Status Cert::fromString(const std::string& data, DataFormat format) {
  RETURN_NOT_OK(::kudu::security::fromString(data, format, &data_));
  if (sk_X509_num(data_.get()) < 1) {
    return Status::RuntimeError(
        "Certificate chain is empty. Expected at least one certificate.");
  }
  return Status::OK();
}

Status Cert::toString(std::string* data, DataFormat format) const {
  return ::kudu::security::toString(data, format, data_.get());
}

Status Cert::fromFile(const std::string& fpath, DataFormat format) {
  RETURN_NOT_OK(::kudu::security::fromFile(fpath, format, &data_));
  if (sk_X509_num(data_.get()) < 1) {
    return Status::RuntimeError(
        "Certificate chain is empty. Expected at least one certificate.");
  }
  return Status::OK();
}

string Cert::subjectName() const {
  return x509NameToString(X509_get_subject_name(getTopOfChainX509()));
}

string Cert::issuerName() const {
  return x509NameToString(X509_get_issuer_name(getTopOfChainX509()));
}

std::optional<string> Cert::userId() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  X509_NAME* name = X509_get_subject_name(getTopOfChainX509());
  char buf[1024];
  int len = X509_NAME_get_text_by_NID(name, NID_userId, buf, arraysize(buf));
  if (len < 0) {
    return {};
  }
  return string(buf, len);
}

std::optional<string> Cert::commonName() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  X509_NAME* name = X509_get_subject_name(getTopOfChainX509());
  if (!name) {
    return {};
  }
  char buf[1024];
  int len =
      X509_NAME_get_text_by_NID(name, NID_commonName, buf, arraysize(buf));
  if (len < 0) {
    return {};
  }
  return string(buf, len);
}

vector<string> Cert::hostnames() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  vector<string> result;
  auto gens = sslMakeUnique(
      reinterpret_cast<GENERAL_NAMES*>(X509_get_ext_d2i(
          getTopOfChainX509(), NID_subject_alt_name, nullptr, nullptr)));
  if (gens) {
    for (int i = 0; i < sk_GENERAL_NAME_num(gens.get()); ++i) {
      GENERAL_NAME* gen = sk_GENERAL_NAME_value(gens.get(), i);
      if (gen->type != GEN_DNS) {
        continue;
      }
      const ASN1_STRING* cstr = gen->d.dNSName;
      if (cstr->type != V_ASN1_IA5STRING || cstr->data == nullptr) {
        LOG(DFATAL) << "invalid DNS name in the SAN field";
        return {};
      }
      result.emplace_back(reinterpret_cast<char*>(cstr->data), cstr->length);
    }
  }
  return result;
}

std::optional<string> Cert::kuduKerberosPrincipal() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  int idx = X509_get_ext_by_NID(
      getTopOfChainX509(), getKuduKerberosPrincipalOidNid(), -1);
  if (idx < 0) {
    return {};
  }
  X509_EXTENSION* ext = X509_get_ext(getTopOfChainX509(), idx);
  ASN1_OCTET_STRING* octetStr = X509_EXTENSION_get_data(ext);
  const unsigned char* octetStrData = octetStr->data;
  long len; // NOLINT
  int tag, xclass;
  if (ASN1_get_object(&octetStrData, &len, &tag, &xclass, octetStr->length) !=
          0 ||
      tag != V_ASN1_UTF8STRING) {
    LOG(DFATAL) << "invalid extension value in cert " << subjectName();
    return {};
  }

  return string(reinterpret_cast<const char*>(octetStrData), len);
}

Status Cert::checkKeyMatch(const PrivateKey& key) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  OPENSSL_RET_NOT_OK(
      X509_check_private_key(getTopOfChainX509(), key.getRawData()),
      "certificate does not match private key");
  return Status::OK();
}

void Cert::adoptAndAddRefRawData(RawDataType* data) {
  DCHECK_EQ(sk_X509_num(data), 1);
  X509* cert = sk_X509_value(data, sk_X509_num(data) - 1);

  DCHECK(cert);
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "OpenSSL < 1.1.0 - need to update"
#else
  OPENSSL_CHECK_OK(X509_up_ref(cert))
      << "X509 use-after-free detected: " << getOpenSslErrors();
#endif
  // We copy the STACK_OF() object, but the copy and the original both
  // internally point to the same elements.
  adoptRawData(sk_X509_dup(data));
}

void Cert::adoptX509(X509* cert) {
  // Free current STACK_OF(X509).
  sk_X509_pop_free(data_.get(), X509_free);
  // Allocate new STACK_OF(X509) and populate with 'cert'.
  STACK_OF(X509)* sk = sk_X509_new_null();
  DCHECK(sk);
  sk_X509_push(sk, cert);
  adoptRawData(sk);
}

void Cert::adoptAndAddRefX509(X509* cert) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "OpenSSL < 1.1.0 - need to update"
#else
  OPENSSL_CHECK_OK(X509_up_ref(cert))
      << "X509 use-after-free detected: " << getOpenSslErrors();
#endif
  adoptX509(cert);
}

Status Cert::getPublicKey(PublicKey* key) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  EVP_PKEY* rawKey = X509_get_pubkey(getTopOfChainX509());
  OPENSSL_RET_IF_NULL(rawKey, "unable to get certificate public key");
  key->adoptRawData(rawKey);
  return Status::OK();
}

Status CertSignRequest::fromString(const std::string& data, DataFormat format) {
  return ::kudu::security::fromString(data, format, &data_);
}

Status CertSignRequest::toString(std::string* data, DataFormat format) const {
  return ::kudu::security::toString(data, format, data_.get());
}

CertSignRequest CertSignRequest::clone() const {
  X509_REQ* clonedReq;
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "OpenSSL < 1.1.0 - need to update"
#else
  // With OpenSSL 1.1, data structure internals are hidden, and there doesn't
  // seem to be a public method that increments data_'s refcount.
  clonedReq = X509_REQ_dup(getRawData());
  CHECK(clonedReq != nullptr)
      << "X509 allocation failure detected: " << getOpenSslErrors();
#endif

  CertSignRequest clone;
  clone.adoptRawData(clonedReq);
  return clone;
}

Status CertSignRequest::getPublicKey(PublicKey* key) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  EVP_PKEY* rawKey = X509_REQ_get_pubkey(data_.get());
  OPENSSL_RET_IF_NULL(rawKey, "unable to get CSR public key");
  key->adoptRawData(rawKey);
  return Status::OK();
}

} // namespace security
} // namespace kudu
