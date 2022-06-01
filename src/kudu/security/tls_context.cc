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

#include "kudu/security/tls_context.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/init.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/user.h"

// Hard code OpenSSL flag values from OpenSSL 1.0.1e[1][2] when compiling
// against OpenSSL 1.0.0 and below. We detect when running against a too-old
// version of OpenSSL using these definitions at runtime so that Kudu has full
// functionality when run against a new OpenSSL version, even if it's compiled
// against an older version.
//
// [1]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/ssl.h#L605-L609
// [2]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/tls1.h#L166-L172
#ifndef SSL_OP_NO_TLSv1
#define SSL_OP_NO_TLSv1 0x04000000U
#endif
#ifndef SSL_OP_NO_TLSv1_1
#define SSL_OP_NO_TLSv1_1 0x10000000U
#endif
#ifndef SSL_OP_NO_TLSv1_3
#define SSL_OP_NO_TLSv1_3 0x20000000U
#endif
#ifndef TLS1_1_VERSION
#define TLS1_1_VERSION 0x0302
#endif
#ifndef TLS1_2_VERSION
#define TLS1_2_VERSION 0x0303
#endif

using strings::Substitute;
using std::string;
using std::unique_lock;
using std::vector;

DEFINE_int32(ipki_server_key_size, 2048,
             "the number of bits for server cert's private key. The server cert "
             "is used for TLS connections to and from clients and other servers.");
TAG_FLAG(ipki_server_key_size, experimental);

DEFINE_bool(create_new_x509_store_each_time, false,
             "use an approach of creating a new x509 store followed by setting it"
             " into SSL context to handle expiration. This is primarily to test"
             " whether rereading new valid certs into a store with expired certs works");
TAG_FLAG(create_new_x509_store_each_time, experimental);

DEFINE_bool(enable_normal_tls, false,
    "Whether to perform normal TLS handshake.");

namespace kudu {
namespace security {

using ca::CertRequestGenerator;

template<> struct SslTypeTraits<SSL> {
  static constexpr auto kFreeFunc = &SSL_free;
};
template<> struct SslTypeTraits<X509_STORE_CTX> {
  static constexpr auto kFreeFunc = &X509_STORE_CTX_free;
};

namespace {

Status CheckMaxSupportedTlsVersion(int tls_version, const char* tls_version_str) {
  // OpenSSL 1.1 and newer supports all of the TLS versions we care about, so
  // the below check is only necessary in older versions of OpenSSL.
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  auto max_supported_tls_version = SSLv23_method()->version;
  DCHECK_GE(max_supported_tls_version, TLS1_VERSION);

  if (max_supported_tls_version < tls_version) {
    return Status::InvalidArgument(
        Substitute("invalid minimum TLS protocol version (--rpc_tls_min_protocol): "
                   "this platform does not support $0", tls_version_str));
  }
#endif
  return Status::OK();
}

// Encode the list of alpn protocols into wire format
std::vector<unsigned char> EncodeAlpn(const std::vector<std::string>& alpns) {
  std::vector<unsigned char> result;
  for (auto p : alpns) {
    CHECK(p.length() <= 255);
    result.push_back((unsigned char)(p.length()));
    for (auto c : p) {
      result.push_back((unsigned char)c);
    }
  }
  return result;
}

} // anonymous namespace

TlsContext::TlsContext()
    : tls_ciphers_(kudu::security::SecurityDefaults::kDefaultTlsCiphers),
      tls_min_protocol_(kudu::security::SecurityDefaults::kDefaultTlsMinVersion),
      lock_(RWMutex::Priority::PREFER_READING),
      trusted_cert_count_(0),
      has_cert_(false),
      is_external_cert_(false),
      enable_normal_tls_(FLAGS_enable_normal_tls) {
  security::InitializeOpenSSL();
}

TlsContext::TlsContext(std::string tls_ciphers, std::string tls_min_protocol)
    : tls_ciphers_(std::move(tls_ciphers)),
      tls_min_protocol_(std::move(tls_min_protocol)),
      lock_(RWMutex::Priority::PREFER_READING),
      trusted_cert_count_(0),
      has_cert_(false),
      is_external_cert_(false),
      enable_normal_tls_(FLAGS_enable_normal_tls) {
  security::InitializeOpenSSL();
}

Status TlsContext::Init() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(!ctx_);

  // NOTE: 'SSLv23 method' sounds like it would enable only SSLv2 and SSLv3, but in fact
  // this is a sort of wildcard which enables all methods (including TLSv1 and later).
  // We explicitly disable SSLv2 and SSLv3 below so that only TLS methods remain.
  // See the discussion on https://trac.torproject.org/projects/tor/ticket/11598 for more
  // info.
  ctx_ = ssl_make_unique(SSL_CTX_new(SSLv23_method()));
  if (!ctx_) {
    return Status::RuntimeError("failed to create TLS context", GetOpenSSLErrors());
  }
  SSL_CTX_set_mode(
      ctx_.get(),
      SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  // Disable SSLv2 and SSLv3 which are vulnerable to various issues such as POODLE.
  // We support versions back to TLSv1.0 since OpenSSL on RHEL 6.4 and earlier does not
  // not support TLSv1.1 or later.
  //
  // Disable SSL/TLS compression to free up CPU resources and be less prone
  // to attacks exploiting the compression feature:
  //   https://tools.ietf.org/html/rfc7525#section-3.3
  auto options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION;

  if (boost::iequals(tls_min_protocol_, "TLSv1.2")) {
    RETURN_NOT_OK(CheckMaxSupportedTlsVersion(TLS1_2_VERSION, "TLSv1.2"));
    options |= SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1;
  } else if (boost::iequals(tls_min_protocol_, "TLSv1.1")) {
    RETURN_NOT_OK(CheckMaxSupportedTlsVersion(TLS1_1_VERSION, "TLSv1.1"));
    options |= SSL_OP_NO_TLSv1;
  } else if (!boost::iequals(tls_min_protocol_, "TLSv1")) {
    return Status::InvalidArgument("unknown value provided for --rpc_tls_min_protocol flag",
                                   tls_min_protocol_);
  }

  // We don't support TLS 1.3 by default
  // because the one-and-a-half-RTT negotiation
  // confuses our RPC negotiation protocol. See KUDU-2871.
  options |= SSL_OP_NO_TLSv1_3;

  SSL_CTX_set_options(ctx_.get(), options);

  OPENSSL_RET_NOT_OK(
      SSL_CTX_set_cipher_list(ctx_.get(), tls_ciphers_.c_str()),
      "failed to set TLS ciphers");

  // Enable ECDH curves. For OpenSSL 1.1.0 and up, this is done automatically.
#ifndef OPENSSL_NO_ECDH
#if OPENSSL_VERSION_NUMBER < 0x10002000L
  // OpenSSL 1.0.1 and below only support setting a single ECDH curve at once.
  // We choose prime256v1 because it's the first curve listed in the "modern
  // compatibility" section of the Mozilla Server Side TLS recommendations,
  // accessed Feb. 2017.
  c_unique_ptr<EC_KEY> ecdh { EC_KEY_new_by_curve_name(NID_X9_62_prime256v1), &EC_KEY_free };
  OPENSSL_RET_IF_NULL(ecdh, "failed to create prime256v1 curve");
  OPENSSL_RET_NOT_OK(SSL_CTX_set_tmp_ecdh(ctx_.get(), ecdh.get()),
                     "failed to set ECDH curve");
#elif OPENSSL_VERSION_NUMBER < 0x10100000L
  // OpenSSL 1.0.2 provides the set_ecdh_auto API which internally figures out
  // the best curve to use.
  OPENSSL_RET_NOT_OK(SSL_CTX_set_ecdh_auto(ctx_.get(), 1),
                     "failed to configure ECDH support");
#endif
#endif

  // TODO(KUDU-1926): is it possible to disable client-side renegotiation? it seems there
  // have been various CVEs related to this feature that we don't need.
  return Status::OK();
}

Status TlsContext::VerifyCertChainUnlocked(const Cert& cert) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  X509_STORE* store = SSL_CTX_get_cert_store(ctx_.get());
  auto store_ctx = ssl_make_unique<X509_STORE_CTX>(X509_STORE_CTX_new());

  OPENSSL_RET_NOT_OK(X509_STORE_CTX_init(store_ctx.get(), store, cert.GetTopOfChainX509(),
                     cert.GetRawData()), "could not init X509_STORE_CTX");
  int rc = X509_verify_cert(store_ctx.get());
  if (rc != 1) {
    int err = X509_STORE_CTX_get_error(store_ctx.get());
    if (err == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
      // It's OK to provide a self-signed cert.
      ERR_clear_error(); // in case it left anything on the queue.
      return Status::OK();
    }

    // Get the cert that failed to verify.
    X509* cur_cert = X509_STORE_CTX_get_current_cert(store_ctx.get());
    string cert_details;
    if (cur_cert) {
      cert_details = Substitute(" (error with cert: subject=$0, issuer=$1)",
                                X509NameToString(X509_get_subject_name(cur_cert)),
                                X509NameToString(X509_get_issuer_name(cur_cert)));
    }

    ERR_clear_error(); // in case it left anything on the queue.
    return Status::RuntimeError(
        Substitute("could not verify certificate chain$0", cert_details),
        X509_verify_cert_error_string(err));
  }
  return Status::OK();
}

Status TlsContext::UseCertificateAndKeyUnlocked(const Cert& cert, const PrivateKey& key) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;

  // Verify that the appropriate CA certs have been loaded into the context
  // before we adopt a cert. Otherwise, client connections without the CA cert
  // available would fail.
  RETURN_NOT_OK(VerifyCertChainUnlocked(cert));

  CHECK(!has_cert_);

  // The order here matters.
  // Look at the documentation of https://linux.die.net/man/3/ssl_ctx_use_privatekey
  // SSL_CTX_use_PrivateKey
  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");
  OPENSSL_RET_NOT_OK(SSL_CTX_use_PrivateKey(ctx_.get(), key.GetRawData()),
                     "failed to use private key");
  has_cert_ = true;
  return Status::OK();
}

Status TlsContext::AddTrustedCertificate(const Cert& c) {
  std::unique_lock<RWMutex> lock(lock_);
  return AddTrustedCertificateUnlocked(c);
}

Status TlsContext::AddTrustedCertificateUnlocked(const Cert& cert, bool use_new_store) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  VLOG(2) << "Trusting certificate " << cert.SubjectName();

  // minor extra penalty of going under lock
  {
    // Workaround for a leak in OpenSSL <1.0.1:
    //
    // If we start trusting a cert, and its internal public-key field hasn't
    // yet been populated, then the first time it's used for verification will
    // populate it. In the case that two threads try to populate it at the same time,
    // one of the thread's copies will be leaked.
    //
    // To avoid triggering the race, we populate the internal public key cache
    // field up front before adding it to the trust store.
    //
    // See OpenSSL commit 33a688e80674aaecfac6d9484ec199daa0ee5b61.
    PublicKey k;
    CHECK_OK(cert.GetPublicKey(&k));
  }

  X509_STORE* cert_store = nullptr;
  if (use_new_store) {
    cert_store = X509_STORE_new();
  } else {
    cert_store = SSL_CTX_get_cert_store(ctx_.get());
  }
  // Iterate through the certificate chain and add each individual certificate to the store.
  for (int i = 0; i < cert.chain_len(); ++i) {
    X509* inner_cert = sk_X509_value(cert.GetRawData(), i);
    int rc = X509_STORE_add_cert(cert_store, inner_cert);
    if (rc <= 0) {
      // Ignore the common case of re-adding a cert that is already in the
      // trust store.
      auto err = ERR_peek_error();
      if (ERR_GET_LIB(err) == ERR_LIB_X509 &&
          ERR_GET_REASON(err) == X509_R_CERT_ALREADY_IN_HASH_TABLE) {
        ERR_clear_error();
        return Status::OK();
      }
      OPENSSL_RET_NOT_OK(rc, "failed to add trusted certificate");
    }
  }
  trusted_cert_count_ += 1;

  if (use_new_store) {
    SSL_CTX_set_cert_store(ctx_.get(), cert_store);
  }
  return Status::OK();
}

Status TlsContext::DumpCertsInfo(std::vector<std::string> *certs_info) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  shared_lock<RWMutex> lock(lock_);
  X509 *x509 = SSL_CTX_get0_certificate(ctx_.get());

  RETURN_NOT_OK(DumpTrustedCertsUnlocked(/*der_or_str = */false, certs_info));

  if (x509) {
    std::string fields;
    DumpCertFieldsUnlocked(x509, &fields);
    certs_info->push_back(fields);
  }

  return Status::OK();
}

void TlsContext::DumpCertFieldsUnlocked(X509 *x509, std::string *cert_details) {
  const ASN1_TIME* notAfter = X509_get0_notAfter(x509);
  int remaining_days_a = 0, remaining_seconds_a = 0;
  int  result = ASN1_TIME_diff(&remaining_days_a, &remaining_seconds_a, NULL, notAfter);

  const ASN1_TIME* notBefore = X509_get0_notBefore(x509);
  int remaining_days_b = 0, remaining_seconds_b = 0;
  result = ASN1_TIME_diff(&remaining_days_b, &remaining_seconds_b, NULL, notBefore);

  *cert_details = Substitute(
      "CERT subject=$0, issuer=$1, notAfterDays=$2, notAfterSeconds=$3,"
      " notBeforeDays=$4, notBeforeSeconds=$5",
      X509NameToString(X509_get_subject_name(x509)),
      X509NameToString(X509_get_issuer_name(x509)),
      remaining_days_a, remaining_seconds_a,
      remaining_days_b, remaining_seconds_b);
}

Status TlsContext::DumpTrustedCertsUnlocked(bool der_or_str, vector<string>* cert_ders) const {
  vector<string> ret;
  auto* cert_store = SSL_CTX_get_cert_store(ctx_.get());

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#define STORE_LOCK(CS) CRYPTO_w_lock(CRYPTO_LOCK_X509_STORE)
#define STORE_UNLOCK(CS) CRYPTO_w_unlock(CRYPTO_LOCK_X509_STORE)
#define STORE_GET_X509_OBJS(CS) (CS)->objs
#define X509_OBJ_GET_TYPE(X509_OBJ) (X509_OBJ)->type
#define X509_OBJ_GET_X509(X509_OBJ) (X509_OBJ)->data.x509
#else
#define STORE_LOCK(CS) CHECK_EQ(1, X509_STORE_lock(CS)) << "Could not lock certificate store"
#define STORE_UNLOCK(CS) CHECK_EQ(1, X509_STORE_unlock(CS)) << "Could not unlock certificate store"
#define STORE_GET_X509_OBJS(CS) X509_STORE_get0_objects(CS)
#define X509_OBJ_GET_TYPE(X509_OBJ) X509_OBJECT_get_type(X509_OBJ)
#define X509_OBJ_GET_X509(X509_OBJ) X509_OBJECT_get0_X509(X509_OBJ)
#endif

  STORE_LOCK(cert_store);
  auto unlock = MakeScopedCleanup([&]() {
      STORE_UNLOCK(cert_store);
    });
  auto* objects = STORE_GET_X509_OBJS(cert_store);
  int num_objects = sk_X509_OBJECT_num(objects);
  for (int i = 0; i < num_objects; i++) {
    auto* obj = sk_X509_OBJECT_value(objects, i);
    if (X509_OBJ_GET_TYPE(obj) != X509_LU_X509) continue;
    auto* x509 = X509_OBJ_GET_X509(obj);
    if (der_or_str) {
      Cert c;
      c.AdoptAndAddRefX509(x509);
      string der;
      RETURN_NOT_OK(c.ToString(&der, DataFormat::DER));
      ret.emplace_back(std::move(der));
    } else {
      string fields;
      DumpCertFieldsUnlocked(x509, &fields);
      ret.emplace_back(std::move(fields));
    }
  }

  cert_ders->swap(ret);
  return Status::OK();
}

namespace {
Status SetCertAttributes(CertRequestGenerator::Config* config) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  RETURN_NOT_OK_PREPEND(GetFQDN(&config->hostname), "could not determine FQDN for CSR");

  // If the server has logged in from a keytab, then we have a 'real' identity,
  // and our desired CN should match the local username mapped from the Kerberos
  // principal name. Otherwise, we'll make up a common name based on the hostname.
  boost::optional<string> principal = GetLoggedInPrincipalFromKeytab();
  if (!principal) {
    string uid;
    RETURN_NOT_OK_PREPEND(GetLoggedInUser(&uid),
                          "couldn't get local username");
    config->user_id = uid;
    return Status::OK();
  }
  string uid;
  RETURN_NOT_OK_PREPEND(security::MapPrincipalToLocalName(*principal, &uid),
                        "could not get local username for krb5 principal");
  config->user_id = uid;
  config->kerberos_principal = *principal;
  return Status::OK();
}
} // anonymous namespace

Status TlsContext::GenerateSelfSignedCertAndKey() {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  // Step 1: generate the private key to be self signed.
  PrivateKey key;
  RETURN_NOT_OK_PREPEND(GeneratePrivateKey(FLAGS_ipki_server_key_size,
                                           &key),
                                           "failed to generate private key");

  // Step 2: generate a CSR so that the self-signed cert can eventually be
  // replaced with a CA-signed cert.
  CertRequestGenerator::Config config;
  RETURN_NOT_OK(SetCertAttributes(&config));
  CertRequestGenerator gen(config);
  RETURN_NOT_OK_PREPEND(gen.Init(), "could not initialize CSR generator");
  CertSignRequest csr;
  RETURN_NOT_OK_PREPEND(gen.GenerateRequest(key, &csr), "could not generate CSR");

  // Step 3: generate a self-signed cert that we can use for terminating TLS
  // connections until we get the CA-signed cert.
  Cert cert;
  RETURN_NOT_OK_PREPEND(ca::CertSigner::SelfSignCert(key, config, &cert),
                        "failed to self-sign cert");

  // Workaround for an OpenSSL memory leak caused by a race in x509v3_cache_extensions.
  // Upon first use of each certificate, this function gets called to parse various
  // fields of the certificate. However, it's racey, so if multiple "first calls"
  // happen concurrently, one call overwrites the cached data from another, causing
  // a leak. Calling this nonsense X509_check_ca() forces the X509 extensions to
  // get cached, so we don't hit the race later. 'VerifyCertChain' also has the
  // effect of triggering the racy codepath.
  ignore_result(X509_check_ca(cert.GetTopOfChainX509()));
  ERR_clear_error(); // in case it left anything on the queue.

  // Step 4: Adopt the new key and cert.
  unique_lock<RWMutex> lock(lock_);
  CHECK(!has_cert_);
  OPENSSL_RET_NOT_OK(SSL_CTX_use_PrivateKey(ctx_.get(), key.GetRawData()),
                     "failed to use private key");
  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");
  has_cert_ = true;
  csr_ = std::move(csr);
  return Status::OK();
}

boost::optional<CertSignRequest> TlsContext::GetCsrIfNecessary() const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  shared_lock<RWMutex> lock(lock_);
  if (csr_) {
    return csr_->Clone();
  }
  return boost::none;
}

// This function is currently not used in prod. Only in unittests
Status TlsContext::AdoptSignedCert(const Cert& cert) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  unique_lock<RWMutex> lock(lock_);

  if (!csr_) {
    // A signed cert has already been adopted.
    return Status::OK();
  }

  // Verify that the appropriate CA certs have been loaded into the context
  // before we adopt a cert. Otherwise, client connections without the CA cert
  // available would fail.
  RETURN_NOT_OK(VerifyCertChainUnlocked(cert));

  PublicKey csr_key;
  RETURN_NOT_OK(csr_->GetPublicKey(&csr_key));
  PublicKey cert_key;
  RETURN_NOT_OK(cert.GetPublicKey(&cert_key));
  bool equals;
  RETURN_NOT_OK(csr_key.Equals(cert_key, &equals));
  if (!equals) {
    return Status::RuntimeError("certificate public key does not match the CSR public key");
  }

  OPENSSL_RET_NOT_OK(SSL_CTX_use_certificate(ctx_.get(), cert.GetTopOfChainX509()),
                     "failed to use certificate");

  // This should never fail since we already compared the cert's public key
  // against the CSR, but better safe than sorry. If this *does* fail, it
  // appears to remove the private key from the SSL_CTX, so we are left in a bad
  // state.
  OPENSSL_CHECK_OK(SSL_CTX_check_private_key(ctx_.get()))
    << "certificate does not match the private key";

  csr_ = boost::none;

  return Status::OK();
}

Status TlsContext::LoadCertificateAndKey(const string& certificate_path,
                                         const string& key_path) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));
  PrivateKey k;
  RETURN_NOT_OK(k.FromFile(key_path, DataFormat::PEM));

  // Verify that the cert and key match.
  RETURN_NOT_OK(c.CheckKeyMatch(k));

  std::unique_lock<RWMutex> lock(lock_);
  RETURN_NOT_OK(UseCertificateAndKeyUnlocked(c, k));
  is_external_cert_ = true;
  return Status::OK();
}

Status TlsContext::LoadCertificateAndPasswordProtectedKey(const string& certificate_path,
                                                          const string& key_path,
                                                          const PasswordCallback& password_cb) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert c;
  RETURN_NOT_OK_PREPEND(c.FromFile(certificate_path, DataFormat::PEM),
                        "failed to load certificate");
  PrivateKey k;
  RETURN_NOT_OK_PREPEND(k.FromFile(key_path, DataFormat::PEM, password_cb),
                        "failed to load private key file");
  // Verify that the cert and key match.
  RETURN_NOT_OK(c.CheckKeyMatch(k));

  std::unique_lock<RWMutex> lock(lock_);

  RETURN_NOT_OK(UseCertificateAndKeyUnlocked(c, k));
  is_external_cert_ = true;
  return Status::OK();
}

Status TlsContext::LoadCertificateAuthority(const string& certificate_path) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));

  std::unique_lock<RWMutex> lock(lock_);
  if (has_cert_) DCHECK(is_external_cert_);
  return AddTrustedCertificateUnlocked(c);
}

Status TlsContext::LoadCertFiles(const string& ca_path,
                                 const string& certificate_path,
                                 const string& key_path,
                                 bool use_new_store) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  Cert ca_cert;
  RETURN_NOT_OK(ca_cert.FromFile(ca_path, DataFormat::PEM));

  Cert c;
  RETURN_NOT_OK(c.FromFile(certificate_path, DataFormat::PEM));

  PrivateKey k;
  RETURN_NOT_OK(k.FromFile(key_path, DataFormat::PEM));

  // Verify that the cert and key match.
  RETURN_NOT_OK(c.CheckKeyMatch(k));

  std::unique_lock<RWMutex> lock(lock_);
  has_cert_ = false;
  is_external_cert_ = false;
  trusted_cert_count_ = 0;

  RETURN_NOT_OK(AddTrustedCertificateUnlocked(ca_cert, use_new_store));

  RETURN_NOT_OK(UseCertificateAndKeyUnlocked(c, k));

  is_external_cert_ = true;
  return Status::OK();
}

Status TlsContext::SetSupportedAlpns(
    const std::vector<std::string>& alpns,
    bool is_server) {
  if (!alpns_.empty()) {
    return Status::OK();
  }

  CHECK(ctx_);
  alpns_ = EncodeAlpn(alpns);

  if (is_server) {
    SSL_CTX_set_alpn_select_cb(ctx_.get(), AlpnSelectCallback, this);
  } else {
    // SSL_CTX_set_alpn_protos return 0 on success
    if (SSL_CTX_set_alpn_protos(
          ctx_.get(),
          alpns_.data(),
          alpns_.size()) != 0) {
      return Status::RuntimeError("failed to set alpn protocols", GetOpenSSLErrors());
    }
  }

  return Status::OK();
}

int TlsContext::AlpnSelectCallback(
    SSL* /* ssl */,
    const unsigned char** out,
    unsigned char* outlen,
    const unsigned char* in,
    unsigned int inlen,
    void* data) {
  auto context = (TlsContext*)data;
  CHECK(context);
  if (context->alpns_.empty()) {
    *out = nullptr;
    *outlen = 0;
  } else if (SSL_select_next_proto(
      (unsigned char**)out,
      outlen,
      context->alpns_.data(),
      context->alpns_.size(),
      in,
      inlen) != OPENSSL_NPN_NEGOTIATED) {
    return SSL_TLSEXT_ERR_ALERT_FATAL;
  }
  return SSL_TLSEXT_ERR_OK;
}

Status TlsContext::CreateSSL(TlsHandshake* handshake) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ctx_);
  CHECK(!handshake->ssl_);
  {
    shared_lock<RWMutex> lock(lock_);
    handshake->adopt_ssl(ssl_make_unique(SSL_new(ctx_.get())));
  }
  if (!handshake->ssl_) {
    return Status::RuntimeError("failed to create SSL handle", GetOpenSSLErrors());
  }

  return Status::OK();
}

Status TlsContext::InitiateHandshake(TlsHandshakeType handshake_type,
                                     TlsHandshake* handshake) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  RETURN_NOT_OK(CreateSSL(handshake));

  SSL_set_bio(handshake->ssl(),
              BIO_new(BIO_s_mem()),
              BIO_new(BIO_s_mem()));

  switch (handshake_type) {
    case TlsHandshakeType::SERVER:
      SSL_set_accept_state(handshake->ssl());
      break;
    case TlsHandshakeType::CLIENT:
      SSL_set_connect_state(handshake->ssl());
      break;
  }

  return Status::OK();
}

} // namespace security
} // namespace kudu
