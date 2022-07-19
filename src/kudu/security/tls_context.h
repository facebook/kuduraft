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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/port.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_handshake.h"
#include "kudu/util/locks.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"
// IWYU pragma: no_include "kudu/security/cert.h"

DECLARE_bool(create_new_x509_store_each_time);

namespace kudu {
namespace security {

class Cert;           // IWYU pragma: keep
class CertSignRequest;// IWYU pragma: keep
class PrivateKey;

// TlsContext wraps data required by the OpenSSL library for creating and
// accepting TLS protected channels. A single TlsContext instance should be used
// per server or client instance.
//
// Internally, a 'TlsContext' manages a single keypair which it uses for
// terminating TLS connections. It also manages a collection of trusted root CA
// certificates (a trust store), as well as a signed certificate for the
// keypair.
//
// When used on a server, the TlsContext can generate a keypair and a
// self-signed certificate, and provide a CSR for transititioning to a CA-signed
// certificate. This allows Kudu servers to start with a self-signed
// certificate, and later adopt a CA-signed certificate as it becomes available.
// See GenerateSelfSignedCertAndKey(), GetCsrIfNecessary(), and
// AdoptSignedCert() for details on how to generate the keypair and self-signed
// cert, access the CSR, and transtition to a CA-signed cert, repectively.
//
// When used in a client or a server, the TlsContext can immediately adopt a
// private key and CA-signed cert using UseCertificateAndKeyUnlocked(). A TlsContext
// only manages a single keypair, so if UseCertificateAndKeyUnlocked() is called,
// GenerateSelfSignedCertAndKey() must not be called, and vice versa.
//
// TlsContext may be used with or without a keypair and cert to initiate TLS
// connections, when mutual TLS authentication is not needed (for example, for
// token or Kerberos authenticated connections).
//
// This class is thread-safe after initialization.
class TlsContext {

 public:

  TlsContext();

  TlsContext(std::string tls_ciphers, std::string tls_min_protocol);

  ~TlsContext() = default;

  Status Init() WARN_UNUSED_RESULT;

  // Returns true if this TlsContext has been configured with a cert and key for
  // use with TLS-encrypted connections.
  bool has_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return has_cert_;
  }

  // Returns true if this TlsContext has been configured with a CA-signed TLS
  // cert and key for use with TLS-encrypted connections. If this method returns
  // true, then 'has_trusted_cert' will also return true.
  bool has_signed_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return has_cert_ && !csr_;
  }

  // Returns true if this TlsContext has at least one certificate in its trust store.
  bool has_trusted_cert() const {
    shared_lock<RWMutex> lock(lock_);
    return trusted_cert_count_ > 0;
  }

  // Adds 'cert' as a trusted root CA certificate.
  //
  // This determines whether other peers are trusted. It also must be called for
  // any CA certificates that are part of the certificate chain for the cert
  // passed in to 'UseCertificateAndKeyUnlocked()' or 'AdoptSignedCert()'.
  //
  // If this cert has already been marked as trusted, this has no effect.
  // @param use_new_store - do we get the current store from the context
  // and push the chain into it, or do we create a fresh new store and set
  // it finally into the context
  Status AddTrustedCertificateUnlocked(const Cert& cert,
      bool use_new_store = false) WARN_UNUSED_RESULT;

  // The version of above function which takes the context lock and is
  // therefore callable from outside.
  Status AddTrustedCertificate(const Cert& c) WARN_UNUSED_RESULT;

  // Dump all of the certs that are currently trusted by this context, in DER
  // form, into 'cert_ders'.
  // @param der_or_str = true, dump the certificate into DER format
  // @param der_or_str = false, dump some fields of the certificate
  // for debugging.
  Status DumpTrustedCertsUnlocked(bool der_or_str,
      std::vector<std::string>* cert_ders) const WARN_UNUSED_RESULT;

  // Dump all the certs in SSL context. This includes both the CA
  // certificates and the servers cert
  // certs_info : All the certificates in SSL dumped.
  Status DumpCertsInfo(std::vector<std::string> *certs_info) const;

  // Static helper function to dump Issuer, Subject and Validity times.
  static void DumpCertFieldsUnlocked(X509 *x509, std::string *cert_details);

  // Uses 'cert' and 'key' as the cert and key for use with TLS connections.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  Status UseCertificateAndKeyUnlocked(const Cert& cert, const PrivateKey& key) WARN_UNUSED_RESULT;

  // Generates a self-signed cert and key for use with TLS connections.
  //
  // This method should only be used on the server. Once this method is called,
  // 'GetCsrIfNecessary' can be used to retrieve a CSR for generating a
  // CA-signed cert for the generated private key, and 'AdoptSignedCert' can be
  // used to transition to using the CA-signed cert with subsequent TLS
  // connections.
  Status GenerateSelfSignedCertAndKey() WARN_UNUSED_RESULT;

  // Returns a new certificate signing request (CSR) in DER format, if this
  // context's cert is self-signed. If the cert is already signed, returns
  // boost::none.
  boost::optional<CertSignRequest> GetCsrIfNecessary() const;

  // Adopts the provided CA-signed certificate for this TLS context.
  //
  // The certificate must correspond to a CSR previously returned by
  // 'GetCsrIfNecessary()'.
  //
  // Checks that the CA that issued the signature on 'cert' is already trusted
  // by this context (e.g. by AddTrustedCertificate()).
  //
  // This has no effect if the instance already has a CA-signed cert.
  Status AdoptSignedCert(const Cert& cert) WARN_UNUSED_RESULT;

  // Convenience functions for loading cert/CA/key from file paths.
  // -------------------------------------------------------------

  // Reload cert files from disk into tls context and SSL X509 store
  // and SSL context.
  // This should be called on a timer by the application, to
  // refresh external certs
  //
  // @param use_new_store - do we get the current store from the context
  // and push the chain into it, or do we create a fresh new store and set
  // it finally into the context
  Status LoadCertFiles(const std::string& ca_path,
                       const std::string& certificate_path,
                       const std::string& key_path,
                       bool use_new_store) WARN_UNUSED_RESULT;

  // Load the server certificate and key (PEM encoded).
  Status LoadCertificateAndKey(const std::string& certificate_path,
                               const std::string& key_path) WARN_UNUSED_RESULT;

  // Load the server certificate and key (PEM encoded), and use the callback
  // 'password_cb' to obtain the password that can decrypt the key.
  Status LoadCertificateAndPasswordProtectedKey(const std::string& certificate_path,
                                                const std::string& key_path,
                                                const PasswordCallback& password_cb)
                                                WARN_UNUSED_RESULT;

  // Load the certificate authority (PEM encoded).
  Status LoadCertificateAuthority(const std::string& certificate_path) WARN_UNUSED_RESULT;

  void SetEnableNormalTLS(bool enable) {
    enable_normal_tls_ = enable;
  }

  bool GetEnableNormalTLS() const {
    return enable_normal_tls_;
  }

  // Set ALPN protocols. ALPN is a mechanism to multiplex multiple protocols on
  // the same server. And enforcing the protocol match between the client and the
  // server can prevent the cross protocol attack. See https://alpaca-attack.com/.
  Status SetSupportedAlpns(const std::vector<std::string>& alpns, bool is_server);

  static int AlpnSelectCallback(
    SSL* /* ssl */,
    const unsigned char** out,
    unsigned char* outlen,
    const unsigned char* in,
    unsigned int inlen,
    void* data);

  // Create openssl handle
  Status CreateSSL(TlsHandshake* handshake) const WARN_UNUSED_RESULT;

  // Initiates a new TlsHandshake instance.
  Status InitiateHandshake(TlsHandshakeType handshake_type,
                           TlsHandshake* handshake) const WARN_UNUSED_RESULT;

  // Return the number of certs that have been marked as trusted.
  // Used by tests.
  int trusted_cert_count_for_tests() const {
    shared_lock<RWMutex> lock(lock_);
    return trusted_cert_count_;
  }

  bool is_external_cert() const { return is_external_cert_; }

 private:

  Status VerifyCertChainUnlocked(const Cert& cert) WARN_UNUSED_RESULT;

  // The cipher suite preferences to use for TLS-secured RPC connections. Uses the OpenSSL
  // cipher preference list format. See man (1) ciphers for more information.
  std::string tls_ciphers_;

  // The minimum protocol version to allow when for securing RPC connections with TLS. May be
  // one of 'TLSv1', 'TLSv1.1', or 'TLSv1.2'.
  std::string tls_min_protocol_;

  // Protects all members.
  //
  // Taken in write mode when any changes are modifying the underlying SSL_CTX
  // using a mutating method (eg SSL_CTX_use_*) or when changing the value of
  // any of our own member variables.
  mutable RWMutex lock_;
  c_unique_ptr<SSL_CTX> ctx_;
  int32_t trusted_cert_count_;
  bool has_cert_;
  bool is_external_cert_;
  boost::optional<CertSignRequest> csr_;

  bool enable_normal_tls_;

  // alpn protocols in wire format
  std::vector<unsigned char> server_alpns_;
  bool client_alpns_are_set_{false};
};

} // namespace security
} // namespace kudu
