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

#include "kudu/security/crypto.h"

#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>
#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
#include <openssl/ossl_typ.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

#include <fmt/core.h>
#include "kudu/security/openssl_util.h"
#include "kudu/security/openssl_util_bio.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace security {

const size_t kNonceSize = 16;

namespace {

// Writing the private key from an EVP_PKEY has a different
// signature than the rest of the write functions, so we
// have to provide this wrapper.
int pemWritePrivateKey(BIO* bio, EVP_PKEY* key) {
  auto rsa = ssl_make_unique(EVP_PKEY_get1_RSA(key));
  return PEM_write_bio_RSAPrivateKey(
      bio, rsa.get(), nullptr, nullptr, 0, nullptr, nullptr);
}

int pemWritePublicKey(BIO* bio, EVP_PKEY* key) {
  auto rsa = ssl_make_unique(EVP_PKEY_get1_RSA(key));
  return PEM_write_bio_RSA_PUBKEY(bio, rsa.get());
}

int derWritePublicKey(BIO* bio, EVP_PKEY* key) {
  auto rsa = ssl_make_unique(EVP_PKEY_get1_RSA(key));
  return i2d_RSA_PUBKEY_bio(bio, rsa.get());
}

} // anonymous namespace

template <>
struct SslTypeTraits<BIGNUM> {
  static constexpr auto kFreeFunc = &BN_free;
};
struct RsaPrivateKeyTraits : public SslTypeTraits<EVP_PKEY> {
  static constexpr auto kReadPemFunc = &PEM_read_bio_PrivateKey;
  static constexpr auto kReadDerFunc = &d2i_PrivateKey_bio;
  static constexpr auto kWritePemFunc = &pemWritePrivateKey;
  static constexpr auto kWriteDerFunc = &i2d_PrivateKey_bio;
};
struct RsaPublicKeyTraits : public SslTypeTraits<EVP_PKEY> {
  static constexpr auto kReadPemFunc = &PEM_read_bio_PUBKEY;
  static constexpr auto kReadDerFunc = &d2i_PUBKEY_bio;
  static constexpr auto kWritePemFunc = &pemWritePublicKey;
  static constexpr auto kWriteDerFunc = &derWritePublicKey;
};
template <>
struct SslTypeTraits<RSA> {
  static constexpr auto kFreeFunc = &RSA_free;
};
template <>
struct SslTypeTraits<EVP_MD_CTX> {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "OpenSSL < 1.1.0 - need to update"
#else
  static constexpr auto kFreeFunc = &EVP_MD_CTX_free;
#endif
};

namespace {

const EVP_MD* getMessageDigest(DigestType digestType) {
  switch (digestType) {
    case DigestType::SHA256:
      return EVP_sha256();
    case DigestType::SHA512:
      return EVP_sha512();
  }
  LOG(FATAL) << "unknown digest type";
}

} // anonymous namespace

Status PublicKey::FromString(const std::string& data, DataFormat format) {
  return ::kudu::security::fromString<RawDataType, RsaPublicKeyTraits>(
      data, format, &data_);
}

Status PublicKey::ToString(std::string* data, DataFormat format) const {
  return ::kudu::security::toString<RawDataType, RsaPublicKeyTraits>(
      data, format, data_.get());
}

Status PublicKey::FromFile(const std::string& fpath, DataFormat format) {
  return ::kudu::security::fromFile<RawDataType, RsaPublicKeyTraits>(
      fpath, format, &data_);
}

Status PublicKey::FromBIO(BIO* bio, DataFormat format) {
  return ::kudu::security::fromBio<RawDataType, RsaPublicKeyTraits>(
      bio, format, &data_);
}

// Modeled after code in $OPENSSL_ROOT/apps/dgst.c
Status PublicKey::VerifySignature(
    DigestType digest,
    const std::string& data,
    const std::string& signature) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  const EVP_MD* md = getMessageDigest(digest);
  auto mdCtx = ssl_make_unique(EVP_MD_CTX_create());

  OPENSSL_RET_NOT_OK(
      EVP_DigestVerifyInit(mdCtx.get(), nullptr, md, nullptr, GetRawData()),
      "error initializing verification digest");
  OPENSSL_RET_NOT_OK(
      EVP_DigestVerifyUpdate(mdCtx.get(), data.data(), data.size()),
      "error verifying data signature");
#if OPENSSL_VERSION_NUMBER < 0x10002000L
#error "This old OpenSSL version is not supported"
#else
  const unsigned char* sigData =
      reinterpret_cast<const unsigned char*>(signature.data());
#endif
  // The success is indicated by return code 1. All other values means
  // either wrong signature or error while performing signature verification.
  const int rc = EVP_DigestVerifyFinal(mdCtx.get(), sigData, signature.size());
  if (rc < 0 || rc > 1) {
    return Status::RuntimeError(
        fmt::format("error verifying data signature: {}", GetOpenSSLErrors()));
  }
  if (rc == 0) {
    // No sense stringifying the internal OpenSSL error, since a bad
    // verification is self-explanatory.
    ERR_clear_error();
    return Status::Corruption("data signature verification failed");
  }

  return Status::OK();
}

Status PublicKey::Equals(const PublicKey& other, bool* equals) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  int cmp = EVP_PKEY_cmp(data_.get(), other.data_.get());
  switch (cmp) {
    case -2:
      return Status::NotSupported("failed to compare public keys");
    case -1: // Key types are different; treat this as not equal
    case 0: // Keys are not equal
      *equals = false;
      return Status::OK();
    case 1:
      *equals = true;
      return Status::OK();
    default:
      return Status::RuntimeError(
          "unexpected public key comparison result", std::to_string(cmp));
  }
}

Status PrivateKey::FromString(const std::string& data, DataFormat format) {
  return ::kudu::security::fromString<RawDataType, RsaPrivateKeyTraits>(
      data, format, &data_);
}

Status PrivateKey::ToString(std::string* data, DataFormat format) const {
  return ::kudu::security::toString<RawDataType, RsaPrivateKeyTraits>(
      data, format, data_.get());
}

Status PrivateKey::FromFile(
    const std::string& fpath,
    DataFormat format,
    const PasswordCallback& passwordCb) {
  return ::kudu::security::fromFile<RawDataType, RsaPrivateKeyTraits>(
      fpath, format, &data_, passwordCb);
}

// The code is modeled after $OPENSSL_ROOT/apps/rsa.c code: there is
// corresponding functionality to read public part from RSA private/public
// keypair.
Status PrivateKey::GetPublicKey(PublicKey* publicKey) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(publicKey);
  auto rsa = ssl_make_unique(EVP_PKEY_get1_RSA(CHECK_NOTNULL(data_.get())));
  if (PREDICT_FALSE(!rsa)) {
    return Status::RuntimeError(GetOpenSSLErrors());
  }
  auto tmp = ssl_make_unique(BIO_new(BIO_s_mem()));
  CHECK(tmp);
  // Export public key in DER format into the temporary buffer.
  OPENSSL_RET_NOT_OK(
      i2d_RSA_PUBKEY_bio(tmp.get(), rsa.get()),
      "error extracting public RSA key");
  // Read the public key into the result placeholder.
  RETURN_NOT_OK(publicKey->FromBIO(tmp.get(), DataFormat::DER));

  return Status::OK();
}

// Modeled after code in $OPENSSL_ROOT/apps/dgst.c
Status PrivateKey::MakeSignature(
    DigestType digest,
    const std::string& data,
    std::string* signature) const {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(signature);
  const EVP_MD* md = getMessageDigest(digest);
  auto mdCtx = ssl_make_unique(EVP_MD_CTX_create());

  OPENSSL_RET_NOT_OK(
      EVP_DigestSignInit(mdCtx.get(), nullptr, md, nullptr, GetRawData()),
      "error initializing signing digest");
  OPENSSL_RET_NOT_OK(
      EVP_DigestSignUpdate(mdCtx.get(), data.data(), data.size()),
      "error signing data");
  size_t sigLen = EVP_PKEY_size(GetRawData());
  static const size_t kSigBufSize = 4 * 1024;
  CHECK(sigLen <= kSigBufSize);
  unsigned char buf[kSigBufSize];
  OPENSSL_RET_NOT_OK(
      EVP_DigestSignFinal(mdCtx.get(), buf, &sigLen),
      "error finalizing data signature");
  *signature = string(reinterpret_cast<char*>(buf), sigLen);

  return Status::OK();
}

Status GeneratePrivateKey(int numBits, PrivateKey* ret) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK(ret);
  InitializeOpenSSL();
  auto key = ssl_make_unique(EVP_PKEY_new());
  {
    auto bn = ssl_make_unique(BN_new());
    OPENSSL_CHECK_OK(BN_set_word(bn.get(), RSA_F4));
    auto rsa = ssl_make_unique(RSA_new());
    OPENSSL_RET_NOT_OK(
        RSA_generate_key_ex(rsa.get(), numBits, bn.get(), nullptr),
        "error generating RSA key");
    OPENSSL_RET_NOT_OK(
        EVP_PKEY_set1_RSA(key.get(), rsa.get()), "error assigning RSA key");
  }
  ret->AdoptRawData(key.release());

  return Status::OK();
}

Status GenerateNonce(string* s) {
  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  CHECK_NOTNULL(s);
  unsigned char buf[kNonceSize];
  OPENSSL_RET_NOT_OK(RAND_bytes(buf, sizeof(buf)), "failed to generate nonce");
  s->assign(reinterpret_cast<char*>(buf), kNonceSize);
  return Status::OK();
}

} // namespace security
} // namespace kudu
