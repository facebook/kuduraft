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

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/strip.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {
namespace security {

// Test for various crypto-related functionality in the security library.
class CryptoTest : public KuduTest {
 public:
  CryptoTest()
      : pem_dir_(GetTestPath("pem")),
        private_key_file_(JoinPathSegments(pem_dir_, "private_key.pem")),
        public_key_file_(JoinPathSegments(pem_dir_, "public_key.pem")),
        corrupted_private_key_file_(
            JoinPathSegments(pem_dir_, "corrupted.private_key.pem")),
        corrupted_public_key_file_(
            JoinPathSegments(pem_dir_, "corrupted.public_key.pem")) {}

  void SetUp() override {
    ASSERT_OK(env_->CreateDir(pem_dir_));
    ASSERT_OK(WriteStringToFile(env_, kCaPrivateKey, private_key_file_));
    ASSERT_OK(WriteStringToFile(env_, kCaPublicKey, public_key_file_));
    ASSERT_OK(WriteStringToFile(
        env_,
        string(kCaPrivateKey, strlen(kCaPrivateKey) / 2),
        corrupted_private_key_file_));
    ASSERT_OK(WriteStringToFile(
        env_,
        string(kCaPublicKey, strlen(kCaPublicKey) / 2),
        corrupted_public_key_file_));
  }

 protected:
  template <typename Key>
  void CheckToAndFromString(const Key& keyRef, DataFormat format) {
    SCOPED_TRACE(
        fmt::format(
            "DataFormat: {}, SignatureType: {}", data_format, signature_type));
    string keyRefStr;
    ASSERT_OK(keyRef.ToString(&keyRefStr, format));
    Key key;
    ASSERT_OK(key.FromString(keyRefStr, format));
    string keyStr;
    ASSERT_OK(key.ToString(&keyStr, format));
    ASSERT_EQ(keyRefStr, keyStr);
  }

  const string pem_dir_;

  const string private_key_file_;
  const string public_key_file_;
  const string corrupted_private_key_file_;
  const string corrupted_public_key_file_;
};

// Check input/output of RSA private keys in PEM format.
TEST_F(CryptoTest, RsaPrivateKeyInputOutputPEM) {
  PrivateKey key;
  ASSERT_OK(key.FromFile(private_key_file_, DataFormat::PEM));
  string keyStr;
  ASSERT_OK(key.ToString(&keyStr, DataFormat::PEM));
  RemoveExtraWhitespace(&keyStr);

  string refKeyStr(kCaPrivateKey);
  RemoveExtraWhitespace(&refKeyStr);
  EXPECT_EQ(refKeyStr, keyStr);
}

// Check input of corrupted RSA private keys in PEM format.
TEST_F(CryptoTest, CorruptedRsaPrivateKeyInputPEM) {
  static const string kFiles[] = {
      corrupted_private_key_file_,
      public_key_file_,
      corrupted_public_key_file_,
      "/bin/sh"};
  for (const auto& file : kFiles) {
    PrivateKey key;
    const Status s = key.FromFile(file, DataFormat::PEM);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
  }
}

// Check input/output of RSA public keys in PEM format.
TEST_F(CryptoTest, RsaPublicKeyInputOutputPEM) {
  PublicKey key;
  ASSERT_OK(key.FromFile(public_key_file_, DataFormat::PEM));
  string keyStr;
  ASSERT_OK(key.ToString(&keyStr, DataFormat::PEM));
  RemoveExtraWhitespace(&keyStr);

  string refKeyStr(kCaPublicKey);
  RemoveExtraWhitespace(&refKeyStr);
  EXPECT_EQ(refKeyStr, keyStr);
}

// Check input of corrupted RSA public keys in PEM format.
TEST_F(CryptoTest, CorruptedRsaPublicKeyInputPEM) {
  static const string kFiles[] = {
      corrupted_public_key_file_,
      private_key_file_,
      corrupted_private_key_file_,
      "/bin/sh"};
  for (const auto& file : kFiles) {
    PublicKey key;
    const Status s = key.FromFile(file, DataFormat::PEM);
    EXPECT_TRUE(s.IsRuntimeError()) << s.ToString();
  }
}

// Check extraction of the public part from RSA private keys par.
TEST_F(CryptoTest, RsaExtractPublicPartFromPrivateKey) {
  // Load the reference RSA private key.
  PrivateKey privateKey;
  ASSERT_OK(privateKey.FromString(kCaPrivateKey, DataFormat::PEM));

  PublicKey publicKey;
  ASSERT_OK(privateKey.GetPublicKey(&publicKey));
  string strPublicKey;
  ASSERT_OK(publicKey.ToString(&strPublicKey, DataFormat::PEM));
  RemoveExtraWhitespace(&strPublicKey);

  string refStrPublicKey(kCaPublicKey);
  RemoveExtraWhitespace(&refStrPublicKey);
  EXPECT_EQ(refStrPublicKey, strPublicKey);
}

class CryptoKeySerDesTest : public CryptoTest,
                            public ::testing::WithParamInterface<DataFormat> {};

// Check the transformation chains for RSA public/private keys:
//   internal -> PEM -> internal -> PEM
//   internal -> DER -> internal -> DER
TEST_P(CryptoKeySerDesTest, ToAndFromString) {
  const auto format = GetParam();

  // Generate private RSA key.
  PrivateKey privateKey;
  ASSERT_OK(GeneratePrivateKey(2048, &privateKey));
  NO_FATALS(CheckToAndFromString(privateKey, format));

  // Extract public part of the key.
  PublicKey publicKey;
  ASSERT_OK(privateKey.GetPublicKey(&publicKey));
  NO_FATALS(CheckToAndFromString(publicKey, format));
}

INSTANTIATE_TEST_CASE_P(
    DataFormats,
    CryptoKeySerDesTest,
    ::testing::Values(DataFormat::DER, DataFormat::PEM));

// Check making crypto signatures against the reference data.
TEST_F(CryptoTest, MakeVerifySignatureRef) {
  static const vector<pair<string, string>> kRefElements = {
      {kDataTiny, kSignatureTinySha512},
      {kDataShort, kSignatureShortSha512},
      {kDataLong, kSignatureLongSha512},
  };

  // Load the reference RSA private key.
  PrivateKey privateKey;
  ASSERT_OK(privateKey.FromString(kCaPrivateKey, DataFormat::PEM));

  // Load the reference RSA public key.
  PublicKey publicKey;
  ASSERT_OK(publicKey.FromString(kCaPublicKey, DataFormat::PEM));

  for (const auto& e : kRefElements) {
    string sig;
    ASSERT_OK(privateKey.MakeSignature(DigestType::SHA512, e.first, &sig));

    // Ad-hoc verification: check the produced signature matches the reference.
    string sigBase64;
    base64Encode(sig, &sigBase64);
    EXPECT_EQ(e.second, sigBase64);

    // Verify the signature cryptographically.
    EXPECT_OK(publicKey.VerifySignature(DigestType::SHA512, e.first, sig));
  }
}

TEST_F(CryptoTest, VerifySignatureWrongData) {
  static const vector<string> kRefSignatures = {
      kSignatureTinySha512,
      kSignatureShortSha512,
      kSignatureLongSha512,
  };

  // Load the reference RSA public key.
  PublicKey key;
  ASSERT_OK(key.FromString(kCaPublicKey, DataFormat::PEM));

  for (const auto& e : kRefSignatures) {
    string signature;
    ASSERT_TRUE(base64Decode(e, &signature));
    Status s =
        key.VerifySignature(DigestType::SHA512, "non-expected-data", signature);
    EXPECT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

TEST_F(CryptoTest, TestGenerateNonce) {
  string nonce;
  ASSERT_OK(GenerateNonce(&nonce));

  // Do some basic validation on the returned nonce.
  ASSERT_EQ(kNonceSize, nonce.size());
  ASSERT_NE(string(kNonceSize, '\0'), nonce);

  // Nonces should be unique, by definition.
  string anotherNonce;
  ASSERT_OK(GenerateNonce(&anotherNonce));
  ASSERT_NE(nonce, anotherNonce);
}

} // namespace security
} // namespace kudu
