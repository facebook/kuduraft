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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/walltime.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

namespace {

SignedTokenPB makeUnsignedToken(int64_t expiration) {
  SignedTokenPB ret;
  TokenPB token;
  token.set_expire_unix_epoch_seconds(expiration);
  CHECK(token.SerializeToString(ret.mutable_token_data()));
  return ret;
}

SignedTokenPB makeIncompatibleToken() {
  SignedTokenPB ret;
  TokenPB token;
  token.set_expire_unix_epoch_seconds(wallTimeNow() + 100);
  token.add_incompatible_features(TokenPB::Feature_MAX + 1);
  CHECK(token.SerializeToString(ret.mutable_token_data()));
  return ret;
}

// Generate public key as a string in DER format for tests.
Status generatePublicKeyStrDer(string* ret) {
  PrivateKey privateKey;
  RETURN_NOT_OK(GeneratePrivateKey(512, &privateKey));
  PublicKey publicKey;
  RETURN_NOT_OK(privateKey.GetPublicKey(&publicKey));
  string publicKeyStrDer;
  RETURN_NOT_OK(publicKey.ToString(&publicKeyStrDer, DataFormat::DER));
  *ret = publicKeyStrDer;
  return Status::OK();
}

// Generate token signing key with the specified parameters.
Status generateTokenSigningKey(
    int64_t seqNum,
    int64_t expireTimeSeconds,
    unique_ptr<TokenSigningPrivateKey>* tsk) {
  {
    unique_ptr<PrivateKey> privateKey(new PrivateKey);
    RETURN_NOT_OK(GeneratePrivateKey(512, privateKey.get()));
    tsk->reset(new TokenSigningPrivateKey(
        seqNum, expireTimeSeconds, std::move(privateKey)));
  }
  return Status::OK();
}

void checkAndAddNextKey(int iterNum, TokenSigner* signer, int64_t* keySeqNum) {
  ASSERT_NE(nullptr, signer);
  ASSERT_NE(nullptr, keySeqNum);
  int64_t seqNum;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer->checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    seqNum = key->keySeqNum();
  }

  for (int i = 0; i < iterNum; ++i) {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer->checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_EQ(seqNum, key->keySeqNum());
    if (i + 1 == iterNum) {
      // Finally, add the key to the TokenSigner.
      ASSERT_OK(signer->addKey(std::move(key)));
    }
  }
  *keySeqNum = seqNum;
}

} // anonymous namespace

class TokenTest : public KuduTest {};

TEST_F(TokenTest, TestInit) {
  TokenSigner signer(10, 10);
  const TokenVerifier& verifier(signer.verifier());

  SignedTokenPB token = makeUnsignedToken(wallTimeNow());
  Status s = signer.signToken(&token);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  static const int64_t kKeySeqNum = 100;
  PrivateKey privateKey;
  ASSERT_OK(GeneratePrivateKey(512, &privateKey));
  string privateKeyStrDer;
  ASSERT_OK(privateKey.ToString(&privateKeyStrDer, DataFormat::DER));
  TokenSigningPrivateKeyPB pb;
  pb.set_rsa_key_der(privateKeyStrDer);
  pb.set_key_seq_num(kKeySeqNum);
  pb.set_expire_unix_epoch_seconds(wallTimeNow() + 120);

  ASSERT_OK(signer.importKeys({pb}));
  vector<TokenSigningPublicKeyPB> publicKeys(verifier.ExportKeys());
  ASSERT_EQ(1, publicKeys.size());
  ASSERT_EQ(kKeySeqNum, publicKeys[0].key_seq_num());

  // It should be possible to sign tokens once the signer is initialized.
  ASSERT_OK(signer.signToken(&token));
  ASSERT_TRUE(token.has_signature());
}

// Verify that TokenSigner does not allow 'holes' in the sequence numbers
// of the generated keys. The idea is to not allow sequences like '1, 5, 6'.
// In general, calling the checkNeedKey() method multiple times and then calling
// the addKey() method once should advance the key sequence number only by 1
// regardless of number checkNeedKey() calls.
//
// This is to make sure that the sequence numbers are not sparse in case if
// running scenarios checkNeedKey()-try-to-store-key-addKey() over and over
// again, given that the 'try-to-store-key' part can fail sometimes.
TEST_F(TokenTest, TestTokenSignerNonSparseSequenceNumbers) {
  static const int kIterNum = 3;
  static const int64_t kAuthnTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);

  int64_t seqNumFirstKey;
  NO_FATALS(checkAndAddNextKey(kIterNum, &signer, &seqNumFirstKey));

  SleepFor(MonoDelta::FromSeconds(kKeyRotationSeconds + 1));

  int64_t seqNumSecondKey;
  NO_FATALS(checkAndAddNextKey(kIterNum, &signer, &seqNumSecondKey));

  ASSERT_EQ(seqNumFirstKey + 1, seqNumSecondKey);
}

// Verify the behavior of the TokenSigner::importKeys() method. In general,
// it should tolerate mix of expired and non-expired keys, even if their
// sequence numbers are intermixed: keys with greater sequence numbers could
// be already expired but keys with lesser sequence numbers could be still
// valid. The idea is to correctly import TSKs generated with different
// validity period settings. This is to address scenarios when the system
// was run with long authn token validity interval and then switched to
// a shorter one.
//
// After importing keys, the TokenSigner should contain only the valid ones.
// In addition, the sequence number of the very first key generated after the
// import should be greater than any sequence number the TokenSigner has seen
// during the import.
TEST_F(TokenTest, TestTokenSignerAddKeyAfterImport) {
  static const int64_t kAuthnTokenValiditySeconds = 8;
  static const int64_t kKeyRotationSeconds = 8;
  static const int64_t kKeyValiditySeconds =
      kAuthnTokenValiditySeconds + 2 * kKeyRotationSeconds;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);
  const TokenVerifier& verifier(signer.verifier());

  static const int64_t kExpiredKeySeqNum = 100;
  static const int64_t kKeySeqNum = kExpiredKeySeqNum - 1;
  {
    // First, try to import already expired key to check that internal key
    // sequence number advances correspondingly.
    PrivateKey privateKey;
    ASSERT_OK(GeneratePrivateKey(512, &privateKey));
    string privateKeyStrDer;
    ASSERT_OK(privateKey.ToString(&privateKeyStrDer, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(privateKeyStrDer);
    pb.set_key_seq_num(kExpiredKeySeqNum);
    pb.set_expire_unix_epoch_seconds(wallTimeNow() - 1);

    ASSERT_OK(signer.importKeys({pb}));
  }

  {
    // Check the result of importing keys: there should be no keys because
    // the only one we tried to import was already expired.
    vector<TokenSigningPublicKeyPB> publicKeys(verifier.ExportKeys());
    ASSERT_TRUE(publicKeys.empty());
  }

  {
    // Now import valid (not yet expired) key, but with sequence number less
    // than of the expired key.
    PrivateKey privateKey;
    ASSERT_OK(GeneratePrivateKey(512, &privateKey));
    string privateKeyStrDer;
    ASSERT_OK(privateKey.ToString(&privateKeyStrDer, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(privateKeyStrDer);
    pb.set_key_seq_num(kKeySeqNum);
    // Set the TSK's expiration time: make the key valid but past its activity
    // interval.
    pb.set_expire_unix_epoch_seconds(
        wallTimeNow() + (kKeyValiditySeconds - 2 * kKeyRotationSeconds - 1));

    ASSERT_OK(signer.importKeys({pb}));
  }

  {
    // Check the result of importing keys.
    vector<TokenSigningPublicKeyPB> publicKeys(verifier.ExportKeys());
    ASSERT_EQ(1, publicKeys.size());
    ASSERT_EQ(kKeySeqNum, publicKeys[0].key_seq_num());
  }

  {
    // The newly imported key should be used to sign tokens.
    SignedTokenPB token = makeUnsignedToken(wallTimeNow());
    ASSERT_OK(signer.signToken(&token));
    ASSERT_TRUE(token.has_signature());
    ASSERT_TRUE(token.has_signing_key_seq_num());
    EXPECT_EQ(kKeySeqNum, token.signing_key_seq_num());
  }

  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_EQ(kExpiredKeySeqNum + 1, key->keySeqNum());
    ASSERT_OK(signer.addKey(std::move(key)));
    bool hasRotated = false;
    ASSERT_OK(signer.tryRotateKey(&hasRotated));
    ASSERT_TRUE(hasRotated);
  }
  {
    // Check the result of generating the new key: the identifier of the new key
    // should be +1 increment from the identifier of the expired imported key.
    vector<TokenSigningPublicKeyPB> publicKeys(verifier.ExportKeys());
    ASSERT_EQ(2, publicKeys.size());
    EXPECT_EQ(kKeySeqNum, publicKeys[0].key_seq_num());
    EXPECT_EQ(kExpiredKeySeqNum + 1, publicKeys[1].key_seq_num());
  }

  // At this point the new key should be used to sign tokens.
  SignedTokenPB token = makeUnsignedToken(wallTimeNow());
  ASSERT_OK(signer.signToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_TRUE(token.has_signing_key_seq_num());
  EXPECT_EQ(kExpiredKeySeqNum + 1, token.signing_key_seq_num());
}

// The addKey() method should not allow to add a key with the sequence number
// less or equal to the sequence number of the most 'recent' key.
TEST_F(TokenTest, TestAddKeyConstraints) {
  {
    TokenSigner signer(1, 1);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));
  }
  {
    TokenSigner signer(1, 1);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    const int64_t keySeqNum = key->keySeqNum();
    key->keySeqNum_ = keySeqNum - 1;
    Status s = signer.addKey(std::move(key));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(), ": invalid key sequence number, should be at least ");
  }
  {
    TokenSigner signer(1, 1);
    static const int64_t kKeySeqNum = 100;
    PrivateKey privateKey;
    ASSERT_OK(GeneratePrivateKey(512, &privateKey));
    string privateKeyStrDer;
    ASSERT_OK(privateKey.ToString(&privateKeyStrDer, DataFormat::DER));
    TokenSigningPrivateKeyPB pb;
    pb.set_rsa_key_der(privateKeyStrDer);
    pb.set_key_seq_num(kKeySeqNum);
    // Make the key already expired.
    pb.set_expire_unix_epoch_seconds(wallTimeNow() - 1);
    ASSERT_OK(signer.importKeys({pb}));

    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    const int64_t keySeqNum = key->keySeqNum();
    ASSERT_GT(keySeqNum, kKeySeqNum);
    key->keySeqNum_ = kKeySeqNum;
    Status s = signer.addKey(std::move(key));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(), ": invalid key sequence number, should be at least ");
  }
}

TEST_F(TokenTest, TestGenerateAuthTokenNoUserName) {
  TokenSigner signer(10, 10);
  SignedTokenPB signedTokenPb;
  const Status& s = signer.generateAuthnToken("", &signedTokenPb);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no username provided for authn token");
}

TEST_F(TokenTest, TestIsCurrentKeyValid) {
  static const int64_t kAuthnTokenValiditySeconds = 1;
  static const int64_t kKeyRotationSeconds = 1;
  static const int64_t kKeyValiditySeconds =
      kAuthnTokenValiditySeconds + 2 * kKeyRotationSeconds;

  TokenSigner signer(kAuthnTokenValiditySeconds, kKeyRotationSeconds);
  EXPECT_FALSE(signer.isCurrentKeyValid());
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));
  }
  EXPECT_TRUE(signer.isCurrentKeyValid());
  SleepFor(MonoDelta::FromSeconds(kKeyValiditySeconds));
  // The key should expire after its validity interval.
  EXPECT_FALSE(signer.isCurrentKeyValid());

  // Anyway, current implementation allows to use an expired key to sign tokens.
  SignedTokenPB token = makeUnsignedToken(wallTimeNow());
  EXPECT_OK(signer.signToken(&token));
}

TEST_F(TokenTest, TestTokenSignerAddKeys) {
  {
    TokenSigner signer(10, 10);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));

    ASSERT_OK(signer.checkNeedKey(&key));
    // It's not time to add next key yet.
    ASSERT_EQ(nullptr, key.get());
  }

  {
    // Special configuration for TokenSigner: rotation interval is zero,
    // so should be able to add two keys right away.
    TokenSigner signer(10, 0);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));

    // Should be able to add next key right away.
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));

    // Active key and next key are already in place: no need for a new key.
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());
  }

  if (AllowSlowTests()) {
    // Special configuration for TokenSigner: short interval for key rotation.
    // It should not need next key right away, but should need next key after
    // the rotation interval.
    static const int64_t kKeyRotationIntervalSeconds = 8;
    TokenSigner signer(10, kKeyRotationIntervalSeconds);
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));

    // Should not need next key right away.
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());

    SleepFor(MonoDelta::FromSeconds(kKeyRotationIntervalSeconds));

    // Should need next key after the rotation interval.
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));

    // Active key and next key are already in place: no need for a new key.
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_EQ(nullptr, key.get());
  }
}

// Test how key rotation works.
TEST_F(TokenTest, TestTokenSignerSignVerifyExport) {
  // Key rotation interval 0 allows adding 2 keys in a row with no delay.
  TokenSigner signer(10, 0);
  const TokenVerifier& verifier(signer.verifier());

  // Should start off with no signing keys.
  ASSERT_TRUE(verifier.ExportKeys().empty());

  // Trying to sign a token when there is no TSK should give an error.
  SignedTokenPB token = makeUnsignedToken(wallTimeNow());
  Status s = signer.signToken(&token);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // Generate and set a new key.
  int64_t signingKeySeqNum;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    signingKeySeqNum = key->keySeqNum();
    ASSERT_GT(signingKeySeqNum, -1);
    ASSERT_OK(signer.addKey(std::move(key)));
  }

  // We should see the key now if we request TSKs starting at a
  // lower sequence number.
  ASSERT_EQ(1, verifier.ExportKeys().size());
  // We should not see the key if we ask for the sequence number
  // that it is assigned.
  ASSERT_EQ(0, verifier.ExportKeys(signingKeySeqNum).size());

  // We should be able to sign a token now.
  ASSERT_OK(signer.signToken(&token));
  ASSERT_TRUE(token.has_signature());
  ASSERT_EQ(signingKeySeqNum, token.signing_key_seq_num());

  // Set next key and check that we return the right keys.
  int64_t nextSigningKeySeqNum;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    nextSigningKeySeqNum = key->keySeqNum();
    ASSERT_GT(nextSigningKeySeqNum, signingKeySeqNum);
    ASSERT_OK(signer.addKey(std::move(key)));
  }
  ASSERT_EQ(2, verifier.ExportKeys().size());
  ASSERT_EQ(1, verifier.ExportKeys(signingKeySeqNum).size());
  ASSERT_EQ(0, verifier.ExportKeys(nextSigningKeySeqNum).size());

  // The first key should be used for signing: the next one is saved
  // for the next round.
  {
    SignedTokenPB token = makeUnsignedToken(wallTimeNow());
    ASSERT_OK(signer.signToken(&token));
    ASSERT_TRUE(token.has_signature());
    ASSERT_EQ(signingKeySeqNum, token.signing_key_seq_num());
  }
}

// Test that the TokenSigner can export its public keys in protobuf form
// via bound TokenVerifier.
TEST_F(TokenTest, TestExportKeys) {
  // Test that the exported public keys don't contain private key material,
  // and have an appropriate expiration.
  const int64_t keyExpSeconds = 30;
  const int64_t keyRotationSeconds = 10;
  TokenSigner signer(
      keyExpSeconds - 2 * keyRotationSeconds, keyRotationSeconds);
  int64_t keySeqNum;
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    keySeqNum = key->keySeqNum();
    ASSERT_OK(signer.addKey(std::move(key)));
  }
  const TokenVerifier& verifier(signer.verifier());
  auto keys = verifier.ExportKeys();
  ASSERT_EQ(1, keys.size());
  const TokenSigningPublicKeyPB& key = keys[0];
  ASSERT_TRUE(key.has_rsa_key_der());
  ASSERT_EQ(keySeqNum, key.key_seq_num());
  ASSERT_TRUE(key.has_expire_unix_epoch_seconds());
  const int64_t now = wallTimeNow();
  ASSERT_GT(key.expire_unix_epoch_seconds(), now);
  ASSERT_LE(key.expire_unix_epoch_seconds(), now + keyExpSeconds);
}

// Test that the TokenVerifier can import keys exported by the TokenSigner
// and then verify tokens signed by it.
TEST_F(TokenTest, TestEndToEnd_Valid) {
  TokenSigner signer(10, 10);
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));
  }

  // Make and sign a token.
  SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() + 600);
  ASSERT_OK(signer.signToken(&signedToken));

  // Try to verify it.
  TokenVerifier verifier;
  ASSERT_OK(verifier.ImportKeys(signer.verifier().ExportKeys()));
  TokenPB token;
  ASSERT_EQ(
      VerificationResult::VALID,
      verifier.VerifyTokenSignature(signedToken, &token));
}

// Test all of the possible cases covered by token verification.
// See VerificationResult.
TEST_F(TokenTest, TestEndToEnd_InvalidCases) {
  // Key rotation interval 0 allows adding 2 keys in a row with no delay.
  TokenSigner signer(10, 0);
  {
    std::unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(signer.checkNeedKey(&key));
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(signer.addKey(std::move(key)));
  }

  TokenVerifier verifier;
  ASSERT_OK(verifier.ImportKeys(signer.verifier().ExportKeys()));

  // Make and sign a token, but corrupt the data in it.
  {
    SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() + 600);
    ASSERT_OK(signer.signToken(&signedToken));
    signedToken.set_token_data("xyz");
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::INVALID_TOKEN,
        verifier.VerifyTokenSignature(signedToken, &token));
  }

  // Make and sign a token, but corrupt the signature.
  {
    SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() + 600);
    ASSERT_OK(signer.signToken(&signedToken));
    signedToken.set_signature("xyz");
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::INVALID_SIGNATURE,
        verifier.VerifyTokenSignature(signedToken, &token));
  }

  // Make and sign a token, but set it to be already expired.
  {
    SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() - 10);
    ASSERT_OK(signer.signToken(&signedToken));
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::EXPIRED_TOKEN,
        verifier.VerifyTokenSignature(signedToken, &token));
  }

  // Make and sign a token which uses an incompatible feature flag.
  {
    SignedTokenPB signedToken = makeIncompatibleToken();
    ASSERT_OK(signer.signToken(&signedToken));
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::INCOMPATIBLE_FEATURE,
        verifier.VerifyTokenSignature(signedToken, &token));
  }

  // Set a new signing key, but don't inform the verifier of it yet. When we
  // verify, we expect the verifier to complain the key is unknown.
  {
    {
      std::unique_ptr<TokenSigningPrivateKey> key;
      ASSERT_OK(signer.checkNeedKey(&key));
      ASSERT_NE(nullptr, key.get());
      ASSERT_OK(signer.addKey(std::move(key)));
      bool hasRotated = false;
      ASSERT_OK(signer.tryRotateKey(&hasRotated));
      ASSERT_TRUE(hasRotated);
    }
    SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() + 600);
    ASSERT_OK(signer.signToken(&signedToken));
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::UNKNOWN_SIGNING_KEY,
        verifier.VerifyTokenSignature(signedToken, &token));
  }

  // Set a new signing key which is already expired, and inform the verifier
  // of all of the current keys. The verifier should recognize the key but
  // know that it's expired.
  {
    {
      unique_ptr<TokenSigningPrivateKey> tsk;
      ASSERT_OK(generateTokenSigningKey(100, wallTimeNow() - 1, &tsk));
      // This direct access is necessary because addKey() does not allow to add
      // an expired key.
      TokenSigningPublicKeyPB tskPublicPb;
      tsk->ExportPublicKeyPB(&tskPublicPb);
      ASSERT_OK(verifier.ImportKeys({tskPublicPb}));
      signer.tskDeque_.push_front(std::move(tsk));
    }

    SignedTokenPB signedToken = makeUnsignedToken(wallTimeNow() + 600);
    // Current implementation allows to use an expired key to sign tokens.
    ASSERT_OK(signer.signToken(&signedToken));
    TokenPB token;
    ASSERT_EQ(
        VerificationResult::EXPIRED_SIGNING_KEY,
        verifier.VerifyTokenSignature(signedToken, &token));
  }
}

// Test functionality of the TokenVerifier::ImportKeys() method.
TEST_F(TokenTest, TestTokenVerifierImportKeys) {
  TokenVerifier verifier;

  // An attempt to import no keys is fine.
  ASSERT_OK(verifier.ImportKeys({}));
  ASSERT_TRUE(verifier.ExportKeys().empty());

  TokenSigningPublicKeyPB tskPublicPb;
  const auto expTime = wallTimeNow() + 600;
  tskPublicPb.set_key_seq_num(100500);
  tskPublicPb.set_expire_unix_epoch_seconds(expTime);
  string publicKeyStrDer;
  ASSERT_OK(generatePublicKeyStrDer(&publicKeyStrDer));
  tskPublicPb.set_rsa_key_der(publicKeyStrDer);

  ASSERT_OK(verifier.ImportKeys({tskPublicPb}));
  {
    const auto& exportedTsksPublicPb = verifier.ExportKeys();
    ASSERT_EQ(1, exportedTsksPublicPb.size());
    EXPECT_EQ(
        tskPublicPb.SerializeAsString(),
        exportedTsksPublicPb[0].SerializeAsString());
  }

  // Re-importing the same key again is fine, and the total number
  // of exported keys should not increase.
  ASSERT_OK(verifier.ImportKeys({tskPublicPb}));
  {
    const auto& exportedTsksPublicPb = verifier.ExportKeys();
    ASSERT_EQ(1, exportedTsksPublicPb.size());
    EXPECT_EQ(
        tskPublicPb.SerializeAsString(),
        exportedTsksPublicPb[0].SerializeAsString());
  }
}

} // namespace security
} // namespace kudu
