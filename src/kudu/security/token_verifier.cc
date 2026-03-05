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

#include "kudu/security/token_verifier.h"

#include <algorithm>
#include <iterator>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/walltime.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

using std::lock_guard;
using std::transform;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace security {

TokenVerifier::TokenVerifier() {}

TokenVerifier::~TokenVerifier() {}

int64_t TokenVerifier::getMaxKnownKeySequenceNumber() const {
  shared_lock l(lock_);
  if (keysBySeq_.empty()) {
    return -1;
  }

  return keysBySeq_.rbegin()->first;
}

// Import a set of public keys provided by the token signer (typically
// running on another node).
Status TokenVerifier::importKeys(const vector<TokenSigningPublicKeyPB>& keys) {
  // Do the construction outside of the lock, to avoid holding the
  // lock while doing lots of allocation.
  vector<unique_ptr<TokenSigningPublicKey>> tsks;
  for (const auto& pb : keys) {
    // Sanity check the key.
    if (!pb.has_rsa_key_der()) {
      return Status::RuntimeError(
          "token-signing public key message must include the signing key");
    }
    if (!pb.has_key_seq_num()) {
      return Status::RuntimeError(
          "token-signing public key message must include the signing key sequence number");
    }
    if (!pb.has_expire_unix_epoch_seconds()) {
      return Status::RuntimeError(
          "token-signing public key message must include an expiration time");
    }
    tsks.emplace_back(new TokenSigningPublicKey{pb});
    RETURN_NOT_OK(tsks.back()->Init());
  }

  std::lock_guard l(lock_);
  for (auto&& tskPtr : tsks) {
    auto keySeqNum = tskPtr->pb().key_seq_num();
    keysBySeq_.emplace(keySeqNum, std::move(tskPtr));
  }
  return Status::OK();
}

std::vector<TokenSigningPublicKeyPB> TokenVerifier::exportKeys(
    int64_t afterSequenceNumber) const {
  vector<TokenSigningPublicKeyPB> ret;
  shared_lock l(lock_);
  ret.reserve(keysBySeq_.size());
  transform(
      keysBySeq_.upper_bound(afterSequenceNumber),
      keysBySeq_.end(),
      back_inserter(ret),
      [](const KeysMap::value_type& e) { return e.second->pb(); });
  return ret;
}

// Verify the signature on the given token.
VerificationResult TokenVerifier::verifyTokenSignature(
    const SignedTokenPB& signedToken,
    TokenPB* token) const {
  if (!signedToken.has_signature() || !signedToken.has_signing_key_seq_num() ||
      !signedToken.has_token_data()) {
    return VerificationResult::InvalidToken;
  }

  if (!token->ParseFromString(signedToken.token_data()) ||
      !token->has_expire_unix_epoch_seconds()) {
    return VerificationResult::InvalidToken;
  }

  int64_t now = wallTimeNow();
  if (token->expire_unix_epoch_seconds() < now) {
    return VerificationResult::ExpiredToken;
  }

  for (auto flag : token->incompatible_features()) {
    if (!TokenPB::Feature_IsValid(flag)) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "received authentication token with unknown feature; "
             "server needs to be updated";
      return VerificationResult::IncompatibleFeature;
    }
  }

  {
    shared_lock l(lock_);
    auto it = keysBySeq_.find(signedToken.signing_key_seq_num());
    if (it == keysBySeq_.end()) {
      return VerificationResult::UnknownSigningKey;
    }
    auto* tsk = it->second.get();
    if (tsk->pb().expire_unix_epoch_seconds() < now) {
      return VerificationResult::ExpiredSigningKey;
    }
    if (!tsk->VerifySignature(signedToken)) {
      return VerificationResult::InvalidSignature;
    }
  }

  return VerificationResult::Valid;
}

const char* verificationResultToString(VerificationResult r) {
  switch (r) {
    case security::VerificationResult::Valid:
      return "valid";
    case security::VerificationResult::InvalidToken:
      return "invalid authentication token";
    case security::VerificationResult::InvalidSignature:
      return "invalid authentication token signature";
    case security::VerificationResult::ExpiredToken:
      return "authentication token expired";
    case security::VerificationResult::ExpiredSigningKey:
      return "authentication token signing key expired";
    case security::VerificationResult::UnknownSigningKey:
      return "authentication token signed with unknown key";
    case security::VerificationResult::IncompatibleFeature:
      return "authentication token uses incompatible feature";
    default:
      LOG(FATAL) << "unexpected VerificationResult value: "
                 << static_cast<int>(r);
  }
}

} // namespace security
} // namespace kudu
