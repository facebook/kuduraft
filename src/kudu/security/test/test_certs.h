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

#include <string>

namespace kudu {
class Status;

namespace security {

//
// Set of certificates and private keys used for certificate generation
// and signing tests (declarations).  See the .cc file for the actual data.
//

// Valid root CA cerificate (PEM format).
extern const char kCaCert[];
// The private key (RSA, 2048 bits) for the certificate above.
// This is 2048 bit RSA key, in PEM format.
extern const char kCaPrivateKey[];
// The public part of the abovementioned private key.
extern const char kCaPublicKey[];

// Expired root CA certificate (PEM format).
extern const char kCaExpiredCert[];
// The private key for the expired CA certificate described above.
// This is 2048 bit RSA key, in PEM format.
extern const char kCaExpiredPrivateKey[];
// The public part of the abovementioned private key.
extern const char kCaExpiredPublicKey[];
// Certificate with multiple DNS hostnames in the SAN field.
extern const char kCertDnsHostnamesInSan[];

extern const char kDataTiny[];
extern const char kSignatureTinySha512[];

extern const char kDataShort[];
extern const char kSignatureShortSha512[];

extern const char kDataLong[];
extern const char kSignatureLongSha512[];

// Creates a matching SSL certificate and unencrypted private key file in 'dir',
// returning their paths in '*certFile' and '*keyFile'.
Status createTestSslCertWithPlainKey(
    const std::string& dir,
    std::string* certFile,
    std::string* keyFile);

// Same as the createTestSslCertWithPlainKey() except that the private key is
// encrypted with a password that is returned in 'keyPassword'.
Status createTestSslCertWithEncryptedKey(
    const std::string& dir,
    std::string* certFile,
    std::string* keyFile,
    std::string* keyPassword);

// Same as the createTestSslCertWithPlainKey() except that the 'certFile' is
// signed by a CA chain ('caCertFile' is a chain of certificates).
Status createTestSslCertSignedByChain(
    const std::string& dir,
    std::string* certFile,
    std::string* keyFile,
    std::string* caCertFile);

// Same as the createTestSslCertWithPlainKey() except that the 'certFile' is
// a chain signed by a root CA ('caCertFile' is only the root CA).
Status createTestSslCertWithChainSignedByRoot(
    const std::string& dir,
    std::string* certFile,
    std::string* keyFile,
    std::string* caCertFile);

Status createTestSslCertForClientAndServer(
    const std::string& dir,
    std::string* clientCertFile,
    std::string* clientKeyFile,
    std::string* serverCertFile,
    std::string* serverKeyFile,
    std::string* caCertFile);

} // namespace security
} // namespace kudu
