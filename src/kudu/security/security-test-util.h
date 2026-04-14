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

#include <ostream>

#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class PrivateKey;
class TlsContext;

Status generateSelfSignedCaForTests(PrivateKey* caKey, Cert* caCert);

// Describes the options for configuring a TlsContext.
enum class PkiConfig {
  // The TLS context has no TLS cert and no trusted certs.
  None,
  // The TLS context has a self-signed TLS cert and no trusted certs.
  SelfSigned,
  // The TLS context has no TLS cert and a trusted cert.
  Trusted,
  // The TLS context has a signed TLS cert and trusts the corresponding signing
  // cert.
  Signed,
  // The TLS context has a externally signed TLS cert and trusts the
  // corresponding signing cert.
  ExternallySigned,
};

// PkiConfig pretty-printer.
std::ostream& operator<<(std::ostream& o, PkiConfig c);

Status configureTlsContext(
    PkiConfig config,
    const Cert& caCert,
    const PrivateKey& caKey,
    TlsContext* tlsContext);

} // namespace security
} // namespace kudu
