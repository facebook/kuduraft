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

#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Socket;

namespace security {

enum class TlsHandshakeType {
  // The local endpoint is the TLS client (initiator).
  Client,
  // The local endpoint is the TLS server (acceptor).
  Server,
};

// Mode for performing verification of the remote peer's identity during a
// handshake.
enum class TlsVerificationMode {
  // SERVER:
  //    No certificate will be requested from the client, and no verification
  //    will be done.
  // CLIENT:
  //    The server's certificate will be obtained but no verification will be
  //    done.
  //    (the server still requires a certificate, even if it is self-signed).
  VerifyNone,

  // BOTH:
  //   The remote peer is required to provide a certificate but no verification
  //   will be done.
  VerifyCertPresentOnly,

  // BOTH:
  // The remote peer is required to have a signed certificate. The certificate
  // will
  // be verified in two ways:
  //  1) The certificate must be signed by a trusted CA (or chain of CAs).
  //  2) Second, the hostname of the remote peer (as determined by reverse DNS
  //  of the
  //    socket address) must match the common name or one of the Subject
  //    Alternative
  //    Names stored in the certificate.
  VerifyRemoteCertAndHost
};

// TlsHandshake manages an ongoing TLS handshake between a client and server.
//
// TlsHandshake instances are default constructed, but must be initialized
// before use using TlsContext::initiateHandshake.
class TlsHandshake {
 public:
  TlsHandshake() = default;
  ~TlsHandshake() = default;

  // Set the verification mode for this handshake. The default verification mode
  // is VERIFY_REMOTE_CERT_AND_HOST.
  //
  // This must be called before the first call to Continue().
  void setVerificationMode(TlsVerificationMode mode) {
    DCHECK(!hasStarted_);
    verificationMode_ = mode;
  }

  // Perform a standard TLS handshake
  Status sslHandshake(std::unique_ptr<Socket>* socket, bool isServer)
      WARN_UNUSED_RESULT;

  // Get selected ALPN protocol
  std::string getSelectedAlpn();

  // Continue or start a new handshake.
  //
  // 'recv' should contain the input buffer from the remote end, or an empty
  // string when the handshake is new.
  //
  // 'send' should contain the output buffer which must be sent to the remote
  // end.
  //
  // Returns Status::OK when the handshake is complete, however the 'send'
  // buffer may contain a message which must still be transmitted to the remote
  // end. If the send buffer is empty after this call and the return is
  // Status::OK, the socket should immediately be wrapped in the TLS channel
  // using 'finish'. If the send buffer is not empty, the message should be sent
  // to the remote end, and then the socket should be wrapped using 'finish'.
  //
  // Returns Status::Incomplete when the handshake must continue for another
  // round of messages.
  //
  // Returns any other status code on error.
  Status continueHandshake(const std::string& recv, std::string* send)
      WARN_UNUSED_RESULT;

  // Finishes the handshake, wrapping the provided socket in the negotiated TLS
  // channel. This 'TlsHandshake' instance should not be used again after
  // calling this.
  Status finish(std::unique_ptr<Socket>* socket) WARN_UNUSED_RESULT;

  // Finish the handshake, using the provided socket to verify the remote peer,
  // but without wrapping the socket.
  Status finishNoWrap(const Socket& socket) WARN_UNUSED_RESULT;

  // Retrieve the local certificate. This will return an error status if there
  // is no local certificate.
  //
  // May only be called after 'finish' or 'finishNoWrap'.
  Status getLocalCert(Cert* cert) const WARN_UNUSED_RESULT;

  // Retrieve the remote peer's certificate. This will return an error status if
  // there is no remote certificate.
  //
  // May only be called after 'finish' or 'finishNoWrap'.
  Status getRemoteCert(Cert* cert) const WARN_UNUSED_RESULT;

  // Retrieve the negotiated cipher suite. Only valid to call after the
  // handshake is complete and before 'finish()'.
  std::string getCipherSuite() const;

  // Retrieve the negotiated TLS protocol version. Only valid to call after the
  // handshake is complete and before 'finish()'.
  std::string getProtocol() const;

  // Retrive the description of the negotiated cipher.
  // Only valid to call after the handshake is complete and before 'finish()'.
  std::string getCipherDescription() const;

 private:
  friend class TlsContext;

  bool hasStarted_ = false;
  TlsVerificationMode verificationMode_ =
      TlsVerificationMode::VerifyRemoteCertAndHost;

  // Set the verification mode on the underlying SSL object.
  void setSslVerify();

  // Set the SSL to use during the handshake. Called once by
  // TlsContext::initiateHandshake before starting the handshake processes.
  void adoptSsl(c_unique_ptr<SSL> ssl) {
    CHECK(!ssl_);
    ssl_ = std::move(ssl);
  }

  SSL* ssl() {
    return ssl_.get();
  }

  // Populates localCert_ and remoteCert_.
  Status getCerts() WARN_UNUSED_RESULT;

  // Verifies that the handshake is valid for the provided socket.
  Status verify(const Socket& socket) const WARN_UNUSED_RESULT;

  // Owned SSL handle.
  c_unique_ptr<SSL> ssl_;

  Cert localCert_;
  Cert remoteCert_;
  std::string selectedAlpn_;
};

} // namespace security
} // namespace kudu
