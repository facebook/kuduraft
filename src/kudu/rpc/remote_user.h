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
#include <utility>

#include <optional>

namespace kudu {
namespace rpc {

// Server-side view of the remote authenticated user.
//
// This class may be read by multiple threads concurrently after
// its initialization during RPC negotiation.
class RemoteUser {
 public:
  // The method by which the remote user authenticated.
  enum Method {
    // No authentication (authentication was not required by the server
    // and the user provided a username but it was not validated in any way)
    kUnauthenticated,
    // Kerberos-authenticated.
    kKerberos,
    // Authenticated by a Kudu authentication token.
    kAuthnToken,
    // Authenticated by a client certificate.
    kClientCert
  };

  Method authenticatedBy() const {
    return authenticatedBy_;
  }

  const std::string& username() const {
    return username_;
  }

  std::optional<std::string> principal() const {
    return principal_;
  }

  void setAuthenticatedByKerberos(std::string username, std::string principal) {
    authenticatedBy_ = kKerberos;
    username_ = std::move(username);
    principal_ = std::move(principal);
  }

  void setUnauthenticated(std::string username) {
    authenticatedBy_ = kUnauthenticated;
    username_ = std::move(username);
    principal_ = {};
  }

  void setAuthenticatedByClientCert(
      std::string username,
      std::optional<std::string> principal) {
    authenticatedBy_ = kClientCert;
    username_ = std::move(username);
    principal_ = std::move(principal);
  }

  void setAuthenticatedByToken(std::string username) {
    authenticatedBy_ = kAuthnToken;
    username_ = std::move(username);
    principal_ = {};
  }

  // Returns a string representation of the object.
  std::string toString() const;

 private:
  // The real username of the remote user. In the case of a Kerberos
  // principal, this has already been mapped to a local username.
  // TODO(todd): actually do the above mapping.
  std::string username_;

  // The full principal of the remote user. This is only set in the
  // case of a strong-authenticated user.
  std::optional<std::string> principal_;

  Method authenticatedBy_ = kUnauthenticated;
};

} // namespace rpc
} // namespace kudu
