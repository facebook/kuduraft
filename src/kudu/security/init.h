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

#include <optional>
#include <string>

#include <folly/SharedMutex.h>

namespace kudu {

class Status;

namespace security {

// Return the full principal (user/host@REALM) that the server has used to
// log in from the keytab.
//
// If the server has not logged in from a keytab, returns {}.
std::optional<std::string> getLoggedInPrincipalFromKeytab();

// Same, but returns the mapped short username.
std::optional<std::string> getLoggedInUsernameFromKeytab();

// Map the given Kerberos principal 'principal' to a short username (i.e. with
// no realm or host component).
//
// This respects the "auth-to-local" mappings from the system krb5.conf.
// However, if no such mapping can be found, we fall back to simply taking the
// first component of the principal.
//
// TODO(todd): move to kerberos_util.h in the later patch in this series (the
// file doesn't exist yet, and trying to avoid rebase pain).
Status mapPrincipalToLocalName(
    const std::string& principal,
    std::string* localName);

} // namespace security
} // namespace kudu
