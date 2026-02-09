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

#include "kudu/rpc/connection_id.h"

#include <cstddef>
#include <utility>

#include <boost/functional/hash/hash.hpp>
#include <glog/logging.h>

#include <fmt/core.h>

using std::string;

namespace kudu {
namespace rpc {

ConnectionId::ConnectionId() {}

ConnectionId::ConnectionId(
    const Sockaddr& remote,
    std::string hostname,
    UserCredentials userCredentials)
    : remote_(remote),
      hostname_(std::move(hostname)),
      userCredentials_(std::move(userCredentials)) {
  CHECK(!hostname_.empty());
}

void ConnectionId::setUserCredentials(UserCredentials userCredentials) {
  DCHECK(userCredentials.hasRealUser());
  userCredentials_ = std::move(userCredentials);
}

string ConnectionId::ToString() const {
  string remote;
  if (hostname_ != remote_.host()) {
    remote = fmt::format("{} ({})", remote_.ToString(), hostname_);
  } else {
    remote = remote_.ToString();
  }

  return fmt::format(
      "{{remote={}, user_credentials={}}}",
      remote,
      userCredentials_.ToString());
}

size_t ConnectionId::HashCode() const {
  size_t seed = 0;
  boost::hash_combine(seed, remote_.HashCode());
  boost::hash_combine(seed, hostname_);
  boost::hash_combine(seed, userCredentials_.hashCode());
  return seed;
}

bool ConnectionId::Equals(const ConnectionId& other) const {
  return remote() == other.remote() && hostname_ == other.hostname_ &&
      userCredentials().equals(other.userCredentials());
}

size_t ConnectionIdHash::operator()(const ConnectionId& connId) const {
  return connId.HashCode();
}

bool ConnectionIdEqual::operator()(
    const ConnectionId& cid1,
    const ConnectionId& cid2) const {
  return cid1.Equals(cid2);
}

} // namespace rpc
} // namespace kudu
