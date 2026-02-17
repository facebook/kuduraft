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

#include <cstddef>
#include <string>

#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

// Client-side user credentials. Currently this is more-or-less a simple wrapper
// around a username string. However, we anticipate moving more credentials such
// as tokens into a per-Proxy structure rather than Messenger-wide, and this
// will be the place to store them.
class UserCredentials {
 public:
  // Real user.
  bool hasRealUser() const;
  void setRealUser(std::string realUser);
  const std::string& realUser() const {
    return realUser_;
  }

  // Sets the real user to the currently logged in user.
  Status setLoggedInRealUser();

  // Returns a string representation of the object.
  std::string toString() const;

  std::size_t hashCode() const;
  bool equals(const UserCredentials& other) const;

 private:
  // Remember to update hashCode() and equals() when new fields are added.
  std::string realUser_;
};

} // namespace rpc
} // namespace kudu
