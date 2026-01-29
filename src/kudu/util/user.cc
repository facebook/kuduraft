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

#include "kudu/util/user.h"

#include <pwd.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;

namespace kudu {
namespace {

Status doGetLoggedInUser(string* userName) {
  DCHECK(userName != nullptr);

  struct passwd pwd;
  struct passwd* result;

  // Get the system-defined limit for usernames. If the value was indeterminate,
  // use a constant that should be more than enough, per the man page.
  int64_t retval = sysconf(_SC_GETPW_R_SIZE_MAX);
  size_t bufSize = retval > 0 ? retval : 16384;

  const unique_ptr<char[]> buf(new char[bufSize]);

  int ret = getpwuid_r(getuid(), &pwd, buf.get(), bufSize, &result);
  if (result == nullptr) {
    if (ret == 0) {
      return Status::NotFound(
          "Current logged-in user not found! This is an unexpected error.");
    } else {
      // Errno in ret
      return Status::RuntimeError(
          "Error calling getpwuid_r()", ErrnoToString(ret), ret);
    }
  }
  *userName = pwd.pw_name;
  return Status::OK();
}

} // anonymous namespace

Status getLoggedInUser(string* userName) {
  static std::once_flag once;
  static string* onceUserName;
  static Status* onceStatus;
  std::call_once(once, []() {
    string u;
    Status s = doGetLoggedInUser(&u);
    debug::ScopedLeakCheckDisabler ignore_leaks;
    onceStatus = new Status(std::move(s));
    onceUserName = new string(std::move(u));
  });

  RETURN_NOT_OK(*onceStatus);
  *userName = *onceUserName;
  return Status::OK();
}

} // namespace kudu
