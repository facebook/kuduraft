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

#include "kudu/util/version_util.h"

#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/util/status.h"

using std::ostream;
using std::string;
using std::vector;
using strings::Split;

namespace kudu {

bool Version::operator==(const Version& other) const {
  return this->major == other.major && this->minor == other.minor &&
      this->maintenance == other.maintenance && this->extra == other.extra;
}

string Version::toString() const {
  return extra.empty()
      ? fmt::format("{}.{}.{}", major, minor, maintenance)
      : fmt::format("{}.{}.{}-{}", major, minor, maintenance, extra);
}

ostream& operator<<(ostream& os, const Version& v) {
  return os << v.toString();
}

Status parseVersion(const string& versionStr, Version* v) {
  static const char* const kDelimiter = "-";

  DCHECK(v);
  const Status invalidVerErr =
      Status::InvalidArgument("invalid version string", versionStr);
  auto vStr = versionStr;
  StripWhiteSpace(&vStr);
  const vector<string> mainAndExtra = Split(vStr, kDelimiter);
  if (mainAndExtra.empty()) {
    return invalidVerErr;
  }
  const vector<string> majMinMaint = Split(mainAndExtra.front(), ".");
  if (majMinMaint.size() != 3) {
    return invalidVerErr;
  }
  Version tempV;
  if (!SimpleAtoi(majMinMaint[0], &tempV.major) ||
      !SimpleAtoi(majMinMaint[1], &tempV.minor) ||
      !SimpleAtoi(majMinMaint[2], &tempV.maintenance)) {
    return invalidVerErr;
  }
  tempV.extra = JoinStringsIterator(
      std::next(mainAndExtra.begin()), mainAndExtra.end(), kDelimiter);
  tempV.rawVersion = versionStr;
  *v = std::move(tempV);

  return Status::OK();
}

} // namespace kudu
