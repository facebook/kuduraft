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

#include "kudu/util/hexdump.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>

#include <glog/logging.h>

#include <fmt/core.h>

#include "kudu/util/logging.h"
#include "kudu/util/slice.h"

namespace kudu {

std::string hexDump(const Slice& slice) {
  if (KUDU_SHOULD_REDACT()) {
    return kRedactionMessage;
  }

  std::string output;
  output.reserve(slice.size() * 5);

  const uint8_t* p = slice.data();

  int rem = slice.size();
  while (rem > 0) {
    const uint8_t* lineP = p;
    int lineLen = std::min(rem, 16);
    int lineRem = lineLen;
    fmt::format_to(
        std::back_inserter(output), "{:06x}: ", lineP - slice.data());

    while (lineRem >= 2) {
      fmt::format_to(
          std::back_inserter(output),
          "{:02x}{:02x} ",
          p[0] & 0xff,
          p[1] & 0xff);
      p += 2;
      lineRem -= 2;
    }

    if (lineRem == 1) {
      fmt::format_to(std::back_inserter(output), "{:02x}   ", p[0] & 0xff);
      p += 1;
      lineRem -= 1;
    }
    DCHECK_EQ(lineRem, 0);

    int padding = (16 - lineLen) / 2;

    for (int i = 0; i < padding; i++) {
      output.append("     ");
    }

    for (int i = 0; i < lineLen; i++) {
      char c = lineP[i];
      if (isprint(c)) {
        output.push_back(c);
      } else {
        output.push_back('.');
      }
    }

    output.push_back('\n');
    rem -= lineLen;
  }
  return output;
}
} // namespace kudu
