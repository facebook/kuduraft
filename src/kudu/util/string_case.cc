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

#include "kudu/util/string_case.h"

#include <cctype>
#include <cstdint>
#include <ostream>

#include <glog/logging.h>

namespace kudu {

using std::string;

void snakeToCamelCase(const std::string& snakeCase, std::string* camelCase) {
  DCHECK_NE(camelCase, &snakeCase) << "Does not support in-place operation";
  camelCase->clear();
  camelCase->reserve(snakeCase.size());

  bool uppercaseNext = true;
  for (char c : snakeCase) {
    if ((c == '_') || (c == '-')) {
      uppercaseNext = true;
      continue;
    }
    if (uppercaseNext) {
      camelCase->push_back(toupper(c));
    } else {
      camelCase->push_back(c);
    }
    uppercaseNext = false;
  }
}

void toUpperCase(const std::string& string, std::string* out) {
  if (out != &string) {
    *out = string;
  }

  for (char& c : *out) {
    c = toupper(c);
  }
}

void capitalize(string* word) {
  uint32_t size = word->size();
  if (size == 0) {
    return;
  }

  (*word)[0] = toupper((*word)[0]);

  for (int i = 1; i < size; i++) {
    (*word)[i] = tolower((*word)[i]);
  }
}

} // namespace kudu
