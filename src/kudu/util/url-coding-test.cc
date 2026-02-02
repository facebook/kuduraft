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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/url-coding.h"

using namespace std; // NOLINT(*)

namespace kudu {

// Tests encoding/decoding of input.  If expectedEncoded is non-empty, the
// encoded string is validated against it.
void testUrl(
    const string& input,
    const string& expectedEncoded,
    bool hiveCompat) {
  string intermediate;
  urlEncode(input, &intermediate, hiveCompat);
  string output;
  if (!expectedEncoded.empty()) {
    EXPECT_EQ(expectedEncoded, intermediate);
  }
  EXPECT_TRUE(urlDecode(intermediate, &output, hiveCompat));
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> inputVector;
  inputVector.resize(input.size());
  if (!input.empty()) {
    memcpy(&inputVector[0], input.c_str(), input.size());
  }
  string intermediate2;
  urlEncode(inputVector, &intermediate2, hiveCompat);
  EXPECT_EQ(intermediate, intermediate2);
}

void testBase64(const string& input, const string& expectedEncoded) {
  string intermediate;
  base64Encode(input, &intermediate);
  string output;
  if (!expectedEncoded.empty()) {
    EXPECT_EQ(intermediate, expectedEncoded);
  }
  EXPECT_TRUE(base64Decode(intermediate, &output));
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> inputVector;
  inputVector.resize(input.size());
  memcpy(&inputVector[0], input.c_str(), input.size());
  string intermediate2;
  base64Encode(inputVector, &intermediate2);
  EXPECT_EQ(intermediate, intermediate2);
}

// Test URL encoding. Check that the values that are put in are the
// same that come out.
TEST(UrlCodingTest, Basic) {
  string input =
      "ABCDEFGHIJKLMNOPQRSTUWXYZ1234567890~!@#$%^&*()<>?,./:\";'{}|[]\\_+-=";
  testUrl(input, "", false);
  testUrl(input, "", true);
}

TEST(UrlCodingTest, HiveExceptions) {
  testUrl(" +", " +", true);
}

TEST(UrlCodingTest, BlankString) {
  testUrl("", "", false);
  testUrl("", "", true);
}

TEST(UrlCodingTest, PathSeparators) {
  testUrl("/home/impala/directory/", "%2Fhome%2Fimpala%2Fdirectory%2F", false);
  testUrl("/home/impala/directory/", "%2Fhome%2Fimpala%2Fdirectory%2F", true);
}

TEST(Base64Test, Basic) {
  testBase64("a", "YQ==");
  testBase64("ab", "YWI=");
  testBase64("abc", "YWJj");
  testBase64("abcd", "YWJjZA==");
  testBase64("abcde", "YWJjZGU=");
  testBase64("abcdef", "YWJjZGVm");
}

TEST(HtmlEscapingTest, Basic) {
  string before = "<html><body>&amp";
  ostringstream after;
  escapeForHtml(before, &after);
  EXPECT_EQ(after.str(), "&lt;html&gt;&lt;body&gt;&amp;amp");
}

} // namespace kudu
