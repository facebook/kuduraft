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
//
// Some portions Copyright 2013 The Chromium Authors. All rights reserved.

#include "kudu/gutil/strings/util.h"

#include <string>

#include <gtest/gtest.h>

namespace kudu {

TEST(StringUtilTest, MatchPatternTest) {
  EXPECT_TRUE(MatchPattern("www.google.com", "*.com"));
  EXPECT_TRUE(MatchPattern("www.google.com", "*"));
  EXPECT_FALSE(MatchPattern("www.google.com", "www*.g*.org"));
  EXPECT_TRUE(MatchPattern("Hello", "H?l?o"));
  EXPECT_FALSE(MatchPattern("www.google.com", "http://*)"));
  EXPECT_FALSE(MatchPattern("www.msn.com", "*.COM"));
  EXPECT_TRUE(MatchPattern("Hello*1234", "He??o\\*1*"));
  EXPECT_FALSE(MatchPattern("", "*.*"));
  EXPECT_TRUE(MatchPattern("", "*"));
  EXPECT_TRUE(MatchPattern("", "?"));
  EXPECT_TRUE(MatchPattern("", ""));
  EXPECT_FALSE(MatchPattern("Hello", ""));
  EXPECT_TRUE(MatchPattern("Hello*", "Hello*"));
  // Stop after a certain recursion depth.
  EXPECT_FALSE(MatchPattern("123456789012345678", "?????????????????*"));

  // Test UTF8 matching.
  EXPECT_TRUE(MatchPattern("heart: \xe2\x99\xa0", "*\xe2\x99\xa0"));
  EXPECT_TRUE(MatchPattern("heart: \xe2\x99\xa0.", "heart: ?."));
  EXPECT_TRUE(MatchPattern("hearts: \xe2\x99\xa0\xe2\x99\xa0", "*"));
  // Invalid sequences should be handled as a single invalid character.
  EXPECT_TRUE(MatchPattern("invalid: \xef\xbf\xbe", "invalid: ?"));
  // If the pattern has invalid characters, it shouldn't match anything.
  EXPECT_FALSE(MatchPattern("\xf4\x90\x80\x80", "\xf4\x90\x80\x80"));

  // This test verifies that consecutive wild cards are collapsed into 1
  // wildcard (when this doesn't occur, MatchPattern reaches it's maximum
  // recursion depth).
  EXPECT_TRUE(MatchPattern("Hello", "He********************************o"));
}

// Test PrefixSuccessor with strings containing 0xff bytes.
// This tests the fix for tautological comparison where comparing
// a signed char to 255 was always false.
TEST(StringUtilTest, PrefixSuccessorWithHighBytes) {
  // Basic cases
  EXPECT_EQ("b", PrefixSuccessor("a"));
  EXPECT_EQ("aab", PrefixSuccessor("aaa"));

  // String ending with 0xff should strip trailing 0xff and increment previous
  EXPECT_EQ("ab", PrefixSuccessor("aa\xff"));
  EXPECT_EQ("b", PrefixSuccessor("a\xff"));

  // Multiple trailing 0xff bytes
  EXPECT_EQ("ab", PrefixSuccessor("aa\xff\xff"));

  // String consisting entirely of 0xff returns empty
  EXPECT_EQ("", PrefixSuccessor("\xff"));
  EXPECT_EQ("", PrefixSuccessor("\xff\xff"));
  EXPECT_EQ("", PrefixSuccessor("\xff\xff\xff"));

  // Empty string returns empty
  EXPECT_EQ("", PrefixSuccessor(""));

  // Test with strings containing null bytes (must use explicit length)
  std::string with_null("\x00\xff", 2);
  EXPECT_EQ(std::string("\x01", 1), PrefixSuccessor(with_null));

  std::string multi_null_ff("\x00\xff\xff\xff", 4);
  EXPECT_EQ(std::string("\x01", 1), PrefixSuccessor(multi_null_ff));
}

// Test FindShortestSeparator with strings containing 0xff bytes.
// This tests the fix for tautological comparison where comparing
// a signed char to 0xff was always false.
TEST(StringUtilTest, FindShortestSeparatorWithHighBytes) {
  std::string separator;

  // When start[diff_index] is 0xff, we should not try to increment it
  // (would overflow), so we just return start.
  // Test case: diff at index 0 where start[0] = 0xff
  FindShortestSeparator(
      "\xff"
      "abc",
      "z"
      "abc",
      &separator);
  EXPECT_EQ(
      "\xff"
      "abc",
      separator);

  // Test case: diff at index 1 where start[1] = 0xff (after common prefix "a")
  FindShortestSeparator(
      "a\xff"
      "bc",
      "az"
      "bc",
      &separator);
  EXPECT_EQ(
      "a\xff"
      "bc",
      separator);

  // Normal case - should find shorter separator when char can be incremented
  // "foobar" vs "foxhunt": diff at index 2 ('o' vs 'x'), result is "fop"
  FindShortestSeparator("foobar", "foxhunt", &separator);
  EXPECT_EQ("fop", separator);

  // Example from header: "abracadabra" vs "bacradabra" => "b"
  FindShortestSeparator("abracadabra", "bacradabra", &separator);
  EXPECT_EQ("b", separator);

  // When the diff is at the last character, just return start
  FindShortestSeparator("abc", "abd", &separator);
  EXPECT_EQ("abc", separator);
}

} // namespace kudu
