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

#include "kudu/gutil/strings/strcat.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>

#include <gtest/gtest.h>

namespace kudu {

// Test AlphaNum construction and conversion for various integer types
TEST(StrCatTest, AlphaNumInt32) {
  // Test positive int32
  AlphaNum pos(static_cast<int32_t>(42));
  EXPECT_EQ("42", std::string(pos.data(), pos.size()));

  // Test negative int32
  AlphaNum neg(static_cast<int32_t>(-42));
  EXPECT_EQ("-42", std::string(neg.data(), neg.size()));

  // Test zero
  AlphaNum zero(static_cast<int32_t>(0));
  EXPECT_EQ("0", std::string(zero.data(), zero.size()));

  // Test maximum int32
  AlphaNum max_val(std::numeric_limits<int32_t>::max());
  EXPECT_EQ("2147483647", std::string(max_val.data(), max_val.size()));

  // Test minimum int32
  AlphaNum min_val(std::numeric_limits<int32_t>::min());
  EXPECT_EQ("-2147483648", std::string(min_val.data(), min_val.size()));
}

TEST(StrCatTest, AlphaNumUInt32) {
  // Test positive uint32
  AlphaNum pos(static_cast<uint32_t>(42));
  EXPECT_EQ("42", std::string(pos.data(), pos.size()));

  // Test zero
  AlphaNum zero(static_cast<uint32_t>(0));
  EXPECT_EQ("0", std::string(zero.data(), zero.size()));

  // Test maximum uint32
  AlphaNum max_val(std::numeric_limits<uint32_t>::max());
  EXPECT_EQ("4294967295", std::string(max_val.data(), max_val.size()));
}

TEST(StrCatTest, AlphaNumInt64) {
  // Test positive int64
  AlphaNum pos(static_cast<int64_t>(12345678901234LL));
  EXPECT_EQ("12345678901234", std::string(pos.data(), pos.size()));

  // Test negative int64
  AlphaNum neg(static_cast<int64_t>(-12345678901234LL));
  EXPECT_EQ("-12345678901234", std::string(neg.data(), neg.size()));

  // Test zero
  AlphaNum zero(static_cast<int64_t>(0));
  EXPECT_EQ("0", std::string(zero.data(), zero.size()));

  // Test maximum int64
  AlphaNum max_val(std::numeric_limits<int64_t>::max());
  EXPECT_EQ("9223372036854775807", std::string(max_val.data(), max_val.size()));

  // Test minimum int64
  AlphaNum min_val(std::numeric_limits<int64_t>::min());
  EXPECT_EQ(
      "-9223372036854775808", std::string(min_val.data(), min_val.size()));
}

TEST(StrCatTest, AlphaNumUInt64) {
  // Test positive uint64
  AlphaNum pos(static_cast<uint64_t>(12345678901234ULL));
  EXPECT_EQ("12345678901234", std::string(pos.data(), pos.size()));

  // Test zero
  AlphaNum zero(static_cast<uint64_t>(0));
  EXPECT_EQ("0", std::string(zero.data(), zero.size()));

  // Test maximum uint64
  AlphaNum max_val(std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(
      "18446744073709551615", std::string(max_val.data(), max_val.size()));
}

TEST(StrCatTest, AlphaNumFloat) {
  // Test positive float
  AlphaNum pos(3.14f);
  std::string result(pos.data(), pos.size());
  EXPECT_TRUE(result.find("3.14") != std::string::npos) << "Result: " << result;

  // Test negative float
  AlphaNum neg(-2.5f);
  result = std::string(neg.data(), neg.size());
  EXPECT_TRUE(result.find("-2.5") != std::string::npos) << "Result: " << result;

  // Test zero
  AlphaNum zero(0.0f);
  result = std::string(zero.data(), zero.size());
  EXPECT_TRUE(result.find("0") != std::string::npos) << "Result: " << result;

  // Test infinity
  AlphaNum inf(std::numeric_limits<float>::infinity());
  result = std::string(inf.data(), inf.size());
  EXPECT_TRUE(
      result.find("inf") != std::string::npos ||
      result.find("Inf") != std::string::npos)
      << "Result: " << result;

  // Test negative infinity
  AlphaNum neg_inf(-std::numeric_limits<float>::infinity());
  result = std::string(neg_inf.data(), neg_inf.size());
  EXPECT_TRUE(
      result.find("-inf") != std::string::npos ||
      result.find("-Inf") != std::string::npos)
      << "Result: " << result;

  // Test NaN
  AlphaNum nan_val(std::numeric_limits<float>::quiet_NaN());
  result = std::string(nan_val.data(), nan_val.size());
  EXPECT_TRUE(
      result.find("nan") != std::string::npos ||
      result.find("NaN") != std::string::npos)
      << "Result: " << result;
}

TEST(StrCatTest, AlphaNumDouble) {
  // Test positive double
  AlphaNum pos(3.141592653589793);
  std::string result(pos.data(), pos.size());
  EXPECT_TRUE(result.find("3.14159") != std::string::npos)
      << "Result: " << result;

  // Test negative double
  AlphaNum neg(-2.718281828459045);
  result = std::string(neg.data(), neg.size());
  EXPECT_TRUE(result.find("-2.71828") != std::string::npos)
      << "Result: " << result;

  // Test zero
  AlphaNum zero(0.0);
  result = std::string(zero.data(), zero.size());
  EXPECT_TRUE(result.find("0") != std::string::npos) << "Result: " << result;

  // Test very small number
  AlphaNum small(1.23456789e-100);
  result = std::string(small.data(), small.size());
  EXPECT_FALSE(result.empty()) << "Small number should produce valid output";
  EXPECT_LT(result.size(), 32) << "Result should fit in buffer";

  // Test very large number
  AlphaNum large(1.23456789e100);
  result = std::string(large.data(), large.size());
  EXPECT_FALSE(result.empty()) << "Large number should produce valid output";
  EXPECT_LT(result.size(), 32) << "Result should fit in buffer";

  // Test infinity
  AlphaNum inf(std::numeric_limits<double>::infinity());
  result = std::string(inf.data(), inf.size());
  EXPECT_TRUE(
      result.find("inf") != std::string::npos ||
      result.find("Inf") != std::string::npos)
      << "Result: " << result;

  // Test negative infinity
  AlphaNum neg_inf(-std::numeric_limits<double>::infinity());
  result = std::string(neg_inf.data(), neg_inf.size());
  EXPECT_TRUE(
      result.find("-inf") != std::string::npos ||
      result.find("-Inf") != std::string::npos)
      << "Result: " << result;

  // Test NaN
  AlphaNum nan_val(std::numeric_limits<double>::quiet_NaN());
  result = std::string(nan_val.data(), nan_val.size());
  EXPECT_TRUE(
      result.find("nan") != std::string::npos ||
      result.find("NaN") != std::string::npos)
      << "Result: " << result;
}

TEST(StrCatTest, AlphaNumBufferSizeVerification) {
  // Verify that all numeric types fit in the 32-byte buffer

  // Int32 should use at most 12 bytes (including null terminator and sign)
  AlphaNum int32_min(std::numeric_limits<int32_t>::min());
  EXPECT_LE(int32_min.size(), 12);

  // Int64 should use at most 22 bytes (including null terminator and sign)
  AlphaNum int64_min(std::numeric_limits<int64_t>::min());
  EXPECT_LE(int64_min.size(), 22);

  // UInt64 should use at most 22 bytes (including null terminator)
  AlphaNum uint64_max(std::numeric_limits<uint64_t>::max());
  EXPECT_LE(uint64_max.size(), 22);

  // Float/double should use at most 30 bytes
  AlphaNum dbl_max(std::numeric_limits<double>::max());
  EXPECT_LE(dbl_max.size(), 30);

  AlphaNum dbl_min(std::numeric_limits<double>::lowest());
  EXPECT_LE(dbl_min.size(), 30);
}

TEST(StrCatTest, StrCatBasic) {
  // Test basic string concatenation with integers
  EXPECT_EQ("hello42", StrCat("hello", 42));
  EXPECT_EQ("The answer is 42", StrCat("The answer is ", 42));

  // Test with multiple arguments
  EXPECT_EQ("abc123def", StrCat("abc", 123, "def"));
  EXPECT_EQ("1-2-3", StrCat(1, "-", 2, "-", 3));
}

TEST(StrCatTest, StrCatMixedTypes) {
  // Test concatenation with mixed numeric types
  std::string result = StrCat("int:", 42, " float:", 3.14f);
  EXPECT_TRUE(result.find("int:42 float:3.14") != std::string::npos)
      << "Result: " << result;

  // Test with int64 and uint64
  int64_t large_int = 1234567890123LL;
  uint64_t large_uint = 9876543210987ULL;
  result = StrCat("int64:", large_int, " uint64:", large_uint);
  EXPECT_TRUE(result.find("1234567890123") != std::string::npos);
  EXPECT_TRUE(result.find("9876543210987") != std::string::npos);
}

TEST(StrCatTest, StrCatNegativeNumbers) {
  // Test with negative numbers
  EXPECT_EQ("negative: -42", StrCat("negative: ", -42));
  EXPECT_EQ("-1-2-3", StrCat(-1, -2, -3));
  std::string result = StrCat("float:", -3.14f);
  EXPECT_TRUE(result.find("-3.14") != std::string::npos)
      << "Result: " << result;
}

TEST(StrCatTest, StrCatZeros) {
  // Test with zeros
  EXPECT_EQ("0", StrCat(0));
  std::string result = StrCat(0.0);
  EXPECT_TRUE(result.find("0") != std::string::npos) << "Result: " << result;
  EXPECT_EQ("zeros:000", StrCat("zeros:", 0, 0, 0));
}

TEST(StrCatTest, StrCatEmptyStrings) {
  // Test with empty strings
  EXPECT_EQ("42", StrCat("", 42));
  EXPECT_EQ("42", StrCat(42, ""));
  EXPECT_EQ("4242", StrCat("", 42, "", 42, ""));
}

TEST(StrCatTest, StrCatLargeNumbers) {
  // Test with maximum values
  int32_t max_int32 = std::numeric_limits<int32_t>::max();
  int64_t max_int64 = std::numeric_limits<int64_t>::max();
  uint64_t max_uint64 = std::numeric_limits<uint64_t>::max();

  std::string result = StrCat("max_int32:", max_int32);
  EXPECT_TRUE(result.find("2147483647") != std::string::npos);

  result = StrCat("max_int64:", max_int64);
  EXPECT_TRUE(result.find("9223372036854775807") != std::string::npos);

  result = StrCat("max_uint64:", max_uint64);
  EXPECT_TRUE(result.find("18446744073709551615") != std::string::npos);
}

TEST(StrCatTest, StrCatSpecialFloats) {
  // Test with special floating-point values
  std::string inf_result =
      StrCat("inf:", std::numeric_limits<float>::infinity());
  EXPECT_TRUE(
      inf_result.find("inf") != std::string::npos ||
      inf_result.find("Inf") != std::string::npos);

  std::string neg_inf_result =
      StrCat("neg_inf:", -std::numeric_limits<double>::infinity());
  EXPECT_TRUE(
      neg_inf_result.find("-inf") != std::string::npos ||
      neg_inf_result.find("-Inf") != std::string::npos);

  std::string nan_result =
      StrCat("nan:", std::numeric_limits<double>::quiet_NaN());
  EXPECT_TRUE(
      nan_result.find("nan") != std::string::npos ||
      nan_result.find("NaN") != std::string::npos);
}

TEST(StrCatTest, StrAppendBasic) {
  // Test basic string append
  std::string s = "hello";
  StrAppend(&s, 42);
  EXPECT_EQ("hello42", s);

  // Test multiple appends
  s = "start";
  StrAppend(&s, " ", 1, " ", 2, " ", 3);
  EXPECT_EQ("start 1 2 3", s);
}

TEST(StrCatTest, StrAppendMixedTypes) {
  std::string s = "prefix:";

  // Append integers
  StrAppend(&s, 123);
  EXPECT_EQ("prefix:123", s);

  // Append floats
  StrAppend(&s, " float:", 3.14f);
  EXPECT_TRUE(s.find("3.14") != std::string::npos);

  // Append int64
  int64_t large_int64 = 9876543210123LL;
  StrAppend(&s, " int64:", large_int64);
  EXPECT_TRUE(s.find("9876543210123") != std::string::npos);
}

TEST(StrCatTest, StrAppendEmptyString) {
  std::string s;

  // Append to empty string
  StrAppend(&s, 42);
  EXPECT_EQ("42", s);

  // Continue appending
  StrAppend(&s, ", ", 43);
  EXPECT_EQ("42, 43", s);
}

TEST(StrCatTest, AlphaNumStringPiece) {
  // Test AlphaNum with StringPiece
  StringPiece sp("test");
  AlphaNum an(sp);
  EXPECT_EQ("test", std::string(an.data(), an.size()));
  EXPECT_EQ(4, an.size());
}

TEST(StrCatTest, AlphaNumStdString) {
  // Test AlphaNum with std::string
  std::string str = "hello world";
  AlphaNum an(str);
  EXPECT_EQ("hello world", std::string(an.data(), an.size()));
  EXPECT_EQ(11, an.size());
}

TEST(StrCatTest, AlphaNumCString) {
  // Test AlphaNum with C string
  const char* cstr = "C string";
  AlphaNum an(cstr);
  EXPECT_EQ("C string", std::string(an.data(), an.size()));
  EXPECT_EQ(8, an.size());
}

TEST(StrCatTest, ManyArguments) {
  // Test StrCat with many arguments (up to 9)
  std::string result = StrCat(1, 2, 3, 4, 5, 6, 7, 8, 9);
  EXPECT_EQ("123456789", result);

  // Test with strings and numbers
  result = StrCat("a", 1, "b", 2, "c", 3, "d", 4, "e");
  EXPECT_EQ("a1b2c3d4e", result);
}

TEST(StrCatTest, StrCatRealWorldUseCases) {
  // Test typical use cases

  // Building error messages
  int error_code = 404;
  std::string msg = StrCat("Error ", error_code, ": Not Found");
  EXPECT_EQ("Error 404: Not Found", msg);

  // Building URLs with numeric IDs
  int user_id = 12345;
  std::string url = StrCat("/api/users/", user_id);
  EXPECT_EQ("/api/users/12345", url);

  // Building log messages with multiple fields
  int64_t timestamp = 1638360000000LL;
  double value = 123.45;
  std::string log = StrCat("[", timestamp, "] Value: ", value);
  EXPECT_TRUE(log.find("1638360000000") != std::string::npos);
  EXPECT_TRUE(log.find("123.45") != std::string::npos);

  // Building formatted output
  int count = 42;
  float percentage = 87.5f;
  std::string output =
      StrCat("Processed ", count, " items (", percentage, "%)");
  EXPECT_TRUE(output.find("42") != std::string::npos);
  EXPECT_TRUE(output.find("87.5") != std::string::npos);
}

} // namespace kudu
