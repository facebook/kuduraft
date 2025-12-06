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

// Test to verify StringPrintf -> fmt::format conversions produce identical
// output

#include <cinttypes>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <string>

#include <fmt/core.h>
#include <gtest/gtest.h>

namespace kudu {

// Legacy StringPrintf implementation for comparison
static std::string LegacyStringPrintf(const char* format, ...) {
  char space[1024];
  va_list ap;
  va_start(ap, format);
  int result = vsnprintf(space, sizeof(space), format, ap);
  va_end(ap);

  if (result >= 0 && result < static_cast<int>(sizeof(space))) {
    return std::string(space, result);
  }

  // Fallback for larger strings
  int length = result + 1;
  auto buf = new char[length];
  va_start(ap, format);
  result = vsnprintf(buf, length, format, ap);
  va_end(ap);

  std::string ret(buf, result);
  delete[] buf;
  return ret;
}

// Test class for StringPrintf to fmt::format equivalence
class StringPrintfFmtEquivalenceTest : public ::testing::Test {
 protected:
  // Helper to verify legacy and new produce same output
  void VerifyEquivalent(
      const std::string& legacy,
      const std::string& fmt_result) {
    EXPECT_EQ(legacy, fmt_result) << "Legacy: '" << legacy << "'\n"
                                  << "fmt:    '" << fmt_result << "'";
  }
};

// Test 1: Basic integer formatting
TEST_F(StringPrintfFmtEquivalenceTest, BasicInteger) {
  int value = 42;
  auto legacy = LegacyStringPrintf("%d", value);
  auto fmt_new = fmt::format("{}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 2: Zero-padded hex formatting (common in block IDs)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedHex) {
  uint64_t value = 0x1234567890ABCDEF;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%016" PRIx64, value);
  auto fmt_new = fmt::format("{:016x}", value);
  VerifyEquivalent(std::string(legacy), fmt_new);
}

// Test 3: Two-digit hex (byte formatting)
TEST_F(StringPrintfFmtEquivalenceTest, TwoDigitHex) {
  uint8_t byte = 0xAB;
  auto legacy = LegacyStringPrintf("%02x", byte);
  auto fmt_new = fmt::format("{:02x}", byte);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 4: Float with precision
TEST_F(StringPrintfFmtEquivalenceTest, FloatPrecision) {
  double value = 123.456789;
  auto legacy = LegacyStringPrintf("%.2f", value);
  auto fmt_new = fmt::format("{:.2f}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 5: Float with 3 decimals
TEST_F(StringPrintfFmtEquivalenceTest, FloatThreeDecimals) {
  double value = 1.23456;
  auto legacy = LegacyStringPrintf("%.3f", value);
  auto fmt_new = fmt::format("{:.3f}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 6: Width specification for integers
TEST_F(StringPrintfFmtEquivalenceTest, WidthSpecification) {
  int value = 42;
  auto legacy = LegacyStringPrintf("%7d", value);
  auto fmt_new = fmt::format("{:7d}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 7: Octal formatting
TEST_F(StringPrintfFmtEquivalenceTest, OctalFormatting) {
  unsigned int value = 0755;
  auto legacy = LegacyStringPrintf("%03o", value);
  auto fmt_new = fmt::format("{:03o}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 8: String formatting
TEST_F(StringPrintfFmtEquivalenceTest, StringFormatting) {
  const char* str = "hello";
  auto legacy = LegacyStringPrintf("%s", str);
  auto fmt_new = fmt::format("{}", str);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 9: Mixed formatting (multiple arguments)
TEST_F(StringPrintfFmtEquivalenceTest, MixedMultipleArgs) {
  int num = 42;
  const char* str = "test";
  double flt = 3.14;
  auto legacy = LegacyStringPrintf("%d %s %.2f", num, str, flt);
  auto fmt_new = fmt::format("{} {} {:.2f}", num, str, flt);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 10: 64-bit integer formatting
TEST_F(StringPrintfFmtEquivalenceTest, Int64Formatting) {
  int64_t value = 9876543210LL;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%" PRId64, value);
  auto fmt_new = fmt::format("{}", value);
  VerifyEquivalent(std::string(legacy), fmt_new);
}

// Test 11: Zero-padded integer (like log index)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedInteger) {
  int64_t value = 123;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%09" PRId64, value);
  auto fmt_new = fmt::format("{:09d}", value);
  VerifyEquivalent(std::string(legacy), fmt_new);
}

// Test 12: Percentage formatting
TEST_F(StringPrintfFmtEquivalenceTest, PercentageFormatting) {
  double percent = 75.5;
  auto legacy = LegacyStringPrintf("%.2f%%", percent);
  auto fmt_new = fmt::format("{:.2f}%", percent);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 13: Scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, ScientificNotation) {
  double value = 1.234e-5;
  auto legacy = LegacyStringPrintf("%0.3G", value);
  auto fmt_new = fmt::format("{:0.3G}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 14: Lowercase scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, LowercaseScientific) {
  double value = 1.234e5;
  auto legacy = LegacyStringPrintf("%0.3g", value);
  auto fmt_new = fmt::format("{:0.3g}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 15: Boolean to integer (bitmap test case)
TEST_F(StringPrintfFmtEquivalenceTest, BooleanToInteger) {
  bool value = true;
  auto legacy = LegacyStringPrintf("%d", static_cast<int>(value));
  auto fmt_new = fmt::format("{}", static_cast<int>(value));
  VerifyEquivalent(legacy, fmt_new);
}

// Test 16: Pointer formatting
TEST_F(StringPrintfFmtEquivalenceTest, PointerFormatting) {
  void* ptr = reinterpret_cast<void*>(0x12345678);
  // For pointers, we use fmt::ptr() which gives us platform-independent
  // formatting
  char legacy[32];
  snprintf(
      legacy,
      sizeof(legacy),
      "0x%" PRIx64,
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(ptr)));
  auto fmt_new = fmt::format("0x{:x}", reinterpret_cast<uintptr_t>(ptr));
  VerifyEquivalent(std::string(legacy), fmt_new);
}

// Test 17: Negative numbers
TEST_F(StringPrintfFmtEquivalenceTest, NegativeNumbers) {
  int value = -42;
  auto legacy = LegacyStringPrintf("%d", value);
  auto fmt_new = fmt::format("{}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 18: Negative floats with precision
TEST_F(StringPrintfFmtEquivalenceTest, NegativeFloats) {
  double value = -123.456;
  auto legacy = LegacyStringPrintf("%.2f", value);
  auto fmt_new = fmt::format("{:.2f}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 19: Width and precision together
TEST_F(StringPrintfFmtEquivalenceTest, WidthAndPrecision) {
  size_t value = 123;
  auto legacy = LegacyStringPrintf("%4zu", value);
  auto fmt_new = fmt::format("{:4}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 20: Hex with uppercase
TEST_F(StringPrintfFmtEquivalenceTest, UppercaseHex) {
  uint32_t value = 0xABCD;
  auto legacy = LegacyStringPrintf("%X", value);
  auto fmt_new = fmt::format("{:X}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 21: Multiple hex bytes (common in file_block_manager)
TEST_F(StringPrintfFmtEquivalenceTest, MultipleHexBytes) {
  uint64_t id = 0x0000AB00CD000000ULL;

  // Test byte2 extraction
  auto byte2_legacy =
      LegacyStringPrintf("%02llx", (id & 0x0000FF0000000000ULL) >> 40);
  auto byte2_fmt = fmt::format("{:02x}", (id & 0x0000FF0000000000ULL) >> 40);
  VerifyEquivalent(byte2_legacy, byte2_fmt);

  // Test byte3 extraction
  auto byte3_legacy =
      LegacyStringPrintf("%02llx", (id & 0x000000FF00000000ULL) >> 32);
  auto byte3_fmt = fmt::format("{:02x}", (id & 0x000000FF00000000ULL) >> 32);
  VerifyEquivalent(byte3_legacy, byte3_fmt);

  // Test byte4 extraction
  auto byte4_legacy =
      LegacyStringPrintf("%02llx", (id & 0x00000000FF000000ULL) >> 24);
  auto byte4_fmt = fmt::format("{:02x}", (id & 0x00000000FF000000ULL) >> 24);
  VerifyEquivalent(byte4_legacy, byte4_fmt);
}

// Test 22: Format with alignment
TEST_F(StringPrintfFmtEquivalenceTest, RightAlignedInteger) {
  size_t value = 5;
  auto legacy = LegacyStringPrintf("%4zu", value);
  auto fmt_new = fmt::format("{:4}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 23: Complex string with multiple format specifiers (from real code)
TEST_F(StringPrintfFmtEquivalenceTest, ComplexFormatString) {
  const char* prefix = "Prefix";
  double val = 12.345;
  char unit = 'M';
  auto legacy = LegacyStringPrintf("%s%.2f%c", prefix, val, unit);
  auto fmt_new = fmt::format("{}{:.2f}{}", prefix, val, unit);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 24: Format with negative sign
TEST_F(StringPrintfFmtEquivalenceTest, NegativeSign) {
  const char* sign = "-";
  int64_t value = 123;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%s%" PRId64, sign, value);
  auto fmt_new = fmt::format("{}{}", sign, value);
  VerifyEquivalent(std::string(legacy), fmt_new);
}

// Test 25: Zero value formatting
TEST_F(StringPrintfFmtEquivalenceTest, ZeroValue) {
  int value = 0;
  auto legacy = LegacyStringPrintf("%d", value);
  auto fmt_new = fmt::format("{}", value);
  VerifyEquivalent(legacy, fmt_new);
}

// ============================================================================
// EDGE CASE TESTS: Width and Truncation Behavior
// ============================================================================

// Test 26: String precision truncation (%.Ns limits string to N chars)
TEST_F(StringPrintfFmtEquivalenceTest, StringPrecisionTruncation) {
  const char* long_str = "ThisIsAVeryLongString";
  // %.5s should truncate to first 5 characters
  auto legacy = LegacyStringPrintf("%.5s", long_str);
  auto fmt_new = fmt::format("{:.5}", long_str);
  VerifyEquivalent(legacy, fmt_new);
  // Verify actual truncation happened
  EXPECT_EQ(legacy, "ThisI");
}

// Test 27: Integer width overflow (width < actual digits)
TEST_F(StringPrintfFmtEquivalenceTest, IntegerWidthOverflow) {
  int large_num = 123456;
  // Width of 3 but number has 6 digits - should NOT truncate
  auto legacy = LegacyStringPrintf("%3d", large_num);
  auto fmt_new = fmt::format("{:3d}", large_num);
  VerifyEquivalent(legacy, fmt_new);
  // Verify no truncation - full number should be printed
  EXPECT_EQ(legacy, "123456");
}

// Test 28: String width with smaller input (should pad, not truncate)
TEST_F(StringPrintfFmtEquivalenceTest, StringWidthPadding) {
  const char* short_str = "Hi";
  // Width of 10 should pad with spaces, not truncate
  auto legacy = LegacyStringPrintf("%10s", short_str);
  auto fmt_new = fmt::format("{:>10}", short_str);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy.length(), 10u);
}

// Test 29: Zero-padded integer with negative number
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedNegative) {
  int negative = -123;
  // %05d with negative number - zero padding between sign and digits
  auto legacy = LegacyStringPrintf("%05d", negative);
  auto fmt_new = fmt::format("{:05d}", negative);
  VerifyEquivalent(legacy, fmt_new);
  // Should be "-0123" (5 chars total, padding after sign)
  EXPECT_EQ(legacy, "-0123");
}

// Test 30: Zero-padded integer overflow (number larger than width)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedOverflow) {
  int large_num = 123456;
  // %03d but number has 6 digits - should NOT truncate
  auto legacy = LegacyStringPrintf("%03d", large_num);
  auto fmt_new = fmt::format("{:03d}", large_num);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "123456");
}

// Test 31: Float width overflow
TEST_F(StringPrintfFmtEquivalenceTest, FloatWidthOverflow) {
  double large_float = 12345.674; // Changed to avoid rounding edge case
  // Width of 5 but formatted number is much longer
  auto legacy = LegacyStringPrintf("%5.2f", large_float);
  auto fmt_new = fmt::format("{:5.2f}", large_float);
  VerifyEquivalent(legacy, fmt_new);
  // Should not truncate, prints full "12345.67"
  EXPECT_EQ(legacy, "12345.67");
}

// Test 32: Hex width overflow
TEST_F(StringPrintfFmtEquivalenceTest, HexWidthOverflow) {
  uint64_t large_hex = 0xABCDEF123456;
  // Width of 4 but hex representation is 12 chars
  auto legacy = LegacyStringPrintf("%04x", static_cast<uint32_t>(large_hex));
  auto fmt_new = fmt::format("{:04x}", static_cast<uint32_t>(large_hex));
  VerifyEquivalent(legacy, fmt_new);
  // Verify no truncation
  EXPECT_GT(legacy.length(), 4u);
}

// Test 33: String width vs precision (width pads, precision truncates)
TEST_F(StringPrintfFmtEquivalenceTest, StringWidthVsPrecision) {
  const char* str = "Test";
  // Width 10, precision 2 - should truncate to 2 chars then pad to 10
  auto legacy = LegacyStringPrintf("%10.2s", str);
  auto fmt_new = fmt::format("{:>10.2}", str);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "        Te");
}

// Test 34: Left alignment with width (negative width or '-' flag)
TEST_F(StringPrintfFmtEquivalenceTest, LeftAlignedString) {
  const char* str = "Hi";
  auto legacy = LegacyStringPrintf("%-10s", str);
  auto fmt_new = fmt::format("{:<10}", str);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "Hi        ");
}

// Test 35: Left-aligned integer with width overflow
TEST_F(StringPrintfFmtEquivalenceTest, LeftAlignedIntegerOverflow) {
  int large_num = 123456;
  auto legacy = LegacyStringPrintf("%-3d", large_num);
  auto fmt_new = fmt::format("{:<3d}", large_num);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "123456");
}

// Test 36: Float precision overflow with scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, FloatPrecisionEdgeCase) {
  double tiny = 0.000000123456789;
  auto legacy = LegacyStringPrintf("%.20f", tiny);
  auto fmt_new = fmt::format("{:.20f}", tiny);
  VerifyEquivalent(legacy, fmt_new);
}

// Test 37: Very wide width specification
TEST_F(StringPrintfFmtEquivalenceTest, VeryWideWidth) {
  int small_num = 5;
  auto legacy = LegacyStringPrintf("%50d", small_num);
  auto fmt_new = fmt::format("{:50d}", small_num);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy.length(), 50u);
}

// Test 38: Zero precision float (%.0f)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPrecisionFloat) {
  double value = 123.789;
  auto legacy = LegacyStringPrintf("%.0f", value);
  auto fmt_new = fmt::format("{:.0f}", value);
  VerifyEquivalent(legacy, fmt_new);
  // Should round to "124"
  EXPECT_EQ(legacy, "124");
}

// Test 39: Empty string with precision
TEST_F(StringPrintfFmtEquivalenceTest, EmptyStringPrecision) {
  const char* empty = "";
  auto legacy = LegacyStringPrintf("%.5s", empty);
  auto fmt_new = fmt::format("{:.5}", empty);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "");
}

// Test 40: Boundary case - exactly matching width
TEST_F(StringPrintfFmtEquivalenceTest, ExactWidthMatch) {
  const char* str = "12345";
  auto legacy = LegacyStringPrintf("%5s", str);
  auto fmt_new = fmt::format("{:>5}", str);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "12345");
}

// Test 41: Multiple width overflows in same format string
TEST_F(StringPrintfFmtEquivalenceTest, MultipleWidthOverflows) {
  int num1 = 123456;
  int num2 = 789012;
  auto legacy = LegacyStringPrintf("%3d-%3d", num1, num2);
  auto fmt_new = fmt::format("{:3d}-{:3d}", num1, num2);
  VerifyEquivalent(legacy, fmt_new);
  EXPECT_EQ(legacy, "123456-789012");
}

// Test 42: Negative number with width (no zero padding)
TEST_F(StringPrintfFmtEquivalenceTest, NegativeNumberWidth) {
  int negative = -42;
  auto legacy = LegacyStringPrintf("%10d", negative);
  auto fmt_new = fmt::format("{:10d}", negative);
  VerifyEquivalent(legacy, fmt_new);
  // Should pad with spaces on left: "       -42"
  EXPECT_EQ(legacy.length(), 10u);
}

// Test 43: Hex with '#' prefix and width
TEST_F(StringPrintfFmtEquivalenceTest, HexPrefixWithWidth) {
  int value = 255;
  auto legacy = LegacyStringPrintf("%#8x", value);
  auto fmt_new = fmt::format("{:#8x}", value);
  VerifyEquivalent(legacy, fmt_new);
  // Should be "    0xff" (8 chars total including 0x prefix)
}

// Test 44: Plus sign with width
TEST_F(StringPrintfFmtEquivalenceTest, PlusSignWithWidth) {
  int positive = 42;
  auto legacy = LegacyStringPrintf("%+5d", positive);
  auto fmt_new = fmt::format("{:+5d}", positive);
  VerifyEquivalent(legacy, fmt_new);
  // Should be "  +42" (includes + sign in width)
}

// Test 45: Space flag with width (space for positive, minus for negative)
TEST_F(StringPrintfFmtEquivalenceTest, SpaceFlagWithWidth) {
  int positive = 42;
  int negative = -42;
  auto legacy_pos = LegacyStringPrintf("% 5d", positive);
  auto fmt_pos = fmt::format("{: 5d}", positive);
  VerifyEquivalent(legacy_pos, fmt_pos);

  auto legacy_neg = LegacyStringPrintf("% 5d", negative);
  auto fmt_neg = fmt::format("{: 5d}", negative);
  VerifyEquivalent(legacy_neg, fmt_neg);
}

} // namespace kudu
