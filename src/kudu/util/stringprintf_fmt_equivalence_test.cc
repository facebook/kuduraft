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
static std::string legacyStringPrintf(const char* format, ...) {
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
  void verifyEquivalent(
      const std::string& legacy,
      const std::string& fmtResult) {
    EXPECT_EQ(legacy, fmtResult) << "Legacy: '" << legacy << "'\n"
                                 << "fmt:    '" << fmtResult << "'";
  }
};

// Test 1: Basic integer formatting
TEST_F(StringPrintfFmtEquivalenceTest, BasicInteger) {
  int value = 42;
  auto legacy = legacyStringPrintf("%d", value);
  auto fmtNew = fmt::format("{}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 2: Zero-padded hex formatting (common in block IDs)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedHex) {
  uint64_t value = 0x1234567890ABCDEF;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%016" PRIx64, value);
  auto fmtNew = fmt::format("{:016x}", value);
  verifyEquivalent(std::string(legacy), fmtNew);
}

// Test 3: Two-digit hex (byte formatting)
TEST_F(StringPrintfFmtEquivalenceTest, TwoDigitHex) {
  uint8_t byte = 0xAB;
  auto legacy = legacyStringPrintf("%02x", byte);
  auto fmtNew = fmt::format("{:02x}", byte);
  verifyEquivalent(legacy, fmtNew);
}

// Test 4: Float with precision
TEST_F(StringPrintfFmtEquivalenceTest, FloatPrecision) {
  double value = 123.456789;
  auto legacy = legacyStringPrintf("%.2f", value);
  auto fmtNew = fmt::format("{:.2f}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 5: Float with 3 decimals
TEST_F(StringPrintfFmtEquivalenceTest, FloatThreeDecimals) {
  double value = 1.23456;
  auto legacy = legacyStringPrintf("%.3f", value);
  auto fmtNew = fmt::format("{:.3f}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 6: Width specification for integers
TEST_F(StringPrintfFmtEquivalenceTest, WidthSpecification) {
  int value = 42;
  auto legacy = legacyStringPrintf("%7d", value);
  auto fmtNew = fmt::format("{:7d}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 7: Octal formatting
TEST_F(StringPrintfFmtEquivalenceTest, OctalFormatting) {
  unsigned int value = 0755;
  auto legacy = legacyStringPrintf("%03o", value);
  auto fmtNew = fmt::format("{:03o}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 8: String formatting
TEST_F(StringPrintfFmtEquivalenceTest, StringFormatting) {
  const char* str = "hello";
  auto legacy = legacyStringPrintf("%s", str);
  auto fmtNew = fmt::format("{}", str);
  verifyEquivalent(legacy, fmtNew);
}

// Test 9: Mixed formatting (multiple arguments)
TEST_F(StringPrintfFmtEquivalenceTest, MixedMultipleArgs) {
  int num = 42;
  const char* str = "test";
  double flt = 3.14;
  auto legacy = legacyStringPrintf("%d %s %.2f", num, str, flt);
  auto fmtNew = fmt::format("{} {} {:.2f}", num, str, flt);
  verifyEquivalent(legacy, fmtNew);
}

// Test 10: 64-bit integer formatting
TEST_F(StringPrintfFmtEquivalenceTest, Int64Formatting) {
  int64_t value = 9876543210LL;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%" PRId64, value);
  auto fmtNew = fmt::format("{}", value);
  verifyEquivalent(std::string(legacy), fmtNew);
}

// Test 11: Zero-padded integer (like log index)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedInteger) {
  int64_t value = 123;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%09" PRId64, value);
  auto fmtNew = fmt::format("{:09d}", value);
  verifyEquivalent(std::string(legacy), fmtNew);
}

// Test 12: Percentage formatting
TEST_F(StringPrintfFmtEquivalenceTest, PercentageFormatting) {
  double percent = 75.5;
  auto legacy = legacyStringPrintf("%.2f%%", percent);
  auto fmtNew = fmt::format("{:.2f}%", percent);
  verifyEquivalent(legacy, fmtNew);
}

// Test 13: Scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, ScientificNotation) {
  double value = 1.234e-5;
  auto legacy = legacyStringPrintf("%0.3G", value);
  auto fmtNew = fmt::format("{:0.3G}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 14: Lowercase scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, LowercaseScientific) {
  double value = 1.234e5;
  auto legacy = legacyStringPrintf("%0.3g", value);
  auto fmtNew = fmt::format("{:0.3g}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 15: Boolean to integer (bitmap test case)
TEST_F(StringPrintfFmtEquivalenceTest, BooleanToInteger) {
  bool value = true;
  auto legacy = legacyStringPrintf("%d", static_cast<int>(value));
  auto fmtNew = fmt::format("{}", static_cast<int>(value));
  verifyEquivalent(legacy, fmtNew);
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
  auto fmtNew = fmt::format("0x{:x}", reinterpret_cast<uintptr_t>(ptr));
  verifyEquivalent(std::string(legacy), fmtNew);
}

// Test 17: Negative numbers
TEST_F(StringPrintfFmtEquivalenceTest, NegativeNumbers) {
  int value = -42;
  auto legacy = legacyStringPrintf("%d", value);
  auto fmtNew = fmt::format("{}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 18: Negative floats with precision
TEST_F(StringPrintfFmtEquivalenceTest, NegativeFloats) {
  double value = -123.456;
  auto legacy = legacyStringPrintf("%.2f", value);
  auto fmtNew = fmt::format("{:.2f}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 19: Width and precision together
TEST_F(StringPrintfFmtEquivalenceTest, WidthAndPrecision) {
  size_t value = 123;
  auto legacy = legacyStringPrintf("%4zu", value);
  auto fmtNew = fmt::format("{:4}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 20: Hex with uppercase
TEST_F(StringPrintfFmtEquivalenceTest, UppercaseHex) {
  uint32_t value = 0xABCD;
  auto legacy = legacyStringPrintf("%X", value);
  auto fmtNew = fmt::format("{:X}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 21: Multiple hex bytes (common in file_block_manager)
TEST_F(StringPrintfFmtEquivalenceTest, MultipleHexBytes) {
  uint64_t id = 0x0000AB00CD000000ULL;

  // Test byte2 extraction
  auto byte2Legacy =
      legacyStringPrintf("%02llx", (id & 0x0000FF0000000000ULL) >> 40);
  auto byte2Fmt = fmt::format("{:02x}", (id & 0x0000FF0000000000ULL) >> 40);
  verifyEquivalent(byte2Legacy, byte2Fmt);

  // Test byte3 extraction
  auto byte3Legacy =
      legacyStringPrintf("%02llx", (id & 0x000000FF00000000ULL) >> 32);
  auto byte3Fmt = fmt::format("{:02x}", (id & 0x000000FF00000000ULL) >> 32);
  verifyEquivalent(byte3Legacy, byte3Fmt);

  // Test byte4 extraction
  auto byte4Legacy =
      legacyStringPrintf("%02llx", (id & 0x00000000FF000000ULL) >> 24);
  auto byte4Fmt = fmt::format("{:02x}", (id & 0x00000000FF000000ULL) >> 24);
  verifyEquivalent(byte4Legacy, byte4Fmt);
}

// Test 22: Format with alignment
TEST_F(StringPrintfFmtEquivalenceTest, RightAlignedInteger) {
  size_t value = 5;
  auto legacy = legacyStringPrintf("%4zu", value);
  auto fmtNew = fmt::format("{:4}", value);
  verifyEquivalent(legacy, fmtNew);
}

// Test 23: Complex string with multiple format specifiers (from real code)
TEST_F(StringPrintfFmtEquivalenceTest, ComplexFormatString) {
  const char* prefix = "Prefix";
  double val = 12.345;
  char unit = 'M';
  auto legacy = legacyStringPrintf("%s%.2f%c", prefix, val, unit);
  auto fmtNew = fmt::format("{}{:.2f}{}", prefix, val, unit);
  verifyEquivalent(legacy, fmtNew);
}

// Test 24: Format with negative sign
TEST_F(StringPrintfFmtEquivalenceTest, NegativeSign) {
  const char* sign = "-";
  int64_t value = 123;
  char legacy[32];
  snprintf(legacy, sizeof(legacy), "%s%" PRId64, sign, value);
  auto fmtNew = fmt::format("{}{}", sign, value);
  verifyEquivalent(std::string(legacy), fmtNew);
}

// Test 25: Zero value formatting
TEST_F(StringPrintfFmtEquivalenceTest, ZeroValue) {
  int value = 0;
  auto legacy = legacyStringPrintf("%d", value);
  auto fmtNew = fmt::format("{}", value);
  verifyEquivalent(legacy, fmtNew);
}

// ============================================================================
// EDGE CASE TESTS: Width and Truncation Behavior
// ============================================================================

// Test 26: String precision truncation (%.Ns limits string to N chars)
TEST_F(StringPrintfFmtEquivalenceTest, StringPrecisionTruncation) {
  const char* longStr = "ThisIsAVeryLongString";
  // %.5s should truncate to first 5 characters
  auto legacy = legacyStringPrintf("%.5s", longStr);
  auto fmtNew = fmt::format("{:.5}", longStr);
  verifyEquivalent(legacy, fmtNew);
  // Verify actual truncation happened
  EXPECT_EQ(legacy, "ThisI");
}

// Test 27: Integer width overflow (width < actual digits)
TEST_F(StringPrintfFmtEquivalenceTest, IntegerWidthOverflow) {
  int largeNum = 123456;
  // Width of 3 but number has 6 digits - should NOT truncate
  auto legacy = legacyStringPrintf("%3d", largeNum);
  auto fmtNew = fmt::format("{:3d}", largeNum);
  verifyEquivalent(legacy, fmtNew);
  // Verify no truncation - full number should be printed
  EXPECT_EQ(legacy, "123456");
}

// Test 28: String width with smaller input (should pad, not truncate)
TEST_F(StringPrintfFmtEquivalenceTest, StringWidthPadding) {
  const char* shortStr = "Hi";
  // Width of 10 should pad with spaces, not truncate
  auto legacy = legacyStringPrintf("%10s", shortStr);
  auto fmtNew = fmt::format("{:>10}", shortStr);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy.length(), 10u);
}

// Test 29: Zero-padded integer with negative number
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedNegative) {
  int negative = -123;
  // %05d with negative number - zero padding between sign and digits
  auto legacy = legacyStringPrintf("%05d", negative);
  auto fmtNew = fmt::format("{:05d}", negative);
  verifyEquivalent(legacy, fmtNew);
  // Should be "-0123" (5 chars total, padding after sign)
  EXPECT_EQ(legacy, "-0123");
}

// Test 30: Zero-padded integer overflow (number larger than width)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPaddedOverflow) {
  int largeNum = 123456;
  // %03d but number has 6 digits - should NOT truncate
  auto legacy = legacyStringPrintf("%03d", largeNum);
  auto fmtNew = fmt::format("{:03d}", largeNum);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "123456");
}

// Test 31: Float width overflow
TEST_F(StringPrintfFmtEquivalenceTest, FloatWidthOverflow) {
  double largeFloat = 12345.674; // Changed to avoid rounding edge case
  // Width of 5 but formatted number is much longer
  auto legacy = legacyStringPrintf("%5.2f", largeFloat);
  auto fmtNew = fmt::format("{:5.2f}", largeFloat);
  verifyEquivalent(legacy, fmtNew);
  // Should not truncate, prints full "12345.67"
  EXPECT_EQ(legacy, "12345.67");
}

// Test 32: Hex width overflow
TEST_F(StringPrintfFmtEquivalenceTest, HexWidthOverflow) {
  uint64_t largeHex = 0xABCDEF123456;
  // Width of 4 but hex representation is 12 chars
  auto legacy = legacyStringPrintf("%04x", static_cast<uint32_t>(largeHex));
  auto fmtNew = fmt::format("{:04x}", static_cast<uint32_t>(largeHex));
  verifyEquivalent(legacy, fmtNew);
  // Verify no truncation
  EXPECT_GT(legacy.length(), 4u);
}

// Test 33: String width vs precision (width pads, precision truncates)
TEST_F(StringPrintfFmtEquivalenceTest, StringWidthVsPrecision) {
  const char* str = "Test";
  // Width 10, precision 2 - should truncate to 2 chars then pad to 10
  auto legacy = legacyStringPrintf("%10.2s", str);
  auto fmtNew = fmt::format("{:>10.2}", str);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "        Te");
}

// Test 34: Left alignment with width (negative width or '-' flag)
TEST_F(StringPrintfFmtEquivalenceTest, LeftAlignedString) {
  const char* str = "Hi";
  auto legacy = legacyStringPrintf("%-10s", str);
  auto fmtNew = fmt::format("{:<10}", str);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "Hi        ");
}

// Test 35: Left-aligned integer with width overflow
TEST_F(StringPrintfFmtEquivalenceTest, LeftAlignedIntegerOverflow) {
  int largeNum = 123456;
  auto legacy = legacyStringPrintf("%-3d", largeNum);
  auto fmtNew = fmt::format("{:<3d}", largeNum);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "123456");
}

// Test 36: Float precision overflow with scientific notation
TEST_F(StringPrintfFmtEquivalenceTest, FloatPrecisionEdgeCase) {
  double tiny = 0.000000123456789;
  auto legacy = legacyStringPrintf("%.20f", tiny);
  auto fmtNew = fmt::format("{:.20f}", tiny);
  verifyEquivalent(legacy, fmtNew);
}

// Test 37: Very wide width specification
TEST_F(StringPrintfFmtEquivalenceTest, VeryWideWidth) {
  int smallNum = 5;
  auto legacy = legacyStringPrintf("%50d", smallNum);
  auto fmtNew = fmt::format("{:50d}", smallNum);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy.length(), 50u);
}

// Test 38: Zero precision float (%.0f)
TEST_F(StringPrintfFmtEquivalenceTest, ZeroPrecisionFloat) {
  double value = 123.789;
  auto legacy = legacyStringPrintf("%.0f", value);
  auto fmtNew = fmt::format("{:.0f}", value);
  verifyEquivalent(legacy, fmtNew);
  // Should round to "124"
  EXPECT_EQ(legacy, "124");
}

// Test 39: Empty string with precision
TEST_F(StringPrintfFmtEquivalenceTest, EmptyStringPrecision) {
  const char* empty = "";
  auto legacy = legacyStringPrintf("%.5s", empty);
  auto fmtNew = fmt::format("{:.5}", empty);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "");
}

// Test 40: Boundary case - exactly matching width
TEST_F(StringPrintfFmtEquivalenceTest, ExactWidthMatch) {
  const char* str = "12345";
  auto legacy = legacyStringPrintf("%5s", str);
  auto fmtNew = fmt::format("{:>5}", str);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "12345");
}

// Test 41: Multiple width overflows in same format string
TEST_F(StringPrintfFmtEquivalenceTest, MultipleWidthOverflows) {
  int num1 = 123456;
  int num2 = 789012;
  auto legacy = legacyStringPrintf("%3d-%3d", num1, num2);
  auto fmtNew = fmt::format("{:3d}-{:3d}", num1, num2);
  verifyEquivalent(legacy, fmtNew);
  EXPECT_EQ(legacy, "123456-789012");
}

// Test 42: Negative number with width (no zero padding)
TEST_F(StringPrintfFmtEquivalenceTest, NegativeNumberWidth) {
  int negative = -42;
  auto legacy = legacyStringPrintf("%10d", negative);
  auto fmtNew = fmt::format("{:10d}", negative);
  verifyEquivalent(legacy, fmtNew);
  // Should pad with spaces on left: "       -42"
  EXPECT_EQ(legacy.length(), 10u);
}

// Test 43: Hex with '#' prefix and width
TEST_F(StringPrintfFmtEquivalenceTest, HexPrefixWithWidth) {
  int value = 255;
  auto legacy = legacyStringPrintf("%#8x", value);
  auto fmtNew = fmt::format("{:#8x}", value);
  verifyEquivalent(legacy, fmtNew);
  // Should be "    0xff" (8 chars total including 0x prefix)
}

// Test 44: Plus sign with width
TEST_F(StringPrintfFmtEquivalenceTest, PlusSignWithWidth) {
  int positive = 42;
  auto legacy = legacyStringPrintf("%+5d", positive);
  auto fmtNew = fmt::format("{:+5d}", positive);
  verifyEquivalent(legacy, fmtNew);
  // Should be "  +42" (includes + sign in width)
}

// Test 45: Space flag with width (space for positive, minus for negative)
TEST_F(StringPrintfFmtEquivalenceTest, SpaceFlagWithWidth) {
  int positive = 42;
  int negative = -42;
  auto legacyPos = legacyStringPrintf("% 5d", positive);
  auto fmtPos = fmt::format("{: 5d}", positive);
  verifyEquivalent(legacyPos, fmtPos);

  auto legacyNeg = legacyStringPrintf("% 5d", negative);
  auto fmtNeg = fmt::format("{: 5d}", negative);
  verifyEquivalent(legacyNeg, fmtNeg);
}

} // namespace kudu
