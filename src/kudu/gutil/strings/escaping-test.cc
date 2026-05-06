// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/gutil/strings/escaping.h"

#include <string>

#include <gtest/gtest.h>

namespace strings {

// Test utf8SafeCEscape with bytes >= 0x80.
// This tests the fix for tautological comparison where comparing
// a signed char to 0x80 was always true (since signed char max is 127).
// With the fix, utf8SafeCEscape correctly preserves UTF-8 bytes.
TEST(EscapingTest, Utf8SafeCEscapeWithHighBytes) {
  // ASCII characters should be preserved as-is (printable)
  EXPECT_EQ("hello", utf8SafeCEscape("hello"));

  // Control characters should be escaped
  EXPECT_EQ("\\n", utf8SafeCEscape("\n"));
  EXPECT_EQ("\\r", utf8SafeCEscape("\r"));
  EXPECT_EQ("\\t", utf8SafeCEscape("\t"));

  // UTF-8 multi-byte sequences (bytes >= 0x80) should be preserved
  // when using utf8SafeCEscape (that's the "Utf8Safe" part)

  // 2-byte UTF-8: U+00E9 (é) = 0xC3 0xA9
  std::string utf8EAcute = "\xc3\xa9";
  EXPECT_EQ(utf8EAcute, utf8SafeCEscape(utf8EAcute));

  // 3-byte UTF-8: U+2660 (♠) = 0xE2 0x99 0xA0
  std::string utf8Spade = "\xe2\x99\xa0";
  EXPECT_EQ(utf8Spade, utf8SafeCEscape(utf8Spade));

  // 4-byte UTF-8: U+1F600 (😀) = 0xF0 0x9F 0x98 0x80
  std::string utf8Grin = "\xf0\x9f\x98\x80";
  EXPECT_EQ(utf8Grin, utf8SafeCEscape(utf8Grin));

  // Mixed ASCII and UTF-8
  std::string mixed = "Hello \xe2\x99\xa0 World";
  EXPECT_EQ(mixed, utf8SafeCEscape(mixed));

  // UTF-8 with control chars that need escaping
  std::string utf8WithNewline = "\xe2\x99\xa0\n\xe2\x99\xa0";
  EXPECT_EQ("\xe2\x99\xa0\\n\xe2\x99\xa0", utf8SafeCEscape(utf8WithNewline));

  // utf8SafeCEscape should preserve high bytes (treating as UTF-8 continuation)
  std::string highByte = "\x80";
  EXPECT_EQ(highByte, utf8SafeCEscape(highByte));

  // Byte at 0xff boundary
  std::string maxByte = "\xff";
  EXPECT_EQ(maxByte, utf8SafeCEscape(maxByte));
}

// Test edge case: byte value 0x7F (DEL) - just below 0x80 threshold
TEST(EscapingTest, Utf8SafeCEscapeBoundary) {
  // 0x7F (DEL) is not printable, should be escaped
  std::string delChar = "\x7f";
  EXPECT_NE(delChar, utf8SafeCEscape(delChar));

  // 0x80 is at the boundary - should be preserved by utf8SafeCEscape
  std::string boundary = "\x80";
  EXPECT_EQ(boundary, utf8SafeCEscape(boundary));
}

} // namespace strings
