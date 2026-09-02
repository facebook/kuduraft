//
// Copyright 1999-2006 and onwards Google, Inc.
//
// Useful string functions and so forth.  This is a grab-bag file.
//
// You might also want to look at memutil.h, which holds mem*()
// equivalents of a lot of the str*() functions in string.h,
// eg memstr, mempbrk, etc.
//
// These functions work fine for UTF-8 strings as long as you can
// consider them to be just byte strings.  For example, due to the
// design of UTF-8 you do not need to worry about accidental matches,
// as long as all your inputs are valid UTF-8 (use \uHHHH, not \xHH or \oOOO).
//
// Caveats:
// * all the lengths in these routines refer to byte counts,
//   not character counts.
// * case-insensitivity in these routines assumes that all the letters
//   in question are in the range A-Z or a-z.
//
// If you need Unicode specific processing (for example being aware of
// Unicode character boundaries, or knowledge of Unicode casing rules,
// or various forms of equivalence and normalization), take a look at
// files in i18n/utf8.

#pragma once

#include <cstddef>

#include <string>

#include "kudu/gutil/strings/stringpiece.h"

// Older functions.

// Finds the first occurrence of a character in at most a given number of bytes
// of a char* string. Returns a pointer to the first occurrence, or NULL if no
// occurrence found in the first sz bytes.
// Never searches past the first null character in the string; therefore, only
// suitable for null-terminated strings.
// WARNING: Removes const-ness of string argument!
inline char* strnchr(const char* buf, char c, int sz) {
  const char* end = buf + sz;
  while (buf != end && *buf) {
    if (*buf == c) {
      return const_cast<char*>(buf);
    }
    ++buf;
  }
  return nullptr;
}

// Finds the first occurrence of the null-terminated needle in at most the first
// haystack_len bytes of haystack. Returns NULL if needle is not found. Returns
// haystack if needle is empty.
// WARNING: Removes const-ness of string argument!
char* strnstr(const char* haystack, const char* needle, size_t haystackLen);

// Returns whether str begins with prefix.
inline bool hasPrefixString(const StringPiece& str, const StringPiece& prefix) {
  return str.startsWith(prefix);
}

// Returns whether str ends with suffix.
inline bool hasSuffixString(const StringPiece& str, const StringPiece& suffix) {
  return str.endsWith(suffix);
}

// Returns true if the string passed in matches the pattern. The pattern
// string can contain wildcards like * and ?
// The backslash character (\) is an escape character for * and ?
// We limit the patterns to having a max of 16 * or ? characters.
// ? matches 0 or 1 character, while * matches 0 or more characters.
bool matchPattern(const StringPiece& str, const StringPiece& pattern);

// Returns the number of times a character occurs in a string for a string
// defined by a pointer to the first character and a pointer just past the last
// character.
inline ptrdiff_t strcount(const char* bufBegin, const char* bufEnd, char c) {
  if (bufBegin == nullptr) {
    return 0;
  }
  if (bufEnd <= bufBegin) {
    return 0;
  }
  ptrdiff_t num = 0;
  for (const char* bp = bufBegin; bp != bufEnd; bp++) {
    if (*bp == c) {
      num++;
    }
  }
  return num;
}
// Returns the number of times a character occurs in a string for a string
// defined by a pointer to the first char and a length:
inline ptrdiff_t strcount(const char* buf, size_t len, char c) {
  return strcount(buf, buf + len, c);
}
// Returns the number of times a character occurs in a string for a C++ string:
inline ptrdiff_t strcount(const std::string& buf, char c) {
  return strcount(buf.c_str(), buf.size(), c);
}

// Returns the smallest lexicographically larger string of equal or smaller
// length. Returns an empty string if there is no such successor (if the input
// is empty or consists entirely of 0xff bytes).
// Useful for calculating the smallest lexicographically larger string
// that will not be prefixed by the input string.
//
// Examples:
// "a" -> "b", "aaa" -> "aab", "aa\xff" -> "ab", "\xff" -> "", "" -> ""
std::string prefixSuccessor(const StringPiece& prefix);

// Fills in *separator with a short string less than limit but greater than or
// equal to start. If limit is greater than start, *separator is the common
// prefix of start and limit, followed by the successor to the next character in
// start. Examples:
// findShortestSeparator("foobar", "foxhunt", &sep) => sep == "fop"
// findShortestSeparator("abracadabra", "bacradabra", &sep) => sep == "b"
// If limit is less than or equal to start, fills in *separator with start.
void findShortestSeparator(
    const StringPiece& start,
    const StringPiece& limit,
    std::string* separator);

namespace strings {

// BSD-style safe and consistent string copy functions.
// Copies |src| to |dst|, where |dstSize| is the total allocated size of |dst|.
// Copies at most |dstSize|-1 characters, and always NULL terminates |dst|, as
// long as |dstSize| is not 0.  Returns the length of |src| in characters.
// If the return value is >= dstSize, then the output was truncated.
// NOTE: All sizes are in number of characters, NOT in bytes.
size_t strlcpy(char* dst, const char* src, size_t dstSize);

} // namespace strings

// Replaces the first occurrence (if replace_all is false) or all occurrences
// (if replace_all is true) of oldsub in s with newsub. In the second version,
// *res must be distinct from all the other arguments.
std::string stringReplace(
    const StringPiece& s,
    const StringPiece& oldsub,
    const StringPiece& newsub,
    bool replaceAll);
void stringReplace(
    const StringPiece& s,
    const StringPiece& oldsub,
    const StringPiece& newsub,
    bool replaceAll,
    std::string* res);

// Finds (case insensitively) the first occurrence of (null terminated) needle
// in at most the first len bytes of haystack. Returns a pointer into haystack,
// or NULL if needle wasn't found.
// WARNING: Removes const-ness of haystack!
const char* gstrncasestr(const char* haystack, const char* needle, size_t len);
