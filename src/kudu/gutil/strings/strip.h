// Copyright 2011 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.h
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.

#pragma once

#include <string>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/stringpiece.h"

// Given a string and a putative prefix, returns the string minus the
// prefix string if the prefix matches, otherwise the original
// string.
std::string stripPrefixString(StringPiece str, const StringPiece& prefix);

// Given a string and a putative suffix, returns the string minus the
// suffix string if the suffix matches, otherwise the original
// string.
std::string stripSuffixString(StringPiece str, const StringPiece& suffix);

// Like stripSuffixString, but return true if the suffix was
// successfully matched.  Write the output to *result.
// It is safe for result to point back to the input string.
bool tryStripSuffixString(
    StringPiece str,
    const StringPiece& suffix,
    std::string* result);

// ----------------------------------------------------------------------
// stripString
//    Replaces any occurrence of the character 'remove' (or the characters
//    in 'remove') with the character 'replacewith'.
//    Good for keeping html characters or protocol characters (\t) out
//    of places where they might cause a problem.
// ----------------------------------------------------------------------
inline void stripString(char* str, char remove, char replacewith) {
  for (; *str; str++) {
    if (*str == remove) {
      *str = replacewith;
    }
  }
}

void stripString(std::string* s, StringPiece remove, char replacewith);

// ----------------------------------------------------------------------
// stripDupCharacters
//    Replaces any repeated occurrence of the character 'dup_char'
//    with single occurrence.  e.g.,
//       stripDupCharacters("a//b/c//d", '/', 0) => "a/b/c/d"
//    Return the number of characters removed
// ----------------------------------------------------------------------
int stripDupCharacters(std::string* s, char dupChar, int startPos);

// ----------------------------------------------------------------------
// StripWhiteSpace
//    "Removes" whitespace from both sides of string.  Pass in a pointer to an
//    array of characters, and its length.  The function changes the pointer
//    and length to refer to a substring that does not contain leading or
//    trailing spaces; it does not modify the string itself.  If the caller is
//    using NUL-terminated strings, it is the caller's responsibility to insert
//    the NUL character at the end of the substring."
//
//    Note: to be completely type safe, this function should be
//    parameterized as a template: template<typename anyChar> void
//    StripWhiteSpace(anyChar** str, int* len), where the expectation
//    is that anyChar could be char, const char, w_char, const w_char,
//    unicode_char, or any other character type we want.  However, we
//    just provided a version for char and const char.  C++ is
//    inconvenient, but correct, here.  Ask Amit is you want to know
//    the type safety details.
// ----------------------------------------------------------------------
void StripWhiteSpace(const char** str, int* len);

//------------------------------------------------------------------------
// StripTrailingWhitespace()
//   Removes whitespace at the end of the string *s.
//------------------------------------------------------------------------
void StripTrailingWhitespace(std::string* s);

//------------------------------------------------------------------------
// StripTrailingNewline(string*)
//   Strips the very last trailing newline or CR+newline from its
//   input, if one exists.  Useful for dealing with MapReduce's text
//   input mode, which appends '\n' to each map input.  Returns true
//   if a newline was stripped.
//------------------------------------------------------------------------
bool StripTrailingNewline(std::string* s);

inline void StripWhiteSpace(char** str, int* len) {
  // The "real" type for StripWhiteSpace is ForAll char types C, take
  // (C, int) as input and return (C, int) as output.  We're using the
  // cast here to assert that we can take a char*, even though the
  // function thinks it's assigning to const char*.
  StripWhiteSpace(const_cast<const char**>(str), len);
}

inline void StripWhiteSpace(StringPiece* str) {
  const char* data = str->data();
  int len = str->size();
  StripWhiteSpace(&data, &len);
  str->set(data, len);
}

void StripWhiteSpace(std::string* str);

namespace strings {

template <typename Collection>
inline void StripWhiteSpaceInCollection(Collection* collection) {
  for (typename Collection::iterator it = collection->begin();
       it != collection->end();
       ++it) {
    StripWhiteSpace(&(*it));
  }
}

} // namespace strings

// ----------------------------------------------------------------------
// StripLeadingWhiteSpace
//    "Removes" whitespace from beginning of string. Returns ptr to first
//    non-whitespace character if one is present, NULL otherwise. Assumes
//    "line" is null-terminated.
// ----------------------------------------------------------------------

inline const char* StripLeadingWhiteSpace(const char* line) {
  // skip leading whitespace
  while (asciiIsSpace(*line)) {
    ++line;
  }

  if ('\0' == *line) { // end of line, no non-whitespace
    return nullptr;
  }

  return line;
}

// StripLeadingWhiteSpace for non-const strings.
inline char* StripLeadingWhiteSpace(char* line) {
  return const_cast<char*>(
      StripLeadingWhiteSpace(const_cast<const char*>(line)));
}

// ----------------------------------------------------------------------
// SkipLeadingWhiteSpace
//    Returns str advanced past white space characters, if any.
//    Never returns NULL.  "str" must be terminated by a null character.
// ----------------------------------------------------------------------
inline const char* SkipLeadingWhiteSpace(const char* str) {
  while (asciiIsSpace(*str)) {
    ++str;
  }
  return str;
}

inline char* SkipLeadingWhiteSpace(char* str) {
  while (asciiIsSpace(*str)) {
    ++str;
  }
  return str;
}
