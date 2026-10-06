// Copyright 2011 Google Inc. All Rights Reserved.
// based on contributions of various authors in strings/strutil_unittest.cc
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.

#include "kudu/gutil/strings/strip.h"

#include <string>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/stringpiece.h"

using std::string;

string stripPrefixString(StringPiece str, const StringPiece& prefix) {
  if (str.startsWith(prefix))
    str.removePrefix(prefix.length());
  return str.asString();
}

string stripSuffixString(StringPiece str, const StringPiece& suffix) {
  if (str.endsWith(suffix))
    str.removeSuffix(suffix.length());
  return str.asString();
}

bool tryStripSuffixString(
    StringPiece str,
    const StringPiece& suffix,
    string* result) {
  const bool hasSuffix = str.endsWith(suffix);
  if (hasSuffix)
    str.removeSuffix(suffix.length());
  str.asString().swap(*result);
  return hasSuffix;
}

// ----------------------------------------------------------------------
// stripString
//    Replaces any occurrence of the character 'remove' (or the characters
//    in 'remove') with the character 'replacewith'.
// ----------------------------------------------------------------------
void stripString(string* s, StringPiece remove, char replacewith) {
  for (char& c : *s) {
    if (remove.find(c) != StringPiece::kNpos) {
      c = replacewith;
    }
  }
}

// ----------------------------------------------------------------------
// StripWhiteSpace
// ----------------------------------------------------------------------
void StripWhiteSpace(const char** str, int* len) {
  // strip off trailing whitespace
  while ((*len) > 0 && asciiIsSpace((*str)[(*len) - 1])) {
    (*len)--;
  }

  // strip off leading whitespace
  while ((*len) > 0 && asciiIsSpace((*str)[0])) {
    (*len)--;
    (*str)++;
  }
}

bool StripTrailingNewline(string* s) {
  if (!s->empty() && (*s)[s->size() - 1] == '\n') {
    if (s->size() > 1 && (*s)[s->size() - 2] == '\r')
      s->resize(s->size() - 2);
    else
      s->resize(s->size() - 1);
    return true;
  }
  return false;
}

void StripWhiteSpace(string* str) {
  int strLength = str->length();

  // Strip off leading whitespace.
  int first = 0;
  while (first < strLength && asciiIsSpace(str->at(first))) {
    ++first;
  }
  // If entire string is white space.
  if (first == strLength) {
    str->clear();
    return;
  }
  if (first > 0) {
    str->erase(0, first);
    strLength -= first;
  }

  // Strip off trailing whitespace.
  int last = strLength - 1;
  while (last >= 0 && asciiIsSpace(str->at(last))) {
    --last;
  }
  if (last != (strLength - 1) && last >= 0) {
    str->erase(last + 1, string::npos);
  }
}

// ----------------------------------------------------------------------
// stripDupCharacters
//    Replaces any repeated occurrence of the character 'repeat_char'
//    with single occurrence.  e.g.,
//       stripDupCharacters("a//b/c//d", '/', 0) => "a/b/c/d"
//    Return the number of characters removed
// ----------------------------------------------------------------------
int stripDupCharacters(string* s, char dupChar, int startPos) {
  if (startPos < 0)
    startPos = 0;

  // remove dups by compaction in-place
  int inputPos = startPos; // current reader position
  int outputPos = startPos; // current writer position
  const int inputEnd = s->size();
  while (inputPos < inputEnd) {
    // keep current character
    const char currChar = (*s)[inputPos];
    if (outputPos != inputPos) // must copy
      (*s)[outputPos] = currChar;
    ++inputPos;
    ++outputPos;

    if (currChar == dupChar) { // skip subsequent dups
      while ((inputPos < inputEnd) && ((*s)[inputPos] == dupChar))
        ++inputPos;
    }
  }
  const int numDeleted = inputPos - outputPos;
  s->resize(s->size() - numDeleted);
  return numDeleted;
}

void StripTrailingWhitespace(string* const s) {
  string::size_type i;
  for (i = s->size(); i > 0 && asciiIsSpace((*s)[i - 1]); --i) {
  }

  s->resize(i);
}
