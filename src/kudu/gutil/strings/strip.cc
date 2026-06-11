// Copyright 2011 Google Inc. All Rights Reserved.
// based on contributions of various authors in strings/strutil_unittest.cc
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.

#include "kudu/gutil/strings/strip.h"

#include <cassert>
#include <cstring>

#include <algorithm>
#include <string>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/stringpiece.h"

using std::string;

string StripPrefixString(StringPiece str, const StringPiece& prefix) {
  if (str.startsWith(prefix))
    str.removePrefix(prefix.length());
  return str.asString();
}

bool TryStripPrefixString(
    StringPiece str,
    const StringPiece& prefix,
    string* result) {
  const bool hasPrefix = str.startsWith(prefix);
  if (hasPrefix)
    str.removePrefix(prefix.length());
  str.asString().swap(*result);
  return hasPrefix;
}

string StripSuffixString(StringPiece str, const StringPiece& suffix) {
  if (str.endsWith(suffix))
    str.removeSuffix(suffix.length());
  return str.asString();
}

bool TryStripSuffixString(
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
// StripString
//    Replaces any occurrence of the character 'remove' (or the characters
//    in 'remove') with the character 'replacewith'.
// ----------------------------------------------------------------------
void StripString(char* str, StringPiece remove, char replacewith) {
  for (; *str != '\0'; ++str) {
    if (remove.find(*str) != StringPiece::kNpos) {
      *str = replacewith;
    }
  }
}

void StripString(char* str, int len, StringPiece remove, char replacewith) {
  char* end = str + len;
  for (; str < end; ++str) {
    if (remove.find(*str) != StringPiece::kNpos) {
      *str = replacewith;
    }
  }
}

void StripString(string* s, StringPiece remove, char replacewith) {
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
// Misc. stripping routines
// ----------------------------------------------------------------------
void StripCurlyBraces(string* s) {
  return StripBrackets('{', '}', s);
}

void StripBrackets(char left, char right, string* s) {
  string::iterator openCurly = find(s->begin(), s->end(), left);
  while (openCurly != s->end()) {
    string::iterator closeCurly = find(openCurly, s->end(), right);
    if (closeCurly == s->end())
      return;
    openCurly = s->erase(openCurly, closeCurly + 1);
    openCurly = find(openCurly, s->end(), left);
  }
}

void StripMarkupTags(string* s) {
  string::iterator openBracket = find(s->begin(), s->end(), '<');
  while (openBracket != s->end()) {
    string::iterator closeBracket = find(openBracket, s->end(), '>');
    if (closeBracket == s->end()) {
      s->erase(openBracket, closeBracket);
      return;
    }

    openBracket = s->erase(openBracket, closeBracket + 1);
    openBracket = find(openBracket, s->end(), '<');
  }
}

string OutputWithMarkupTagsStripped(const string& s) {
  string result(s);
  StripMarkupTags(&result);
  return result;
}

int TrimStringLeft(string* s, const StringPiece& remove) {
  int i = 0;
  while (i < s->size() && memchr(remove.data(), (*s)[i], remove.size())) {
    ++i;
  }
  if (i > 0)
    s->erase(0, i);
  return i;
}

int TrimStringRight(string* s, const StringPiece& remove) {
  int i = s->size(), trimmed = 0;
  while (i > 0 && memchr(remove.data(), (*s)[i - 1], remove.size())) {
    --i;
  }
  if (i < s->size()) {
    trimmed = s->size() - i;
    s->erase(i);
  }
  return trimmed;
}

// ----------------------------------------------------------------------
// Various removal routines
// ----------------------------------------------------------------------
int strrm(char* str, char c) {
  char *src, *dest;
  for (src = dest = str; *src != '\0'; ++src)
    if (*src != c)
      *(dest++) = *src;
  *dest = '\0';
  return dest - str;
}

int memrm(char* str, int strlen, char c) {
  char *src, *dest;
  for (src = dest = str; strlen-- > 0; ++src)
    if (*src != c)
      *(dest++) = *src;
  return dest - str;
}

int strrmm(char* str, const char* chars) {
  char *src, *dest;
  for (src = dest = str; *src != '\0'; ++src) {
    bool skip = false;
    for (const char* c = chars; *c != '\0'; c++) {
      if (*src == *c) {
        skip = true;
        break;
      }
    }
    if (!skip)
      *(dest++) = *src;
  }
  *dest = '\0';
  return dest - str;
}

int strrmm(string* str, const string& chars) {
  size_t strLen = str->length();
  size_t inIndex = str->find_first_of(chars);
  if (inIndex == string::npos)
    return strLen;

  size_t outIndex = inIndex++;

  while (inIndex < strLen) {
    char c = (*str)[inIndex++];
    if (chars.find(c) == string::npos)
      (*str)[outIndex++] = c;
  }

  str->resize(outIndex);
  return outIndex;
}

// ----------------------------------------------------------------------
// StripDupCharacters
//    Replaces any repeated occurrence of the character 'repeat_char'
//    with single occurrence.  e.g.,
//       StripDupCharacters("a//b/c//d", '/', 0) => "a/b/c/d"
//    Return the number of characters removed
// ----------------------------------------------------------------------
int StripDupCharacters(string* s, char dupChar, int startPos) {
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

// ----------------------------------------------------------------------
// RemoveExtraWhitespace()
//   Remove leading, trailing, and duplicate internal whitespace.
// ----------------------------------------------------------------------
void RemoveExtraWhitespace(string* s) {
  assert(s != nullptr);
  // Empty strings clearly have no whitespace, and this code assumes that
  // string length is greater than 0
  if (s->empty())
    return;

  int inputPos = 0; // current reader position
  int outputPos = 0; // current writer position
  const int inputEnd = s->size();
  // Strip off leading space
  while (inputPos < inputEnd && asciiIsSpace((*s)[inputPos]))
    inputPos++;

  while (inputPos < inputEnd - 1) {
    char c = (*s)[inputPos];
    char next = (*s)[inputPos + 1];
    // Copy each non-whitespace character to the right position.
    // For a block of whitespace, print the last one.
    if (!asciiIsSpace(c) || !asciiIsSpace(next)) {
      if (outputPos != inputPos) { // only copy if needed
        (*s)[outputPos] = c;
      }
      outputPos++;
    }
    inputPos++;
  }
  // Pick up the last character if needed.
  char c = (*s)[inputEnd - 1];
  if (!asciiIsSpace(c))
    (*s)[outputPos++] = c;

  s->resize(outputPos);
}

//------------------------------------------------------------------------
// See comment in header file for a complete description.
//------------------------------------------------------------------------
void StripLeadingWhiteSpace(string* str) {
  char const* const leading =
      StripLeadingWhiteSpace(const_cast<char*>(str->c_str()));
  if (leading != nullptr) {
    string const tmp(leading);
    str->assign(tmp);
  } else {
    str->assign("");
  }
}

void StripTrailingWhitespace(string* const s) {
  string::size_type i;
  for (i = s->size(); i > 0 && asciiIsSpace((*s)[i - 1]); --i) {
  }

  s->resize(i);
}

// ----------------------------------------------------------------------
// TrimRunsInString
//    Removes leading and trailing runs, and collapses middle
//    runs of a set of characters into a single character (the
//    first one specified in 'remove').  Useful for collapsing
//    runs of repeated delimiters, whitespace, etc.  E.g.,
//    TrimRunsInString(&s, " :,()") removes leading and trailing
//    delimiter chars and collapses and converts internal runs
//    of delimiters to single ' ' characters, so, for example,
//    "  a:(b):c  " -> "a b c"
//    "first,last::(area)phone, ::zip" -> "first last area phone zip"
// ----------------------------------------------------------------------
void TrimRunsInString(string* s, StringPiece remove) {
  string::iterator dest = s->begin();
  string::iterator srcEnd = s->end();
  for (string::iterator src = s->begin(); src != srcEnd;) {
    if (remove.find(*src) == StringPiece::kNpos) {
      *(dest++) = *(src++);
    } else {
      // Skip to the end of this run of chars that are in 'remove'.
      for (++src; src != srcEnd; ++src) {
        if (remove.find(*src) == StringPiece::kNpos) {
          if (dest != s->begin()) {
            // This is an internal run; collapse it.
            *(dest++) = remove[0];
          }
          *(dest++) = *(src++);
          break;
        }
      }
    }
  }
  s->erase(dest, srcEnd);
}

// ----------------------------------------------------------------------
// RemoveNullsInString
//    Removes any internal \0 characters from the string.
// ----------------------------------------------------------------------
void RemoveNullsInString(string* s) {
  s->erase(remove(s->begin(), s->end(), '\0'), s->end());
}
