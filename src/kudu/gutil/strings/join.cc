// Copyright 2008 and onwards Google Inc.  All rights reserved.

#include "kudu/gutil/strings/join.h"

#include <cstring> // IWYU pragma: keep
#include <memory>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/escaping.h"

using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

// ----------------------------------------------------------------------
// JoinUsing()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    the return result is dynamically allocated using "new char[]".
//    It is the caller's responsibility to "delete []" the
//
//    If resultLengthP is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsing(
    const vector<const char*>& components,
    const char* delim,
    int* resultLengthP) {
  const int numComponents = components.size();
  const int delimLength = strlen(delim);
  int numChars = (numComponents > 1) ? delimLength * (numComponents - 1) : 0;
  for (int i = 0; i < numComponents; ++i) {
    numChars += strlen(components[i]);
  }

  auto resBuffer = new char[numChars + 1];
  return JoinUsingToBuffer(
      components, delim, numChars + 1, resBuffer, resultLengthP);
}

// ----------------------------------------------------------------------
// JoinUsingToBuffer()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    User supplies the result buffer with specified buffer size.
//    The result is also returned for convenience.
//
//    If resultLengthP is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsingToBuffer(
    const vector<const char*>& components,
    const char* delim,
    int resultBufferSize,
    char* resultBuffer,
    int* resultLengthP) {
  CHECK(resultBuffer != nullptr);
  const int numComponents = components.size();
  const int maxStrLen = resultBufferSize - 1;
  char* currDest = resultBuffer;
  int numChars = 0;
  for (int i = 0; (i < numComponents) && (numChars < maxStrLen); ++i) {
    const char* currSrc = components[i];
    while ((*currSrc != '\0') && (numChars < maxStrLen)) {
      *currDest = *currSrc;
      ++numChars;
      ++currDest;
      ++currSrc;
    }
    if (i != (numComponents - 1)) { // not the last component ==> add separator
      currSrc = delim;
      while ((*currSrc != '\0') && (numChars < maxStrLen)) {
        *currDest = *currSrc;
        ++numChars;
        ++currDest;
        ++currSrc;
      }
    }
  }

  if (resultBufferSize > 0) {
    *currDest = '\0'; // add null termination
  }
  if (resultLengthP != nullptr) { // set string length value
    *resultLengthP = numChars;
  }

  return resultBuffer;
}

// ----------------------------------------------------------------------
// JoinStrings()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    it uses strings instead of char *s.
//
// ----------------------------------------------------------------------

void JoinStringsInArray(
    string const* const* components,
    int numComponents,
    const char* delim,
    string* result) {
  CHECK(result != nullptr);
  result->clear();
  for (int i = 0; i < numComponents; i++) {
    if (i > 0) {
      (*result) += delim;
    }
    (*result) += *(components[i]);
  }
}

void JoinStringsInArray(
    string const* components,
    int numComponents,
    const char* delim,
    string* result) {
  JoinStringsIterator(components, components + numComponents, delim, result);
}

// ----------------------------------------------------------------------
// JoinMapKeysAndValues()
// JoinVectorKeysAndValues()
//    This merges the keys and values of a string -> string map or pair
//    of strings vector, with one delim (intra_delim) between each key
//    and its associated value and another delim (inter_delim) between
//    each key/value pair.  The result is returned in a string (passed
//    as the last argument).
// ----------------------------------------------------------------------

void JoinMapKeysAndValues(
    const map<string, string>& components,
    const StringPiece& intraDelim,
    const StringPiece& interDelim,
    string* result) {
  JoinKeysAndValuesIterator(
      components.begin(), components.end(), intraDelim, interDelim, result);
}

void JoinVectorKeysAndValues(
    const vector<pair<string, string>>& components,
    const StringPiece& intraDelim,
    const StringPiece& interDelim,
    string* result) {
  JoinKeysAndValuesIterator(
      components.begin(), components.end(), intraDelim, interDelim, result);
}

// ----------------------------------------------------------------------
// JoinCSVLine()
//    This function is the inverse of SplitCSVLineWithDelimiter() in that the
//    string returned by JoinCSVLineWithDelimiter() can be passed to
//    SplitCSVLineWithDelimiter() to get the original string vector back.
//    Quotes and escapes the elements of original_cols according to CSV quoting
//    rules, and the joins the escaped quoted strings with commas using
//    JoinStrings().  Note that JoinCSVLineWithDelimiter() will not necessarily
//    return the same string originally passed in to
//    SplitCSVLineWithDelimiter(), since SplitCSVLineWithDelimiter() can handle
//    gratuitous spacing and quoting. 'output' must point to an empty string.
//
//    Example:
//     [Google], [x], [Buchheit, Paul], [string with " quoite in it], [ space ]
//     --->  [Google,x,"Buchheit, Paul","string with "" quote in it"," space "]
// ----------------------------------------------------------------------
void JoinCSVLineWithDelimiter(
    const vector<string>& cols,
    char delimiter,
    string* output) {
  CHECK(output);
  CHECK(output->empty());
  vector<string> quotedCols;

  const string delimiterStr(1, delimiter);
  const string escapeChars = delimiterStr + "\"";

  // If the string contains the delimiter or " anywhere, or begins or ends with
  // whitespace (ie asciiIsSpace() returns true), escape all double-quotes and
  // bracket the string in double quotes. string.rbegin() evaluates to the last
  // character of the string.
  for (const auto& col : cols) {
    if ((col.find_first_of(escapeChars) != string::npos) ||
        (!col.empty() &&
         (asciiIsSpace(*col.begin()) || asciiIsSpace(*col.rbegin())))) {
      // Double the original size, for escaping, plus two bytes for
      // the bracketing double-quotes, and one byte for the closing \0.
      int size = 2 * col.size() + 3;
      const std::unique_ptr<char[]> buf(new char[size]);

      // Leave space at beginning and end for bracketing double-quotes.
      int escapedSize =
          strings::EscapeStrForCSV(col.c_str(), buf.get() + 1, size - 2);
      CHECK_GE(escapedSize, 0) << "Buffer somehow wasn't large enough.";
      CHECK_GE(size, escapedSize + 3)
          << "Buffer should have one space at the beginning for a "
          << "double-quote, one at the end for a double-quote, and "
          << "one at the end for a closing '\\0'";
      *buf.get() = '"';
      *((buf.get() + 1) + escapedSize) = '"';
      *((buf.get() + 1) + escapedSize + 1) = '\0';
      quotedCols.emplace_back(buf.get(), buf.get() + escapedSize + 2);
    } else {
      quotedCols.push_back(col);
    }
  }
  JoinStrings(quotedCols, delimiterStr, output);
}

void JoinCSVLine(const vector<string>& cols, string* output) {
  JoinCSVLineWithDelimiter(cols, ',', output);
}

string JoinCSVLine(const vector<string>& cols) {
  string output;
  JoinCSVLine(cols, &output);
  return output;
}
