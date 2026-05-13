// Copyright 2008 and onwards Google Inc.  All rights reserved.
//
// Maintainer: Greg Miller <jgm@google.com>

#include "kudu/gutil/strings/split.h"

#include <algorithm>
#include <cstdlib>
#include <iterator>

#include "kudu/gutil/strings/util.h"

using std::string;
using std::vector;

// Implementations for some of the Split2 API. Much of the Split2 API is
// templated so it exists in header files, either strings/split.h or
// strings/split_iternal.h.
namespace strings {
namespace delimiter {

namespace {

// This genericFind() template function encapsulates the finding algorithm
// shared between the Literal and AnyOf delimiters. The FindPolicy template
// parameter allows each delimiter to customize the actual find function to use
// and the length of the found delimiter. For example, the Literal delimiter
// will ultimately use StringPiece::find(), and the AnyOf delimiter will use
// StringPiece::findFirstOf().
template <typename FindPolicy>
StringPiece
genericFind(StringPiece text, StringPiece delimiter, FindPolicy findPolicy) {
  if (delimiter.empty() && text.length() > 0) {
    // Special case for empty string delimiters: always return a zero-length
    // StringPiece referring to the item at position 1.
    return StringPiece(text.begin() + 1, 0);
  }
  int foundPos = StringPiece::kNpos;
  StringPiece found(text.end(), 0); // By default, not found
  foundPos = findPolicy.find(text, delimiter);
  if (foundPos != StringPiece::kNpos) {
    found.set(text.data() + foundPos, findPolicy.length(delimiter));
  }
  return found;
}

// Finds using StringPiece::find(), therefore the length of the found delimiter
// is delimiter.length().
struct LiteralPolicy {
  int find(StringPiece text, StringPiece delimiter) {
    return text.find(delimiter);
  }
  int length(StringPiece delimiter) {
    return delimiter.length();
  }
};

// Finds using StringPiece::findFirstOf(), therefore the length of the found
// delimiter is 1.
struct AnyOfPolicy {
  size_t find(StringPiece text, StringPiece delimiter) {
    return text.findFirstOf(delimiter);
  }
  int length(StringPiece delimiter) {
    return 1;
  }
};

} // namespace

//
// Literal
//

Literal::Literal(StringPiece sp) : delimiter_(sp.toString()) {}

StringPiece Literal::find(StringPiece text) const {
  return genericFind(text, delimiter_, LiteralPolicy());
}

//
// AnyOf
//

AnyOf::AnyOf(StringPiece sp) : delimiters_(sp.toString()) {}

StringPiece AnyOf::find(StringPiece text) const {
  return genericFind(text, delimiters_, AnyOfPolicy());
}

} // namespace delimiter
} // namespace strings

//
// ==================== LEGACY SPLIT FUNCTIONS ====================
//

using ::strings::delimiter::AnyOf;

namespace {

// Overload of appendToImpl() that is optimized for appending to vector<string>.
// This version eliminates a couple string copies by using a vector<StringPiece>
// as the intermediate container.
template <typename Splitter>
void appendToImpl(vector<string>* container, Splitter splitter) {
  vector<StringPiece> vsp = splitter; // Calls implicit conversion operator.
  size_t containerSize = container->size();
  container->resize(containerSize + vsp.size());
  for (const auto& sp : vsp) {
    sp.copyToString(&(*container)[containerSize++]);
  }
}

// Appends the results of a call to strings::split() to the specified container.
// This function is used with the new strings::split() API to implement the
// append semantics of the legacy Split*() functions.
//
// The "Splitter" template parameter is intended to be a
// ::strings::internal::Splitter<>, which is the return value of a call to
// strings::split(). Sample usage:
//
//   vector<string> v;
//   ... add stuff to "v" ...
//   appendTo(&v, strings::split("a,b,c", ","));
//
template <typename Container, typename Splitter>
void appendTo(Container* container, Splitter splitter) {
  if (container->empty()) {
    // "Appending" to an empty container is by far the common case. For this we
    // assign directly to the output container, which is more efficient than
    // explicitly appending.
    *container = splitter; // Calls implicit conversion operator.
  } else {
    appendToImpl(container, splitter);
  }
}

} // anonymous namespace

// ----------------------------------------------------------------------
// splitStringAllowEmpty
//    Split a string using a character delimiter. Append the components
//    to 'result'.  If there are consecutive delimiters, this function
//    will return corresponding empty strings.
// ----------------------------------------------------------------------
void splitStringAllowEmpty(
    const string& full,
    const char* delim,
    vector<string>* result) {
  appendTo(result, strings::split(full, AnyOf(delim)));
}

// If we know how much to allocate for a vector of strings, we can
// allocate the vector<string> only once and directly to the right size.
// This saves in between 33-66 % of memory space needed for the result,
// and runs faster in the microbenchmarks.
//
// The reserve is only implemented for the single character delim.
//
// The implementation for counting is cut-and-pasted from
// splitStringToIteratorUsing. I could have written my own counting iterator,
// and use the existing template function, but probably this is more clear
// and more sure to get optimized to reasonable code.
static int calculateReserveForVector(const string& full, const char* delim) {
  int count = 0;
  if (delim[0] != '\0' && delim[1] == '\0') {
    // Optimize the common case where delim is a single character.
    char c = delim[0];
    const char* p = full.data();
    const char* end = p + full.size();
    while (p != end) {
      if (*p == c) { // This could be optimized with hasless(v,1) trick.
        ++p;
      } else {
        while (++p != end && *p != c) {
          // Skip to the next occurence of the delimiter.
        }
        ++count;
      }
    }
  }
  return count;
}

// ----------------------------------------------------------------------
// splitStringUsing()
//    Split a string using a character delimiter. Append the components
//    to 'result'.
//
// Note: For multi-character delimiters, this routine will split on *ANY* of
// the characters in the string, not the entire string as a single delimiter.
// ----------------------------------------------------------------------
template <typename StringType, typename ITR>
static inline void splitStringToIteratorUsing(
    const StringType& full,
    const char* delim,
    ITR& result) {
  // Optimize the common case where delim is a single character.
  if (delim[0] != '\0' && delim[1] == '\0') {
    char c = delim[0];
    const char* p = full.data();
    const char* end = p + full.size();
    while (p != end) {
      if (*p == c) {
        ++p;
      } else {
        const char* start = p;
        while (++p != end && *p != c) {
          // Skip to the next occurence of the delimiter.
        }
        *result++ = StringType(start, p - start);
      }
    }
    return;
  }

  string::size_type beginIndex, endIndex;
  beginIndex = full.find_first_not_of(delim);
  while (beginIndex != string::npos) {
    endIndex = full.find_first_of(delim, beginIndex);
    if (endIndex == string::npos) {
      *result++ = full.substr(beginIndex);
      return;
    }
    *result++ = full.substr(beginIndex, (endIndex - beginIndex));
    beginIndex = full.find_first_not_of(delim, endIndex);
  }
}

void splitStringUsing(
    const string& full,
    const char* delim,
    vector<string>* result) {
  result->reserve(result->size() + calculateReserveForVector(full, delim));
  std::back_insert_iterator<vector<string>> it(*result);
  splitStringToIteratorUsing(full, delim, it);
}
