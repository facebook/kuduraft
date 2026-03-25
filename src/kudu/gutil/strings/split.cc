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

// This GenericFind() template function encapsulates the finding algorithm
// shared between the Literal and AnyOf delimiters. The FindPolicy template
// parameter allows each delimiter to customize the actual find function to use
// and the length of the found delimiter. For example, the Literal delimiter
// will ultimately use StringPiece::find(), and the AnyOf delimiter will use
// StringPiece::find_first_of().
template <typename FindPolicy>
StringPiece
GenericFind(StringPiece text, StringPiece delimiter, FindPolicy find_policy) {
  if (delimiter.empty() && text.length() > 0) {
    // Special case for empty string delimiters: always return a zero-length
    // StringPiece referring to the item at position 1.
    return StringPiece(text.begin() + 1, 0);
  }
  int found_pos = StringPiece::kNpos;
  StringPiece found(text.end(), 0); // By default, not found
  found_pos = find_policy.Find(text, delimiter);
  if (found_pos != StringPiece::kNpos) {
    found.set(text.data() + found_pos, find_policy.Length(delimiter));
  }
  return found;
}

// Finds using StringPiece::find(), therefore the length of the found delimiter
// is delimiter.length().
struct LiteralPolicy {
  int Find(StringPiece text, StringPiece delimiter) {
    return text.find(delimiter);
  }
  int Length(StringPiece delimiter) {
    return delimiter.length();
  }
};

// Finds using StringPiece::find_first_of(), therefore the length of the found
// delimiter is 1.
struct AnyOfPolicy {
  size_t Find(StringPiece text, StringPiece delimiter) {
    return text.find_first_of(delimiter);
  }
  int Length(StringPiece delimiter) {
    return 1;
  }
};

} // namespace

//
// Literal
//

Literal::Literal(StringPiece sp) : delimiter_(sp.toString()) {}

StringPiece Literal::Find(StringPiece text) const {
  return GenericFind(text, delimiter_, LiteralPolicy());
}

//
// AnyOf
//

AnyOf::AnyOf(StringPiece sp) : delimiters_(sp.toString()) {}

StringPiece AnyOf::Find(StringPiece text) const {
  return GenericFind(text, delimiters_, AnyOfPolicy());
}

} // namespace delimiter
} // namespace strings

//
// ==================== LEGACY SPLIT FUNCTIONS ====================
//

using ::strings::delimiter::AnyOf;

namespace {

// Overload of AppendToImpl() that is optimized for appending to vector<string>.
// This version eliminates a couple string copies by using a vector<StringPiece>
// as the intermediate container.
template <typename Splitter>
void AppendToImpl(vector<string>* container, Splitter splitter) {
  vector<StringPiece> vsp = splitter; // Calls implicit conversion operator.
  size_t container_size = container->size();
  container->resize(container_size + vsp.size());
  for (const auto& sp : vsp) {
    sp.copyToString(&(*container)[container_size++]);
  }
}

// Appends the results of a call to strings::Split() to the specified container.
// This function is used with the new strings::Split() API to implement the
// append semantics of the legacy Split*() functions.
//
// The "Splitter" template parameter is intended to be a
// ::strings::internal::Splitter<>, which is the return value of a call to
// strings::Split(). Sample usage:
//
//   vector<string> v;
//   ... add stuff to "v" ...
//   AppendTo(&v, strings::Split("a,b,c", ","));
//
template <typename Container, typename Splitter>
void AppendTo(Container* container, Splitter splitter) {
  if (container->empty()) {
    // "Appending" to an empty container is by far the common case. For this we
    // assign directly to the output container, which is more efficient than
    // explicitly appending.
    *container = splitter; // Calls implicit conversion operator.
  } else {
    AppendToImpl(container, splitter);
  }
}

} // anonymous namespace

// ----------------------------------------------------------------------
// SplitStringAllowEmpty
//    Split a string using a character delimiter. Append the components
//    to 'result'.  If there are consecutive delimiters, this function
//    will return corresponding empty strings.
// ----------------------------------------------------------------------
void SplitStringAllowEmpty(
    const string& full,
    const char* delim,
    vector<string>* result) {
  AppendTo(result, strings::Split(full, AnyOf(delim)));
}

// If we know how much to allocate for a vector of strings, we can
// allocate the vector<string> only once and directly to the right size.
// This saves in between 33-66 % of memory space needed for the result,
// and runs faster in the microbenchmarks.
//
// The reserve is only implemented for the single character delim.
//
// The implementation for counting is cut-and-pasted from
// SplitStringToIteratorUsing. I could have written my own counting iterator,
// and use the existing template function, but probably this is more clear
// and more sure to get optimized to reasonable code.
static int CalculateReserveForVector(const string& full, const char* delim) {
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
// SplitStringUsing()
//    Split a string using a character delimiter. Append the components
//    to 'result'.
//
// Note: For multi-character delimiters, this routine will split on *ANY* of
// the characters in the string, not the entire string as a single delimiter.
// ----------------------------------------------------------------------
template <typename StringType, typename ITR>
static inline void SplitStringToIteratorUsing(
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

  string::size_type begin_index, end_index;
  begin_index = full.find_first_not_of(delim);
  while (begin_index != string::npos) {
    end_index = full.find_first_of(delim, begin_index);
    if (end_index == string::npos) {
      *result++ = full.substr(begin_index);
      return;
    }
    *result++ = full.substr(begin_index, (end_index - begin_index));
    begin_index = full.find_first_not_of(delim, end_index);
  }
}

void SplitStringUsing(
    const string& full,
    const char* delim,
    vector<string>* result) {
  result->reserve(result->size() + CalculateReserveForVector(full, delim));
  std::back_insert_iterator<vector<string>> it(*result);
  SplitStringToIteratorUsing(full, delim, it);
}
