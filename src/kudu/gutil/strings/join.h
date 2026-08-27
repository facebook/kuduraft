// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for joining strings and numbers using a delimiter.
//
#pragma once

#include <iterator>
#include <string>

#include "kudu/gutil/strings/strcat.h" // For backward compatibility.
#include "kudu/gutil/strings/stringpiece.h"

// ----------------------------------------------------------------------
// JoinStrings(), JoinStringsIterator(), JoinStringsInArray()
//
//    JoinStrings concatenates a container of strings into a C++ string,
//    using the string "delim" as a separator between components.
//    "components" can be any sequence container whose values are C++ strings
//    or StringPieces. More precisely, "components" must support STL container
//    iteration; i.e. it must have begin() and end() methods with appropriate
//    semantics, which return forward iterators whose value type is
//    string or StringPiece. Repeated string fields of protocol messages
//    satisfy these requirements.
//
//    JoinStringsIterator is the same as JoinStrings, except that the input
//    strings are specified with a pair of iterators. The requirements on
//    the iterators are the same as the requirements on components.begin()
//    and components.end() for JoinStrings.
//
//    JoinStringsInArray is the same as JoinStrings, but operates on
//    an array of C++ strings or string pointers.
//
//    There are two flavors of each function, one flavor returns the
//    concatenated string, another takes a pointer to the target string. In
//    the latter case the target string is cleared and overwritten.
// ----------------------------------------------------------------------
template <class CONTAINER>
void JoinStrings(
    const CONTAINER& components,
    const StringPiece& delim,
    std::string* result);
template <class CONTAINER>
std::string JoinStrings(const CONTAINER& components, const StringPiece& delim);

template <class ITERATOR>
void JoinStringsIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim,
    std::string* result);
template <class ITERATOR>
std::string JoinStringsIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim);

// Join the keys of a map using the specified delimiter.
template <typename ITERATOR>
void joinKeysIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim,
    std::string* result) {
  result->clear();
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter == start) {
      strAppend(result, iter->first);
    } else {
      strAppend(result, delim, iter->first);
    }
  }
}

template <typename ITERATOR>
std::string joinKeysIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim) {
  std::string result;
  joinKeysIterator(start, end, delim, &result);
  return result;
}

// ----------------------------------------------------------------------
// Definitions of above JoinStrings* methods
// ----------------------------------------------------------------------
template <class CONTAINER>
inline void JoinStrings(
    const CONTAINER& components,
    const StringPiece& delim,
    std::string* result) {
  JoinStringsIterator(components.begin(), components.end(), delim, result);
}

template <class CONTAINER>
inline std::string JoinStrings(
    const CONTAINER& components,
    const StringPiece& delim) {
  std::string result;
  JoinStrings(components, delim, &result);
  return result;
}

// Join the strings produced by calling 'functor' on each element of
// 'components'.
template <class CONTAINER, typename FUNC>
std::string joinMapped(
    const CONTAINER& components,
    const FUNC& functor,
    const StringPiece& delim) {
  std::string result;
  bool appendDelim = false;
  for (const auto& component : components) {
    if (appendDelim) {
      result.append(delim.data(), delim.size());
    } else {
      appendDelim = true;
    }
    result.append(functor(component));
  }
  return result;
}

template <class ITERATOR>
void JoinStringsIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim,
    std::string* result) {
  result->clear();

  // Precompute resulting length so we can reserve() memory in one shot.
  if (start != end) {
    int length = delim.size() * (distance(start, end) - 1);
    for (ITERATOR iter = start; iter != end; ++iter) {
      length += iter->size();
    }
    result->reserve(length);
  }

  // Now combine everything.
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter != start) {
      result->append(delim.data(), delim.size());
    }
    result->append(iter->data(), iter->size());
  }
}

template <class ITERATOR>
inline std::string JoinStringsIterator(
    const ITERATOR& start,
    const ITERATOR& end,
    const StringPiece& delim) {
  std::string result;
  JoinStringsIterator(start, end, delim, &result);
  return result;
}

// ----------------------------------------------------------------------
// JoinElements()
//    This merges a container of any type supported by strAppend() with delim
//    inserted as separators between components.  This is essentially a
//    templatized version of joinUsingToBuffer().
//
// JoinElementsIterator()
//    Same as JoinElements(), except that the input elements are specified
//    with a pair of forward iterators.
// ----------------------------------------------------------------------

template <class ITERATOR>
void JoinElementsIterator(
    ITERATOR first,
    ITERATOR last,
    StringPiece delim,
    std::string* result) {
  result->clear();
  for (ITERATOR it = first; it != last; ++it) {
    if (it != first) {
      strAppend(result, delim);
    }
    strAppend(result, *it);
  }
}

template <class CONTAINER>
inline void JoinElements(
    const CONTAINER& components,
    StringPiece delim,
    std::string* result) {
  JoinElementsIterator(components.begin(), components.end(), delim, result);
}

template <class CONTAINER>
inline std::string JoinElements(
    const CONTAINER& components,
    StringPiece delim) {
  std::string result;
  JoinElements(components, delim, &result);
  return result;
}
