// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for splitting strings into substrings.
//
// This file contains functions for splitting strings. The new and recommended
// API for string splitting is the strings::Split() function. The old API is a
// large collection of standalone functions declared at the bottom of this file
// in the global scope.
//
// TODO(user): Rough migration plan from old API to new API
// (1) Add comments to old Split*() functions showing how to do the same things
//     with the new API.
// (2) Reimplement some of the old Split*() functions in terms of the new
//     Split() API. This will allow deletion of code in split.cc.
// (3) (Optional) Replace old Split*() API calls at call sites with calls to new
//     Split() API.
//
#pragma once

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include <cstdint>

#include "kudu/gutil/strings/split_internal.h" // IWYU pragma: export
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"

namespace strings {

//                              The new Split API
//                                  aka Split2
//                              aka strings::Split()
//
// This string splitting API consists of a Split() function in the ::strings
// namespace and a handful of delimiter objects in the ::strings::delimiter
// namespace (more on delimiter objects below). The Split() function always
// takes two arguments: the text to be split and the delimiter on which to split
// the text. An optional third argument may also be given, which is a Predicate
// functor that will be used to filter the results, e.g., to skip empty strings
// (more on predicates below). The Split() function adapts the returned
// collection to the type specified by the caller.
//
// Example 1:
//   // Splits the given string on commas. Returns the results in a
//   // vector of strings.
//   vector<string> v = strings::Split("a,b,c", ",");
//   assert(v.size() == 3);
//
// Example 2:
//   // By default, empty strings are *included* in the output. See the
//   // strings::SkipEmpty predicate below to omit them.
//   vector<string> v = strings::Split("a,b,,c", ",");
//   assert(v.size() == 4);  // "a", "b", "", "c"
//   v = strings::Split("", ",");
//   assert(v.size() == 1);  // v contains a single ""
//
// Example 3:
//   // Splits the string as in the previous example, except that the results
//   // are returned as StringPiece objects. Note that because we are storing
//   // the results within StringPiece objects, we have to ensure that the input
//   // string outlives any results.
//   vector<StringPiece> v = strings::Split("a,b,c", ",");
//   assert(v.size() == 3);
//
// Example 4:
//   // Stores results in a set<string>.
//   set<string> a = strings::Split("a,b,c,a,b,c", ",");
//   assert(a.size() == 3);
//
// Example 5:
//   // Stores results in a map. The map implementation assumes that the input
//   // is provided as a series of key/value pairs. For example, the 0th element
//   // resulting from the split will be stored as a key to the 1st element. If
//   // an odd number of elements are resolved, the last element is paired with
//   // a default-constructed value (e.g., empty string).
//   map<string, string> m = strings::Split("a,b,c", ",");
//   assert(m.size() == 2);
//   assert(m["a"] == "b");
//   assert(m["c"] == "");  // last component value equals ""
//
// Example 6:
//   // Splits on the empty string, which results in each character of the input
//   // string becoming one element in the output collection.
//   vector<string> v = strings::Split("abc", "");
//   assert(v.size() == 3);
//
// Example 7:
//   // Stores first two split strings as the members in an std::pair.
//   std::pair<string, string> p = strings::Split("a,b,c", ",");
//   EXPECT_EQ("a", p.first);
//   EXPECT_EQ("b", p.second);
//   // "c" is omitted because std::pair can hold only two elements.
//
// As illustrated above, the Split() function adapts the returned collection to
// the type specified by the caller. The returned collections may contain
// string, StringPiece, Cord, or any object that has a constructor (explicit or
// not) that takes a single StringPiece argument. This pattern works for all
// standard STL containers including vector, list, deque, set, multiset, map,
// and multimap, non-standard containers including hash_set and hash_map, and
// even std::pair which is not actually a container.
//
// Splitting to std::pair is an interesting case because it can hold only two
// elements and is not a collection type. When splitting to an std::pair the
// first two split strings become the std::pair's .first and .second members
// respectively. The remaining split substrings are discarded. If there are less
// than two split substrings, the empty string is used for the corresponding
// std::pair member.
//
// The strings::Split() function can be used multiple times to perform more
// complicated splitting logic, such as intelligently parsing key-value pairs.
// For example
//
//   // The input string "a=b=c,d=e,f=,g" becomes
//   // { "a" => "b=c", "d" => "e", "f" => "", "g" => "" }
//   map<string, string> m;
//   for (StringPiece sp : strings::Split("a=b=c,d=e,f=,g", ",")) {
//     m.insert(strings::Split(sp, strings::delimiter::Limit("=", 1)));
//   }
//   EXPECT_EQ("b=c", m.find("a")->second);
//   EXPECT_EQ("e", m.find("d")->second);
//   EXPECT_EQ("", m.find("f")->second);
//   EXPECT_EQ("", m.find("g")->second);
//
// The above example stores the results in an std::map. But depending on your
// data requirements, you can just as easily store the results in an
// std::multimap or even a vector<std::pair<>>.
//
//
//                                  Delimiters
//
// The Split() function also takes a second argument that is a delimiter. This
// delimiter is actually an object that defines the boundaries between elements
// in the provided input. If a string (const char*, ::string, or StringPiece) is
// passed in place of an explicit Delimiter object, the argument is implicitly
// converted to a ::strings::delimiter::Literal.
//
// With this split API comes the formal concept of a Delimiter (big D). A
// Delimiter is an object with a Find() function that knows how find the first
// occurrence of itself in a given StringPiece. Models of the Delimiter concept
// represent specific kinds of delimiters, such as single characters,
// substrings, or even regular expressions.
//
// The following Delimiter objects are provided as part of the Split() API:
//
//   - Literal (default)
//   - AnyOf
//   - Limit
//
// The following are examples of using some provided Delimiter objects:
//
// Example 1:
//   // Because a string literal is converted to a strings::delimiter::Literal,
//   // the following two splits are equivalent.
//   vector<string> v1 = strings::Split("a,b,c", ",");           // (1)
//   using ::strings::delimiter::Literal;
//   vector<string> v2 = strings::Split("a,b,c", Literal(","));  // (2)
//
// Example 2:
//   // Splits on any of the characters specified in the delimiter string.
//   using ::strings::delimiter::AnyOf;
//   vector<string> v = strings::Split("a,b;c-d", AnyOf(",;-"));
//   assert(v.size() == 4);
//
// Example 3:
//   // Uses the Limit meta-delimiter to limit the number of matches a delimiter
//   // can have. In this case, the delimiter of a Literal comma is limited to
//   // to matching at most one time. The last element in the returned
//   // collection will contain all unsplit pieces, which may contain instances
//   // of the delimiter.
//   using ::strings::delimiter::Limit;
//   vector<string> v = strings::Split("a,b,c", Limit(",", 1));
//   assert(v.size() == 2);  // Limited to 1 delimiter; so two elements found
//   assert(v[0] == "a");
//   assert(v[1] == "b,c");
//
//
//                                  Predicates
//
// Predicates can filter the results of a Split() operation by determining
// whether or not a resultant element is included in the result set. A predicate
// may be passed as an *optional* third argument to the Split() function.
//
// Predicates are unary functions (or functors) that take a single StringPiece
// argument and return bool indicating whether the argument should be included
// (true) or excluded (false).
//
// One example where this is useful is when filtering out empty substrings. By
// default, empty substrings may be returned by strings::Split(), which is
// similar to the way split functions work in other programming languages. For
// example:
//
//   // Empty strings *are* included in the returned collection.
//   vector<string> v = strings::Split(",a,,b,", ",");
//   assert(v.size() ==  5);  // v[0] == "", v[1] == "a", v[2] == "", ...
//
// These empty strings can be filtered out of the results by simply passing the
// provided SkipEmpty predicate as the third argument to the Split() function.
// SkipEmpty does not consider a string containing all whitespace to be empty.
// For that behavior use the SkipWhitespace predicate. For example:
//
// Example 1:
//   // Uses SkipEmpty to omit empty strings. Strings containing whitespace are
//   // not empty and are therefore not skipped.
//   using strings::SkipEmpty;
//   vector<string> v = strings::Split(",a, ,b,", ",", SkipEmpty());
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == " ");  // <-- The whitespace makes the string not empty.
//   assert(v[2] == "b");
//
// Example 2:
//   // Uses SkipWhitespace to skip all strings that are either empty or contain
//   // only whitespace.
//   using strings::SkipWhitespace;
//   vector<string> v = strings::Split(",a, ,b,", ",",  SkipWhitespace());
//   assert(v.size() == 2);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//
//
//                     Differences between Split1 and Split2
//
// Split2 is the strings::Split() API described above. Split1 is a name for the
// collection of legacy Split*() functions declared later in this file. Most of
// the Split1 functions follow a set of conventions that don't necessarily match
// the conventions used in Split2. The following are some of the important
// differences between Split1 and Split2:
//
// Split1 -> Split2
// ----------------
// Append -> Assign:
//   The Split1 functions all returned their output collections via a pointer to
//   an out parameter as is typical in Google code. In some cases the comments
//   explicitly stated that results would be *appended* to the output
//   collection. In some cases it was ambiguous whether results were appended.
//   This ambiguity is gone in the Split2 API as results are always assigned to
//   the output collection, never appended.
//
// AnyOf -> Literal:
//   Most Split1 functions treated their delimiter argument as a string of
//   individual byte delimiters. For example, a delimiter of ",;" would split on
//   "," and ";", not the substring ",;". This behavior is equivalent to the
//   Split2 delimiter strings::delimiter::AnyOf, which is *not* the default. By
//   default, strings::Split() splits using strings::delimiter::Literal() which
//   would treat the whole string ",;" as a single delimiter string.
//
// SkipEmpty -> allow empty:
//   Most Split1 functions omitted empty substrings in the results. To keep
//   empty substrings one would have to use an explicitly named
//   Split*AllowEmpty() function. This behavior is reversed in Split2. By
//   default, strings::Split() *allows* empty substrings in the output. To skip
//   them, use the strings::SkipEmpty predicate.
//
// string -> user's choice:
//   Most Split1 functions return collections of string objects. Some return
//   char*, but the type returned is dictated by each Split1 function. With
//   Split2 the caller can choose which string-like object to return. (Note:
//   char* C-strings are not supported in Split2--use StringPiece instead).
//

// Definitions of the main Split() function.
template <typename Delimiter>
inline internal::Splitter<Delimiter> Split(StringPiece text, Delimiter d) {
  return internal::Splitter<Delimiter>(text, d);
}

template <typename Delimiter, typename Predicate>
inline internal::Splitter<Delimiter, Predicate>
Split(StringPiece text, Delimiter d, Predicate p) {
  return internal::Splitter<Delimiter, Predicate>(text, d, p);
}

namespace delimiter {
// A Delimiter object represents a single separator, such as a character,
// literal string, or regular expression. A Delimiter object must have the
// following member:
//
//   StringPiece Find(StringPiece text);
//
// This Find() member function should return a StringPiece referring to the next
// occurrence of the represented delimiter within the given string text. If no
// delimiter is found in the given text, a zero-length StringPiece referring to
// text.end() should be returned (e.g., StringPiece(text.end(), 0)). It is
// important that the returned StringPiece always be within the bounds of the
// StringPiece given as an argument--it must not refer to a string that is
// physically located outside of the given string. The following example is a
// simple Delimiter object that is created with a single char and will look for
// that char in the text given to the Find() function:
//
//   struct SimpleDelimiter {
//     const char c_;
//     explicit SimpleDelimiter(char c) : c_(c) {}
//     StringPiece Find(StringPiece text) {
//       int pos = text.find(c_);
//       if (pos == StringPiece::kNpos) return StringPiece(text.end(), 0);
//       return StringPiece(text, pos, 1);
//     }
//   };

// Represents a literal string delimiter. Examples:
//
//   using ::strings::delimiter::Literal;
//   vector<string> v = strings::Split("a=>b=>c", Literal("=>"));
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
// The next example uses the empty string as a delimiter.
//
//   using ::strings::delimiter::Literal;
//   vector<string> v = strings::Split("abc", Literal(""));
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
class Literal {
 public:
  explicit Literal(StringPiece sp);
  StringPiece Find(StringPiece text) const;

 private:
  const std::string delimiter_;
};

// Represents a delimiter that will match any of the given byte-sized
// characters. AnyOf is similar to Literal, except that AnyOf uses
// StringPiece::find_first_of() and Literal uses StringPiece::find(). AnyOf
// examples:
//
//   using ::strings::delimiter::AnyOf;
//   vector<string> v = strings::Split("a,b=c", AnyOf(",="));
//
//   assert(v.size() == 3);
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c");
//
// If AnyOf is given the empty string, it behaves exactly like Literal and
// matches each individual character in the input string.
//
// Note: The string passed to AnyOf is assumed to be a string of single-byte
// ASCII characters. AnyOf does not work with multi-byte characters.
class AnyOf {
 public:
  explicit AnyOf(StringPiece sp);
  StringPiece Find(StringPiece text) const;

 private:
  const std::string delimiters_;
};

// Wraps another delimiter and sets a max number of matches for that delimiter.
// Create LimitImpls using the Limit() function. Example:
//
//   using ::strings::delimiter::Limit;
//   vector<string> v = strings::Split("a,b,c,d", Limit(",", 2));
//
//   assert(v.size() == 3);  // Split on 2 commas, giving a vector with 3 items
//   assert(v[0] == "a");
//   assert(v[1] == "b");
//   assert(v[2] == "c,d");
//
template <typename Delimiter>
class LimitImpl {
 public:
  LimitImpl(Delimiter delimiter, int limit)
      : delimiter_(std::move(delimiter)), limit_(limit), count_(0) {}
  StringPiece Find(StringPiece text) {
    if (count_++ == limit_) {
      return StringPiece(text.end(), 0); // No more matches.
    }
    return delimiter_.Find(text);
  }

 private:
  Delimiter delimiter_;
  const int limit_;
  int count_;
};

// Overloaded Limit() function to create LimitImpl<> objects. Uses the Delimiter
// Literal as the default if string-like objects are passed as the delimiter
// parameter. This is similar to the overloads for Split() below.
template <typename Delimiter>
inline LimitImpl<Delimiter> Limit(Delimiter delim, int limit) {
  return LimitImpl<Delimiter>(delim, limit);
}

inline LimitImpl<Literal> Limit(const char* s, int limit) {
  return LimitImpl<Literal>(Literal(s), limit);
}

inline LimitImpl<Literal> Limit(const std::string& s, int limit) {
  return LimitImpl<Literal>(Literal(s), limit);
}

inline LimitImpl<Literal> Limit(StringPiece s, int limit) {
  return LimitImpl<Literal>(Literal(s), limit);
}

} // namespace delimiter

//
// Predicates are functors that return bool indicating whether the given
// StringPiece should be included in the split output. If the predicate returns
// false then the string will be excluded from the output from strings::Split().
//

// Returns false if the given StringPiece is empty, indicating that the
// strings::Split() API should omit the empty string.
//
// vector<string> v = Split(" a , ,,b,", ",", SkipEmpty());
// EXPECT_THAT(v, ElementsAre(" a ", " ", "b"));
struct SkipEmpty {
  bool operator()(StringPiece sp) const {
    return !sp.empty();
  }
};

// Returns false if the given StringPiece is empty or contains only whitespace,
// indicating that the strings::Split() API should omit the string.
//
// vector<string> v = Split(" a , ,,b,", ",", SkipWhitespace());
// EXPECT_THAT(v, ElementsAre(" a ", "b"));
struct SkipWhitespace {
  bool operator()(StringPiece sp) const {
    StripWhiteSpace(&sp);
    return !sp.empty();
  }
};

// Split() function overloads to effectively give Split() a default Delimiter
// type of Literal. If Split() is called and a string is passed as the delimiter
// instead of an actual Delimiter object, then one of these overloads will be
// invoked and will create a Splitter<Literal> with the delimiter string.
//
// Since Split() is a function template above, these overload signatures need to
// be explicit about the string type so they match better than the templated
// version. These functions are overloaded for:
//
//   - const char*
//   - const string&
//   - StringPiece

inline internal::Splitter<delimiter::Literal> Split(
    StringPiece text,
    const char* delimiter) {
  return internal::Splitter<delimiter::Literal>(
      text, delimiter::Literal(delimiter));
}

inline internal::Splitter<delimiter::Literal> Split(
    StringPiece text,
    const std::string& delimiter) {
  return internal::Splitter<delimiter::Literal>(
      text, delimiter::Literal(delimiter));
}

inline internal::Splitter<delimiter::Literal> Split(
    StringPiece text,
    StringPiece delimiter) {
  return internal::Splitter<delimiter::Literal>(
      text, delimiter::Literal(delimiter));
}

// Same overloads as above, but also including a Predicate argument.
template <typename Predicate>
inline internal::Splitter<delimiter::Literal, Predicate>
Split(StringPiece text, const char* delimiter, Predicate p) {
  return internal::Splitter<delimiter::Literal, Predicate>(
      text, delimiter::Literal(delimiter), p);
}

template <typename Predicate>
inline internal::Splitter<delimiter::Literal, Predicate>
Split(StringPiece text, const std::string& delimiter, Predicate p) {
  return internal::Splitter<delimiter::Literal, Predicate>(
      text, delimiter::Literal(delimiter), p);
}

template <typename Predicate>
inline internal::Splitter<delimiter::Literal, Predicate>
Split(StringPiece text, StringPiece delimiter, Predicate p) {
  return internal::Splitter<delimiter::Literal, Predicate>(
      text, delimiter::Literal(delimiter), p);
}

} // namespace strings

// ----------------------------------------------------------------------
// SplitStringUsing()
//    Splits a string using one or more byte delimiters, presented as a
//    nul-terminated c string. Append the components to 'result'. If there are
//    consecutive delimiters, this function skips over all of them.
// ----------------------------------------------------------------------
void SplitStringUsing(
    const std::string& full,
    const char* delimiters,
    std::vector<std::string>* result);

// ----------------------------------------------------------------------
// SplitStringAllowEmpty()
//
// Split a string using one or more byte delimiters, presented as a
// nul-terminated c string. Append the components to 'result'. If there are
// consecutive delimiters, this function will return corresponding empty
// strings.  If you want to drop the empty strings, try SplitStringUsing().
//
// If "full" is the empty string, yields an empty string as the only value.
//
// ==> NEW API: Consider using the new Split API defined above. <==
//
//   using strings::Split;
//   using strings::delimiter::AnyOf;
//
//   vector<string> v = Split(full, AnyOf(delimiter));
//
// For even better performance, store the result in a vector<StringPiece> to
// avoid string copies.
// ----------------------------------------------------------------------
void SplitStringAllowEmpty(
    const std::string& full,
    const char* delim,
    std::vector<std::string>* result);
