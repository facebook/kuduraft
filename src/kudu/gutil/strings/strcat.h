// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Merges strings or numbers with no delimiter.
//
#pragma once

#include <cstring>
#include <string>

#include <folly/Conv.h>
#include <glog/logging.h>
#include <cstdint>
#include "kudu/gutil/strings/stringpiece.h"

// The AlphaNum type was designed to be used as the parameter type for StrCat().
// I suppose that any routine accepting either a string or a number could accept
// it.  The basic idea is that by accepting a "const AlphaNum &" as an argument
// to your function, your callers will automagically convert bools, integers,
// and floating point values to strings for you.
//
// Conversion from 8-bit values is not accepted because if it were, then an
// attempt to pass ':' instead of ":" might result in a 58 ending up in your
// result.
//
// Bools convert to "0" or "1".
//
// Floating point values are converted to a string which, if passed to strtod(),
// would produce the exact same original double (except in case of NaN; all NaNs
// are considered the same value). We try to keep the string short but it's not
// guaranteed to be as short as possible.
//
// This class has implicit constructors.
// Style guide exception granted:
// http://goto/style-guide-exception-20978288
//
struct AlphaNum {
  StringPiece piece;
  // Buffer size for converting numbers to strings. 32 bytes is sufficient for:
  // - Int32, UInt32: up to 12 bytes
  // - Int64, UInt64: up to 22 bytes
  // - float, double: up to 30 bytes
  char digits[32];

  // No bool ctor -- bools convert to an integral type.
  // A bool ctor would also convert incoming pointers (bletch).

  AlphaNum(int32_t i32)
      : piece(convertToBuffer(i32)) {} // NOLINT(google-explicit-constructor)
  AlphaNum(uint32_t u32)
      : piece(convertToBuffer(u32)) {} // NOLINT(google-explicit-constructor)
  AlphaNum(int64_t i64)
      : piece(convertToBuffer(i64)) {} // NOLINT(google-explicit-constructor)
  AlphaNum(uint64_t u64)
      : piece(convertToBuffer(u64)) {} // NOLINT(google-explicit-constructor)

#if defined(__APPLE__)
  AlphaNum(size_t size)
      : piece(convertToBuffer(size)) {} // NOLINT(google-explicit-constructor)
#endif

  AlphaNum(float f)
      : piece(convertToBuffer(f)) {} // NOLINT(google-explicit-constructor)
  AlphaNum(double f)
      : piece(convertToBuffer(f)) {} // NOLINT(google-explicit-constructor)

  AlphaNum(const char* cStr) // NOLINT(google-explicit-constructor)
      : piece(cStr) {}
  AlphaNum(StringPiece pc) // NOLINT(google-explicit-constructor)
      : piece(pc) {}
  AlphaNum(const std::string& s) // NOLINT(google-explicit-constructor)
      : piece(s) {}

  StringPiece::size_type size() const {
    return piece.size();
  }
  const char* data() const {
    return piece.data();
  }

 private:
  // Use ":" not ':'
  AlphaNum(char c); // NOLINT(google-explicit-constructor)

  template <typename T>
  StringPiece convertToBuffer(T value) {
    // Use a small std::string which benefits from SSO (Small String
    // Optimization). For short strings (<= ~22 bytes on most platforms), no
    // heap allocation occurs. This is simpler and more maintainable than manual
    // buffer writing.
    std::string str = folly::to<std::string>(value);
    size_t len = str.size();
    CHECK_LT(len, sizeof(digits))
        << "Buffer overflow in AlphaNum::convertToBuffer: "
        << "converted string length " << len << " exceeds buffer size "
        << sizeof(digits);
    memcpy(digits, str.data(), len);
    digits[len] = '\0';
    return StringPiece(digits, len);
  }
};

extern AlphaNum kEmptyAlphaNum;

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or numbers, with no delimiter.  This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, StringPieces, strings, bool values,
//    and numeric values.
//
//    Don't use this for user-visible strings.  The localization process
//    works poorly on strings built up out of fragments.
//
//    For clarity and performance, don't use StrCat when appending to a
//    string.  In particular, avoid using any of these (anti-)patterns:
//      str.append(StrCat(...)
//      str += StrCat(...)
//      str = StrCat(str, ...)
//    where the last is the worse, with the potential to change a loop
//    from a linear time operation with O(1) dynamic allocations into a
//    quadratic time operation with O(n) dynamic allocations.  StrAppend
//    is a better choice than any of the above, subject to the restriction
//    of StrAppend(&str, a, b, c, ...) that none of the a, b, c, ... may
//    be a reference into str.
// ----------------------------------------------------------------------

std::string StrCat(const AlphaNum& a);
std::string StrCat(const AlphaNum& a, const AlphaNum& b);
std::string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c);
std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d);
std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e);
std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f);
std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g);
std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h);

namespace strings {
namespace internal {

// Do not call directly - this is not part of the public API.
std::string StrCatNineOrMore(const AlphaNum* a1, ...);

} // namespace internal
} // namespace strings

// Support 9 or more arguments
inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u,
    const AlphaNum& v) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      &v,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u,
    const AlphaNum& v,
    const AlphaNum& w) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      &v,
      &w,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u,
    const AlphaNum& v,
    const AlphaNum& w,
    const AlphaNum& x) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      &v,
      &w,
      &x,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u,
    const AlphaNum& v,
    const AlphaNum& w,
    const AlphaNum& x,
    const AlphaNum& y) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      &v,
      &w,
      &x,
      &y,
      nullAlphanum);
}

inline std::string StrCat(
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f,
    const AlphaNum& g,
    const AlphaNum& h,
    const AlphaNum& i,
    const AlphaNum& j,
    const AlphaNum& k,
    const AlphaNum& l,
    const AlphaNum& m,
    const AlphaNum& n,
    const AlphaNum& o,
    const AlphaNum& p,
    const AlphaNum& q,
    const AlphaNum& r,
    const AlphaNum& s,
    const AlphaNum& t,
    const AlphaNum& u,
    const AlphaNum& v,
    const AlphaNum& w,
    const AlphaNum& x,
    const AlphaNum& y,
    const AlphaNum& z) {
  const AlphaNum* nullAlphanum = nullptr;
  return strings::internal::StrCatNineOrMore(
      &a,
      &b,
      &c,
      &d,
      &e,
      &f,
      &g,
      &h,
      &i,
      &j,
      &k,
      &l,
      &m,
      &n,
      &o,
      &p,
      &q,
      &r,
      &s,
      &t,
      &u,
      &v,
      &w,
      &x,
      &y,
      &z,
      nullAlphanum);
}

// ----------------------------------------------------------------------
// StrAppend()
//    Same as above, but adds the output to the given string.
//    WARNING: For speed, StrAppend does not try to check each of its input
//    arguments to be sure that they are not a subset of the string being
//    appended to.  That is, while this will work:
//
//    string s = "foo";
//    s += s;
//
//    This will not (necessarily) work:
//
//    string s = "foo";
//    StrAppend(&s, s);
//
//    Note: while StrCat supports appending up to 12 arguments, StrAppend
//    is currently limited to 9.  That's rarely an issue except when
//    automatically transforming StrCat to StrAppend, and can easily be
//    worked around as consecutive calls to StrAppend are quite efficient.
// ----------------------------------------------------------------------

void StrAppend(std::string* dest, const AlphaNum& a);
void StrAppend(std::string* dest, const AlphaNum& a, const AlphaNum& b);
void StrAppend(
    std::string* dest,
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c);
void StrAppend(
    std::string* dest,
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d);

// Support up to 9 params by using a default empty AlphaNum.
void StrAppend(
    std::string* dest,
    const AlphaNum& a,
    const AlphaNum& b,
    const AlphaNum& c,
    const AlphaNum& d,
    const AlphaNum& e,
    const AlphaNum& f = kEmptyAlphaNum,
    const AlphaNum& g = kEmptyAlphaNum,
    const AlphaNum& h = kEmptyAlphaNum,
    const AlphaNum& i = kEmptyAlphaNum);
