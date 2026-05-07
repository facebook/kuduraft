// Copyright 2010 Google Inc. All Rights Reserved.
// Maintainer: mec@google.com (Michael Chastain)
//
// Convert strings to numbers or numbers to strings.

#pragma once

#include <cinttypes>
#include <cstddef>
#include <ctime>
#include <functional>
#include <limits>
#include <string>

#include <cstdint>

#include <fmt/core.h>
#include "kudu/gutil/int128.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"

// START DOXYGEN NumbersFunctions grouping
/* @defgroup NumbersFunctions
 * @{ */

// Convert a fingerprint to 16 hex digits.
std::string fpToString(uint64_t fp);

// Formats a Uint128 as a 32-digit hex string.
std::string uint128ToHexString(kudu::Uint128 ui128);

// Convert strings to numeric values, with strict error checking.
// Leading and trailing spaces are allowed.
// Negative inputs are not allowed for unsigned ints (unlike strtoul).
// Numbers must be in base 10; see the _base variants below for other bases.
// Returns false on errors (including overflow/underflow).
bool safe_strto32(const char* str, int32_t* value);
bool safe_strto64(const char* str, int64_t* value);
bool safe_strtou32(const char* str, uint32_t* value);
bool safe_strtou64(const char* str, uint64_t* value);
// Convert strings to floating point values.
// Leading and trailing spaces are allowed.
// Values may be rounded on over- and underflow.
bool safe_strtof(const char* str, float* value);
bool safe_strtod(const char* str, double* value);

bool safe_strto32(const std::string& str, int32_t* value);
bool safe_strto64(const std::string& str, int64_t* value);
bool safe_strtou32(const std::string& str, uint32_t* value);
bool safe_strtou64(const std::string& str, uint64_t* value);
bool safe_strtof(const std::string& str, float* value);
bool safe_strtod(const std::string& str, double* value);

// Parses buffer_size many characters from startptr into value.
bool safe_strto32(const char* startptr, int buffer_size, int32_t* value);
bool safe_strto64(const char* startptr, int buffer_size, int64_t* value);

// Parses with a fixed base between 2 and 36. For base 16, leading "0x" is ok.
// If base is set to 0, its value is inferred from the beginning of str:
// "0x" means base 16, "0" means base 8, otherwise base 10 is used.
bool safe_strto32_base(const char* str, int32_t* value, int base);
bool safe_strto64_base(const char* str, int64_t* value, int base);
bool safe_strtou32_base(const char* str, uint32_t* value, int base);
bool safe_strtou64_base(const char* str, uint64_t* value, int base);

bool safe_strto32_base(const std::string& str, int32_t* value, int base);
bool safe_strto64_base(const std::string& str, int64_t* value, int base);
bool safe_strtou32_base(const std::string& str, uint32_t* value, int base);
bool safe_strtou64_base(const std::string& str, uint64_t* value, int base);

bool safe_strto32_base(
    const char* startptr,
    int buffer_size,
    int32_t* value,
    int base);
bool safe_strto64_base(
    const char* startptr,
    int buffer_size,
    int64_t* value,
    int base);

// u64tostr_base36()
//    The inverse of safe_strtou64_base, converts the number agument to
//    a string representation in base-36.
//    Conversion fails if buffer is too small to to hold the string and
//    terminating NUL.
//    Returns number of bytes written, not including terminating NUL.
//    Return value 0 indicates error.
size_t u64tostr_base36(uint64_t number, size_t buf_size, char* buffer);

// Similar to atoi(s), except s could be like "16k", "32M", "2G", "4t".
uint64_t atoi_kmgt(const char* s);
inline uint64_t atoi_kmgt(const std::string& s) {
  return atoi_kmgt(s.c_str());
}

// ----------------------------------------------------------------------
// FastIntToBuffer()
// fastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// fastTimeToBuffer()
//    These are intended for speed.  FastIntToBuffer() assumes the
//    integer is non-negative.  fastHexToBuffer() puts output in
//    hex rather than decimal.  fastTimeToBuffer() puts the output
//    into RFC822 format.
//
//    FastHex64ToBuffer() puts a 64-bit unsigned value in hex-format,
//    padded to exactly 16 bytes (plus one byte for '\0')
//
//    FastHex32ToBuffer() puts a 32-bit unsigned value in hex-format,
//    padded to exactly 8 bytes (plus one byte for '\0')
//
//    All functions take the output buffer as an arg.  FastInt() uses
//    at most 22 bytes, FastTime() uses exactly 30 bytes.  They all
//    return a pointer to the beginning of the output, which for
//    FastHex() may not be the beginning of the input buffer.  (For
//    all others, we guarantee that it is.)
//
//    NOTE: In 64-bit land, sizeof(time_t) is 8, so it is possible
//    to pass to fastTimeToBuffer() a time whose year cannot be
//    represented in 4 digits. In this case, the output buffer
//    will contain the string "Invalid:<value>"
// ----------------------------------------------------------------------

// Previously documented minimums -- the buffers provided must be at least this
// long, though these numbers are subject to change:
//     Int32, UInt32:        12 bytes
//     Int64, UInt64, Hex:   22 bytes
//     Time:                 30 bytes
//     Hex32:                 9 bytes
//     Hex64:                17 bytes
// Use kFastToBufferSize rather than hardcoding constants.
static const int kFastToBufferSize = 32;

char* fastInt32ToBuffer(int32_t i, char* buffer);
char* fastInt64ToBuffer(int64_t i, char* buffer);
char* fastUInt32ToBuffer(uint32_t i, char* buffer);
char* fastUInt64ToBuffer(uint64_t i, char* buffer);
char* fastHexToBuffer(int i, char* buffer) MUST_USE_RESULT;
char* fastTimeToBuffer(time_t t, char* buffer);
char* fastHex64ToBuffer(uint64_t i, char* buffer);
char* fastHex32ToBuffer(uint32_t i, char* buffer);

// at least 22 bytes long
inline char* fastIntToBuffer(int i, char* buffer) {
  return (
      sizeof(i) == 4 ? fastInt32ToBuffer(i, buffer)
                     : fastInt64ToBuffer(i, buffer));
}
inline char* fastUIntToBuffer(unsigned int i, char* buffer) {
  return (
      sizeof(i) == 4 ? fastUInt32ToBuffer(i, buffer)
                     : fastUInt64ToBuffer(i, buffer));
}

// ----------------------------------------------------------------------
// FastInt32ToBufferLeft()
// FastUInt32ToBufferLeft()
// FastInt64ToBufferLeft()
// FastUInt64ToBufferLeft()
// FastInt128ToBufferLeft()
// FastUInt128ToBufferLeft()
//
// Like the Fast*ToBuffer() functions above, these are intended for speed.
// Unlike the Fast*ToBuffer() functions, however, these functions write
// their output to the beginning of the buffer (hence the name, as the
// output is left-aligned).  The caller is responsible for ensuring that
// the buffer has enough space to hold the output.
//
// Returns a pointer to the end of the string (i.e. the null character
// terminating the string).
// ----------------------------------------------------------------------

char* fastInt32ToBufferLeft(int32_t i, char* buffer); // at least 12 bytes
char* fastUInt32ToBufferLeft(uint32_t i, char* buffer); // at least 12 bytes
char* fastInt64ToBufferLeft(int64_t i, char* buffer); // at least 22 bytes
char* fastUInt64ToBufferLeft(uint64_t i, char* buffer); // at least 22 bytes
char* fastInt128ToBufferLeft(__int128 i, char* buffer);
char* fastUInt128ToBufferLeft(unsigned __int128 i, char* buffer);

// Just define these in terms of the above.
inline char* fastUInt32ToBuffer(uint32_t i, char* buffer) {
  fastUInt32ToBufferLeft(i, buffer);
  return buffer;
}
inline char* fastUInt64ToBuffer(uint64_t i, char* buffer) {
  fastUInt64ToBufferLeft(i, buffer);
  return buffer;
}

// ----------------------------------------------------------------------
// hexDigitsPrefix()
//  returns 1 if buf is prefixed by "num_digits" of hex digits
//  returns 0 otherwise.
//  The function checks for '\0' for string termination.
// ----------------------------------------------------------------------
int hexDigitsPrefix(const char* buf, int num_digits);

// ----------------------------------------------------------------------
// consumeStrayLeadingZeroes
//    Eliminates all leading zeroes (unless the string itself is composed
//    of nothing but zeroes, in which case one is kept: 0...0 becomes 0).
void consumeStrayLeadingZeroes(std::string* str);

// ----------------------------------------------------------------------
// parseLeadingInt32Value
//    A simple parser for int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use parseLeadingDec32Value.
// --------------------------------------------------------------------
int32_t parseLeadingInt32Value(const char* str, int32_t deflt);
inline int32_t parseLeadingInt32Value(const std::string& str, int32_t deflt) {
  return parseLeadingInt32Value(str.c_str(), deflt);
}

// parseLeadingUInt32Value
//    A simple parser for uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use parseLeadingUDec32Value.
// --------------------------------------------------------------------
uint32_t parseLeadingUInt32Value(const char* str, uint32_t deflt);
inline uint32_t parseLeadingUInt32Value(
    const std::string& str,
    uint32_t deflt) {
  return parseLeadingUInt32Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// parseLeadingDec32Value
//    A simple parser for decimal int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: parseLeadingDec64Value
// --------------------------------------------------------------------
int32_t parseLeadingDec32Value(const char* str, int32_t deflt);
inline int32_t parseLeadingDec32Value(const std::string& str, int32_t deflt) {
  return parseLeadingDec32Value(str.c_str(), deflt);
}

// parseLeadingUDec32Value
//    A simple parser for decimal uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: parseLeadingUDec64Value
// --------------------------------------------------------------------
uint32_t parseLeadingUDec32Value(const char* str, uint32_t deflt);
inline uint32_t parseLeadingUDec32Value(
    const std::string& str,
    uint32_t deflt) {
  return parseLeadingUDec32Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// parseLeadingUInt64Value
// parseLeadingInt64Value
// parseLeadingHex64Value
// parseLeadingDec64Value
// parseLeadingUDec64Value
//    A simple parser for long long values.
//    Returns the parsed value if a
//    valid integer is found; else returns deflt
// --------------------------------------------------------------------
uint64_t parseLeadingUInt64Value(const char* str, uint64_t deflt);
inline uint64_t parseLeadingUInt64Value(
    const std::string& str,
    uint64_t deflt) {
  return parseLeadingUInt64Value(str.c_str(), deflt);
}
int64_t parseLeadingInt64Value(const char* str, int64_t deflt);
inline int64_t parseLeadingInt64Value(const std::string& str, int64_t deflt) {
  return parseLeadingInt64Value(str.c_str(), deflt);
}
uint64_t parseLeadingHex64Value(const char* str, uint64_t deflt);
inline uint64_t parseLeadingHex64Value(const std::string& str, uint64_t deflt) {
  return parseLeadingHex64Value(str.c_str(), deflt);
}
int64_t parseLeadingDec64Value(const char* str, int64_t deflt);
inline int64_t parseLeadingDec64Value(const std::string& str, int64_t deflt) {
  return parseLeadingDec64Value(str.c_str(), deflt);
}
uint64_t parseLeadingUDec64Value(const char* str, uint64_t deflt);
inline uint64_t parseLeadingUDec64Value(
    const std::string& str,
    uint64_t deflt) {
  return parseLeadingUDec64Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// parseLeadingDoubleValue
//    A simple parser for double values. Returns the parsed value
//    if a valid double is found; else returns deflt. It does not
//    check if str is entirely consumed.
// --------------------------------------------------------------------
double parseLeadingDoubleValue(const char* str, double deflt);
inline double parseLeadingDoubleValue(const std::string& str, double deflt) {
  return parseLeadingDoubleValue(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// parseLeadingBoolValue()
//    A recognizer of boolean string values. Returns the parsed value
//    if a valid value is found; else returns deflt.  This skips leading
//    whitespace, is case insensitive, and recognizes these forms:
//    0/1, false/true, no/yes, n/y
// --------------------------------------------------------------------
bool parseLeadingBoolValue(const char* str, bool deflt);
inline bool parseLeadingBoolValue(const std::string& str, bool deflt) {
  return parseLeadingBoolValue(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// autoDigitStrCmp
// autoDigitLessThan
// strictAutoDigitLessThan
// AutodigitLess
// AutodigitGreater
// StrictAutodigitLess
// StrictAutodigitGreater
//    These are like less<string> and greater<string>, except when a
//    run of digits is encountered at corresponding points in the two
//    arguments.  Such digit strings are compared numerically instead
//    of lexicographically.  Therefore if you sort by
//    "AutodigitLess", some machine names might get sorted as:
//        exaf1
//        exaf2
//        exaf10
//    When using "strict" comparison (autoDigitStrCmp with the strict flag
//    set to true, or the strict version of the other functions),
//    strings that represent equal numbers will not be considered equal if
//    the string representations are not identical.  That is, "01" < "1" in
//    strict mode, but "01" == "1" otherwise.
// ----------------------------------------------------------------------

int autoDigitStrCmp(
    const char* a,
    int alen,
    const char* b,
    int blen,
    bool strict);

bool autoDigitLessThan(const char* a, int alen, const char* b, int blen);

bool strictAutoDigitLessThan(const char* a, int alen, const char* b, int blen);

struct AutodigitLess
    : public std::
          binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return autoDigitLessThan(a.data(), a.size(), b.data(), b.size());
  }
};

struct AutodigitGreater
    : public std::
          binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return autoDigitLessThan(b.data(), b.size(), a.data(), a.size());
  }
};

struct StrictAutodigitLess
    : public std::
          binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return strictAutoDigitLessThan(a.data(), a.size(), b.data(), b.size());
  }
};

struct StrictAutodigitGreater
    : public std::
          binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return strictAutoDigitLessThan(b.data(), b.size(), a.data(), a.size());
  }
};

// ----------------------------------------------------------------------
// SimpleItoa()
//    Description: converts an integer to a string.
//    Faster than printf("%d").
//
//    Return value: string
// ----------------------------------------------------------------------
inline std::string simpleItoa(int32_t i) {
  char buf[16]; // Longest is -2147483648
  return std::string(buf, fastInt32ToBufferLeft(i, buf));
}

// We need this overload because otherwise simpleItoa(5U) wouldn't compile.
inline std::string simpleItoa(uint32_t i) {
  char buf[16]; // Longest is 4294967295
  return std::string(buf, fastUInt32ToBufferLeft(i, buf));
}

inline std::string simpleItoa(int64_t i) {
  char buf[32]; // Longest is -9223372036854775808
  return std::string(buf, fastInt64ToBufferLeft(i, buf));
}

// We need this overload because otherwise simpleItoa(5ULL) wouldn't compile.
inline std::string simpleItoa(uint64_t i) {
  char buf[32]; // Longest is 18446744073709551615
  return std::string(buf, fastUInt64ToBufferLeft(i, buf));
}

inline std::string simpleItoa(__int128 i) {
  char buf[64]; // Longest is -170141183460469231731687303715884105728
  return std::string(buf, fastInt128ToBufferLeft(i, buf));
}

inline std::string simpleItoa(unsigned __int128 i) {
  char buf[64]; // Longest is 340282366920938463463374607431768211455
  return std::string(buf, fastUInt128ToBufferLeft(i, buf));
}

// simpleAtoi converts a string to an integer.
// Uses safe_strto?() for actual parsing, so strict checking is
// applied, which is to say, the string must be a base-10 integer, optionally
// followed or preceded by whitespace, and value has to be in the range of
// the corresponding integer type.
//
// Returns true if parsing was successful.
template <typename int_type>
bool MUST_USE_RESULT simpleAtoi(const char* s, int_type* out) {
  // Must be of integer type (not pointer type), with more than 16-bitwidth.
  KUDU_COMPILE_ASSERT(
      sizeof(*out) == 4 || sizeof(*out) == 8, SimpleAtoiWorksWith32Or64BitInts);
  if (std::numeric_limits<int_type>::is_signed) { // Signed
    if (sizeof(*out) == 64 / 8) { // 64-bit
      return safe_strto64(s, reinterpret_cast<int64_t*>(out));
    } else { // 32-bit
      return safe_strto32(s, reinterpret_cast<int32_t*>(out));
    }
  } else { // Unsigned
    if (sizeof(*out) == 64 / 8) { // 64-bit
      return safe_strtou64(s, reinterpret_cast<uint64_t*>(out));
    } else { // 32-bit
      return safe_strtou32(s, reinterpret_cast<uint32_t*>(out));
    }
  }
}

template <typename int_type>
bool MUST_USE_RESULT simpleAtoi(const std::string& s, int_type* out) {
  return simpleAtoi(s.c_str(), out);
}

// ----------------------------------------------------------------------
// simpleDtoa()
// simpleFtoa()
// doubleToBuffer()
// floatToBuffer()
//    Description: converts a double or float to a string which, if
//    passed to strtod(), will produce the exact same original double
//    (except in case of NaN; all NaNs are considered the same value).
//    We try to keep the string short but it's not guaranteed to be as
//    short as possible.
//
//    doubleToBuffer() and floatToBuffer() write the text to the given
//    buffer and return it.  The buffer must be at least
//    kDoubleToBufferSize bytes for doubles and kFloatToBufferSize
//    bytes for floats.  kFastToBufferSize is also guaranteed to be large
//    enough to hold either.
//
//    Return value: string
// ----------------------------------------------------------------------
std::string simpleDtoa(double value);
std::string simpleFtoa(float value);

char* doubleToBuffer(double i, char* buffer);
char* floatToBuffer(float i, char* buffer);

// In practice, doubles should never need more than 24 bytes and floats
// should never need more than 14 (including null terminators), but we
// overestimate to be safe.
static const int kDoubleToBufferSize = 32;
static const int kFloatToBufferSize = 24;

// ----------------------------------------------------------------------
// simpleItoaWithCommas()
//    Description: converts an integer to a string.
//    Puts commas every 3 spaces.
//    Faster than printf("%d")?
//
//    Return value: string
// ----------------------------------------------------------------------
std::string simpleItoaWithCommas(int32_t i);
std::string simpleItoaWithCommas(uint32_t i);
std::string simpleItoaWithCommas(int64_t i);
std::string simpleItoaWithCommas(uint64_t i);

// ----------------------------------------------------------------------
// itoaKmgt()
//    Description: converts an integer to a string
//    Truncates values to K, G, M or T as appropriate
//    Opposite of atoi_kmgt()
//    e.g. 3000 -> 2K   57185920 -> 45M
//
//    Return value: string
// ----------------------------------------------------------------------
std::string itoaKmgt(int64_t i);

// ----------------------------------------------------------------------
// parseDoubleRange()
//    Parse an expression in 'text' of the form: <double><sep><double>
//    where <double> may be a double-precision number and <sep> is a
//    single char or "..", and must be one of the chars in parameter
//    'separators', which may contain '-' or '.' (which means "..") or
//    any chars not allowed in a double. If allow_unbounded_markers,
//    <double> may also be a '?' to indicate unboundedness (if on the
//    left of <sep>, means unbounded below; if on the right, means
//    unbounded above). Depending on num_required_bounds, which may be
//    0, 1, or 2, <double> may also be the empty string, indicating
//    unboundedness. If require_separator is false, then a single
//    <double> is acceptable and is parsed as a range bounded from
//    below. We also check that the character following the range must
//    be in acceptable_terminators. If null_terminator_ok, then it is
//    also OK if the range ends in \0 or after len chars. If
//    allow_currency is true, the first <double> may be optionally
//    preceded by a '$', in which case *is_currency will be true, and
//    the second <double> may similarly be preceded by a '$'. In these
//    cases, the '$' will be ignored (otherwise it's an error). If
//    allow_comparators is true, the expression in 'text' may also be
//    of the form <comparator><double>, where <comparator> is '<' or
//    '>' or '<=' or '>='. separators and require_separator are
//    ignored in this format, but all other parameters function as for
//    the first format. Return true if the expression parsed
//    successfully; false otherwise. If successful, output params are:
//    'end', which points to the char just beyond the expression;
//    'from' and 'to' are set to the values of the <double>s, and are
//    -inf and inf (or unchanged, depending on dont_modify_unbounded)
//    if unbounded. Output params are undefined if false is
//    returned. len is the input length, or -1 if text is
//    '\0'-terminated, which is more efficient.
// ----------------------------------------------------------------------
struct DoubleRangeOptions {
  const char* separators;
  bool requireSeparator;
  const char* acceptableTerminators;
  bool nullTerminatorOk;
  bool allowUnboundedMarkers;
  uint32_t numRequiredBounds;
  bool dontModifyUnbounded;
  bool allowCurrency;
  bool allowComparators;
};

// NOTE: The instruction below creates a Module titled
// NumbersFunctions within the auto-generated Doxygen documentation.
// This instruction is needed to expose global functions that are not
// within a namespace.
//
bool parseDoubleRange(
    const char* text,
    int len,
    const char** end,
    double* from,
    double* to,
    bool* is_currency,
    const DoubleRangeOptions& opts);

// END DOXYGEN SplitFunctions grouping
/* @} */

// These functions are deprecated.
// Do not use in new code.

// DEPRECATED(wadetregaskis).  Just call fmt::format or simpleFtoa.
std::string floatToString(float f, const char* format);

// DEPRECATED(wadetregaskis).  Just call fmt::format or simpleItoa.
std::string intToString(int i, const char* format);

// DEPRECATED(wadetregaskis).  Just call fmt::format or simpleItoa.
std::string int64ToString(int64_t i64, const char* format);

// DEPRECATED(wadetregaskis).  Just call fmt::format or simpleItoa.
std::string uint64ToString(uint64_t ui64, const char* format);

// DEPRECATED(wadetregaskis).  Just call fmt::format.
inline std::string floatToString(float f) {
  return fmt::format("{:.7f}", f);
}

// DEPRECATED(wadetregaskis).  Just call StringPrintf.
inline std::string intToString(int i) {
  return fmt::format("{:7d}", i);
}

// DEPRECATED(wadetregaskis).  Just call StringPrintf.
inline std::string int64ToString(int64_t i64) {
  return fmt::format("{:7d}", i64);
}

// DEPRECATED(wadetregaskis).  Just call StringPrintf.
inline std::string uint64ToString(uint64_t ui64) {
  return fmt::format("{:7}", ui64);
}
