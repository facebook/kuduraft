// Copyright 2006 Google Inc. All Rights Reserved.
// Authors: Numerous. Principal maintainers are csilvers and zunger.
//
// This is a grab-bag file for string utilities involved in escaping and
// unescaping strings in various ways. Who knew there were so many?
//
// NOTE: Although the functions declared here have been imported into
// the global namespace, the using statements are slated for removal.
// Do not refer to these symbols without properly namespace-qualifying
// them with "strings::". Of course you may also use "using" statements
// within a .cc file.
//
// There are more escaping functions in:
//   webutil/html/tagutils.h (Escaping strings for HTML, PRE, JavaScript, etc.)
//   webutil/url/url.h (Escaping for URL's, both RFC-2396 and other methods)
//   template/template_modifiers.h (All sorts of stuff)
//   util/regex/re2/re2.h (Escaping for literals within regular expressions
//                         - see RE2::QuoteMeta).
// And probably many more places, as well.
#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/charset.h"
#include "kudu/gutil/strings/stringpiece.h"

namespace strings {

// ----------------------------------------------------------------------
// escapeStrForCsv()
//    Escapes the quotes in 'src' by doubling them. This is necessary
//    for generating CSV files (see SplitCSVLine).
//    Returns the number of characters written into dest (not counting
//    the \0) or -1 if there was insufficient space.
//
//    Example: [some "string" to test] --> [some ""string"" to test]
// ----------------------------------------------------------------------
int escapeStrForCsv(const char* src, char* dest, int destLen);

// ----------------------------------------------------------------------
// unescapeCEscapeSequences()
//    Copies "source" to "dest", rewriting C-style escape sequences
//    -- '\n', '\r', '\\', '\ooo', etc -- to their ASCII
//    equivalents.  "dest" must be sufficiently large to hold all
//    the characters in the rewritten string (i.e. at least as large
//    as strlen(source) + 1 should be safe, since the replacements
//    are always shorter than the original escaped sequences).  It's
//    safe for source and dest to be the same.  RETURNS the length
//    of dest.
//
//    It allows hex sequences \xhh, or generally \xhhhhh with an
//    arbitrary number of hex digits, but all of them together must
//    specify a value of a single byte (e.g. \x0045 is equivalent
//    to \x45, and \x1234 is erroneous). If the value is too large,
//    it is truncated to 8 bits and an error is set. This is also
//    true of octal values that exceed 0xff.
//
//    It also allows escape sequences of the form \uhhhh (exactly four
//    hex digits, upper or lower case) or \Uhhhhhhhh (exactly eight
//    hex digits, upper or lower case) to specify a Unicode code
//    point. The dest array will contain the UTF8-encoded version of
//    that code-point (e.g., if source contains \u2019, then dest will
//    contain the three bytes 0xE2, 0x80, and 0x99). For the inverse
//    transformation, use UniLib::UTF8EscapeString
//    (util/utf8/public/unilib.h), not cEscapeString.
//
//    Errors: In the first form of the call, errors are reported with
//    LOG(ERROR). The same is true for the second form of the call if
//    the pointer to the string vector is NULL; otherwise, error
//    messages are stored in the vector. In either case, the effect on
//    the dest array is not defined, but rest of the source will be
//    processed.
//
//    *** DEPRECATED: Use cUnescape() in new code ***
//    ----------------------------------------------------------------------
int unescapeCEscapeSequences(const char* source, char* dest);
int unescapeCEscapeSequences(
    const char* source,
    char* dest,
    std::vector<std::string>* errors);

// ----------------------------------------------------------------------
// unescapeCEscapeString()
//    This does the same thing as unescapeCEscapeSequences, but creates
//    a new string. The caller does not need to worry about allocating
//    a dest buffer. This should be used for non performance critical
//    tasks such as printing debug messages. It is safe for src and dest
//    to be the same.
//
//    The second call stores its errors in a supplied string vector.
//    If the string vector pointer is NULL, it reports the errors with LOG().
//
//    In the first and second calls, the length of dest is returned. In the
//    the third call, the new string is returned.
//
//    *** DEPRECATED: Use cUnescape() in new code ***
// ----------------------------------------------------------------------
int unescapeCEscapeString(const std::string& src, std::string* dest);
int unescapeCEscapeString(
    const std::string& src,
    std::string* dest,
    std::vector<std::string>* errors);
std::string unescapeCEscapeString(const std::string& src);

// ----------------------------------------------------------------------
// cUnescape()
//    Copies "source" to "dest", rewriting C-style escape sequences
//    -- '\n', '\r', '\\', '\ooo', etc -- to their ASCII
//    equivalents.  "dest" must be sufficiently large to hold all
//    the characters in the rewritten string (i.e. at least as large
//    as source.size() should be safe, since the replacements
//    are never longer than the original escaped sequences).  It's
//    safe for source and dest to be the same.  RETURNS true if
//    conversion was successful, false otherwise. Stores the size of
//    the result in 'dest_len'.
//
//    It allows hex sequences \xhh, or generally \xhhhhh with an
//    arbitrary number of hex digits, but all of them together must
//    specify a value of a single byte (e.g. \x0045 is equivalent
//    to \x45, and \x1234 is erroneous). If the value is too large,
//    an error is set. This is also true of octal values that exceed 0xff.
//
//    It also allows escape sequences of the form \uhhhh (exactly four
//    hex digits, upper or lower case) or \Uhhhhhhhh (exactly eight
//    hex digits, upper or lower case) to specify a Unicode code
//    point. The dest array will contain the UTF8-encoded version of
//    that code-point (e.g., if source contains \u2019, then dest will
//    contain the three bytes 0xE2, 0x80, and 0x99). For the inverse
//    transformation, use UniLib::UTF8EscapeString
//    (util/utf8/public/unilib.h), not cEscapeString.
//
//    Errors: Sets the description of the first encountered error in
//    'error'. To disable error reporting, set 'error' to NULL.
// ----------------------------------------------------------------------
bool cUnescape(
    const StringPiece& source,
    char* dest,
    int* destLen,
    std::string* error);

bool cUnescape(
    const StringPiece& source,
    std::string* dest,
    std::string* error);

// A version with no error reporting.
inline bool cUnescape(const StringPiece& source, std::string* dest) {
  return cUnescape(source, dest, nullptr);
}

// ----------------------------------------------------------------------
// cUnescapeForNullTerminatedString()
//
// This has the same behavior as cUnescape, except that each octal, hex,
// or Unicode escape sequence that resolves to a null character ('\0')
// is left in its original escaped form.  The result is a
// display-formatted string that can be interpreted as a null-terminated
// const char* and will not be cut short if it contains embedded null
// characters.
//
// ----------------------------------------------------------------------

bool cUnescapeForNullTerminatedString(
    const StringPiece& source,
    char* dest,
    int* destLen,
    std::string* error);

bool cUnescapeForNullTerminatedString(
    const StringPiece& source,
    std::string* dest,
    std::string* error);

// A version with no error reporting.
inline bool cUnescapeForNullTerminatedString(
    const StringPiece& source,
    std::string* dest) {
  return cUnescapeForNullTerminatedString(source, dest, NULL);
}

// ----------------------------------------------------------------------
// cEscapeString()
// cHexEscapeString()
// utf8SafeCEscapeString()
// utf8SafeCHexEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap. The 'Hex' version uses
//    hexadecimal rather than octal sequences. The 'Utf8Safe' version
//    doesn't touch UTF-8 bytes.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or -1 if there was insufficient space.
//
//    Currently only \n, \r, \t, ", ', \ and !asciiIsPrint() chars are escaped.
// ----------------------------------------------------------------------
int cEscapeString(const char* src, int srcLen, char* dest, int destLen);
int cHexEscapeString(const char* src, int srcLen, char* dest, int destLen);
int utf8SafeCEscapeString(const char* src, int srcLen, char* dest, int destLen);
int utf8SafeCHexEscapeString(
    const char* src,
    int srcLen,
    char* dest,
    int destLen);

// ----------------------------------------------------------------------
// cEscape()
// cHexEscape()
// utf8SafeCEscape()
// utf8SafeCHexEscape()
//    More convenient form of cEscapeString: returns result as a "string".
//    This version is slower than cEscapeString() because it does more
//    allocation.  However, it is much more convenient to use in
//    non-speed-critical code like logging messages etc.
// ----------------------------------------------------------------------
std::string cEscape(const StringPiece& src);
std::string cHexEscape(const StringPiece& src);
std::string utf8SafeCEscape(const StringPiece& src);
std::string utf8SafeCHexEscape(const StringPiece& src);

// ----------------------------------------------------------------------
// backslashEscape()
//    Given a string and a list of characters to escape, replace any
//    instance of one of those characters with \ + that character. For
//    example, when exporting maps to /varz, label values need to have
//    all dots escaped. Appends the result to dest.
// backslashUnescape()
//    Replace \ + any of the indicated "unescape me" characters with just
//    that character. Appends the result to dest.
//
//    IMPORTANT:
//    This function does not escape \ by default, so if you do not include
//    it in the chars to escape you will most certainly get an undesirable
//    result. That is, it won't be a reversible operation:
//      string src = "foo\\:bar";
//      backslashUnescape(backslashEscape(src, ":"), ":") == "foo\\\\:bar"
//    On the other hand, for all strings "src", the following is true:
//      backslashUnescape(backslashEscape(src, ":\\"), ":\\") == src
// ----------------------------------------------------------------------
void backslashEscape(
    const StringPiece& src,
    const strings::CharSet& toEscape,
    std::string* dest);
void backslashUnescape(
    const StringPiece& src,
    const strings::CharSet& toUnescape,
    std::string* dest);

inline std::string backslashEscape(
    const StringPiece& src,
    const strings::CharSet& toEscape) {
  std::string s;
  backslashEscape(src, toEscape, &s);
  return s;
}

inline std::string backslashUnescape(
    const StringPiece& src,
    const strings::CharSet& toUnescape) {
  std::string s;
  backslashUnescape(src, toUnescape, &s);
  return s;
}

// ----------------------------------------------------------------------
// quotedPrintableUnescape()
//    Check out http://www.cis.ohio-state.edu/htbin/rfc/rfc2045.html for
//    more details, only briefly implemented. But from the web...
//    Quoted-printable is an encoding method defined in the MIME
//    standard. It is used primarily to encode 8-bit text (such as text
//    that includes foreign characters) into 7-bit US ASCII, creating a
//    document that is mostly readable by humans, even in its encoded
//    form. All MIME compliant applications can decode quoted-printable
//    text, though they may not necessarily be able to properly display the
//    document as it was originally intended. As quoted-printable encoding
//    is implemented most commonly, printable ASCII characters (values 33
//    through 126, excluding 61), tabs and spaces that do not appear at the
//    end of lines, and end-of-line characters are not encoded. Other
//    characters are represented by an equal sign (=) immediately followed
//    by that character's hexadecimal value. Lines that are longer than 76
//    characters are shortened by line breaks, with the equal sign marking
//    where the breaks occurred.
//
//    Note that quotedPrintableUnescape is different from 'Q'-encoding as
//    defined in rfc2047. In particular, This does not treat '_'s as spaces.
//
//    See qEncodingUnescape().
//
//    Copies "src" to "dest", rewriting quoted printable escape sequences
//    =XX to their ASCII equivalents. src is not null terminated, instead
//    specify len. I recommend that slen<szdest, but we honor szdest
//    anyway.
//    RETURNS the length of dest.
// ----------------------------------------------------------------------
int quotedPrintableUnescape(const char* src, int slen, char* dest, int szdest);

// ----------------------------------------------------------------------
// qEncodingUnescape()
//    This is very similar to quotedPrintableUnescape except that we convert
//    '_'s into spaces. (See RFC 2047)
//    http://www.faqs.org/rfcs/rfc2047.html.
//
//    Copies "src" to "dest", rewriting q-encoding escape sequences
//    =XX to their ASCII equivalents. src is not null terminated, instead
//    specify len. I recommend that slen<szdest, but we honour szdest
//    anyway.
//    RETURNS the length of dest.
// ----------------------------------------------------------------------
int qEncodingUnescape(const char* src, int slen, char* dest, int szdest);

// ----------------------------------------------------------------------
// base64Unescape()
// webSafeBase64Unescape()
//    Copies "src" to "dest", where src is in base64 and is written to its
//    ASCII equivalents. src is not null terminated, instead specify len.
//    I recommend that slen<szdest, but we honor szdest anyway.
//    RETURNS the length of dest, or -1 if src contains invalid chars.
//    The WebSafe variation use '-' instead of '+' and '_' instead of '/'.
//    The variations that store into a string clear the string first, and
//    return false (with dest empty) if src contains invalid chars; for
//    these versions src and dest must be different strings.
// ----------------------------------------------------------------------
int base64Unescape(const unsigned char* src, int slen, char* dest, int szdest);
bool base64Unescape(const unsigned char* src, int slen, std::string* dest);
inline bool base64Unescape(const std::string& src, std::string* dest) {
  return base64Unescape(
      reinterpret_cast<const unsigned char*>(src.data()), src.size(), dest);
}

int webSafeBase64Unescape(
    const unsigned char* src,
    int slen,
    char* dest,
    int szdest);
bool webSafeBase64Unescape(
    const unsigned char* src,
    int slen,
    std::string* dest);
inline bool webSafeBase64Unescape(const std::string& src, std::string* dest) {
  return webSafeBase64Unescape(
      reinterpret_cast<const unsigned char*>(src.data()), src.size(), dest);
}

// Return the length to use for the output buffer given to the base64 escape
// routines. Make sure to use the same value for doPadding in both.
// This function may return incorrect results if given inputLen values that
// are extremely high, which should happen rarely.
int calculateBase64EscapedLen(int inputLen, bool doPadding);
// Use this version when calling base64Escape without a doPadding arg.
int calculateBase64EscapedLen(int inputLen);

// ----------------------------------------------------------------------
// base64Escape()
// webSafeBase64Escape()
//    Encode "src" to "dest" using base64 encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least calculateBase64EscapedLen() length.
//    RETURNS the length of dest.
//    The WebSafe variation use '-' instead of '+' and '_' instead of '/'
//    so that we can place the out in the URL or cookies without having
//    to escape them.  It also has an extra parameter "do_padding",
//    which when set to false will prevent padding with "=".
// ----------------------------------------------------------------------
int base64Escape(const unsigned char* src, int slen, char* dest, int szdest);
int webSafeBase64Escape(
    const unsigned char* src,
    int slen,
    char* dest,
    int szdest,
    bool doPadding);
// Encode src into dest with padding.
void base64Escape(const std::string& src, std::string* dest);
// Encode src into dest web-safely without padding.
void webSafeBase64Escape(const std::string& src, std::string* dest);
// Encode src into dest web-safely with padding.
void webSafeBase64EscapeWithPadding(const std::string& src, std::string* dest);

void base64Escape(
    const unsigned char* src,
    int szsrc,
    std::string* dest,
    bool doPadding);
void webSafeBase64Escape(
    const unsigned char* src,
    int szsrc,
    std::string* dest,
    bool doPadding);

// ----------------------------------------------------------------------
// base32Unescape()
//    Copies "src" to "dest", where src is in base32 and is written to its
//    ASCII equivalents. src is not null terminated, instead specify len.
//    RETURNS the length of dest, or -1 if src contains invalid chars.
// ----------------------------------------------------------------------
int base32Unescape(const char* src, int slen, char* dest, int szdest);
bool base32Unescape(const char* src, int slen, std::string* dest);
inline bool base32Unescape(const std::string& src, std::string* dest) {
  return base32Unescape(src.data(), src.size(), dest);
}

// ----------------------------------------------------------------------
// base32Escape()
//    Encode "src" to "dest" using base32 encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least calculateBase32EscapedLen() length.
//    RETURNS the length of dest. RETURNS 0 if szsrc is zero, or szdest is
//    too small to fit the fully encoded result.  'dest' is padded with '='.
//
//    Note that this is "Base 32 Encoding" from RFC 4648 section 6.
// ----------------------------------------------------------------------
int base32Escape(
    const unsigned char* src,
    size_t szsrc,
    char* dest,
    size_t szdest);
bool base32Escape(const std::string& src, std::string* dest);

// ----------------------------------------------------------------------
// base32HexEscape()
//    Encode "src" to "dest" using base32hex encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least calculateBase32EscapedLen() length.
//    RETURNS the length of dest. RETURNS 0 if szsrc is zero, or szdest is
//    too small to fit the fully encoded result.  'dest' is padded with '='.
//
//    Note that this is "Base 32 Encoding with Extended Hex Alphabet"
//    from RFC 4648 section 7.
// ----------------------------------------------------------------------
int base32HexEscape(
    const unsigned char* src,
    size_t szsrc,
    char* dest,
    size_t szdest);
bool base32HexEscape(const std::string& src, std::string* dest);

// Return the length to use for the output buffer given to the base32 escape
// routines.  This function may return incorrect results if given inputLen
// values that are extremely high, which should happen rarely.
int calculateBase32EscapedLen(size_t inputLen);

// ----------------------------------------------------------------------
// eightBase32DigitsToTenHexDigits()
// tenHexDigitsToEightBase32Digits()
//    Convert base32 to and from hex.
//
//   for eightBase32DigitsToTenHexDigits():
//     *in must point to 8 base32 digits.
//     *out must point to 10 bytes.
//
//   for tenHexDigitsToEightBase32Digits():
//     *in must point to 10 hex digits.
//     *out must point to 8 bytes.
//
//   Note that the base64 functions above are different. They convert base64
//   to and from binary data. We convert to and from string representations
//   of hex. They deal with arbitrary lengths and we deal with single,
//   whole base32 quanta.
//
//   See RFC3548 at http://www.ietf.org/rfc/rfc3548.txt
//   for details on base32.
// ----------------------------------------------------------------------
void eightBase32DigitsToTenHexDigits(const unsigned char* in, char* out);
void tenHexDigitsToEightBase32Digits(const char* in, char* out);

// ----------------------------------------------------------------------
// eightBase32DigitsToFiveBytes()
// fiveBytesToEightBase32Digits()
//   Convert base32 to and from binary
//
//   for eightBase32DigitsToTenHexDigits():
//     *in must point to 8 base32 digits.
//     *out must point to 5 bytes.
//
//   for tenHexDigitsToEightBase32Digits():
//     *in must point to 5 bytes.
//     *out must point to 8 bytes.
//
//   Note that the base64 functions above are different.  They deal with
//   arbitrary lengths and we deal with single, whole base32 quanta.
// ----------------------------------------------------------------------
void eightBase32DigitsToFiveBytes(
    const unsigned char* in,
    unsigned char* bytesOut);
void fiveBytesToEightBase32Digits(const unsigned char* inBytes, char* out);

// ----------------------------------------------------------------------
// escapeFileName()
// unescapeFileName()
//   Utility functions to (un)escape strings to make them suitable for use in
//   filenames. Characters not in [a-zA-Z0-9-_.] will be escaped into %XX.
//   E.g: "Hello, world!" will be escaped as "Hello%2c%20world%21"
//
//   NB that this function escapes slashes, so the output will be a flat
//   filename and will not keep the directory structure. Slashes are replaced
//   with '~', instead of a %XX sequence to make it easier for people to
//   understand the escaped form when the original string is a file path.
//
//   WARNING: filenames produced by these functions may not be compatible with
//   Colossus FS. In particular, the '%' character has a special meaning in
//   CFS.
//
//   The versions that receive a string for the output will append to it.
// ----------------------------------------------------------------------
void escapeFileName(const StringPiece& src, std::string* dst);
void unescapeFileName(const StringPiece& src, std::string* dst);
inline std::string escapeFileName(const StringPiece& src) {
  std::string r;
  escapeFileName(src, &r);
  return r;
}
inline std::string unescapeFileName(const StringPiece& src) {
  std::string r;
  unescapeFileName(src, &r);
  return r;
}

// ----------------------------------------------------------------------
// Here is a utility method to change hex chars to ints
// ----------------------------------------------------------------------

inline int hexDigitToInt(char c) {
  /* Assume ASCII. */
  DCHECK('0' == 0x30 && 'A' == 0x41 && 'a' == 0x61);
  DCHECK(asciiIsXdigit(c));
  int x = static_cast<unsigned char>(c);
  if (x > '9') {
    x += 9;
  }
  return x & 0xf;
}

// ----------------------------------------------------------------------
// a2bHex()
//  Description: Ascii-to-Binary hex conversion.  This converts
//         2*'num' hexadecimal characters to 'num' binary data.
//        Return value: 'num' bytes of binary data (via the 'to' argument)
// ----------------------------------------------------------------------
void a2bHex(const char* from, unsigned char* to, int num);
void a2bHex(const char* from, char* to, int num);
void a2bHex(const char* from, std::string* to, int num);
std::string a2bHex(const std::string& a);

// ----------------------------------------------------------------------
// a2bBin()
//  Description: Ascii-to-Binary binary conversion.  This converts
//        a.size() binary characters (ascii '0' or '1') to
//        ceil(a.size()/8) bytes of binary data.  The first character is
//        considered the most significant if byteOrderMsb is set.  a is
//        considered to be padded with trailing 0s if its size is not a
//        multiple of 8.
//        Return value: ceil(a.size()/8) bytes of binary data
// ----------------------------------------------------------------------
std::string a2bBin(const std::string& a, bool byteOrderMsb);

// ----------------------------------------------------------------------
// b2aHex()
//  Description: Binary-to-Ascii hex conversion.  This converts
//   'num' bytes of binary to a 2*'num'-character hexadecimal representation
//    Return value: 2*'num' characters of ascii text (via the 'to' argument)
// ----------------------------------------------------------------------
void b2aHex(const unsigned char* from, char* to, int num);
void b2aHex(const unsigned char* from, std::string* to, int num);

// ----------------------------------------------------------------------
// b2aHex()
//  Description: Binary-to-Ascii hex conversion.  This converts
//   'num' bytes of binary to a 2*'num'-character hexadecimal representation
//    Return value: 2*'num' characters of ascii string
// ----------------------------------------------------------------------
std::string b2aHex(const char* from, int num);
std::string b2aHex(const StringPiece& b);

// ----------------------------------------------------------------------
// b2aBin()
//  Description: Binary-to-Ascii binary conversion.  This converts
//   b.size() bytes of binary to a 8*b.size() character representation
//   (ascii '0' or '1').  The highest order bit in each byte is returned
//   first in the string if byteOrderMsb is set.
//   Return value: 8*b.size() characters of ascii text
// ----------------------------------------------------------------------
std::string b2aBin(const std::string& b, bool byteOrderMsb);

// ----------------------------------------------------------------------
// shellEscape
//   Make a shell command argument from a string.
//   Returns a Bourne shell string literal such that, once the shell finishes
//   expanding the argument, the argument passed on to the program being
//   run will be the same as whatever you passed in.
//   NOTE: This is "ported" from python2.2's commands.mkarg(); it should be
//         safe for Bourne shell syntax (i.e. sh, bash), but mileage may vary
//         with other shells.
// ----------------------------------------------------------------------
std::string shellEscape(StringPiece src);

// Reads at most bytesToRead from binaryString and writes it to
// asciiString in lower case hex.
void byteStringToAscii(
    const std::string& binaryString,
    int bytesToRead,
    std::string* asciiString);

inline std::string byteStringToAscii(
    const std::string& binaryString,
    int bytesToRead) {
  std::string result;
  byteStringToAscii(binaryString, bytesToRead, &result);
  return result;
}

// Converts the hex from asciiString into binary data and
// writes the binary data into binaryString.
// Empty input successfully converts to empty output.
// Returns false and may modify output if it is
// unable to parse the hex string.
bool byteStringFromAscii(
    const std::string& asciiString,
    std::string* binaryString);

// Clean up a multi-line string to conform to Unix line endings.
// Reads from src and appends to dst, so usually dst should be empty.
// If there is no line ending at the end of a non-empty string, it can
// be added automatically.
//
// Four different types of input are correctly handled:
//
//   - Unix/Linux files: line ending is LF, pass through unchanged
//
//   - DOS/Windows files: line ending is CRLF: convert to LF
//
//   - Legacy Mac files: line ending is CR: convert to LF
//
//   - Garbled files: random line endings, covert gracefully
//                    lonely CR, lonely LF, CRLF: convert to LF
//
//   @param src The multi-line string to convert
//   @param dst The converted string is appended to this string
//   @param autoEndLastLine Automatically terminate the last line
//
//   Limitations:
//
//     This does not do the right thing for CRCRLF files created by
//     broken programs that do another Unix->DOS conversion on files
//     that are already in CRLF format.
void cleanStringLineEndings(
    const std::string& src,
    std::string* dst,
    bool autoEndLastLine);

// Same as above, but transforms the argument in place.
void cleanStringLineEndings(std::string* str, bool autoEndLastLine);

} // namespace strings
