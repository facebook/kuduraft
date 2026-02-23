// Copyright 2007 Google Inc. All Rights Reserved.
//
// Character classification functions similar to standard <ctype.h>.
// Some C++ implementations provide locale-sensitive implementations
// of some <ctype.h> functions.  These ascii_* functions are
// hard-wired for ASCII.  Hard-wired for ASCII is much faster.
//
// asciiIsAlnum, asciiIsAscii, asciiIsDigit, asciiIsLower,
// asciiIsPrint, asciiIsSpace, asciiIsXdigit
//   Similar to the <ctype.h> functions with similar names.
//   Input parameter is an unsigned char.  Return value is a bool.
//   If the input has a numerical value greater than 127
//   then the output is "false".
//
// asciiToLower, asciiToUpper
//   Similar to the <ctype.h> functions with similar names.
//   Input parameter is an unsigned char.  Return value is a char.
//   If the input is not an ascii {lower,upper}-case letter
//   (including numerical values greater than 127)
//   then the output is the same as the input.

#pragma once

// Array of character information.  This is an implementation detail.
// The individual bits do not have names because the array definition is
// already tightly coupled to these functions.  Names would just make it
// harder to read and debug.

#define kApb kAsciiPropertyBits
extern const unsigned char kAsciiPropertyBits[256];

// Public functions.

static inline bool asciiIsAlnum(unsigned char c) {
  return kApb[c] & 0x04;
}
static inline bool asciiIsSpace(unsigned char c) {
  return kApb[c] & 0x08;
}
static inline bool asciiIsXdigit(unsigned char c) {
  return kApb[c] & 0x80;
}

static inline bool asciiIsDigit(unsigned char c) {
  return c >= '0' && c <= '9';
}

static inline bool asciiIsPrint(unsigned char c) {
  return c >= 32 && c < 127;
}

static inline bool asciiIsLower(unsigned char c) {
  return c >= 'a' && c <= 'z';
}

static inline bool asciiIsAscii(unsigned char c) {
  return c < 128;
}
#undef kApb

extern const unsigned char kAsciiToLower[256];
static inline char asciiToLower(unsigned char c) {
  return kAsciiToLower[c];
}
extern const unsigned char kAsciiToUpper[256];
static inline char asciiToUpper(unsigned char c) {
  return kAsciiToUpper[c];
}
