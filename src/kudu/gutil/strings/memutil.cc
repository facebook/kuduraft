//
// Copyright (C) 2001 and onwards Google, Inc.
//

#include "kudu/gutil/strings/memutil.h"

// This is significantly faster for case-sensitive matches with very
// few possible matches.  See unit test for benchmarks.
const char* memmatch(
    const char* phaystack,
    size_t haylen,
    const char* pneedle,
    size_t neelen) {
  if (0 == neelen) {
    return phaystack; // even if haylen is 0
  }
  if (haylen < neelen)
    return nullptr;

  const char* match;
  const char* hayend = phaystack + haylen - neelen + 1;
  // A C-style cast is used here to work around the fact that memchr returns a
  // void* on Posix-compliant systems and const void* on Windows.
  while (
      (match = static_cast<const char*>(
           memchr(phaystack, pneedle[0], hayend - phaystack)))) {
    if (memcmp(match, pneedle, neelen) == 0)
      return match;
    else
      phaystack = match + 1;
  }
  return nullptr;
}
