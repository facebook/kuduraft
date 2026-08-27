//
// Copyright (C) 2001 and onwards Google, Inc.
//
// (Please see comments in strutil.h near the include of <asm/string.h>
//  if you feel compelled to try to provide more efficient implementations
//  of these routines.)
//
// These routines provide mem versions of standard C string routines,
// such a strpbrk.  They function exactly the same as the str version,
// so if you wonder what they are, replace the word "mem" by
// "str" and check out the man page.  I could return void*, as the
// strutil.h mem*() routines tend to do, but I return char* instead
// since this is by far the most common way these functions are called.
//
// The difference between the mem and str versions is the mem version
// takes a pointer and a length, rather than a NULL-terminated string.
// The memcase* routines defined here assume the locale is "C"
// (they use asciiToLower instead of tolower).
//
// These routines are based on the BSD library.
//
// Here's a list of routines from string.h, and their mem analogues.
// Functions in lowercase are defined in string.h; those in UPPERCASE
// are defined here:
//
// strlen                  --
// strcat strncat          MEMCAT
// strcpy strncpy          memcpy
// --                      memccpy   (very cool function, btw)
// --                      memmove
// --                      memset
// strcmp strncmp          memcmp
// strcasecmp strncasecmp  MEMCASECMP
// strchr                  memchr
// strcoll                 --
// strxfrm                 --
// strdup strndup          MEMDUP
// strrchr                 MEMRCHR
// strspn                  MEMSPN
// strcspn                 MEMCSPN
// strpbrk                 MEMPBRK
// strstr                  MEMSTR MEMMEM
// (g)strcasestr           MEMCASESTR MEMCASEMEM
// strtok                  --
// strprefix               MEMPREFIX      (strprefix is from strutil.h)
// strcaseprefix           MEMCASEPREFIX  (strcaseprefix is from strutil.h)
// strsuffix               MEMSUFFIX      (strsuffix is from strutil.h)
// strcasesuffix           MEMCASESUFFIX  (strcasesuffix is from strutil.h)
// --                      MEMIS
// --                      MEMCASEIS
// strcount                MEMCOUNT       (strcount is from strutil.h)

#pragma once

#include <cstddef>
#include <cstring> // to get the POSIX mem*() routines

// This is significantly faster for case-sensitive matches with very
// few possible matches.  See unit test for benchmarks.
const char* memmatch(
    const char* phaystack,
    size_t haylen,
    const char* pneedle,
    size_t neelen);
