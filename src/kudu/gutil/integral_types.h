// Copyright 2010 Google Inc. All Rights Reserved.
//
// Basic integer type definitions for various platforms
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//
#pragma once

#include <inttypes.h>

// These typedefs are also defined in base/google.swig. In the
// SWIG environment, we use those definitions and avoid duplicate
// definitions here with an ifdef. The definitions should be the
// same in both files, and ideally be only defined in this file.
#ifndef SWIG
// Standard typedefs
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef __int128 int128;

// NOTE: unsigned types are DANGEROUS in loops and other arithmetical
// places.  Use the signed types unless your variable represents a bit
// pattern (eg a hash value) or you really need the extra bit.  Do NOT
// use 'unsigned' to express "this value should always be positive";
// use assertions for this.

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

#endif /* SWIG */
