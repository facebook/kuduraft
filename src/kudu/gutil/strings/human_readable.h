// Copyright 2007 Google Inc. All Rights Reserved.
//
// A collection of methods to convert back and forth between a number
// and a human-readable string representing the number.

#pragma once

#include <cstdint>
#include <string>

#include "kudu/gutil/macros.h"

//                                 WARNING
// HumanReadable{NumBytes, Int} don't give you the standard set of SI prefixes.
//
// HumanReadableNumBytes uses binary powers -- 1M = 1 << 20 -- but for numbers
// less than 1024, it adds the suffix "B" for "bytes." It is OK when you need
// to print a literal number of bytes, but can be awfully confusing for
// anything else.
//
// HumanReadableInt uses decimal powers -- 1M = 10^3 -- but prints
// 'B'-for-billion instead of 'G'-for-giga. It's good for representing
// true numbers, like how many documents are in a repository.
// HumanReadableNum is the same as HumanReadableInt but has additional
// support for DoubleToString(), where smaller numbers will print more
// (up to 3) decimal places.
//
// If you want SI prefixes, use the functions in si_prefix.h instead; for
// example, strings::si_prefix::ToDecimalString(1053.2) == "1.05k".

class HumanReadableNumBytes {
 public:
  // Converts between an int64_t representing a number of bytes and a
  // human readable string representing the same number.
  // e.g. 1000000 -> "976.6K".
  //  Note that calling these two functions in succession isn't a
  //  noop, since ToString() may round.
  static std::string toString(int64_t numBytes);

  // TODO(user): Maybe change this class to use SIPrefix?

 private:
  DISALLOW_IMPLICIT_CONSTRUCTORS(HumanReadableNumBytes);
};

class HumanReadableElapsedTime {
 public:
  // Converts a time interval as double to a human readable
  // string. For example:
  //   0.001       -> "1 ms"
  //   10.0        -> "10 s"
  //   933120.0    -> "10.8 days"
  //   39420000.0  -> "1.25 years"
  //   -10         -> "-10 s"
  static std::string toShortString(double seconds);

 private:
  DISALLOW_IMPLICIT_CONSTRUCTORS(HumanReadableElapsedTime);
};
