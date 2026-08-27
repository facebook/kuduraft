// Copyright 2007 Google Inc. All Rights Reserved.

#include "kudu/gutil/strings/human_readable.h"

#include <limits>

#include <glog/logging.h>

#include <fmt/format.h>

using std::string;

namespace {

template <typename T>
const char* getNegStr(T* value) {
  if (*value < 0) {
    *value = -(*value);
    return "-";
  } else {
    return "";
  }
}

} // namespace

string HumanReadableNumBytes::toString(int64_t numBytes) {
  if (numBytes == std::numeric_limits<int64_t>::min()) {
    // Special case for number with not representable nagation.
    return "-8E";
  }

  const char* negStr = getNegStr(&numBytes);

  // Special case for bytes.
  if (numBytes < 1024LL) {
    // No fractions for bytes.
    return fmt::format("{}{}B", negStr, numBytes);
  }

  static const char kUnits[] = "KMGTPE"; // int64 only goes up to E.
  const char* unit = kUnits;
  while (numBytes >= 1024LL * 1024LL) {
    numBytes /= 1024LL;
    ++unit;
    CHECK(unit < kUnits + arraysize(kUnits));
  }

  if (*unit == 'K') {
    return fmt::format("{}{:.1f}{}", negStr, numBytes / 1024.0, *unit);
  } else {
    return fmt::format("{}{:.2f}{}", negStr, numBytes / 1024.0, *unit);
  }
}

// Abbreviations used here are acceptable English abbreviations
// without the ending period (".") for brevity, except for uncommon
// abbreviations, in which case the entire word is spelled out. ("mo"
// and "mos" are not good abbreviations for "months" -- with or
// without the period). If needed, one can add a
// HumanReadableTime::toShortString() for shorter abbreviations or one
// for always spelling out the unit, HumanReadableTime::toStringLong().
string HumanReadableElapsedTime::toShortString(double seconds) {
  string humanReadable;

  if (seconds < 0) {
    humanReadable = "-";
    seconds = -seconds;
  }

  // Start with ns and keep going up to years.
  if (seconds < 0.000001) {
    fmt::format_to(
        std::back_inserter(humanReadable),
        "{:0.3g} ns",
        seconds * 1000000000.0);
    return humanReadable;
  }
  if (seconds < 0.001) {
    fmt::format_to(
        std::back_inserter(humanReadable), "{:0.3g} us", seconds * 1000000.0);
    return humanReadable;
  }
  if (seconds < 1.0) {
    fmt::format_to(
        std::back_inserter(humanReadable), "{:0.3g} ms", seconds * 1000.0);
    return humanReadable;
  }
  if (seconds < 60.0) {
    fmt::format_to(std::back_inserter(humanReadable), "{:0.3g} s", seconds);
    return humanReadable;
  }
  seconds /= 60.0;
  if (seconds < 60.0) {
    fmt::format_to(std::back_inserter(humanReadable), "{:0.3g} min", seconds);
    return humanReadable;
  }
  seconds /= 60.0;
  if (seconds < 24.0) {
    fmt::format_to(std::back_inserter(humanReadable), "{:0.3g} h", seconds);
    return humanReadable;
  }
  seconds /= 24.0;
  if (seconds < 30.0) {
    fmt::format_to(std::back_inserter(humanReadable), "{:0.3g} days", seconds);
    return humanReadable;
  }
  if (seconds < 365.2425) {
    fmt::format_to(
        std::back_inserter(humanReadable),
        "{:0.3g} months",
        seconds / 30.436875);
    return humanReadable;
  }
  seconds /= 365.2425;
  fmt::format_to(std::back_inserter(humanReadable), "{:0.3g} years", seconds);
  return humanReadable;
}
