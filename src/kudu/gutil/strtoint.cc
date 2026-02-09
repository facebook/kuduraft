// Copyright 2008 Google Inc. All Rights Reserved.
//
// Architecture-neutral plug compatible replacements for strtol() friends.
// See strtoint.h for details on how to use this component.
//

#include <cerrno>
#include <climits>
#include <limits>

#include "kudu/gutil/strtoint.h"

// Replacement strto[u]l functions that have identical overflow and underflow
// characteristics for both ILP-32 and LP-64 platforms, including errno
// preservation for error-free calls.
int32_t strto32Adapter(const char* nptr, char** endptr, int base) {
  const int savedErrno = errno;
  errno = 0;
  const long result = strtol(nptr, endptr, base);
  if (errno == ERANGE && result == LONG_MIN) {
    return std::numeric_limits<int32_t>::min();
  } else if (errno == ERANGE && result == LONG_MAX) {
    return std::numeric_limits<int32_t>::max();
  } else if (errno == 0 && result < std::numeric_limits<int32_t>::min()) {
    errno = ERANGE;
    return std::numeric_limits<int32_t>::min();
  } else if (errno == 0 && result > std::numeric_limits<int32_t>::max()) {
    errno = ERANGE;
    return std::numeric_limits<int32_t>::max();
  }
  if (errno == 0) {
    errno = savedErrno;
  }
  return static_cast<int32_t>(result);
}

uint32_t strtou32Adapter(const char* nptr, char** endptr, int base) {
  const int savedErrno = errno;
  errno = 0;
  const unsigned long result = strtoul(nptr, endptr, base);
  if (errno == ERANGE && result == ULONG_MAX) {
    return std::numeric_limits<uint32_t>::max();
  } else if (errno == 0 && result > std::numeric_limits<uint32_t>::max()) {
    errno = ERANGE;
    return std::numeric_limits<uint32_t>::max();
  }
  if (errno == 0) {
    errno = savedErrno;
  }
  return static_cast<uint32_t>(result);
}
