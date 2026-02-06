// Copyright 2008 Google Inc.  All rights reserved.

#include "kudu/gutil/strings/substitute.h"

#include <cstdint>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/escaping.h"

using std::string;

namespace strings {

using internal::SubstituteArg;

const SubstituteArg SubstituteArg::kNoArg;

// Returns the number of args in arg_array which were passed explicitly
// to Substitute().
static int countSubstituteArgs(const SubstituteArg* const* argsArray) {
  int count = 0;
  while (argsArray[count] != &SubstituteArg::kNoArg) {
    ++count;
  }
  return count;
}

namespace internal {
int substitutedSize(
    std::string_view format,
    const SubstituteArg* const* argsArray) {
  int size = 0;
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (i + 1 >= format.size()) {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return 0;
      } else if (asciiIsDigit(format[i + 1])) {
        int index = format[i + 1] - '0';
        if (argsArray[index]->size() == -1) {
          LOG(DFATAL)
              << "strings::Substitute format string invalid: asked for \"$"
              << index << "\", but only " << countSubstituteArgs(argsArray)
              << " args were given.  Full format string was: \""
              << CEscape(format) << "\".";
          return 0;
        }
        size += argsArray[index]->size();
        ++i; // Skip next char.
      } else if (format[i + 1] == '$') {
        ++size;
        ++i; // Skip next char.
      } else {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return 0;
      }
    } else {
      ++size;
    }
  }
  return size;
}

char* substituteToBuffer(
    std::string_view format,
    const SubstituteArg* const* argsArray,
    char* target) {
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (asciiIsDigit(format[i + 1])) {
        const SubstituteArg* src = argsArray[format[i + 1] - '0'];
        memcpy(target, src->data(), src->size());
        target += src->size();
        ++i; // Skip next char.
      } else if (format[i + 1] == '$') {
        *target++ = '$';
        ++i; // Skip next char.
      }
    } else {
      *target++ = format[i];
    }
  }
  return target;
}

} // namespace internal

void substituteAndAppend(
    string* output,
    std::string_view format,
    const SubstituteArg& arg0,
    const SubstituteArg& arg1,
    const SubstituteArg& arg2,
    const SubstituteArg& arg3,
    const SubstituteArg& arg4,
    const SubstituteArg& arg5,
    const SubstituteArg& arg6,
    const SubstituteArg& arg7,
    const SubstituteArg& arg8,
    const SubstituteArg& arg9) {
  const SubstituteArg* const argsArray[] = {
      &arg0,
      &arg1,
      &arg2,
      &arg3,
      &arg4,
      &arg5,
      &arg6,
      &arg7,
      &arg8,
      &arg9,
      nullptr};

  // Determine total size needed.
  int size = substitutedSize(format, argsArray);
  if (size == 0) {
    return;
  }

  // Build the string.
  int originalSize = output->size();
  STLStringResizeUninitialized(output, originalSize + size);
  char* target = output->data() + originalSize;

  target = substituteToBuffer(format, argsArray, target);
  DCHECK_EQ(target - output->data(), output->size());
}

SubstituteArg::SubstituteArg(const void* value) {
  KUDU_COMPILE_ASSERT(
      sizeof(scratch_) >= sizeof(value) * 2 + 2, fix_sizeof_scratch_);
  if (value == nullptr) {
    text_ = "NULL";
    size_ = strlen(text_);
  } else {
    char* ptr = scratch_ + sizeof(scratch_);
    uintptr_t num = reinterpret_cast<uintptr_t>(value);
    static const char kHexDigits[] = "0123456789abcdef";
    do {
      *--ptr = kHexDigits[num & 0xf];
      num >>= 4;
    } while (num != 0);
    *--ptr = 'x';
    *--ptr = '0';
    text_ = ptr;
    size_ = scratch_ + sizeof(scratch_) - ptr;
  }
}

} // namespace strings
