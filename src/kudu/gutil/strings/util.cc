//
// Copyright (C) 1999-2005 Google, Inc.
//

// TODO(user): visit each const_cast.  Some of them are no longer necessary
// because last Single Unix Spec and grte v2 are more const-y.

#include "kudu/gutil/strings/util.h"

#include <algorithm>
#include <cstring>
#include <ctime>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/utf/utf.h"

using std::min;
using std::string;

#ifdef OS_WINDOWS
#ifdef min // windows.h defines this to something silly
#undef min
#endif
#endif

// Use this instead of gmtime_r if you want to build for Windows.
// Windows doesn't have a 'gmtime_r', but it has the similar 'gmtime_s'.
// TODO(user): Probably belongs in //base:time_support.{cc|h}.
static struct tm* portableSafeGmtime(const time_t* timep, struct tm* result) {
#ifdef OS_WINDOWS
  return gmtime_s(result, timep) == 0 ? result : NULL;
#else
  return gmtime_r(timep, result);
#endif // OS_WINDOWS
}

char* strnstr(const char* haystack, const char* needle, size_t haystackLen) {
  if (*needle == '\0') {
    return const_cast<char*>(haystack);
  }
  size_t needleLen = strlen(needle);
  char* where;
  while ((where = strnchr(haystack, *needle, haystackLen)) != nullptr) {
    if (where - haystack + needleLen > haystackLen) {
      return nullptr;
    }
    if (strncmp(where, needle, needleLen) == 0) {
      return where;
    }
    haystackLen -= where + 1 - haystack;
    haystack = where + 1;
  }
  return nullptr;
}

// ----------------------------------------------------------------------
// stringReplace()
//    Give me a string and two patterns "old" and "new", and I replace
//    the first instance of "old" in the string with "new", if it
//    exists.  If "replace_all" is true then call this repeatedly until it
//    fails.  RETURN a new string, regardless of whether the replacement
//    happened or not.
// ----------------------------------------------------------------------

string stringReplace(
    const StringPiece& s,
    const StringPiece& oldsub,
    const StringPiece& newsub,
    bool replaceAll) {
  string ret;
  stringReplace(s, oldsub, newsub, replaceAll, &ret);
  return ret;
}

// ----------------------------------------------------------------------
// stringReplace()
//    Replace the "old" pattern with the "new" pattern in a string,
//    and append the result to "res".  If replace_all is false,
//    it only replaces the first instance of "old."
// ----------------------------------------------------------------------

void stringReplace(
    const StringPiece& s,
    const StringPiece& oldsub,
    const StringPiece& newsub,
    bool replaceAll,
    string* res) {
  if (oldsub.empty()) {
    res->append(s.data(), s.length()); // If empty, append the given string.
    return;
  }

  StringPiece::size_type startPos = 0;
  StringPiece::size_type pos;
  do {
    pos = s.find(oldsub, startPos);
    if (pos == StringPiece::kNpos) {
      break;
    }
    res->append(s.data() + startPos, pos - startPos);
    res->append(newsub.data(), newsub.length());
    // Start searching again after the "old".
    startPos = pos + oldsub.length();
  } while (replaceAll);
  res->append(s.data() + startPos, s.length() - startPos);
}

// ----------------------------------------------------------------------
// gstrncasestr is a case-insensitive strnstr.
//    Finds the occurence of the (null-terminated) needle in the
//    haystack, where no more than len bytes of haystack is searched.
//    Characters that appear after a '\0' in the haystack are not searched.
//
// This function uses asciiToLower() instead of tolower(), for speed.
// ----------------------------------------------------------------------
const char* gstrncasestr(const char* haystack, const char* needle, size_t len) {
  char c, sc;

  if ((c = *needle++) != 0) {
    c = asciiToLower(c);
    size_t needleLen = strlen(needle);
    do {
      do {
        if (len-- <= needleLen || 0 == (sc = *haystack++))
          return nullptr;
      } while (asciiToLower(sc) != c);
    } while (strncasecmp(haystack, needle, needleLen) != 0);
    haystack--;
  }
  return haystack;
}

// TODO(user): add a microbenchmark and revisit
// the optimizations done here.
//
// Several converters use this table to reduce
// division and modulo operations.
extern const char kTwoAsciiDigits[100][2];

const char kTwoAsciiDigits[100][2] = {
    {'0', '0'}, {'0', '1'}, {'0', '2'}, {'0', '3'}, {'0', '4'}, {'0', '5'},
    {'0', '6'}, {'0', '7'}, {'0', '8'}, {'0', '9'}, {'1', '0'}, {'1', '1'},
    {'1', '2'}, {'1', '3'}, {'1', '4'}, {'1', '5'}, {'1', '6'}, {'1', '7'},
    {'1', '8'}, {'1', '9'}, {'2', '0'}, {'2', '1'}, {'2', '2'}, {'2', '3'},
    {'2', '4'}, {'2', '5'}, {'2', '6'}, {'2', '7'}, {'2', '8'}, {'2', '9'},
    {'3', '0'}, {'3', '1'}, {'3', '2'}, {'3', '3'}, {'3', '4'}, {'3', '5'},
    {'3', '6'}, {'3', '7'}, {'3', '8'}, {'3', '9'}, {'4', '0'}, {'4', '1'},
    {'4', '2'}, {'4', '3'}, {'4', '4'}, {'4', '5'}, {'4', '6'}, {'4', '7'},
    {'4', '8'}, {'4', '9'}, {'5', '0'}, {'5', '1'}, {'5', '2'}, {'5', '3'},
    {'5', '4'}, {'5', '5'}, {'5', '6'}, {'5', '7'}, {'5', '8'}, {'5', '9'},
    {'6', '0'}, {'6', '1'}, {'6', '2'}, {'6', '3'}, {'6', '4'}, {'6', '5'},
    {'6', '6'}, {'6', '7'}, {'6', '8'}, {'6', '9'}, {'7', '0'}, {'7', '1'},
    {'7', '2'}, {'7', '3'}, {'7', '4'}, {'7', '5'}, {'7', '6'}, {'7', '7'},
    {'7', '8'}, {'7', '9'}, {'8', '0'}, {'8', '1'}, {'8', '2'}, {'8', '3'},
    {'8', '4'}, {'8', '5'}, {'8', '6'}, {'8', '7'}, {'8', '8'}, {'8', '9'},
    {'9', '0'}, {'9', '1'}, {'9', '2'}, {'9', '3'}, {'9', '4'}, {'9', '5'},
    {'9', '6'}, {'9', '7'}, {'9', '8'}, {'9', '9'}};

static inline void putTwoDigits(int i, char* p) {
  DCHECK_GE(i, 0);
  DCHECK_LT(i, 100);
  p[0] = kTwoAsciiDigits[i][0];
  p[1] = kTwoAsciiDigits[i][1];
}

char* fastTimeToBuffer(time_t s, char* buffer) {
  if (s == 0) {
    time(&s);
  }

  struct tm tm;
  if (portableSafeGmtime(&s, &tm) == nullptr) {
    // Error message must fit in 30-char buffer.
    memcpy(buffer, "Invalid:", sizeof("Invalid:"));
    fastInt64ToBufferLeft(s, buffer + strlen(buffer));
    return buffer;
  }

  // strftime format: "%a, %d %b %Y %H:%M:%S GMT",
  // but strftime does locale stuff which we do not want
  // plus strftime takes > 10x the time of hard code

  const char* weekdayName = "Xxx";
  switch (tm.tm_wday) {
    default: {
      LOG(FATAL) << "tm.tm_wday: " << tm.tm_wday;
    }
    case 0:
      weekdayName = "Sun";
      break;
    case 1:
      weekdayName = "Mon";
      break;
    case 2:
      weekdayName = "Tue";
      break;
    case 3:
      weekdayName = "Wed";
      break;
    case 4:
      weekdayName = "Thu";
      break;
    case 5:
      weekdayName = "Fri";
      break;
    case 6:
      weekdayName = "Sat";
      break;
  }

  const char* monthName = "Xxx";
  switch (tm.tm_mon) {
    default: {
      LOG(FATAL) << "tm.tm_mon: " << tm.tm_mon;
    }
    case 0:
      monthName = "Jan";
      break;
    case 1:
      monthName = "Feb";
      break;
    case 2:
      monthName = "Mar";
      break;
    case 3:
      monthName = "Apr";
      break;
    case 4:
      monthName = "May";
      break;
    case 5:
      monthName = "Jun";
      break;
    case 6:
      monthName = "Jul";
      break;
    case 7:
      monthName = "Aug";
      break;
    case 8:
      monthName = "Sep";
      break;
    case 9:
      monthName = "Oct";
      break;
    case 10:
      monthName = "Nov";
      break;
    case 11:
      monthName = "Dec";
      break;
  }

  // Write out the buffer.

  memcpy(buffer + 0, weekdayName, 3);
  buffer[3] = ',';
  buffer[4] = ' ';

  putTwoDigits(tm.tm_mday, buffer + 5);
  buffer[7] = ' ';

  memcpy(buffer + 8, monthName, 3);
  buffer[11] = ' ';

  int32_t year = tm.tm_year + 1900;
  putTwoDigits(year / 100, buffer + 12);
  putTwoDigits(year % 100, buffer + 14);
  buffer[16] = ' ';

  putTwoDigits(tm.tm_hour, buffer + 17);
  buffer[19] = ':';

  putTwoDigits(tm.tm_min, buffer + 20);
  buffer[22] = ':';

  putTwoDigits(tm.tm_sec, buffer + 23);

  // includes ending NUL
  memcpy(buffer + 25, " GMT", 5);

  return buffer;
}

static bool isWildcard(Rune character) {
  return character == '*' || character == '?';
}

// Move the strings pointers to the point where they start to differ.
template <typename CHAR, typename NEXT>
static void eatSameChars(
    const CHAR** pattern,
    const CHAR* patternEnd,
    const CHAR** string,
    const CHAR* stringEnd,
    NEXT next) {
  const CHAR* escape = nullptr;
  while (*pattern != patternEnd && *string != stringEnd) {
    if (!escape && isWildcard(**pattern)) {
      // We don't want to match wildcard here, except if it's escaped.
      return;
    }

    // Check if the escapement char is found. If so, skip it and move to the
    // next character.
    if (!escape && **pattern == '\\') {
      escape = *pattern;
      next(pattern, patternEnd);
      continue;
    }

    // Check if the chars match, if so, increment the ptrs.
    const CHAR* patternNext = *pattern;
    const CHAR* stringNext = *string;
    Rune patternChar = next(&patternNext, patternEnd);
    if (patternChar == next(&stringNext, stringEnd) &&
        patternChar != Runeerror && patternChar <= Runemax) {
      *pattern = patternNext;
      *string = stringNext;
    } else {
      // Uh ho, it did not match, we are done. If the last char was an
      // escapement, that means that it was an error to advance the ptr here,
      // let's put it back where it was. This also mean that the matchPattern
      // function will return false because if we can't match an escape char
      // here, then no one will.
      if (escape) {
        *pattern = escape;
      }
      return;
    }

    escape = nullptr;
  }
}

template <typename CHAR, typename NEXT>
static void eatWildcard(const CHAR** pattern, const CHAR* end, NEXT next) {
  while (*pattern != end) {
    if (!isWildcard(**pattern))
      return;
    next(pattern, end);
  }
}

template <typename CHAR, typename NEXT>
static bool matchPatternT(
    const CHAR* eval,
    const CHAR* evalEnd,
    const CHAR* pattern,
    const CHAR* patternEnd,
    int depth,
    NEXT next) {
  const int kMaxDepth = 16;
  if (depth > kMaxDepth)
    return false;

  // Eat all the matching chars.
  eatSameChars(&pattern, patternEnd, &eval, evalEnd, next);

  // If the string is empty, then the pattern must be empty too, or contains
  // only wildcards.
  if (eval == evalEnd) {
    eatWildcard(&pattern, patternEnd, next);
    return pattern == patternEnd;
  }

  // Pattern is empty but not string, this is not a match.
  if (pattern == patternEnd)
    return false;

  // If this is a question mark, then we need to compare the rest with
  // the current string or the string with one character eaten.
  const CHAR* nextPattern = pattern;
  next(&nextPattern, patternEnd);
  if (pattern[0] == '?') {
    if (matchPatternT(eval, evalEnd, nextPattern, patternEnd, depth + 1, next))
      return true;
    const CHAR* nextEval = eval;
    next(&nextEval, evalEnd);
    if (matchPatternT(
            nextEval, evalEnd, nextPattern, patternEnd, depth + 1, next))
      return true;
  }

  // This is a *, try to match all the possible substrings with the remainder
  // of the pattern.
  if (pattern[0] == '*') {
    // Collapse duplicate wild cards (********** into *) so that the
    // method does not recurse unnecessarily. http://crbug.com/52839
    eatWildcard(&nextPattern, patternEnd, next);

    while (eval != evalEnd) {
      if (matchPatternT(
              eval, evalEnd, nextPattern, patternEnd, depth + 1, next))
        return true;
      eval++;
    }

    // We reached the end of the string, let see if the pattern contains only
    // wildcards.
    if (eval == evalEnd) {
      eatWildcard(&pattern, patternEnd, next);
      if (pattern != patternEnd)
        return false;
      return true;
    }
  }

  return false;
}

struct NextCharUtf8 {
  Rune operator()(const char** p, const char* end) {
    Rune c;
    int offset = charntorune(&c, *p, static_cast<int>(end - *p));
    *p += offset;
    return c;
  }
};

bool matchPattern(const StringPiece& eval, const StringPiece& pattern) {
  return matchPatternT(
      eval.data(),
      eval.data() + eval.size(),
      pattern.data(),
      pattern.data() + pattern.size(),
      0,
      NextCharUtf8());
}

string prefixSuccessor(const StringPiece& prefix) {
  // We can increment the last character in the string and be done
  // unless that character is 255, in which case we have to erase the
  // last character and increment the previous character, unless that
  // is 255, etc. If the string is empty or consists entirely of
  // 255's, we just return the empty string.
  bool done = false;
  string limit(prefix.data(), prefix.size());
  int index = limit.length() - 1;
  while (!done && index >= 0) {
    if (static_cast<unsigned char>(limit[index]) == 255) {
      limit.erase(index);
      index--;
    } else {
      limit[index]++;
      done = true;
    }
  }
  if (!done) {
    return "";
  } else {
    return limit;
  }
}

void findShortestSeparator(
    const StringPiece& start,
    const StringPiece& limit,
    string* separator) {
  // Find length of common prefix
  size_t minLength = min(start.size(), limit.size());
  size_t diffIndex = 0;
  while ((diffIndex < minLength) && (start[diffIndex] == limit[diffIndex])) {
    diffIndex++;
  }

  if (diffIndex >= minLength) {
    // Handle the case where either string is a prefix of the other
    // string, or both strings are identical.
    start.copyToString(separator);
    return;
  }

  if (diffIndex + 1 == start.size()) {
    // If the first difference is in the last character, do not bother
    // incrementing that character since the separator will be no
    // shorter than "start".
    start.copyToString(separator);
    return;
  }

  if (static_cast<unsigned char>(start[diffIndex]) == 0xff) {
    // Avoid overflow when incrementing start[diffIndex]
    start.copyToString(separator);
    return;
  }

  separator->assign(start.data(), diffIndex);
  separator->push_back(start[diffIndex] + 1);
  if (*separator >= limit) {
    // Never pick a separator that causes confusion with "limit"
    start.copyToString(separator);
  }
}

namespace {

template <typename CHAR>
size_t lcpyT(CHAR* dst, const CHAR* src, size_t dstSize) {
  for (size_t i = 0; i < dstSize; ++i) {
    if ((dst[i] = src[i]) == 0) // We hit and copied the terminating NULL.
      return i;
  }

  // We were left off at dstSize.  We over copied 1 byte.  Null terminate.
  if (dstSize != 0)
    dst[dstSize - 1] = 0;

  // Count the rest of the |src|, and return it's length in characters.
  while (src[dstSize])
    ++dstSize;
  return dstSize;
}

} // namespace

size_t strings::strlcpy(char* dst, const char* src, size_t dstSize) {
  return lcpyT<char>(dst, src, dstSize);
}
