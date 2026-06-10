//
// Copyright (C) 1999-2005 Google, Inc.
//

// TODO(user): visit each const_cast.  Some of them are no longer necessary
// because last Single Unix Spec and grte v2 are more const-y.

#include "kudu/gutil/strings/util.h"

#include <algorithm>
#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/stl_util.h" // for string_as_array, stlAppendToString
#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/utf/utf.h"

using std::min;
using std::string;
using std::swap;
using std::vector;

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

const char* strnprefix(
    const char* haystack,
    int haystackSize,
    const char* needle,
    int needleSize) {
  if (needleSize > haystackSize) {
    return nullptr;
  } else {
    if (strncmp(haystack, needle, needleSize) == 0) {
      return haystack + needleSize;
    } else {
      return nullptr;
    }
  }
}

const char* strncaseprefix(
    const char* haystack,
    int haystackSize,
    const char* needle,
    int needleSize) {
  if (needleSize > haystackSize) {
    return nullptr;
  } else {
    if (strncasecmp(haystack, needle, needleSize) == 0) {
      return haystack + needleSize;
    } else {
      return nullptr;
    }
  }
}

char* strcasesuffix(char* str, const char* suffix) {
  const int lenStr = strlen(str);
  const int lenSuffix = strlen(suffix);
  char* strBeginningOfTheEnd = str + lenStr - lenSuffix;

  if (lenStr >= lenSuffix && 0 == strcasecmp(strBeginningOfTheEnd, suffix)) {
    return (strBeginningOfTheEnd);
  } else {
    return (nullptr);
  }
}

const char* strnsuffix(
    const char* haystack,
    int haystackSize,
    const char* needle,
    int needleSize) {
  if (needleSize > haystackSize) {
    return nullptr;
  } else {
    const char* start = haystack + haystackSize - needleSize;
    if (strncmp(start, needle, needleSize) == 0) {
      return start;
    } else {
      return nullptr;
    }
  }
}

const char* strncasesuffix(
    const char* haystack,
    int haystackSize,
    const char* needle,
    int needleSize) {
  if (needleSize > haystackSize) {
    return nullptr;
  } else {
    const char* start = haystack + haystackSize - needleSize;
    if (strncasecmp(start, needle, needleSize) == 0) {
      return start;
    } else {
      return nullptr;
    }
  }
}

char* strchrnth(const char* str, const char& c, int n) {
  if (str == nullptr)
    return nullptr;
  if (n <= 0)
    return const_cast<char*>(str);
  const char* sp;
  int k = 0;
  for (sp = str; *sp != '\0'; sp++) {
    if (*sp == c) {
      ++k;
      if (k >= n)
        break;
    }
  }
  return (k < n) ? nullptr : const_cast<char*>(sp);
}

char* adjustedLastPos(const char* str, char separator, int n) {
  if (str == nullptr)
    return nullptr;
  const char* pos = nullptr;
  if (n > 0)
    pos = strchrnth(str, separator, n);

  // if n <= 0 or separator appears fewer than n times, get the last occurrence
  if (pos == nullptr)
    pos = strrchr(str, separator);
  return const_cast<char*>(pos);
}

// ----------------------------------------------------------------------
// Misc. routines
// ----------------------------------------------------------------------

bool isAscii(const char* str, int len) {
  const char* end = str + len;
  while (str < end) {
    if (!asciiIsAscii(*str++)) {
      return false;
    }
  }
  return true;
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
// globalReplaceSubstring()
//    Replaces all instances of a substring in a string.  Does nothing
//    if 'substring' is empty.  Returns the number of replacements.
//
//    NOTE: The string pieces must not overlap s.
// ----------------------------------------------------------------------

int globalReplaceSubstring(
    const StringPiece& substring,
    const StringPiece& replacement,
    string* s) {
  CHECK(s != nullptr);
  if (s->empty() || substring.empty())
    return 0;
  string tmp;
  int numReplacements = 0;
  size_t pos = 0;
  for (size_t matchPos = s->find(substring.data(), pos, substring.length());
       matchPos != string::npos;
       pos = matchPos + substring.length(),
              matchPos = s->find(substring.data(), pos, substring.length())) {
    ++numReplacements;
    // Append the original content before the match.
    tmp.append(*s, pos, matchPos - pos);
    // Append the replacement for the match.
    tmp.append(replacement.begin(), replacement.end());
  }
  // Append the content after the last match. If no replacements were made, the
  // original string is left untouched.
  if (numReplacements > 0) {
    tmp.append(*s, pos, s->length() - pos);
    s->swap(tmp);
  }
  return numReplacements;
}

//---------------------------------------------------------------------------
// removeStrings()
//   Remove the strings from v given by the (sorted least -> greatest)
//   numbers in indices.
//   Order of v is *not* preserved.
//---------------------------------------------------------------------------
void removeStrings(vector<string>* v, const vector<int>& indices) {
  assert(v);
  assert(indices.size() <= v->size());
  // go from largest index to smallest so that smaller indices aren't
  // invalidated
  for (int lcv = indices.size() - 1; lcv >= 0; --lcv) {
#ifndef NDEBUG
    // verify that indices is sorted least->greatest
    if (indices.size() >= 2 && lcv > 0)
      // use LT and not LE because we should never see repeat indices
      CHECK_LT(indices[lcv - 1], indices[lcv]);
#endif
    assert(indices[lcv] >= 0);
    assert(indices[lcv] < v->size());
    swap((*v)[indices[lcv]], v->back());
    v->pop_back();
  }
}

// ----------------------------------------------------------------------
// gstrcasestr is a case-insensitive strstr. Eventually we should just
// use the GNU libc version of strcasestr, but it isn't compiled into
// RedHat Linux by default in version 6.1.
//
// This function uses asciiToLower() instead of tolower(), for speed.
// ----------------------------------------------------------------------

char* gstrcasestr(const char* haystack, const char* needle) {
  char c, sc;
  size_t len;

  if ((c = *needle++) != 0) {
    c = asciiToLower(c);
    len = strlen(needle);
    do {
      do {
        if ((sc = *haystack++) == 0)
          return nullptr;
      } while (asciiToLower(sc) != c);
    } while (strncasecmp(haystack, needle, len) != 0);
    haystack--;
  }
  // This is a const violation but strstr() also returns a char*.
  return const_cast<char*>(haystack);
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

// ----------------------------------------------------------------------
// gstrncasestr is a case-insensitive strnstr.
//    Finds the occurence of the (null-terminated) needle in the
//    haystack, where no more than len bytes of haystack is searched.
//    Characters that appear after a '\0' in the haystack are not searched.
//
//    This function uses asciiToLower() instead of tolower(), for speed.
// ----------------------------------------------------------------------
char* gstrncasestr(char* haystack, const char* needle, size_t len) {
  return const_cast<char*>(
      gstrncasestr(static_cast<const char*>(haystack), needle, len));
}
// ----------------------------------------------------------------------
// gstrncasestrSplit performs a case insensitive search
// on (prefix, nonAlpha, suffix).
// ----------------------------------------------------------------------
char* gstrncasestrSplit(
    const char* str,
    const char* prefix,
    char nonAlpha,
    const char* suffix,
    size_t n) {
  int preLen = prefix == nullptr ? 0 : strlen(prefix);
  int sufLen = suffix == nullptr ? 0 : strlen(suffix);

  // adjust the string and its length to avoid unnessary searching.
  // an added benefit is to avoid unnecessary range checks in the if
  // statement in the inner loop.
  if (sufLen + preLen >= n)
    return nullptr;
  str += preLen;
  n -= preLen;
  n -= sufLen;

  const char* where = nullptr;

  // for every occurance of nonAlpha in the string ...
  while ((where = static_cast<const char*>(memchr(str, nonAlpha, n))) !=
         nullptr) {
    // ... test whether it is followed by suffix and preceded by prefix
    if ((!sufLen || strncasecmp(where + 1, suffix, sufLen) == 0) &&
        (!preLen || strncasecmp(where - preLen, prefix, preLen) == 0)) {
      return const_cast<char*>(where - preLen);
    }
    // if not, advance the pointer, and adjust the length according
    n -= (where + 1) - str;
    str = where + 1;
  }

  return nullptr;
}

// ----------------------------------------------------------------------
// strcasestrAlnum is like a case-insensitive strstr, except that it
// ignores non-alphanumeric characters in both strings for the sake of
// comparison.
//
// This function uses asciiIsAlnum() instead of isalnum() and
// asciiToLower() instead of tolower(), for speed.
//
// E.g. strcasestrAlnum("i use google all the time", " !!Google!! ")
// returns pointer to "google all the time"
// ----------------------------------------------------------------------
char* strcasestrAlnum(const char* haystack, const char* needle) {
  const char* haystackPtr;
  const char* needlePtr;

  // Skip non-alnums at beginning
  while (!asciiIsAlnum(*needle))
    if (*needle++ == '\0')
      return const_cast<char*>(haystack);
  needlePtr = needle;

  // Skip non-alnums at beginning
  while (!asciiIsAlnum(*haystack))
    if (*haystack++ == '\0')
      return nullptr;
  haystackPtr = haystack;

  while (*needlePtr != '\0') {
    // Non-alnums - advance
    while (!asciiIsAlnum(*needlePtr))
      if (*needlePtr++ == '\0')
        return const_cast<char*>(haystack);

    while (!asciiIsAlnum(*haystackPtr))
      if (*haystackPtr++ == '\0')
        return nullptr;

    if (asciiToLower(*needlePtr) == asciiToLower(*haystackPtr)) {
      // Case-insensitive match - advance
      needlePtr++;
      haystackPtr++;
    } else {
      // No match - rollback to next start point in haystack
      haystack++;
      while (!asciiIsAlnum(*haystack))
        if (*haystack++ == '\0')
          return nullptr;
      haystackPtr = haystack;
      needlePtr = needle;
    }
  }
  return const_cast<char*>(haystack);
}

// ----------------------------------------------------------------------
// countSubstring()
//    Return the number times a "substring" appears in the "text"
//    NOTE: this function's complexity is O(|text| * |substring|)
//          It is meant for short "text" (such as to ensure the
//          printf format string has the right number of arguments).
//          DO NOT pass in long "text".
// ----------------------------------------------------------------------
int countSubstring(StringPiece text, StringPiece substring) {
  CHECK_GT(substring.length(), 0);

  int count = 0;
  StringPiece::size_type curr = 0;
  while (StringPiece::kNpos != (curr = text.find(substring, curr))) {
    ++count;
    ++curr;
  }
  return count;
}

// ----------------------------------------------------------------------
// strstrDelimited()
//    Just like strstr(), except it ensures that the needle appears as
//    a complete item (or consecutive series of items) in a delimited
//    list.
//
//    Like strstr(), returns haystack if needle is empty, or NULL if
//    either needle/haystack is NULL.
// ----------------------------------------------------------------------
const char*
strstrDelimited(const char* haystack, const char* needle, char delim) {
  if (!needle || !haystack)
    return nullptr;
  if (*needle == '\0')
    return haystack;

  int needleLen = strlen(needle);

  while (true) {
    // Skip any leading delimiters.
    while (*haystack == delim)
      ++haystack;

    // Walk down the haystack, matching every character in the needle.
    const char* thisMatch = haystack;
    int i = 0;
    for (; i < needleLen; i++) {
      if (*haystack != needle[i]) {
        // We ran out of haystack or found a non-matching character.
        break;
      }
      ++haystack;
    }

    // If we matched the whole needle, ensure that it's properly delimited.
    if (i == needleLen && (*haystack == '\0' || *haystack == delim)) {
      return thisMatch;
    }

    // No match. Consume non-delimiter characters until we run out of them.
    while (*haystack != delim) {
      if (*haystack == '\0')
        return nullptr;
      ++haystack;
    }
  }
  LOG(FATAL) << "Unreachable statement";
}

// ----------------------------------------------------------------------
// Older versions of libc have a buggy strsep.
// ----------------------------------------------------------------------

char* gstrsep(char** stringp, const char* delim) {
  char* s;
  const char* spanp;
  int c, sc;
  char* tok;

  if ((s = *stringp) == nullptr)
    return nullptr;

  tok = s;
  while (true) {
    c = *s++;
    spanp = delim;
    do {
      if ((sc = *spanp++) == c) {
        if (c == 0)
          s = nullptr;
        else
          s[-1] = 0;
        *stringp = s;
        return tok;
      }
    } while (sc != 0);
  }

  return nullptr; /* should not happen */
}

void fastStringAppend(string* s, const char* data, int len) {
  stlAppendToString(s, data, len);
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

// ----------------------------------------------------------------------
// strdupWithNew()
// strndupWithNew()
//
//    strdupWithNew() is the same as strdup() except that the memory
//    is allocated by new[] and hence an exception will be generated
//    if out of memory.
//
//    strndupWithNew() is the same as strdupWithNew() except that it will
//    copy up to the specified number of characters.  This function
//    is useful when we want to copy a substring out of a string
//    and didn't want to (or cannot) modify the string
// ----------------------------------------------------------------------
char* strdupWithNew(const char* theString) {
  if (theString == nullptr)
    return nullptr;
  else
    return strndupWithNew(theString, strlen(theString));
}

char* strndupWithNew(const char* theString, int maxLength) {
  if (theString == nullptr)
    return nullptr;

  auto result = new char[maxLength + 1];
  result[maxLength] = '\0'; // terminate the string because strncpy might not
  return strncpy(result, theString, maxLength);
}

// ----------------------------------------------------------------------
// scanForFirstWord()
//    This function finds the first word in the string "theString" given.
//    A word is defined by consecutive !asciiIsSpace() characters.
//    If no valid words are found,
//        return NULL and *endPtr will contain junk
//    else
//        return the beginning of the first word and
//        *endPtr will store the address of the first invalid character
//        (asciiIsSpace() or '\0').
//
//    Precondition: (endPtr != NULL)
// ----------------------------------------------------------------------
const char* scanForFirstWord(const char* theString, const char** endPtr) {
  CHECK(endPtr != nullptr) << ": precondition violated";

  if (theString == nullptr) // empty string
    return nullptr;

  const char* curr = theString;
  while ((*curr != '\0') && asciiIsSpace(*curr)) // skip initial spaces
    ++curr;

  if (*curr == '\0') // no valid word found
    return nullptr;

  // else has a valid word
  const char* firstWord = curr;

  // now locate the end of the word
  while ((*curr != '\0') && !asciiIsSpace(*curr))
    ++curr;

  *endPtr = curr;
  return firstWord;
}

// ----------------------------------------------------------------------
// advanceIdentifier()
//    This function returns a pointer past the end of the longest C-style
//    identifier that is a prefix of str or NULL if str does not start with
//    one.  A C-style identifier begins with an ASCII letter or underscore
//    and continues with ASCII letters, digits, or underscores.
// ----------------------------------------------------------------------
const char* advanceIdentifier(const char* str) {
  // Not using isalpha and isalnum so as not to rely on the locale.
  // We could have used asciiIsAlpha and asciiIsAlnum.
  char ch = *str++;
  if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'))
    return nullptr;
  while (true) {
    ch = *str;
    if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
          (ch >= '0' && ch <= '9') || ch == '_'))
      return str;
    str++;
  }
}

// ----------------------------------------------------------------------
// isIdentifier()
//    This function returns true if str is a C-style identifier.
//    A C-style identifier begins with an ASCII letter or underscore
//    and continues with ASCII letters, digits, or underscores.
// ----------------------------------------------------------------------
bool isIdentifier(const char* str) {
  const char* end = advanceIdentifier(str);
  return end && *end == '\0';
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

// ----------------------------------------------------------------------
// findTagValuePair
//    Given a string of the form
//    <something><attrSep><tag><tagValueSep><value><attrSep>...<stringTerm>
//    where the part before the first attrSep is optional,
//    this function extracts the first tag and value, if any.
//    The function returns true if successful, in which case "tag" and "value"
//    are set to point to the beginning of the tag and the value, respectively,
//    and "tagLen" and "valueLen" are set to the respective lengths.
// ----------------------------------------------------------------------

bool findTagValuePair(
    const char* argStr,
    char tagValueSeparator,
    char attributeSeparator,
    char stringTerminal,
    char** tag,
    int* tagLen,
    char** value,
    int* valueLen) {
  char* inStr = const_cast<char*>(argStr); // For msvc8.
  if (inStr == nullptr)
    return false;
  char tvSepOrTerm[3] = {tagValueSeparator, stringTerminal, '\0'};
  char attrSepOrTerm[3] = {attributeSeparator, stringTerminal, '\0'};

  // Look for beginning of tag
  *tag = strpbrk(inStr, attrSepOrTerm);
  // If stringTerminal is '\0', strpbrk won't find it but return null.
  if (*tag == nullptr || **tag == stringTerminal)
    *tag = inStr;
  else
    (*tag)++; // Move past separator
  // Now look for value...
  char* tvSepPos = strpbrk(*tag, tvSepOrTerm);
  if (tvSepPos == nullptr || *tvSepPos == stringTerminal)
    return false;
  // ...and end of value
  char* attrSepPos = strpbrk(tvSepPos, attrSepOrTerm);

  *tagLen = tvSepPos - *tag;
  *value = tvSepPos + 1;
  if (attrSepPos != nullptr)
    *valueLen = attrSepPos - *value;
  else
    *valueLen = strlen(*value);
  return true;
}

void uniformInsertString(string* s, int interval, const char* separator) {
  const size_t separatorLen = strlen(separator);

  if (interval < 1 || // invalid interval
      s->empty() || // nothing to do
      separatorLen == 0) // invalid separator
    return;

  int numInserts = (s->size() - 1) / interval; // -1 to avoid appending at end
  if (numInserts == 0) // nothing to do
    return;

  string tmp;
  tmp.reserve(s->size() + numInserts * separatorLen + 1);

  for (int i = 0; i < numInserts; ++i) {
    // append this interval
    tmp.append(*s, i * interval, interval);
    // append a separator
    tmp.append(separator, separatorLen);
  }

  // append the tail
  const size_t tailPos = numInserts * interval;
  tmp.append(*s, tailPos, s->size() - tailPos);

  s->swap(tmp);
}

void insertString(
    string* s,
    const vector<uint32_t>& indices,
    char const* separator) {
  const unsigned numIndices(indices.size());
  if (numIndices == 0) {
    return; // nothing to do...
  }

  const unsigned separatorLen(strlen(separator));
  if (separatorLen == 0) {
    return; // still nothing to do...
  }

  string tmp;
  const unsigned sLen(s->size());
  tmp.reserve(sLen + separatorLen * numIndices);

  vector<uint32_t>::const_iterator const indEnd(indices.end());
  auto indPos(indices.begin());

  uint32_t lastPos(0);
  while (indPos != indEnd) {
    const uint32_t pos(*indPos);
    DCHECK_GE(pos, lastPos);
    DCHECK_LE(pos, sLen);

    tmp.append(s->substr(lastPos, pos - lastPos));
    tmp.append(separator);

    lastPos = pos;
    ++indPos;
  }
  tmp.append(s->substr(lastPos));

  s->swap(tmp);
}

//------------------------------------------------------------------------
// findNth()
//  return index of nth occurrence of c in the string,
//  or string::npos if n > number of occurrences of c.
//  (returns string::npos = -1 if n <= 0)
//------------------------------------------------------------------------
int findNth(StringPiece s, char c, int n) {
  size_t pos = string::npos;

  for (int i = 0; i < n; ++i) {
    pos = s.findFirstOf(c, pos + 1);
    if (pos == StringPiece::kNpos) {
      break;
    }
  }
  return pos;
}

//------------------------------------------------------------------------
// reverseFindNth()
//  return index of nth-to-last occurrence of c in the string,
//  or string::npos if n > number of occurrences of c.
//  (returns string::npos if n <= 0)
//------------------------------------------------------------------------
int reverseFindNth(StringPiece s, char c, int n) {
  if (n <= 0) {
    return static_cast<int>(StringPiece::kNpos);
  }

  size_t pos = s.size();

  for (int i = 0; i < n; ++i) {
    // If pos == 0, we return StringPiece::kNpos right away. Otherwise,
    // the following findLastOf call would take (pos - 1) as string::npos,
    // which means it would again search the entire input string.
    if (pos == 0) {
      return static_cast<int>(StringPiece::kNpos);
    }
    pos = s.findLastOf(c, pos - 1);
    if (pos == string::npos) {
      break;
    }
  }
  return pos;
}

namespace strings {

// findEol()
// Returns the location of the next end-of-line sequence.

StringPiece findEol(StringPiece s) {
  for (size_t i = 0; i < s.length(); ++i) {
    if (s[i] == '\n') {
      return StringPiece(s.data() + i, 1);
    }
    if (s[i] == '\r') {
      if (i + 1 < s.length() && s[i + 1] == '\n') {
        return StringPiece(s.data() + i, 2);
      } else {
        return StringPiece(s.data() + i, 1);
      }
    }
  }
  return StringPiece(s.data() + s.length(), 0);
}

} // namespace strings

//------------------------------------------------------------------------
// onlyWhitespace()
//  return true if string s contains only whitespace characters
//------------------------------------------------------------------------
bool onlyWhitespace(const StringPiece& s) {
  for (const auto& c : s) {
    if (!asciiIsSpace(c))
      return false;
  }
  return true;
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

string immediateSuccessor(const StringPiece& s) {
  // Return the input string, with an additional NUL byte appended.
  string out;
  out.reserve(s.size() + 1);
  out.append(s.data(), s.size());
  out.push_back('\0');
  return out;
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

int safeSnprintf(char* str, size_t size, const char* format, ...) {
  va_list printargs;
  va_start(printargs, format);
  int ncw = vsnprintf(str, size, format, printargs);
  va_end(printargs);
  return (ncw < size && ncw >= 0) ? ncw : 0;
}

bool getlineFromStdioFile(FILE* file, string* str, char delim) {
  str->erase();
  while (true) {
    if (feof(file) || ferror(file)) {
      return false;
    }
    int c = getc(file);
    if (c == EOF)
      return false;
    if (c == delim)
      return true;
    str->push_back(c);
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
