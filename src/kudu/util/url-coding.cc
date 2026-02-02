// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#include "kudu/util/url-coding.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <exception>
#include <iterator>
#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/function.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <glog/logging.h>

using std::string;
using std::vector;
using namespace boost::archive::iterators; // NOLINT(*)

namespace kudu {

// Hive selectively encodes characters. This is the whitelist of
// characters it will encode.
// See common/src/java/org/apache/hadoop/hive/common/FileUtils.java
// in the Hive source code for the source of this list.
static boost::function<bool(char)> kHiveShouldEscape =
    boost::is_any_of("\"#%\\*/:=?\u00FF"); // NOLINT(*)

// It is more convenient to maintain the complement of the set of
// characters to escape when not in Hive-compat mode.
static boost::function<bool(char)> kShouldNotEscape =
    boost::is_any_of("-_.~"); // NOLINT(*)

static inline void
urlEncode(const char* in, int inLen, string* out, bool hiveCompat) {
  (*out).reserve(inLen);
  std::ostringstream ss;
  for (int i = 0; i < inLen; ++i) {
    const char ch = in[i];
    // Escape the character iff a) we are in Hive-compat mode and the
    // character is in the Hive whitelist or b) we are not in
    // Hive-compat mode, and the character is not alphanumeric or one
    // of the four commonly excluded characters.
    if ((hiveCompat && kHiveShouldEscape(ch)) ||
        (!hiveCompat && !(isalnum(ch) || kShouldNotEscape(ch)))) {
      ss << '%' << std::uppercase << std::hex << static_cast<uint32_t>(ch);
    } else {
      ss << ch;
    }
  }

  (*out) = ss.str();
}

void urlEncode(const vector<uint8_t>& in, string* out, bool hiveCompat) {
  if (in.empty()) {
    *out = "";
  } else {
    urlEncode(
        reinterpret_cast<const char*>(&in[0]), in.size(), out, hiveCompat);
  }
}

void urlEncode(const string& in, string* out, bool hiveCompat) {
  urlEncode(in.c_str(), in.size(), out, hiveCompat);
}

string urlEncodeToString(const std::string& in, bool hiveCompat) {
  string ret;
  urlEncode(in, &ret, hiveCompat);
  return ret;
}

// Adapted from
// http://www.boost.org/doc/libs/1_40_0/doc/html/boost_asio/
//   example/http/server3/request_handler.cpp
// See http://www.boost.org/LICENSE_1_0.txt for license for this method.
bool urlDecode(const string& in, string* out, bool hiveCompat) {
  out->clear();
  out->reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    if (in[i] == '%') {
      if (i + 3 <= in.size()) {
        int value = 0;
        std::istringstream is(in.substr(i + 1, 2));
        if (is >> std::hex >> value) {
          (*out) += static_cast<char>(value);
          i += 2;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (!hiveCompat && in[i] == '+') { // Hive does not encode ' ' as
                                              // '+'
      (*out) += ' ';
    } else {
      (*out) += in[i];
    }
  }
  return true;
}

static inline void
base64Encode(const char* in, int inLen, std::ostringstream* out) {
  using base64_encode = base64_from_binary<transform_width<const char*, 6, 8>>;
  // Base64 encodes 8 byte chars as 6 bit values.
  std::ostringstream::pos_type lenBefore = out->tellp();
  copy(
      base64_encode(in),
      base64_encode(in + inLen),
      std::ostream_iterator<char>(*out));
  int bytesWritten = out->tellp() - lenBefore;
  // Pad with = to make it valid base64 encoded string
  int numPad = bytesWritten % 4;
  if (numPad != 0) {
    numPad = 4 - numPad;
    for (int i = 0; i < numPad; ++i) {
      (*out) << "=";
    }
  }
  DCHECK_EQ(out->str().size() % 4, 0);
}

void base64Encode(const vector<uint8_t>& in, string* out) {
  if (in.empty()) {
    *out = "";
  } else {
    std::ostringstream ss;
    base64Encode(in, &ss);
    *out = ss.str();
  }
}

void base64Encode(const vector<uint8_t>& in, std::ostringstream* out) {
  if (!in.empty()) {
    // Boost does not like non-null terminated strings
    string tmp(reinterpret_cast<const char*>(&in[0]), in.size());
    base64Encode(tmp.c_str(), tmp.size(), out);
  }
}

void base64Encode(const string& in, string* out) {
  std::ostringstream ss;
  base64Encode(in.c_str(), in.size(), &ss);
  *out = ss.str();
}

void base64Encode(const string& in, std::ostringstream* out) {
  base64Encode(in.c_str(), in.size(), out);
}

bool base64Decode(const string& in, string* out) {
  using base64_decode =
      transform_width<binary_from_base64<string::const_iterator>, 8, 6>;
  string tmp = in;
  // Replace padding with base64 encoded NULL
  replace(tmp.begin(), tmp.end(), '=', 'A');
  try {
    *out = string(base64_decode(tmp.begin()), base64_decode(tmp.end()));
  } catch (std::exception&) {
    return false;
  }

  // Remove trailing '\0' that were added as padding.  Since \0 is special,
  // the boost functions get confused so do this manually.
  int numPaddedChars = 0;
  for (int i = out->size() - 1; i >= 0; --i) {
    if ((*out)[i] != '\0') {
      break;
    }
    ++numPaddedChars;
  }
  out->resize(out->size() - numPaddedChars);
  return true;
}

void escapeForHtml(const string& in, std::ostringstream* out) {
  DCHECK(out != nullptr);
  for (const char& c : in) {
    switch (c) {
      case '<':
        (*out) << "&lt;";
        break;
      case '>':
        (*out) << "&gt;";
        break;
      case '&':
        (*out) << "&amp;";
        break;
      default:
        (*out) << c;
    }
  }
}

std::string escapeForHtmlToString(const std::string& in) {
  std::ostringstream str;
  escapeForHtml(in, &str);
  return str.str();
}

} // namespace kudu
