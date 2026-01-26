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

#ifndef KUDU_LOGGING_TEST_UTIL_H
#define KUDU_LOGGING_TEST_UTIL_H

#include <glog/logging.h>
#include <string>
#include <vector>

namespace kudu {

// GLog sink that keeps an internal buffer of messages that have been logged.
class StringVectorSink : public google::LogSink {
 public:
  void send(
      google::LogSeverity severity,
      const char* fullFilename,
      const char* baseFilename,
      int line,
      const struct ::tm* tmTime,
      const char* message,
      size_t messageLen) override {
    loggedMsgs_.push_back(
        ToString(severity, baseFilename, line, tmTime, message, messageLen));
  }

  std::vector<std::string>& loggedMsgs() {
    return loggedMsgs_;
  }

 private:
  std::vector<std::string> loggedMsgs_;
};

// RAII wrapper around registering a LogSink with GLog.
struct ScopedRegisterSink {
  explicit ScopedRegisterSink(google::LogSink* s) : sink(s) {
    google::AddLogSink(sink);
  }
  ~ScopedRegisterSink() {
    google::RemoveLogSink(sink);
  }

  google::LogSink* sink;
};

} // namespace kudu

#endif
