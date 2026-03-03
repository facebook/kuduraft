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

#pragma once

#include <thread>

#include <glog/logging.h>
#include <string>

#include <fmt/core.h>
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

namespace kudu {

// Periodically checks the number of open file descriptors belonging to this
// process, crashing if it exceeds some upper bound.
class PeriodicOpenFdChecker {
 public:
  // path_pattern: a glob-style pattern of which paths should be included while
  //               counting file descriptors
  // upper_bound:  the maximum number of file descriptors that should be open
  //               at any point in time
  PeriodicOpenFdChecker(Env* env, std::string pathPattern, int upperBound)
      : env_(env),
        pathPattern_(std::move(pathPattern)),
        initialFdCount_(CountOpenFds(env, pathPattern_)),
        maxFdCount_(upperBound + initialFdCount_),
        running_(1),
        started_(false) {}

  ~PeriodicOpenFdChecker() {
    stop();
  }

  void start() {
    DCHECK(!started_);
    running_.reset(1);
    checkThread_ = std::thread(&PeriodicOpenFdChecker::checkThread, this);
    started_ = true;
  }

  void stop() {
    if (started_) {
      running_.countDown();
      checkThread_.join();
      started_ = false;
    }
  }

 private:
  void checkThread() {
    LOG(INFO) << fmt::format(
        "Periodic open fd checker starting for path pattern {}"
        "(initial: {} max: {})",
        pathPattern_,
        initialFdCount_,
        maxFdCount_);
    do {
      int openFdCount = CountOpenFds(env_, pathPattern_);
      KLOG_EVERY_N_SECS(INFO, 1)
          << fmt::format("Open fd count: {}/{}", openFdCount, maxFdCount_);
      CHECK_LE(openFdCount, maxFdCount_);
    } while (!running_.waitFor(MonoDelta::FromMilliseconds(100)));
  }

  Env* env_;
  const std::string pathPattern_;
  const int initialFdCount_;
  const int maxFdCount_;

  CountDownLatch running_;
  std::thread checkThread_;
  bool started_;
};

} // namespace kudu
