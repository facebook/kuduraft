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

#include "kudu/util/fault_injection.h"

#include <unistd.h>

#include <mutex>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

namespace kudu {
namespace fault_injection {

namespace {
static std::once_flag gRandomOnce;
Random* gRandom;

void initRandom() {
  LOG(WARNING) << "FAULT INJECTION ENABLED!";
  LOG(WARNING) << "THIS SERVER MAY CRASH!";

  debug::ScopedLeakCheckDisabler d;
  gRandom = new Random(getRandomSeed32());
  KUDU_ANNONTATE_BENIGN_RACE_SIZED(
      gRandom, sizeof(Random), "Racy random numbers are OK");
}

} // anonymous namespace

void doMaybeFault(const char* faultStr, double fraction) {
  std::call_once(gRandomOnce, initRandom);
  if (PREDICT_TRUE(gRandom->NextDoubleFraction() >= fraction)) {
    return;
  }
  LOG(ERROR) << "Injecting fault: " << faultStr << " (process will exit)";
  // _exit will exit the program without running atexit handlers. This more
  // accurately simulates a crash.
  _exit(kExitStatus);
}

void doInjectRandomLatency(double maxLatencyMs) {
  std::call_once(gRandomOnce, initRandom);
  SleepFor(
      MonoDelta::FromMilliseconds(
          gRandom->NextDoubleFraction() * maxLatencyMs));
}

void doInjectFixedLatency(int32_t latencyMs) {
  SleepFor(MonoDelta::FromMilliseconds(latencyMs));
}

bool doMaybeTrue(double fraction) {
  std::call_once(gRandomOnce, initRandom);
  return PREDICT_FALSE(gRandom->NextDoubleFraction() <= fraction);
}

} // namespace fault_injection
} // namespace kudu
