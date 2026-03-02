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

#include "kudu/rpc/rpcz_store.h"

#include <algorithm> // IWYU pragma: keep

#include <cstdint>
#include <mutex> // for unique_lock
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/stringpiece.h"

#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_header.pb.h"

#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"

DEFINE_bool(
    rpc_dump_all_traces,
    false,
    "If true, dump all RPC traces at INFO level");
TAG_FLAG(rpc_dump_all_traces, advanced);
TAG_FLAG(rpc_dump_all_traces, runtime);

DEFINE_int32(
    rpc_duration_too_long_ms,
    1000,
    "Threshold (in milliseconds) above which a RPC is considered too long and its "
    "duration and method name are logged at INFO level. The time measured is between "
    "when a RPC is accepted and when its call handler completes.");
TAG_FLAG(rpc_duration_too_long_ms, advanced);
TAG_FLAG(rpc_duration_too_long_ms, runtime);

using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

void RpczStore::logTrace(InboundCall* call) {
  int durationMs = call->timing().totalDuration().ToMilliseconds();

  if (call->header_.has_timeout_millis() &&
      call->header_.timeout_millis() > 0) {
    double logThreshold = call->header_.timeout_millis() * 0.75f;
    if (durationMs > logThreshold) {
      // TODO: consider pushing this onto another thread since it may be slow.
      // The traces may also be too large to fit in a log message.
      int64_t timeoutMs = call->header_.timeout_millis();
      LOG(WARNING) << call->toString() << " took " << durationMs << " ms "
                   << "("
                   << HumanReadableElapsedTime::toShortString(durationMs * .001)
                   << "). " << "Client timeout " << timeoutMs << " ms " << "("
                   << HumanReadableElapsedTime::toShortString(timeoutMs * .001)
                   << ")";
      string s = call->trace()->DumpToString();
      if (!s.empty()) {
        LOG(WARNING) << "Trace:\n" << s;
      }
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_rpc_dump_all_traces)) {
    LOG(INFO) << call->toString() << " took " << durationMs << "ms. Trace:";
    call->trace()->Dump(&LOG(INFO), true);
  } else if (durationMs > FLAGS_rpc_duration_too_long_ms) {
    LOG(INFO) << call->toString() << " took " << durationMs << "ms. "
              << "Request Metrics: " << call->trace()->MetricsAsJSON();
  }
}

} // namespace rpc
} // namespace kudu
