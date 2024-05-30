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

#include "kudu/kserver/kserver.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(
    server_thread_pool_max_thread_count,
    -1,
    "Maximum number of threads to allow in each server-wide thread "
    "pool. If -1, Kudu will automatically calculate this value. It "
    "is an error to use a value of 0.");
TAG_FLAG(server_thread_pool_max_thread_count, advanced);
TAG_FLAG(server_thread_pool_max_thread_count, evolving);
DEFINE_int32(
    raft_thread_pool_idle_timeout_second,
    3600,
    "Max idle time for a raft worker thread before it dies");
DEFINE_int32(
    raft_thread_pool_max_size,
    0,
    "Max threads in the raft thread pool");
DEFINE_int32(
    raft_thread_pool_min_size,
    0,
    "Min threads in the raft thread pool");

static bool ValidateThreadPoolThreadLimit(
    const char* /*flagname*/,
    int32_t value) {
  if (value == 0 || value < -1) {
    LOG(ERROR) << "Invalid thread pool thread limit: cannot be " << value;
    return false;
  }
  return true;
}
DEFINE_validator(
    server_thread_pool_max_thread_count,
    &ValidateThreadPoolThreadLimit);

using std::string;
using strings::Substitute;

namespace kudu {

using server::ServerBaseOptions;

namespace kserver {

namespace {

int GetThreadPoolThreadLimit(Env* env) {
  // Maximize this process' running thread limit first, if possible.
  static std::once_flag once;
  std::call_once(once, [&]() {
    env->IncreaseResourceLimit(
        Env::ResourceLimitType::RUNNING_THREADS_PER_EUID);
  });

  uint64_t rlimit =
      env->GetResourceLimit(Env::ResourceLimitType::RUNNING_THREADS_PER_EUID);
  // See server_thread_pool_max_thread_count.
  if (FLAGS_server_thread_pool_max_thread_count == -1) {
    // Use both pid_max and threads-max as possible upper bounds.
    faststring buf;
    uint64_t buf_val;
    for (const auto& proc_file :
         {"/proc/sys/kernel/pid_max", "/proc/sys/kernel/threads-max"}) {
      if (ReadFileToString(env, proc_file, &buf).ok() &&
          safe_strtou64(buf.ToString(), &buf_val)) {
        rlimit = std::min(rlimit, buf_val);
      }
    }

    // Callers of this function expect a signed 32-bit integer, so we need to
    // further cap the limit just in case it's too large.
    rlimit = std::min(rlimit, static_cast<uint64_t>(kint32max));

    // Take only 10% of the calculated limit; we don't want to hog the system.
    return static_cast<int32_t>(rlimit) / 10;
  }
  LOG_IF(FATAL, FLAGS_server_thread_pool_max_thread_count > rlimit)
      << Substitute(
             "Configured server-wide thread pool running thread limit "
             "(server_thread_pool_max_thread_count) $0 exceeds euid running "
             "thread limit (ulimit) $1",
             FLAGS_server_thread_pool_max_thread_count,
             rlimit);
  return FLAGS_server_thread_pool_max_thread_count;
}

} // anonymous namespace

KuduServer::KuduServer(
    string name,
    const ServerBaseOptions& options,
    const string& metric_namespace)
    : ServerBase(std::move(name), options, metric_namespace) {}

Status KuduServer::Init() {
  RETURN_NOT_OK(ServerBase::Init());

  // These pools are shared by all replicas hosted by this server, and thus
  // are capped at a portion of the overall per-euid thread resource limit.
  int server_wide_pool_limit = GetThreadPoolThreadLimit(fs_manager_->env());
  RETURN_NOT_OK(
      ThreadPoolBuilder("raft")
          .set_trace_metric_prefix("raft")
          .set_min_threads(FLAGS_raft_thread_pool_min_size)
          .set_max_threads(
              FLAGS_raft_thread_pool_max_size ? FLAGS_raft_thread_pool_max_size
                                              : server_wide_pool_limit)
          .set_idle_timeout(MonoDelta::FromSeconds(
              static_cast<double>(FLAGS_raft_thread_pool_idle_timeout_second)))
          .Build(&raft_pool_));

  return Status::OK();
}

Status KuduServer::Start() {
  RETURN_NOT_OK(ServerBase::Start());
  return Status::OK();
}

void KuduServer::Shutdown() {
  // Shut down the messenger early, waiting for any reactor threads to finish
  // running. This ensures that any ref-counted objects inside closures run by
  // reactor threads will be destroyed before we shut down server-wide thread
  // pools below, which is important because those objects may own tokens
  // belonging to the pools.
  //
  // Note: prior to this call, it is assumed that any incoming RPCs deferred
  // from reactor threads have already been cleaned up.
  if (messenger_) {
    messenger_->Shutdown();
  }

  // The shutdown order here shouldn't matter; shutting down the messenger
  // first ensures that all outstanding RaftConsensus instances are destroyed.
  // Thus, there shouldn't be lingering activity on any of these pools.
  if (raft_pool_) {
    raft_pool_->Shutdown();
  }

  ServerBase::Shutdown();
}

} // namespace kserver
} // namespace kudu
