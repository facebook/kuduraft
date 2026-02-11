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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#ifndef KUDU_CONSENSUS_LOG_TEST_UTIL_H
#define KUDU_CONSENSUS_LOG_TEST_UTIL_H

#include "kudu/consensus/log.h"

#include <glog/logging.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace log {

// Append a single batch of 'count' NoOps to the log.
// If 'size' is not NULL, increments it by the expected increase in log size.
// Increments 'opId''s index once for each operation logged.
inline Status AppendNoOpsToLogSync(
    const std::shared_ptr<clock::Clock>& clock,
    Log* log,
    consensus::OpId* opId,
    int count,
    int* size = nullptr) {
  std::vector<consensus::ReplicateRefPtr> replicates;
  for (int i = 0; i < count; i++) {
    consensus::ReplicateRefPtr replicate = makeScopedRefptrReplicate(
        new consensus::ReplicateMsg(), consensus::Source::Memory);
    consensus::ReplicateMsg* repl = replicate->get();

    repl->mutable_id()->CopyFrom(*opId);
    repl->set_op_type(consensus::NO_OP);
    repl->set_timestamp(clock->Now().ToUint64());

    // Increment opId.
    opId->set_index(opId->index() + 1);

    if (size) {
      // If we're tracking the sizes we need to account for the fact that the
      // Log wraps the log entry in an LogEntryBatchPB, and each actual entry
      // will have a one-byte tag.
      *size += repl->ByteSize() + 1;
    }
    replicates.push_back(replicate);
  }

  // Account for the entry batch header and wrapper PB.
  if (size) {
    *size += log::kEntryHeaderSizeV2 + 5;
  }

  Synchronizer s;
  RETURN_NOT_OK(log->asyncAppendReplicates(replicates, s.AsStatusCallback()));
  return s.Wait();
}

inline Status AppendNoOpToLogSync(
    const std::shared_ptr<clock::Clock>& clock,
    Log* log,
    consensus::OpId* opId,
    int* size = nullptr) {
  return AppendNoOpsToLogSync(clock, log, opId, 1, size);
}

// Corrupts the last segment of the provided log by either truncating it
// or modifying a byte at the given offset.
enum CorruptionType { kTruncateFile, kFlipByte };

inline Status CorruptLogFile(
    Env* env,
    const std::string& logPath,
    CorruptionType type,
    int corruptionOffset) {
  faststring buf;
  RETURN_NOT_OK_PREPEND(
      ReadFileToString(env, logPath, &buf), "Couldn't read log");

  switch (type) {
    case kTruncateFile:
      buf.resize(corruptionOffset);
      break;
    case kFlipByte:
      CHECK_LT(corruptionOffset, buf.size());
      buf[corruptionOffset] ^= 0xff;
      break;
  }

  // Rewrite the file with the corrupt log.
  RETURN_NOT_OK_PREPEND(
      WriteStringToFile(env, Slice(buf), logPath),
      "Couldn't rewrite corrupt log file");

  return Status::OK();
}

} // namespace log
} // namespace kudu

#endif // KUDU_CONSENSUS_LOG_TEST_UTIL_H
