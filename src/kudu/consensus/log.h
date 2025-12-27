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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include <folly/SharedMutex.h>
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/callback.h" // IWYU pragma: keep
#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

DECLARE_bool(raft_derived_log_mode);

namespace kudu {

class CompressionCodec;
class FsManager;
class MetricEntity;
class ThreadPool;
class WritableFile;
struct WritableFileOptions;

namespace consensus {
class OpId;
class ReplicateMsg;
class ReplicateMsgWrapper;

// After completing bootstrap, some of the results need to be plumbed through
// into the consensus implementation.
struct ConsensusBootstrapInfo {
  ConsensusBootstrapInfo()
      : last_id(MinimumOpId()), last_committed_id(MinimumOpId()) {}

  ~ConsensusBootstrapInfo() = default;

  // The id of the last operation in the log
  OpId last_id;

  // The id of the last committed operation in the log.
  OpId last_committed_id;

  // REPLICATE messages which were in the log with no accompanying
  // COMMIT. These need to be passed along to consensus init in order
  // to potentially commit them.
  //
  // These are owned by the ConsensusBootstrapInfo instance.
  std::vector<ReplicateRefPtr> orphaned_replicates;

 private:
  DISALLOW_COPY_AND_ASSIGN(ConsensusBootstrapInfo);
  ConsensusBootstrapInfo(ConsensusBootstrapInfo&&) = delete;
  ConsensusBootstrapInfo& operator=(ConsensusBootstrapInfo&&) = delete;
};

struct ReadContext {
  const std::string* for_peer_uuid = nullptr;
  const std::string* for_peer_host = nullptr;
  uint32_t for_peer_port = 0;
  bool route_via_proxy = false;
  // Whether to report errors to error manager.
  bool report_errors = true;
  // When we need to read from warm storage, we can block on the stream
  // initialization. Otherwise, we will return Status::Uninitialized while
  // stream is being initialized.
  bool block_for_init = false;
  // Skip reading from cache and directly read from log files
  bool skip_log_cache = false;
  // Whether we allow reading from warm storage
  bool enable_warm_storage_reads = false;
};

} // namespace consensus

namespace log {

struct LogEntryBatchLogicalSize;
struct LogMetrics;
struct RetentionIndexes;
class LogIndex;
class LogReader;

/**
 * Log interface that should be implemented by an user of the replication
 * library.
 */
class Log {
 public:
  // Opens or continues a log and sets 'log' to the newly built Log.
  // After a successful Open() the Log is ready to receive entries.
  static Status Open(
      const LogOptions& options,
      FsManager* fs_manager,
      const std::string& tablet_id,
      const std::shared_ptr<MetricEntity>& metric_entity,
      std::shared_ptr<Log>* log);

  virtual ~Log();

  // Initializes a new one or continues an existing log.
  virtual Status Init() = 0;

  // Append the given set of replicate messages, asynchronously.
  // This requires that the replicates have already been assigned OpIds.
  virtual Status asyncAppendReplicates(
      const std::vector<consensus::ReplicateRefPtr>& replicates,
      const StatusCallback& callback) = 0;

  // Same as above but passes a ReplicateMsgWrapper downstream so that the
  // implementation can decide to write compressed or uncompressed msg to disk
  virtual Status asyncAppendReplicates(
      const std::vector<consensus::ReplicateMsgWrapper>& wrappers,
      const StatusCallback& callback);

  // Syncs all state and closes the log.
  virtual Status Close() = 0;

  // This is used during Start of RaftConsensus.
  // During Raft startup a Consensus Bootstrap object is required to
  // start the instance from where it crashed. In abstracted logs like
  // MySQL, this bootstrap_info object is populated by log abstraction during
  // Log::Init and plumbed via this function to RaftConsensus::Start
  virtual std::shared_ptr<consensus::ConsensusBootstrapInfo> GetRecoveryInfo()
      const {
    return bootstrap_;
  }

  // Clear orphaned replicates from bootstrap info
  virtual void ClearOrphanedReplicates() {
    CHECK(bootstrap_);
    bootstrap_->orphaned_replicates.clear();
  }

  // index_if_truncated - if caller e.g. Log Cache passes in index_if_truncated,
  // the log specialization is expected to return the index of truncation
  virtual Status truncateOpsAfter(
      int64_t index,
      int64_t* index_if_truncated = nullptr) = 0;

  // Returns a reader that is able to read through the previous segments,
  // provided the log is initialized and not yet closed. After being closed,
  // this function will return NULL, but existing reader references will
  // remain live.
  std::shared_ptr<LogReader> reader() const {
    return reader_;
  }

  // Get ID of tablet.
  const std::string& tablet_id() const {
    return tablet_id_;
  }

  // Returns this Log's FsManager.
  FsManager* GetFsManager();

  // Virtual functions to override LogReader, LogCache, LogIndex,
  // ReadableLogSegment etc.
  virtual Status readReplicatesInRange(
      int64_t starting_at,
      int64_t up_to,
      int64_t max_bytes_to_read,
      const consensus::ReadContext& context,
      std::vector<consensus::ReplicateRefPtr>* replicates) const = 0;

  virtual Status lookupOpId(int64_t op_index, consensus::OpId* op_id) const;

 protected:
  friend class LogTest;
  friend class LogTestBase;
  friend class LogFactory;
  FRIEND_TEST(LogTestOptionalCompression, TestMultipleEntriesInABatch);
  FRIEND_TEST(LogTestOptionalCompression, TestReadLogWithReplacedReplicates);
  FRIEND_TEST(LogTest, TestWriteAndReadToAndFromInProgressSegment);

  // Log state.
  enum LogState { kLogInitialized, kLogWriting, kLogClosed };

  Log(LogOptions options,
      FsManager* fs_manager,
      std::string log_path,
      std::string tablet_id,
      std::shared_ptr<MetricEntity> metric_entity);

  std::string LogPrefix() const;

  LogOptions options_;
  FsManager* fs_manager_;
  std::string log_dir_;

  // The ID of the tablet this log is dedicated to.
  std::string tablet_id_;

  std::atomic<LogState> log_state_;

  // A reader for the previous segments that were not yet GC'd.
  //
  // Will be NULL after the log is Closed().
  std::shared_ptr<LogReader> reader_;

  // Index which translates between operation indexes and the position
  // of the operation in the log.
  std::shared_ptr<LogIndex> log_index_;

  std::shared_ptr<MetricEntity> metric_entity_;
  std::unique_ptr<LogMetrics> metrics_;

  std::shared_ptr<kudu::consensus::ConsensusBootstrapInfo> bootstrap_;

  DISALLOW_COPY_AND_ASSIGN(Log);
  Log(Log&&) = delete;
  Log& operator=(Log&&) = delete;
};

// Log Factory which enables the Raft based application
// to create the appropriate log implementation.
// Application e.g. MySQL is expected to specialize
// the LogFactory class to create the derived log implementation
// object.
class LogFactory {
 public:
  LogFactory() = default;
  virtual ~LogFactory() = default;
  LogFactory(const LogFactory&) = delete;
  LogFactory& operator=(const LogFactory&) = delete;
  LogFactory(LogFactory&&) = delete;
  LogFactory& operator=(LogFactory&&) = delete;
  virtual Status createLog(
      LogOptions options,
      FsManager* fs_manager,
      std::string log_path,
      std::string tablet_id,
      std::shared_ptr<MetricEntity> metric_entity,
      std::shared_ptr<Log>* new_log) = 0;
};

// Indicates which log indexes should be retained for different purposes.
//
// When default-constructed, starts with maximum indexes, indicating no
// logs need to be retained for either purposes.
struct RetentionIndexes {
  explicit RetentionIndexes(
      int64_t durability = std::numeric_limits<int64_t>::max(),
      int64_t peers = std::numeric_limits<int64_t>::max(),
      int64_t region_durability = std::numeric_limits<int64_t>::max())
      : for_durability(durability),
        for_peers(peers),
        for_region_durability(region_durability) {}

  // The minimum log entry index which *must* be retained in order to
  // preserve durability and the ability to restart the local node
  // from its WAL.
  int64_t for_durability;

  // The minimum log entry index which *should* be retained in order to
  // catch up other peers hosting this same tablet. These entries may
  // still be GCed in the case that they are from very old log segments
  // or the log has become too large.
  int64_t for_peers;

  // The minimum log entry index which *should* be retained in order to ensure
  // 'region-durability'. Region durability is currently defined as the OpId
  // that is replicated to atleast one additional region (other than the
  // leader's region). Useful only when the raft ring is configured to have
  // nodes in different region
  int64_t for_region_durability;
};

} // namespace log
} // namespace kudu
