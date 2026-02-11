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

#include "kudu/consensus/log.h"

#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>

#include <boost/range/adaptor/reversed.hpp>
#include <fmt/core.h>

#include <folly/ScopeGuard.h>

#include "kudu/consensus/log_metrics.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/replicate_msg_wrapper.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"

namespace kudu::log {

using consensus::OpId;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

Status Log::Open(
    const LogOptions& options,
    FsManager* fs_manager,
    const std::string& tablet_id,
    const std::shared_ptr<MetricEntity>& metric_entity,
    std::shared_ptr<Log>* log) {
  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_id);
  RETURN_NOT_OK(
      env_util::createDirIfMissing(fs_manager->env(), tablet_wal_path));

  std::shared_ptr<Log> new_log;
  if (options.logFactory) {
    RETURN_NOT_OK(options.logFactory->createLog(
        options,
        fs_manager,
        tablet_wal_path,
        tablet_id,
        metric_entity,
        &new_log));
  } else {
    return Status::NotSupported("No log factory provided");
  }
  RETURN_NOT_OK(new_log->Init());
  log->swap(new_log);
  return Status::OK();
}

Log::Log(
    LogOptions options,
    FsManager* fs_manager,
    string log_path,
    string tablet_id,
    std::shared_ptr<MetricEntity> metric_entity)
    : options_(std::move(options)),
      fs_manager_(fs_manager),
      log_dir_(std::move(log_path)),
      tablet_id_(std::move(tablet_id)),
      log_state_(kLogInitialized),
      metric_entity_(std::move(metric_entity)),
      bootstrap_(std::make_shared<consensus::ConsensusBootstrapInfo>()) {
  if (metric_entity_) {
    metrics_.reset(new LogMetrics(metric_entity_));
  }
}

Status Log::asyncAppendReplicates(
    const vector<consensus::ReplicateMsgWrapper>& wrappers,
    const StatusCallback& callback) {
  vector<consensus::ReplicateRefPtr> uncompressed_msgs;
  uncompressed_msgs.reserve(wrappers.size());

  for (const auto& wrapper : wrappers) {
    uncompressed_msgs.push_back(wrapper.GetUncompressedMsg());
  }
  // By default we write uncompressed msgs to disk but a derived class can
  // choose to write compressed msgs instead
  return asyncAppendReplicates(uncompressed_msgs, callback);
}

FsManager* Log::GetFsManager() {
  return fs_manager_;
}

std::string Log::LogPrefix() const {
  return fmt::format("T {} P {}: ", tablet_id_, fs_manager_->uuid());
}

Log::~Log() {
  // Close() of log is now called from simple_tablet_manager
}

} // namespace kudu::log
