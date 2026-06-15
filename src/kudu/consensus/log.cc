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
    FsManager* fsManager,
    const std::string& tabletId,
    const std::shared_ptr<MetricEntity>& metricEntity,
    std::shared_ptr<Log>* log) {
  string tabletWalPath = fsManager->getTabletWalDir(tabletId);
  RETURN_NOT_OK(env_util::createDirIfMissing(fsManager->env(), tabletWalPath));

  std::shared_ptr<Log> newLog;
  if (options.logFactory) {
    RETURN_NOT_OK(options.logFactory->createLog(
        options, fsManager, tabletWalPath, tabletId, metricEntity, &newLog));
  } else {
    return Status::NotSupported("No log factory provided");
  }
  RETURN_NOT_OK(newLog->Init());
  log->swap(newLog);
  return Status::OK();
}

Log::Log(
    LogOptions options,
    FsManager* fsManager,
    string logPath,
    string tabletId,
    std::shared_ptr<MetricEntity> metricEntity)
    : options_(std::move(options)),
      fs_manager_(fsManager),
      log_dir_(std::move(logPath)),
      tablet_id_(std::move(tabletId)),
      log_state_(kLogInitialized),
      metric_entity_(std::move(metricEntity)),
      bootstrap_(std::make_shared<consensus::ConsensusBootstrapInfo>()) {
  if (metric_entity_) {
    metrics_.reset(new LogMetrics(metric_entity_));
  }
}

Status Log::asyncAppendReplicates(
    const vector<consensus::ReplicateMsgWrapper>& wrappers,
    const StatusCallback& callback) {
  vector<consensus::ReplicateRefPtr> uncompressedMsgs;
  uncompressedMsgs.reserve(wrappers.size());

  for (const auto& wrapper : wrappers) {
    uncompressedMsgs.push_back(wrapper.getUncompressedMsg());
  }
  // By default we write uncompressed msgs to disk but a derived class can
  // choose to write compressed msgs instead
  return asyncAppendReplicates(uncompressedMsgs, callback);
}

FsManager* Log::GetFsManager() {
  return fs_manager_;
}

std::string Log::logPrefix() const {
  return fmt::format("T {} P {}: ", tablet_id_, fs_manager_->uuid());
}

Log::~Log() {
  // Close() of log is now called from simple_tablet_manager
}

} // namespace kudu::log
