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
#include "kudu/consensus/consensus_meta_manager.h"

#include <memory>
#include <mutex>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/routing.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/status.h"

namespace kudu::consensus {

using std::lock_guard;
using std::shared_ptr;
using std::string;

ConsensusMetadataManager::ConsensusMetadataManager(FsManager* fsManager)
    : fsManager_(DCHECK_NOTNULL(fsManager)) {}

Status ConsensusMetadataManager::createCMeta(
    const string& tabletId,
    const RaftConfigPB& config,
    int64_t initialTerm,
    ConsensusMetadataCreateMode createMode,
    std::shared_ptr<ConsensusMetadata>* cmetaOut) {
  std::shared_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(
      ConsensusMetadata::Create(
          fsManager_,
          tabletId,
          fsManager_->uuid(),
          config,
          initialTerm,
          createMode,
          &cmeta),
      fmt::format(
          "Unable to create consensus metadata for tablet {}", tabletId));

  lock_guard<Mutex> l(cmetaLock_);
  auto [it, inserted] = cmetaCache_.insert({tabletId, cmeta});
  if (!inserted) {
    return Status::AlreadyPresent(
        fmt::format(
            "ConsensusMetadata instance for {} already exists", tabletId));
  }
  if (cmetaOut) {
    *cmetaOut = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadataManager::loadCMeta(
    const string& tabletId,
    std::shared_ptr<ConsensusMetadata>* cmetaOut) {
  {
    lock_guard<Mutex> l(cmetaLock_);

    // Try to get the cmeta instance from cache first.
    auto it = cmetaCache_.find(tabletId);
    if (it != cmetaCache_.end()) {
      if (cmetaOut) {
        *cmetaOut = it->second;
      }
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  std::shared_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(
      ConsensusMetadata::Load(fsManager_, tabletId, fsManager_->uuid(), &cmeta),
      fmt::format("Unable to load consensus metadata for tablet {}", tabletId));

  // Cache and return the loaded ConsensusMetadata.
  {
    lock_guard<Mutex> l(cmetaLock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we check the insert succeeded.
    auto result = cmetaCache_.insert({tabletId, cmeta});
    CHECK(result.second) << "ConsensusMetadata already exists for tablet "
                         << tabletId;
  }

  if (cmetaOut) {
    *cmetaOut = std::move(cmeta);
  }
  return Status::OK();
}

Status ConsensusMetadataManager::loadOrCreateCMeta(
    const string& tabletId,
    const RaftConfigPB& config,
    int64_t initialTerm,
    ConsensusMetadataCreateMode createMode,
    std::shared_ptr<ConsensusMetadata>* cmetaOut) {
  Status s = loadCMeta(tabletId, cmetaOut);
  if (s.IsNotFound()) {
    return createCMeta(tabletId, config, initialTerm, createMode, cmetaOut);
  }
  return s;
}

Status ConsensusMetadataManager::deleteCMeta(const string& tabletId) {
  {
    lock_guard<Mutex> l(cmetaLock_);
    cmetaCache_.erase(
        tabletId); // OK to delete an uncached cmeta; ignore the return value.
  }
  RETURN_NOT_OK_PREPEND(
      ConsensusMetadata::DeleteOnDiskData(fsManager_, tabletId),
      fmt::format(
          "Unable to delete consensus metadata for tablet {}", tabletId));
  return Status::OK();
}

Status ConsensusMetadataManager::createDrt(
    const std::string& tabletId,
    RaftConfigPB raftConfig,
    ProxyTopologyPB proxyTopology,
    std::shared_ptr<DurableRoutingTable>* drtOut) {
  shared_ptr<DurableRoutingTable> drt;
  RETURN_NOT_OK_PREPEND(
      DurableRoutingTable::create(
          fsManager_,
          tabletId,
          std::move(raftConfig),
          std::move(proxyTopology),
          &drt),
      fmt::format(
          "Unable to create durable routing table for tablet {}", tabletId));

  lock_guard<Mutex> l(drtLock_);
  auto [it, inserted] = drtCache_.insert({tabletId, drt});
  if (!inserted) {
    return Status::AlreadyPresent(
        fmt::format(
            "DurableRoutingTable instance for {} already exists", tabletId));
  }
  if (drtOut) {
    *drtOut = std::move(drt);
  }
  return Status::OK();
}

// Load DurableRoutingTable.
Status ConsensusMetadataManager::loadDrt(
    const std::string& tabletId,
    RaftConfigPB raftConfig,
    shared_ptr<DurableRoutingTable>* drtOut) {
  {
    lock_guard<Mutex> l(drtLock_);

    // Try to get the cmeta instance from cache first.
    auto it = drtCache_.find(tabletId);
    if (it != drtCache_.end()) {
      if (drtOut) {
        *drtOut = it->second;
      }
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  shared_ptr<DurableRoutingTable> drt;
  RETURN_NOT_OK_PREPEND(
      DurableRoutingTable::load(
          fsManager_,
          tabletId,
          std::move(raftConfig),
          DurableRoutingTable::LoadOptions::kCreateEmptyIfDoesNotExist,
          &drt),
      fmt::format(
          "Unable to load durable routing table for tablet {}", tabletId));

  // Cache and return the loaded DurableRoutingTable.
  {
    lock_guard<Mutex> l(drtLock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we check the insert succeeded.
    auto result = drtCache_.insert({tabletId, drt});
    CHECK(result.second) << "DurableRoutingTable already exists for tablet "
                         << tabletId;
  }

  if (drtOut) {
    *drtOut = std::move(drt);
  }
  return Status::OK();
}

// Load or Create DurableRoutingTable.
Status ConsensusMetadataManager::loadOrCreateDrt(
    const std::string& tabletId,
    RaftConfigPB raftConfig,
    ProxyTopologyPB proxyTopology,
    std::shared_ptr<DurableRoutingTable>* drtOut) {
  Status s = loadDrt(tabletId, raftConfig, drtOut);
  if (s.IsNotFound()) {
    return createDrt(
        tabletId, std::move(raftConfig), std::move(proxyTopology), drtOut);
  }
  return s;
}

Status ConsensusMetadataManager::deleteDrt(const string& tabletId) {
  {
    lock_guard<Mutex> l(drtLock_);
    drtCache_.erase(
        tabletId); // OK to delete an uncached DRT; ignore the return value.
  }
  return DurableRoutingTable::deleteOnDiskData(fsManager_, tabletId);
}

} // namespace kudu::consensus
