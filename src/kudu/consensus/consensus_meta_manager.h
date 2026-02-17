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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/routing.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/mutex.h"

namespace kudu {
class FsManager;
class Status;

namespace consensus {
class DurableRoutingTable;
class RaftConfigPB;

// API and implementation for a consensus metadata "manager" that controls
// access to consensus metadata across a server instance. This abstracts
// the handling of consensus metadata persistence.
//
// A single manager instance can be plumbed throughout the various classes that
// deal with reading, creating, and modifying consensus metadata so that we
// don't have to pass individual consensus metadata instances around. It also
// provides flexibility to change the underlying implementation of
// ConsensusMetadata in the future.
//
// This class is ONLY thread-safe across different tablets. Concurrent access
// to Create(), Load(), or Delete() for the same tablet id is thread-hostile
// and must be externally synchronized. Failure to do so may result in a crash.
class ConsensusMetadataManager {
 public:
  explicit ConsensusMetadataManager(FsManager* fsManager);
  ~ConsensusMetadataManager() = default;

  // Create a ConsensusMetadata instance keyed by 'tabletId'.
  // Returns an error if a ConsensusMetadata instance with that key already
  // exists.
  Status createCMeta(
      const std::string& tabletId,
      const RaftConfigPB& config,
      int64_t initialTerm,
      ConsensusMetadataCreateMode createMode =
          ConsensusMetadataCreateMode::FlushOnCreate,
      std::shared_ptr<ConsensusMetadata>* cmetaOut = nullptr);

  // Load the ConsensusMetadata instance keyed by 'tabletId'.
  // Returns an error if it cannot be found, either in 'cmetaCache_' or on
  // disk.
  Status loadCMeta(
      const std::string& tabletId,
      std::shared_ptr<ConsensusMetadata>* cmetaOut = nullptr);

  // Load the ConsensusMetadata instance keyed by 'tabletId' if it exists,
  // otherwise create it using the given parameters 'config' and
  // 'initialTerm'. If the instance already exists, those parameters are
  // ignored.
  Status loadOrCreateCMeta(
      const std::string& tabletId,
      const RaftConfigPB& config,
      int64_t initialTerm,
      ConsensusMetadataCreateMode createMode =
          ConsensusMetadataCreateMode::FlushOnCreate,
      std::shared_ptr<ConsensusMetadata>* cmetaOut = nullptr);

  // Permanently delete the ConsensusMetadata instance keyed by 'tabletId'.
  // Returns Status::NotFound if the instance does not exist on disk.
  // Returns another error if the cmeta instance exists but cannot be deleted
  // for some reason, perhaps due to a permissions or I/O-related issue.
  Status deleteCMeta(const std::string& tabletId);

  // Create DurableRoutingTable.
  Status createDrt(
      const std::string& tabletId,
      RaftConfigPB raftConfig,
      ProxyTopologyPB proxyTopology,
      std::shared_ptr<DurableRoutingTable>* drtOut = nullptr);

  // Load DurableRoutingTable.
  Status loadDrt(
      const std::string& tabletId,
      RaftConfigPB raftConfig,
      std::shared_ptr<DurableRoutingTable>* drtOut = nullptr);

  // Load or Create DurableRoutingTable.
  Status loadOrCreateDrt(
      const std::string& tabletId,
      RaftConfigPB raftConfig,
      ProxyTopologyPB proxyTopology,
      std::shared_ptr<DurableRoutingTable>* drtOut = nullptr);

  // Delete DurableRoutingTable.
  Status deleteDrt(const std::string& tabletId);

 private:
  FsManager* const fsManager_;

  // Lock protecting cmetaCache_.
  Mutex cmetaLock_;

  // Cache for ConsensusMetadata objects (tabletId => cmeta).
  std::unordered_map<std::string, std::shared_ptr<ConsensusMetadata>>
      cmetaCache_;

  Mutex drtLock_;
  // Cache for DurableRoutingTable objects (tabletId => drt).
  std::unordered_map<std::string, std::shared_ptr<DurableRoutingTable>>
      drtCache_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadataManager);
};

} // namespace consensus
} // namespace kudu
