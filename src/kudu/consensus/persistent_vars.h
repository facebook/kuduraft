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

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>

#include <optional>

#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/threading/thread_collision_warner.h"

namespace kudu {

class FsManager;
class Status;

namespace consensus {

class PersistentVarsManager; // IWYU pragma: keep
class PersistentVarsTest; // IWYU pragma: keep

// Provides methods to read and write persistent variables.
// This class is not thread-safe and requires external synchronization.
class PersistentVars {
 public:
  // Specify whether we are allowed to overwrite an existing file when flushing.
  enum FlushMode { kOverwrite, kNoOverwrite };

  // Accessor for whether starting elections is allowed
  bool isStartElectionAllowed() const;

  // Allow/Disallow starting elections
  void setAllowStartElection(bool val);

  // A RPC token used to show proof that we belong to a certain Raft ring
  //
  // This method, unlike the rest is thread-safe, but uses relaxed memory order
  // I.e. You cannot use it for synchronization other process states based on
  // code ordering
  std::shared_ptr<const std::string> raftRpcToken() const;

  // Change the RPC token, {} unsets the token
  void setRaftRpcToken(std::optional<std::string> rpcToken);

  // Fetches compression dict from PB
  const std::string& compressionDictionary() const;

  // Sets compression dict in PB
  void setCompressionDictionary(const std::string& dict);

  // Persist current state of the protobuf to disk.
  Status flush(FlushMode flushMode = kOverwrite);

  // Destructor must be public for std::shared_ptr
  ~PersistentVars() = default;

 private:
  friend class PersistentVarsManager;

  PersistentVars(
      FsManager* fsManager,
      std::string tabletId,
      std::string peerUuid);

  // Create a PersistentVars object; the encoded PB is flushed to disk before
  // returning
  static Status create(
      FsManager* fsManager,
      const std::string& tabletId,
      const std::string& peerUuid,
      std::shared_ptr<PersistentVars>* persistentVarsOut = nullptr);

  // Load a PersistentVars object from disk.
  // Returns Status::NotFound if the file could not be found. May return other
  // Status codes if unable to read the file.
  static Status load(
      FsManager* fsManager,
      const std::string& tabletId,
      const std::string& peerUuid,
      std::shared_ptr<PersistentVars>* persistentVarsOut = nullptr);

  // Check whether the persistent_vars file exists for the given tablet
  static bool fileExists(FsManager* fsManager, const std::string& tabletId);

  std::string logPrefix() const;

  FsManager* const fsManager_;
  const std::string tabletId_;
  const std::string peerUuid_;

  // A "atomic" cached value of raftRpcToken
  std::shared_ptr<const std::string> raftRpcTokenCache_;

  // This fake mutex helps ensure that this PersistentVars object stays
  // externally synchronized.
  DFAKE_MUTEX(fakeLock_);

  // Durable fields.
  PersistentVarsPB pb_;

  DISALLOW_COPY_AND_ASSIGN(PersistentVars);
  PersistentVars(PersistentVars&&) = delete;
  PersistentVars& operator=(PersistentVars&&) = delete;
};

} // namespace consensus
} // namespace kudu
