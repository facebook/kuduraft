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
#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"

namespace kudu {

class FsManager;
class Status;

namespace consensus {

class PersistentVarsManager; // IWYU pragma: keep
class PersistentVarsTest;    // IWYU pragma: keep

// Provides methods to read and write persistent variables.
// This class is not thread-safe and requires external synchronization.
class PersistentVars : public RefCountedThreadSafe<PersistentVars> {
 public:

  // Specify whether we are allowed to overwrite an existing file when flushing.
  enum FlushMode {
    OVERWRITE,
    NO_OVERWRITE
  };

  // Accessor for whether starting elections is allowed
  bool is_start_election_allowed() const;

  // Allow/Disallow starting elections
  void set_allow_start_election(bool val);

  // A RPC token used to show proof that we belong to a certain Raft ring
  //
  // This method, unlike the rest is thread-safe, but uses relaxed memory order
  // I.e. You cannot use it for synchronization other process states based on
  // code ordering
  std::shared_ptr<const std::string> raft_rpc_token() const;

  // Change the RPC token, boost::none unsets the token
  void set_raft_rpc_token(boost::optional<std::string> rpc_token);

  // Persist current state of the protobuf to disk.
  Status Flush(FlushMode flush_mode = OVERWRITE);

 private:
  friend class RefCountedThreadSafe<PersistentVars>;
  friend class PersistentVarsManager;

  PersistentVars(FsManager* fs_manager,
                 std::string tablet_id,
                 std::string peer_uuid);

  // Create a PersistentVars object; the encoded PB is flushed to disk before
  // returning
  static Status Create(FsManager* fs_manager,
                       const std::string& tablet_id,
                       const std::string& peer_uuid,
                       scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Load a PersistentVars object from disk.
  // Returns Status::NotFound if the file could not be found. May return other
  // Status codes if unable to read the file.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     const std::string& peer_uuid,
                     scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Check whether the persistent_vars file exists for the given tablet
  static bool FileExists(FsManager* fs_manager, const std::string& tablet_id);

  std::string LogPrefix() const;

  FsManager* const fs_manager_;
  const std::string tablet_id_;
  const std::string peer_uuid_;

  // A "atomic" cached value of raft_rpc_token
  std::shared_ptr<const std::string> raft_rpc_token_cache_;

  // This fake mutex helps ensure that this PersistentVars object stays
  // externally synchronized.
  DFAKE_MUTEX(fake_lock_);

  // Durable fields.
  PersistentVarsPB pb_;

  DISALLOW_COPY_AND_ASSIGN(PersistentVars);
};

} // namespace consensus
} // namespace kudu
