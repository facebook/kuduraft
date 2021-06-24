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

#include <gtest/gtest_prod.h>

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

enum class PersistentVarsCreateMode {
  FLUSH_ON_CREATE,
  NO_FLUSH_ON_CREATE,
};

// Provides methods to read, write, and persist consensus-related metadata.
// This partly corresponds to Raft Figure 2's "Persistent state on all servers".
//
// In addition to the persistent state, this class also provides access to some
// transient state. This includes the peer that this node considers to be the
// leader of the configuration, as well as the "pending" configuration, if any.
//
// Conceptually, a pending configuration is one that has been proposed via a config
// change operation (AddServer or RemoveServer from Chapter 4 of Diego Ongaro's
// Raft thesis) but has not yet been committed. According to the above spec,
// as soon as a server hears of a new cluster membership configuration, it must
// be adopted (even prior to be committed).
//
// The data structure difference between a committed configuration and a pending one
// is that opid_index (the index in the log of the committed config change
// operation) is always set in a committed configuration, while it is always unset in
// a pending configuration.
//
// Finally, this class exposes the concept of an "active" configuration, which means
// the pending configuration if a pending configuration is set, otherwise the committed
// configuration.
//
// This class is not thread-safe and requires external synchronization.
class PersistentVars : public RefCountedThreadSafe<PersistentVars> {
 public:

  // Specify whether we are allowed to overwrite an existing file when flushing.
  enum FlushMode {
    OVERWRITE,
    NO_OVERWRITE
  };

  // Accessor for whether failure detector is disabled
  bool is_failure_detector_disabled() const;

  // Enable failure detector - make the peer take action if heartbeats from the
  // leader fail
  void enable_failure_detector();

  // Disable failure detector - make the peer ignore heartbeat failures from
  // the leader
  void disable_failure_detector();

  // Persist current state of the protobuf to disk.
  Status Flush(FlushMode flush_mode = OVERWRITE);

  int64_t flush_count_for_tests() const {
    return flush_count_for_tests_;
  }

 private:
  friend class RefCountedThreadSafe<PersistentVars>;
  friend class PersistentVarsManager;

  FRIEND_TEST(PersistentVarsTest, TestCreateLoad);
  FRIEND_TEST(PersistentVarsTest, TestDeferredCreateLoad);
  FRIEND_TEST(PersistentVarsTest, TestCreateNoOverwrite);
  FRIEND_TEST(PersistentVarsTest, TestFailedLoad);
  FRIEND_TEST(PersistentVarsTest, TestFlush);
  FRIEND_TEST(PersistentVarsTest, TestActiveRole);
  FRIEND_TEST(PersistentVarsTest, TestToConsensusStatePB);
  FRIEND_TEST(PersistentVarsTest, TestMergeCommittedConsensusStatePB);

  PersistentVars(FsManager* fs_manager,
                 std::string tablet_id,
                 std::string peer_uuid);

  // Create a PersistentVars object with provided initial state.
  // If 'create_mode' is set to FLUSH_ON_CREATE, the encoded PB is flushed to
  // disk before returning. Otherwise, if 'create_mode' is set to
  // NO_FLUSH_ON_CREATE, the caller must explicitly call Flush() on the
  // returned object to get the bytes onto disk.
  static Status Create(FsManager* fs_manager,
                       const std::string& tablet_id,
                       const std::string& peer_uuid,
                       PersistentVarsCreateMode create_mode =
                           PersistentVarsCreateMode::FLUSH_ON_CREATE,
                       scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Load a PersistentVars object from disk.
  // Returns Status::NotFound if the file could not be found. May return other
  // Status codes if unable to read the file.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     const std::string& peer_uuid,
                     scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Delete the PersistentVars file associated with the given tablet from
  // disk. Returns Status::NotFound if the on-disk data is not found.
  static Status DeleteOnDiskData(FsManager* fs_manager, const std::string& tablet_id);

  std::string LogPrefix() const;

  FsManager* const fs_manager_;
  const std::string tablet_id_;
  const std::string peer_uuid_;

  // This fake mutex helps ensure that this PersistentVars object stays
  // externally synchronized.
  DFAKE_MUTEX(fake_lock_);

  // The number of times the metadata has been flushed to disk.
  int64_t flush_count_for_tests_;

  // Durable fields.
  PersistentVarsPB pb_;

  DISALLOW_COPY_AND_ASSIGN(PersistentVars);
};

} // namespace consensus
} // namespace kudu
