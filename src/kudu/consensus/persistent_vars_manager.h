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

#include "kudu/consensus/persistent_vars.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/mutex.h"

namespace kudu {
class FsManager;
class Status;

namespace consensus {
// API and implementation for a consensus metadata "manager" that controls
// access to consensus metadata across a server instance. This abstracts
// the handling of consensus metadata persistence.
//
// A single manager instance can be plumbed throughout the various classes that
// deal with reading, creating, and modifying consensus metadata so that we
// don't have to pass individual consensus metadata instances around. It also
// provides flexibility to change the underlying implementation of
// PersistentVars in the future.
//
// This class is ONLY thread-safe across different tablets. Concurrent access
// to Create(), Load(), or Delete() for the same tablet id is thread-hostile
// and must be externally synchronized. Failure to do so may result in a crash.
class PersistentVarsManager : public RefCountedThreadSafe<PersistentVarsManager> {
 public:
  explicit PersistentVarsManager(FsManager* fs_manager);

  // Create a PersistentVars instance keyed by 'tablet_id'.
  // Returns an error if a PersistentVars instance with that key already exists.
  Status CreatePersistentVars(const std::string& tablet_id,
                     PersistentVarsCreateMode create_mode =
                         PersistentVarsCreateMode::FLUSH_ON_CREATE,
                     scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Load the PersistentVars instance keyed by 'tablet_id'.
  // Returns an error if it cannot be found, either in 'persistent_vars_cache_' or on
  // disk.
  Status LoadPersistentVars(const std::string& tablet_id,
                   scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Load the PersistentVars instance keyed by 'tablet_id' if it exists,
  // otherwise create it using the given parameters 'config' and
  // 'initial_term'. If the instance already exists, those parameters are
  // ignored.
  Status LoadOrCreatePersistentVars(const std::string& tablet_id,
                           PersistentVarsCreateMode create_mode =
                               PersistentVarsCreateMode::FLUSH_ON_CREATE,
                           scoped_refptr<PersistentVars>* persistent_vars_out = nullptr);

  // Permanently delete the PersistentVars instance keyed by 'tablet_id'.
  // Returns Status::NotFound if the instance does not exist on disk.
  // Returns another error if the persistent_vars instance exists but cannot be deleted
  // for some reason, perhaps due to a permissions or I/O-related issue.
  Status DeletePersistentVars(const std::string& tablet_id);

private:
  friend class RefCountedThreadSafe<PersistentVarsManager>;

  FsManager* const fs_manager_;

  // Lock protecting persistent_vars_cache_.
  Mutex persistent_vars_lock_;

  // Cache for PersistentVars objects (tablet_id => persistent_vars).
  std::unordered_map<std::string, scoped_refptr<PersistentVars>> persistent_vars_cache_;

  DISALLOW_COPY_AND_ASSIGN(PersistentVarsManager);
};

} // namespace consensus
} // namespace kudu
