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
// to Create() or Load() for the same tablet id is thread-hostile and must be
// externally synchronized. Failure to do so may result in a crash.
class PersistentVarsManager {
 public:
  explicit PersistentVarsManager(FsManager* fs_manager);
  ~PersistentVarsManager() = default;

  // Create a PersistentVars instance keyed by 'tablet_id'.
  // Returns an error if a PersistentVars instance with that key already exists.
  Status createPersistentVars(
      const std::string& tablet_id,
      std::shared_ptr<PersistentVars>* persistent_vars_out = nullptr);

  // Load the PersistentVars instance keyed by 'tablet_id'.
  // Returns an error if it cannot be found, either in 'persistentVarsCache_'
  // or on disk.
  Status loadPersistentVars(
      const std::string& tablet_id,
      std::shared_ptr<PersistentVars>* persistent_vars_out = nullptr);

  // Check whether the Persistent Vars file exists for a given tablet
  bool persistentVarsFileExists(const std::string& tablet_id) const;

 private:
  FsManager* const fs_manager_;

  // Lock protecting persistentVarsCache_.
  Mutex persistentVarsLock_;

  // Cache for PersistentVars objects (tablet_id => persistent_vars).
  std::unordered_map<std::string, std::shared_ptr<PersistentVars>>
      persistentVarsCache_;

  DISALLOW_COPY_AND_ASSIGN(PersistentVarsManager);
};

} // namespace consensus
} // namespace kudu
