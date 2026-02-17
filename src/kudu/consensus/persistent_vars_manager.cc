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
#include "kudu/consensus/persistent_vars_manager.h"

#include <memory>
#include <mutex>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/persistent_vars.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/status.h"

namespace kudu::consensus {

using std::lock_guard;
using std::string;

PersistentVarsManager::PersistentVarsManager(FsManager* fsManager)
    : fsManager_(DCHECK_NOTNULL(fsManager)) {}

Status PersistentVarsManager::createPersistentVars(
    const string& tabletId,
    std::shared_ptr<PersistentVars>* persistentVarsOut) {
  std::shared_ptr<PersistentVars> persistentVars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::create(
          fsManager_, tabletId, fsManager_->uuid(), &persistentVars),
      fmt::format(
          "Unable to create consensus metadata for tablet {}", tabletId));

  lock_guard<Mutex> l(persistentVarsLock_);
  auto [it, inserted] = persistentVarsCache_.insert({tabletId, persistentVars});
  if (!inserted) {
    return Status::AlreadyPresent(
        fmt::format("PersistentVars instance for {} already exists", tabletId));
  }
  if (persistentVarsOut) {
    *persistentVarsOut = std::move(persistentVars);
  }
  return Status::OK();
}

Status PersistentVarsManager::loadPersistentVars(
    const string& tabletId,
    std::shared_ptr<PersistentVars>* persistentVarsOut) {
  {
    lock_guard<Mutex> l(persistentVarsLock_);

    // Try to get the persistentVars instance from cache first.
    auto it = persistentVarsCache_.find(tabletId);
    if (it != persistentVarsCache_.end()) {
      if (persistentVarsOut) {
        *persistentVarsOut = it->second;
      }
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  std::shared_ptr<PersistentVars> persistentVars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::load(
          fsManager_, tabletId, fsManager_->uuid(), &persistentVars),
      fmt::format("Unable to load persistent vars for tablet {}", tabletId));

  // Cache and return the loaded PersistentVars.
  {
    lock_guard<Mutex> l(persistentVarsLock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we use insert with CHECK.
    auto [it, inserted] =
        persistentVarsCache_.insert({tabletId, persistentVars});
    CHECK(inserted) << "Tablet ID already exists: " << tabletId;
  }

  if (persistentVarsOut) {
    *persistentVarsOut = std::move(persistentVars);
  }
  return Status::OK();
}

bool PersistentVarsManager::persistentVarsFileExists(
    const std::string& tabletId) const {
  return PersistentVars::fileExists(fsManager_, tabletId);
}

} // namespace kudu::consensus
