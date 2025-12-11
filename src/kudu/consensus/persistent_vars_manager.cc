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

PersistentVarsManager::PersistentVarsManager(FsManager* fs_manager)
    : fs_manager_(DCHECK_NOTNULL(fs_manager)) {}

Status PersistentVarsManager::CreatePersistentVars(
    const string& tablet_id,
    std::shared_ptr<PersistentVars>* persistent_vars_out) {
  std::shared_ptr<PersistentVars> persistent_vars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::Create(
          fs_manager_, tablet_id, fs_manager_->uuid(), &persistent_vars),
      fmt::format(
          "Unable to create consensus metadata for tablet {}", tablet_id));

  lock_guard<Mutex> l(persistent_vars_lock_);
  auto [it, inserted] =
      persistent_vars_cache_.insert({tablet_id, persistent_vars});
  if (!inserted) {
    return Status::AlreadyPresent(
        fmt::format(
            "PersistentVars instance for {} already exists", tablet_id));
  }
  if (persistent_vars_out) {
    *persistent_vars_out = std::move(persistent_vars);
  }
  return Status::OK();
}

Status PersistentVarsManager::LoadPersistentVars(
    const string& tablet_id,
    std::shared_ptr<PersistentVars>* persistent_vars_out) {
  {
    lock_guard<Mutex> l(persistent_vars_lock_);

    // Try to get the persistent_vars instance from cache first.
    auto it = persistent_vars_cache_.find(tablet_id);
    if (it != persistent_vars_cache_.end()) {
      if (persistent_vars_out) {
        *persistent_vars_out = it->second;
      }
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  std::shared_ptr<PersistentVars> persistent_vars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::Load(
          fs_manager_, tablet_id, fs_manager_->uuid(), &persistent_vars),
      fmt::format("Unable to load persistent vars for tablet {}", tablet_id));

  // Cache and return the loaded PersistentVars.
  {
    lock_guard<Mutex> l(persistent_vars_lock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we use insert with CHECK.
    auto [it, inserted] =
        persistent_vars_cache_.insert({tablet_id, persistent_vars});
    CHECK(inserted) << "Tablet ID already exists: " << tablet_id;
  }

  if (persistent_vars_out) {
    *persistent_vars_out = std::move(persistent_vars);
  }
  return Status::OK();
}

bool PersistentVarsManager::PersistentVarsFileExists(
    const std::string& tablet_id) const {
  return PersistentVars::FileExists(fs_manager_, tablet_id);
}

} // namespace kudu::consensus
