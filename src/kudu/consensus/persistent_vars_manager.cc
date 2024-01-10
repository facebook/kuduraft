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

#include "kudu/consensus/persistent_vars.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu::consensus {

using std::lock_guard;
using std::string;
using strings::Substitute;

PersistentVarsManager::PersistentVarsManager(FsManager* fs_manager)
    : fs_manager_(DCHECK_NOTNULL(fs_manager)) {}

Status PersistentVarsManager::CreatePersistentVars(
    const string& tablet_id,
    scoped_refptr<PersistentVars>* persistent_vars_out) {
  scoped_refptr<PersistentVars> persistent_vars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::Create(
          fs_manager_, tablet_id, fs_manager_->uuid(), &persistent_vars),
      Substitute(
          "Unable to create consensus metadata for tablet $0", tablet_id));

  lock_guard<Mutex> l(persistent_vars_lock_);
  if (!InsertIfNotPresent(
          &persistent_vars_cache_, tablet_id, persistent_vars)) {
    return Status::AlreadyPresent(
        Substitute("PersistentVars instance for $0 already exists", tablet_id));
  }
  if (persistent_vars_out) {
    *persistent_vars_out = std::move(persistent_vars);
  }
  return Status::OK();
}

Status PersistentVarsManager::LoadPersistentVars(
    const string& tablet_id,
    scoped_refptr<PersistentVars>* persistent_vars_out) {
  {
    lock_guard<Mutex> l(persistent_vars_lock_);

    // Try to get the persistent_vars instance from cache first.
    scoped_refptr<PersistentVars>* cached_persistent_vars =
        FindOrNull(persistent_vars_cache_, tablet_id);
    if (cached_persistent_vars) {
      if (persistent_vars_out) {
        *persistent_vars_out = *cached_persistent_vars;
      }
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  scoped_refptr<PersistentVars> persistent_vars;
  RETURN_NOT_OK_PREPEND(
      PersistentVars::Load(
          fs_manager_, tablet_id, fs_manager_->uuid(), &persistent_vars),
      Substitute("Unable to load persistent vars for tablet $0", tablet_id));

  // Cache and return the loaded PersistentVars.
  {
    lock_guard<Mutex> l(persistent_vars_lock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we use InsertOrDie().
    InsertOrDie(&persistent_vars_cache_, tablet_id, persistent_vars);
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
