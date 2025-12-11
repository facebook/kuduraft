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

#include "kudu/fs/error_manager.h"

#include <mutex>
#include <string>
#include <utility>

#include "kudu/gutil/bind.h"

using std::string;

namespace kudu::fs {

// Default error-handling callback that no-ops.
static void DoNothingErrorNotification(const string& /* uuid */) {}

FsErrorManager::FsErrorManager() {
  auto [it1, inserted1] = callbacks_.emplace(
      ErrorHandlerType::DISK_ERROR, Bind(DoNothingErrorNotification));
  CHECK(inserted1) << "Failed to insert DISK_ERROR callback";
  auto [it2, inserted2] = callbacks_.emplace(
      ErrorHandlerType::NO_AVAILABLE_DISKS, Bind(DoNothingErrorNotification));
  CHECK(inserted2) << "Failed to insert NO_AVAILABLE_DISKS callback";
  auto [it3, inserted3] = callbacks_.emplace(
      ErrorHandlerType::CFILE_CORRUPTION, Bind(DoNothingErrorNotification));
  CHECK(inserted3) << "Failed to insert CFILE_CORRUPTION callback";
}

void FsErrorManager::SetErrorNotificationCb(
    ErrorHandlerType e,
    ErrorNotificationCb cb) {
  std::lock_guard<Mutex> l(lock_);
  callbacks_.insert_or_assign(e, std::move(cb));
}

void FsErrorManager::UnsetErrorNotificationCb(ErrorHandlerType e) {
  std::lock_guard<Mutex> l(lock_);
  callbacks_.insert_or_assign(e, Bind(DoNothingErrorNotification));
}

void FsErrorManager::RunErrorNotificationCb(
    ErrorHandlerType e,
    const string& uuid) const {
  std::lock_guard<Mutex> l(lock_);
  auto it = callbacks_.find(e);
  CHECK(it != callbacks_.end()) << "Map key not found: " << e;
  it->second.Run(uuid);
}

} // namespace kudu::fs
