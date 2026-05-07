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

#include "kudu/consensus/log_anchor_registry.h"

#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/port.h"

namespace kudu::log {

using consensus::kInvalidOpIdIndex;
using std::string;

LogAnchorRegistry::LogAnchorRegistry() = default;

LogAnchorRegistry::~LogAnchorRegistry() {
  CHECK(anchors_.empty());
}

void LogAnchorRegistry::registerAnchor(
    int64_t logIndex,
    const string& owner,
    LogAnchor* anchor) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  registerUnlocked(logIndex, owner, anchor);
}

Status LogAnchorRegistry::updateRegistration(
    int64_t logIndex,
    const std::string& owner,
    LogAnchor* anchor) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  RETURN_NOT_OK_PREPEND(
      unregisterUnlocked(anchor),
      "Unable to swap registration, anchor not registered")
  registerUnlocked(logIndex, owner, anchor);
  return Status::OK();
}

Status LogAnchorRegistry::unregister(LogAnchor* anchor) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  return unregisterUnlocked(anchor);
}

Status LogAnchorRegistry::unregisterIfAnchored(LogAnchor* anchor) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  if (!anchor->isRegistered_) {
    return Status::OK();
  }
  return unregisterUnlocked(anchor);
}

Status LogAnchorRegistry::getEarliestRegisteredLogIndex(int64_t* logIndex) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  auto iter = anchors_.begin();
  if (iter == anchors_.end()) {
    return Status::NotFound("No anchors in registry");
  }

  // Since this is a sorted map, the first element is the one we want.
  *logIndex = iter->first;
  return Status::OK();
}

size_t LogAnchorRegistry::getAnchorCountForTests() const {
  std::lock_guard<SimpleSpinlock> l(lock_);
  return anchors_.size();
}

std::string LogAnchorRegistry::dumpAnchorInfo() const {
  string buf;
  std::lock_guard<SimpleSpinlock> l(lock_);
  MonoTime now = MonoTime::Now();
  for (const AnchorMultiMap::value_type& entry : anchors_) {
    const LogAnchor* anchor = entry.second;
    DCHECK(anchor->isRegistered_);
    if (!buf.empty()) {
      buf += ", ";
    }
    buf += fmt::format(
        "LogAnchor[index={}, age={}s, owner={}]",
        anchor->logIndex_,
        (now - anchor->whenRegistered_).ToSeconds(),
        anchor->owner_);
  }
  return buf;
}

void LogAnchorRegistry::registerUnlocked(
    int64_t logIndex,
    const std::string& owner,
    LogAnchor* anchor) {
  DCHECK(anchor != nullptr);
  DCHECK(!anchor->isRegistered_);

  anchor->logIndex_ = logIndex;
  anchor->owner_.assign(owner);
  anchor->isRegistered_ = true;
  anchor->whenRegistered_ = MonoTime::Now();
  AnchorMultiMap::value_type value(logIndex, anchor);
  anchors_.insert(value);
}

Status LogAnchorRegistry::unregisterUnlocked(LogAnchor* anchor) {
  DCHECK(anchor != nullptr);
  DCHECK(anchor->isRegistered_);

  auto iter = anchors_.find(anchor->logIndex_);
  while (iter != anchors_.end()) {
    if (iter->second == anchor) {
      anchor->isRegistered_ = false;
      anchors_.erase(iter);
      // No need for the iterator to remain valid since we return here.
      return Status::OK();
    }
    ++iter;
  }
  return Status::NotFound(
      fmt::format(
          "Anchor with index {} and owner {} not found",
          anchor->logIndex_,
          anchor->owner_));
}

LogAnchor::LogAnchor() : isRegistered_(false), logIndex_(kInvalidOpIdIndex) {}

LogAnchor::~LogAnchor() {
  CHECK(!isRegistered_) << "Attempted to destruct a registered LogAnchor";
}

MinLogIndexAnchorer::MinLogIndexAnchorer(
    LogAnchorRegistry* registry,
    string owner)
    : registry_(DCHECK_NOTNULL(registry)),
      owner_(std::move(owner)),
      minimumLogIndex_(kInvalidOpIdIndex) {}

MinLogIndexAnchorer::~MinLogIndexAnchorer() {
  CHECK_OK(releaseAnchor());
}

void MinLogIndexAnchorer::anchorIfMinimum(int64_t logIndex) {
  std::lock_guard<SimpleSpinlock> l(lock_);
  if (PREDICT_FALSE(minimumLogIndex_ == kInvalidOpIdIndex)) {
    minimumLogIndex_ = logIndex;
    registry_->registerAnchor(minimumLogIndex_, owner_, &anchor_);
  } else if (logIndex < minimumLogIndex_) {
    minimumLogIndex_ = logIndex;
    CHECK_OK(registry_->updateRegistration(minimumLogIndex_, owner_, &anchor_));
  }
}

Status MinLogIndexAnchorer::releaseAnchor() {
  std::lock_guard<SimpleSpinlock> l(lock_);
  if (PREDICT_TRUE(minimumLogIndex_ != kInvalidOpIdIndex)) {
    return registry_->unregister(&anchor_);
  }
  return Status::OK(); // If there were no inserts, return OK.
}

int64_t MinLogIndexAnchorer::minimumLogIndex() const {
  std::lock_guard<SimpleSpinlock> l(lock_);
  return minimumLogIndex_;
}

} // namespace kudu::log
