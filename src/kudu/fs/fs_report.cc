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
#include "kudu/fs/fs_report.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace fs {

using std::cout;
using std::string;
using std::unordered_map;
using std::vector;

///////////////////////////////////////////////////////////////////////////////
// MissingBlockCheck
///////////////////////////////////////////////////////////////////////////////

void MissingBlockCheck::mergeFrom(const MissingBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string MissingBlockCheck::toString() const {
  // Missing blocks are fatal so the IDs are logged in their entirety to ease
  // troubleshooting.
  //
  // Aggregate missing blocks across tablets.
  unordered_map<string, vector<string>> missingBlocksByTabletId;
  for (const auto& mb : entries) {
    missingBlocksByTabletId[mb.tabletId].emplace_back(mb.blockId.toString());
  }

  // Add the summary.
  string s = fmt::format("Total missing blocks: {}\n", entries.size());

  // Add an entry for each tablet.
  for (const auto& e : missingBlocksByTabletId) {
    s += fmt::format(
        "Fatal error: tablet {} missing blocks: [ {} ]\n",
        e.first,
        JoinStrings(e.second, ", "));
  }

  return s;
}

MissingBlockCheck::Entry::Entry(BlockId b, string t)
    : blockId(b), tabletId(std::move(t)) {}

///////////////////////////////////////////////////////////////////////////////
// OrphanedBlockCheck
///////////////////////////////////////////////////////////////////////////////

void OrphanedBlockCheck::mergeFrom(const OrphanedBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string OrphanedBlockCheck::toString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t orphanedBlockCountRepaired = 0;
  int64_t orphanedBlockBytes = 0;
  int64_t orphanedBlockBytesRepaired = 0;
  for (const auto& ob : entries) {
    if (ob.repaired) {
      orphanedBlockCountRepaired++;
    }
    orphanedBlockBytes += ob.length;
    if (ob.repaired) {
      orphanedBlockBytesRepaired += ob.length;
    }
  }

  return fmt::format(
      "Total orphaned blocks: {} ({} repaired)\n"
      "Total orphaned block bytes: {} ({} repaired)\n",
      entries.size(),
      orphanedBlockCountRepaired,
      orphanedBlockBytes,
      orphanedBlockBytesRepaired);
}

OrphanedBlockCheck::Entry::Entry(BlockId b, int64_t l)
    : blockId(b), length(l), repaired(false) {}

///////////////////////////////////////////////////////////////////////////////
// LbmFullContainerSpaceCheck
///////////////////////////////////////////////////////////////////////////////

void LbmFullContainerSpaceCheck::mergeFrom(
    const LbmFullContainerSpaceCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LbmFullContainerSpaceCheck::toString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t fullContainerSpaceCountRepaired = 0;
  int64_t fullContainerSpaceBytes = 0;
  int64_t fullContainerSpaceBytesRepaired = 0;
  for (const auto& fcp : entries) {
    if (fcp.repaired) {
      fullContainerSpaceCountRepaired++;
    }
    fullContainerSpaceBytes += fcp.excessBytes;
    if (fcp.repaired) {
      fullContainerSpaceBytesRepaired += fcp.excessBytes;
    }
  }

  return fmt::format(
      "Total full LBM containers with extra space: {} ({} repaired)\n"
      "Total full LBM container extra space in bytes: {} ({} repaired)\n",
      entries.size(),
      fullContainerSpaceCountRepaired,
      fullContainerSpaceBytes,
      fullContainerSpaceBytesRepaired);
}

LbmFullContainerSpaceCheck::Entry::Entry(string c, int64_t e)
    : container(std::move(c)), excessBytes(e), repaired(false) {}

///////////////////////////////////////////////////////////////////////////////
// LbmIncompleteContainerCheck
///////////////////////////////////////////////////////////////////////////////

void LbmIncompleteContainerCheck::mergeFrom(
    const LbmIncompleteContainerCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LbmIncompleteContainerCheck::toString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t incompleteContainerCountRepaired = 0;
  for (const auto& ic : entries) {
    if (ic.repaired) {
      incompleteContainerCountRepaired++;
    }
  }

  return fmt::format(
      "Total incomplete LBM containers: {} ({} repaired)\n",
      entries.size(),
      incompleteContainerCountRepaired);
}

LbmIncompleteContainerCheck::Entry::Entry(string c)
    : container(std::move(c)), repaired(false) {}

///////////////////////////////////////////////////////////////////////////////
// LbmMalformedRecordCheck
///////////////////////////////////////////////////////////////////////////////

void LbmMalformedRecordCheck::mergeFrom(const LbmMalformedRecordCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LbmMalformedRecordCheck::toString() const {
  // Malformed records are fatal so they're logged in their entirety to ease
  // troubleshooting.
  string s;
  for (const auto& mr : entries) {
    s += fmt::format(
        "Fatal error: malformed record in container {}: {}\n",
        mr.container,
        pb_util::SecureDebugString(mr.record));
  }
  return s;
}

LbmMalformedRecordCheck::Entry::Entry(string c, BlockRecordPB* r)
    : container(std::move(c)) {
  record.Swap(r);
}

///////////////////////////////////////////////////////////////////////////////
// LbmMisalignedBlockCheck
///////////////////////////////////////////////////////////////////////////////

void LbmMisalignedBlockCheck::mergeFrom(const LbmMisalignedBlockCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LbmMisalignedBlockCheck::toString() const {
  // Misaligned blocks should be rare so they're logged in their entirety to
  // ease troubleshooting.
  string s;
  for (const auto& mb : entries) {
    s += fmt::format(
        "Misaligned block in container {}: {}\n",
        mb.container,
        mb.blockId.toString());
  }
  return s;
}

LbmMisalignedBlockCheck::Entry::Entry(string c, BlockId b)
    : container(std::move(c)), blockId(b) {}

///////////////////////////////////////////////////////////////////////////////
// LbmPartialRecordCheck
///////////////////////////////////////////////////////////////////////////////

void LbmPartialRecordCheck::mergeFrom(const LbmPartialRecordCheck& other) {
  entries.insert(entries.end(), other.entries.begin(), other.entries.end());
}

string LbmPartialRecordCheck::toString() const {
  // Aggregate interesting stats from all of the entries.
  int64_t partialRecordsRepaired = 0;
  for (const auto& pr : entries) {
    if (pr.repaired) {
      partialRecordsRepaired++;
    }
  }

  return fmt::format(
      "Total LBM partial records: {} ({} repaired)\n",
      entries.size(),
      partialRecordsRepaired);
}

LbmPartialRecordCheck::Entry::Entry(string c, int64_t o)
    : container(std::move(c)), offset(o), repaired(false) {}

///////////////////////////////////////////////////////////////////////////////
// FsReport::Stats
///////////////////////////////////////////////////////////////////////////////

void FsReport::Stats::mergeFrom(const FsReport::Stats& other) {
  liveBlockCount += other.liveBlockCount;
  liveBlockBytes += other.liveBlockBytes;
  liveBlockBytesAligned += other.liveBlockBytesAligned;
  lbmContainerCount += other.lbmContainerCount;
  lbmFullContainerCount += other.lbmFullContainerCount;
}

string FsReport::Stats::toString() const {
  return fmt::format(
      "Total live blocks: {}\n"
      "Total live bytes: {}\n"
      "Total live bytes (after alignment): {}\n"
      "Total number of LBM containers: {} ({} full)\n",
      liveBlockCount,
      liveBlockBytes,
      liveBlockBytesAligned,
      lbmContainerCount,
      lbmFullContainerCount);
}

///////////////////////////////////////////////////////////////////////////////
// FsReport
///////////////////////////////////////////////////////////////////////////////

void FsReport::mergeFrom(const FsReport& other) {
  DCHECK_EQ(metadataDir, other.metadataDir);
  DCHECK_EQ(walDir, other.walDir);

  dataDirs.insert(dataDirs.end(), other.dataDirs.begin(), other.dataDirs.end());

  stats.mergeFrom(other.stats);

#define MERGE_ONE_CHECK(c)           \
  if ((c) && other.c) {              \
    (c)->mergeFrom(other.c.value()); \
  } else if (other.c) {              \
    (c) = other.c;                   \
  }

  MERGE_ONE_CHECK(missingBlockCheck);
  MERGE_ONE_CHECK(orphanedBlockCheck);
  MERGE_ONE_CHECK(fullContainerSpaceCheck);
  MERGE_ONE_CHECK(incompleteContainerCheck);
  MERGE_ONE_CHECK(malformedRecordCheck);
  MERGE_ONE_CHECK(misalignedBlockCheck);
  MERGE_ONE_CHECK(partialRecordCheck);

#undef MERGE_ONE_CHECK
}

string FsReport::toString() const {
  string s;
  s += "FS layout report\n";
  s += "--------------------\n";
  s += "wal directory: " + walDir + "\n";
  s += "metadata directory: " + metadataDir + "\n";
  s += fmt::format(
      "{} data directories: {}\n",
      dataDirs.size(),
      JoinStrings(dataDirs, ", "));
  s += stats.toString();

#define TOSTRING_ONE_CHECK(c, name)      \
  if ((c)) {                             \
    s += (c)->toString();                \
  } else {                               \
    s += "Did not check for " name "\n"; \
  }

  TOSTRING_ONE_CHECK(missingBlockCheck, "missing blocks");
  TOSTRING_ONE_CHECK(orphanedBlockCheck, "orphaned blocks");
  TOSTRING_ONE_CHECK(
      fullContainerSpaceCheck, "full LBM containers with extra space");
  TOSTRING_ONE_CHECK(incompleteContainerCheck, "incomplete LBM containers");
  TOSTRING_ONE_CHECK(malformedRecordCheck, "malformed LBM records");
  TOSTRING_ONE_CHECK(misalignedBlockCheck, "misaligned LBM blocks");
  TOSTRING_ONE_CHECK(partialRecordCheck, "partial LBM records");

#undef TOSTRING_ONE_CHECK
  return s;
}

Status FsReport::checkForFatalErrors() const {
  if (hasFatalErrors()) {
    return Status::Corruption(
        "found at least one fatal error in block manager on-disk state. "
        "See block manager consistency report for details");
  }
  return Status::OK();
}

bool FsReport::hasFatalErrors() const {
  return (missingBlockCheck && !missingBlockCheck->entries.empty()) ||
      (malformedRecordCheck && !malformedRecordCheck->entries.empty());
}

Status FsReport::logAndCheckForFatalErrors() const {
  LOG(INFO) << toString();
  return checkForFatalErrors();
}

Status FsReport::printAndCheckForFatalErrors() const {
  cout << toString();
  return checkForFatalErrors();
}

} // namespace fs
} // namespace kudu
