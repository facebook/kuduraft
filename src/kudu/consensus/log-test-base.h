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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#ifndef KUDU_CONSENSUS_LOG_TEST_BASE_H
#define KUDU_CONSENSUS_LOG_TEST_BASE_H

#include "kudu/consensus/log-test-util.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
// #include "kudu/common/wire_protocol-test-util.h"
#include <fmt/core.h>
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/util.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace log {

constexpr char kTestTable[] = "test-log-table";
constexpr char kTestTableId[] = "test-log-table-id";
constexpr char kTestTablet[] = "test-log-tablet";
constexpr bool kAppendSync = true;
constexpr bool kAppendAsync = false;

class LogTestBase : public KuduTest {
 public:
  typedef std::pair<int, int> DeltaId;

  LogTestBase()
      : schema_(getSimpleTestSchema()),
        logAnchorRegistry_(new LogAnchorRegistry) {}

  void SetUp() override {
    KuduTest::SetUp();
    currentIndex_ = kStartIndex;
    fsManager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    metricRegistry_.reset(new MetricRegistry());
    metricEntity_ = METRIC_ENTITY_server.instantiate(
        metricRegistry_.get(), "log-test-base");
    ASSERT_OK(fsManager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fsManager_->Open());

    clock_.reset(new clock::HybridClock());
    ASSERT_OK(clock_->init());
  }

  void TearDown() override {
    KuduTest::TearDown();
  }

  Status buildLog() {
    Schema schemaWithIds = SchemaBuilder(schema_).Build();
    return Log::Open(
        options_,
        fsManager_.get(),
        kTestTablet,
        schemaWithIds,
        0, // schema_version
        metricEntity_,
        &log_);
  }

  void checkRightNumberOfSegmentFiles(int expected) {
    // Test that we actually have the expected number of files in the fs.
    // We should have n segments plus '.' and '..'
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(
        JoinPathSegments(fsManager_->GetWalsRootDir(), kTestTablet), &files));
    int count = 0;
    for (const std::string& s : files) {
      if (hasPrefixString(s, FsManager::kWalFileNamePrefix)) {
        count++;
      }
    }
    ASSERT_EQ(expected, count);
  }

  void entriesToIdList(std::vector<uint32_t>* ids) {
    for (const auto& entry : entries_) {
      VLOG(2) << "Entry contents: " << pb_util::SecureDebugString(*entry);
      if (entry->type() == REPLICATE) {
        ids->push_back(entry->replicate().id().index());
      }
    }
  }

  static void checkReplicateResult(
      const consensus::ReplicateRefPtr& msg,
      const Status& s) {
    CHECK_OK(s);
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  Status appendReplicateBatch(
      const consensus::OpId& opId,
      bool sync = kAppendSync) {
    consensus::ReplicateRefPtr replicate = makeScopedRefptrReplicate(
        std::make_unique<consensus::ReplicateMsg>(), Source::Memory);
    replicate->get()->set_op_type(consensus::WRITE_OP);
    replicate->get()->mutable_id()->CopyFrom(opId);
    replicate->get()->set_timestamp(clock_->now().toUint64());
    tserver::WriteRequestPB* batchRequest =
        replicate->get()->mutable_write_request();
    RETURN_NOT_OK(SchemaToPB(schema_, batchRequest->mutable_schema()));
    addTestRowToPb(
        RowOperationsPB::INSERT,
        schema_,
        opId.index(),
        0,
        "this is a test insert",
        batchRequest->mutable_row_operations());
    addTestRowToPb(
        RowOperationsPB::UPDATE,
        schema_,
        opId.index() + 1,
        0,
        "this is a test mutate",
        batchRequest->mutable_row_operations());
    batchRequest->set_tablet_id(kTestTablet);
    return appendReplicateBatch(replicate, sync);
  }

  // Appends the provided batch to the log.
  Status appendReplicateBatch(
      const consensus::ReplicateRefPtr& replicate,
      bool sync = kAppendSync) {
    if (sync) {
      Synchronizer s;
      RETURN_NOT_OK(
          log_->AsyncAppendReplicates({replicate}, s.asStatusCallback()));
      return s.wait();
    }
    // AsyncAppendReplicates does not free the ReplicateMsg on completion, so we
    // need to pass it through to our callback.
    return log_->AsyncAppendReplicates(
        {replicate}, Bind(&LogTestBase::checkReplicateResult, replicate));
  }

  static void checkCommitResult(const Status& s) {
    CHECK_OK(s);
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  Status appendCommit(
      const consensus::OpId& originalOpId,
      bool sync = kAppendSync) {
    // The mrs id for the insert.
    constexpr int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    constexpr int kTargetRsId = 0;
    constexpr int kTargetDeltaId = 0;

    return appendCommit(
        originalOpId, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  Status appendCommit(
      const consensus::OpId& originalOpId,
      int mrsId,
      int rsId,
      int dmsId,
      bool sync = kAppendSync) {
    std::unique_ptr<consensus::CommitMsg> commit(new consensus::CommitMsg);
    commit->set_op_type(consensus::WRITE_OP);

    commit->mutable_commited_op_id()->CopyFrom(originalOpId);

    tablet::TxResultPB* result = commit->mutable_result();

    tablet::OperationResultPB* insert = result->add_ops();
    insert->add_mutated_stores()->set_mrs_id(mrsId);

    tablet::OperationResultPB* mutate = result->add_ops();
    tablet::MemStoreTargetPB* target = mutate->add_mutated_stores();
    target->set_dms_id(dmsId);
    target->set_rs_id(rsId);
    return appendCommit(std::move(commit), sync);
  }

  // Append a COMMIT message for 'originalOpId', but with results
  // indicating that the associated writes failed due to
  // "NotFound" errors.
  Status appendCommitWithNotFoundOpResults(
      const consensus::OpId& originalOpId) {
    std::unique_ptr<consensus::CommitMsg> commit(new consensus::CommitMsg);
    commit->set_op_type(consensus::WRITE_OP);
    commit->mutable_commited_op_id()->CopyFrom(originalOpId);

    tablet::TxResultPB* result = commit->mutable_result();

    tablet::OperationResultPB* insert = result->add_ops();
    statusToPb(
        Status::NotFound("fake failed write"), insert->mutable_failed_status());
    tablet::OperationResultPB* mutate = result->add_ops();
    statusToPb(
        Status::NotFound("fake failed write"), mutate->mutable_failed_status());

    return appendCommit(std::move(commit));
  }

  Status appendCommit(
      std::unique_ptr<consensus::CommitMsg> commit,
      bool sync = kAppendSync) {
    if (sync) {
      Synchronizer s;
      RETURN_NOT_OK(
          log_->AsyncAppendCommit(std::move(commit), s.asStatusCallback()));
      return s.wait();
    }
    return log_->AsyncAppendCommit(
        std::move(commit), Bind(&LogTestBase::checkCommitResult));
  }

  // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  Status appendReplicateBatchAndCommitEntryPairsToLog(
      int count,
      bool sync = kAppendSync) {
    for (int i = 0; i < count; i++) {
      consensus::OpId opid = consensus::MakeOpId(1, currentIndex_);
      RETURN_NOT_OK(appendReplicateBatch(opid));
      RETURN_NOT_OK(appendCommit(opid, sync));
      currentIndex_ += 1;
    }
    return Status::OK();
  }

  // Append a single NO_OP entry. Increments opId by one.
  // If non-NULL, and if the write is successful, 'size' is incremented
  // by the size of the written operation.
  Status appendNoOp(consensus::OpId* opId, int* size = nullptr) {
    return appendNoOpToLogSync(clock_, log_.get(), opId, size);
  }

  // Append a number of no-op entries to the log.
  // Increments opId's index by the number of records written.
  // If non-NULL, 'size' keeps track of the size of the operations
  // successfully written.
  Status appendNoOps(consensus::OpId* opId, int num, int* size = nullptr) {
    for (int i = 0; i < num; i++) {
      RETURN_NOT_OK(appendNoOp(opId, size));
    }
    return Status::OK();
  }

  Status rollLog() {
    RETURN_NOT_OK(log_->AsyncAllocateSegment());
    return log_->RollOver();
  }

  std::string dumpSegmentsToString(const SegmentSequence& segments) {
    std::string dump;
    for (const std::shared_ptr<ReadableLogSegment>& segment : segments) {
      dump.append("------------\n");
      dump += fmt::format(
          "Segment: {}, Path: {}\n",
          segment->header().sequence_number(),
          segment->path());
      dump += fmt::format(
          "Header: {}\n", pb_util::SecureShortDebugString(segment->header()));
      if (segment->hasFooter()) {
        dump += fmt::format(
            "Footer: {}\n", pb_util::SecureShortDebugString(segment->footer()));
      } else {
        dump.append("Footer: None or corrupt.");
      }
    }
    return dump;
  }

 protected:
  enum { kStartIndex = 1 };

  const Schema schema_;
  std::unique_ptr<FsManager> fsManager_;
  std::unique_ptr<MetricRegistry> metricRegistry_;
  std::shared_ptr<MetricEntity> metricEntity_;
  std::shared_ptr<Log> log_;
  int64_t currentIndex_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  LogEntries entries_;
  std::shared_ptr<LogAnchorRegistry> logAnchorRegistry_;
  std::shared_ptr<clock::Clock> clock_;
};

} // namespace log
} // namespace kudu

#endif
