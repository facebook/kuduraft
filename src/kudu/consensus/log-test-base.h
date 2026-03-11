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
        log_anchor_registry_(new LogAnchorRegistry) {}

  void SetUp() override {
    KuduTest::SetUp();
    current_index_ = kStartIndex;
    fs_manager_.reset(new FsManager(env_, GetTestPath("fs_root")));
    metric_registry_.reset(new MetricRegistry());
    metric_entity_ = METRIC_ENTITY_server.Instantiate(
        metric_registry_.get(), "log-test-base");
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());

    clock_.reset(new clock::HybridClock());
    ASSERT_OK(clock_->init());
  }

  void TearDown() override {
    KuduTest::TearDown();
  }

  Status BuildLog() {
    Schema schemaWithIds = SchemaBuilder(schema_).Build();
    return Log::Open(
        options_,
        fs_manager_.get(),
        kTestTablet,
        schemaWithIds,
        0, // schema_version
        metric_entity_,
        &log_);
  }

  void CheckRightNumberOfSegmentFiles(int expected) {
    // Test that we actually have the expected number of files in the fs.
    // We should have n segments plus '.' and '..'
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(
        JoinPathSegments(fs_manager_->GetWalsRootDir(), kTestTablet), &files));
    int count = 0;
    for (const std::string& s : files) {
      if (hasPrefixString(s, FsManager::kWalFileNamePrefix)) {
        count++;
      }
    }
    ASSERT_EQ(expected, count);
  }

  void EntriesToIdList(std::vector<uint32_t>* ids) {
    for (const auto& entry : entries_) {
      VLOG(2) << "Entry contents: " << pb_util::SecureDebugString(*entry);
      if (entry->type() == REPLICATE) {
        ids->push_back(entry->replicate().id().index());
      }
    }
  }

  static void CheckReplicateResult(
      const consensus::ReplicateRefPtr& msg,
      const Status& s) {
    CHECK_OK(s);
  }

  // Appends a batch with size 2 (1 insert, 1 mutate) to the log.
  Status AppendReplicateBatch(
      const consensus::OpId& opid,
      bool sync = kAppendSync) {
    consensus::ReplicateRefPtr replicate = makeScopedRefptrReplicate(
        new consensus::ReplicateMsg(), Source::Memory);
    replicate->get()->set_op_type(consensus::WRITE_OP);
    replicate->get()->mutable_id()->CopyFrom(opid);
    replicate->get()->set_timestamp(clock_->now().toUint64());
    tserver::WriteRequestPB* batch_request =
        replicate->get()->mutable_write_request();
    RETURN_NOT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
    addTestRowToPb(
        RowOperationsPB::INSERT,
        schema_,
        opid.index(),
        0,
        "this is a test insert",
        batch_request->mutable_row_operations());
    addTestRowToPb(
        RowOperationsPB::UPDATE,
        schema_,
        opid.index() + 1,
        0,
        "this is a test mutate",
        batch_request->mutable_row_operations());
    batch_request->set_tablet_id(kTestTablet);
    return AppendReplicateBatch(replicate, sync);
  }

  // Appends the provided batch to the log.
  Status AppendReplicateBatch(
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
        {replicate}, Bind(&LogTestBase::CheckReplicateResult, replicate));
  }

  static void CheckCommitResult(const Status& s) {
    CHECK_OK(s);
  }

  // Append a commit log entry containing one entry for the insert and one
  // for the mutate.
  Status AppendCommit(
      const consensus::OpId& originalOpId,
      bool sync = kAppendSync) {
    // The mrs id for the insert.
    constexpr int kTargetMrsId = 1;

    // The rs and delta ids for the mutate.
    constexpr int kTargetRsId = 0;
    constexpr int kTargetDeltaId = 0;

    return AppendCommit(
        originalOpId, kTargetMrsId, kTargetRsId, kTargetDeltaId, sync);
  }

  Status AppendCommit(
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
    return AppendCommit(std::move(commit), sync);
  }

  // Append a COMMIT message for 'originalOpId', but with results
  // indicating that the associated writes failed due to
  // "NotFound" errors.
  Status AppendCommitWithNotFoundOpResults(
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

    return AppendCommit(std::move(commit));
  }

  Status AppendCommit(
      std::unique_ptr<consensus::CommitMsg> commit,
      bool sync = kAppendSync) {
    if (sync) {
      Synchronizer s;
      RETURN_NOT_OK(
          log_->AsyncAppendCommit(std::move(commit), s.asStatusCallback()));
      return s.wait();
    }
    return log_->AsyncAppendCommit(
        std::move(commit), Bind(&LogTestBase::CheckCommitResult));
  }

  // Appends 'count' ReplicateMsgs and the corresponding CommitMsgs to the log
  Status AppendReplicateBatchAndCommitEntryPairsToLog(
      int count,
      bool sync = kAppendSync) {
    for (int i = 0; i < count; i++) {
      consensus::OpId opid = consensus::MakeOpId(1, current_index_);
      RETURN_NOT_OK(AppendReplicateBatch(opid));
      RETURN_NOT_OK(AppendCommit(opid, sync));
      current_index_ += 1;
    }
    return Status::OK();
  }

  // Append a single NO_OP entry. Increments op_id by one.
  // If non-NULL, and if the write is successful, 'size' is incremented
  // by the size of the written operation.
  Status AppendNoOp(consensus::OpId* op_id, int* size = nullptr) {
    return appendNoOpToLogSync(clock_, log_.get(), op_id, size);
  }

  // Append a number of no-op entries to the log.
  // Increments op_id's index by the number of records written.
  // If non-NULL, 'size' keeps track of the size of the operations
  // successfully written.
  Status AppendNoOps(consensus::OpId* op_id, int num, int* size = nullptr) {
    for (int i = 0; i < num; i++) {
      RETURN_NOT_OK(AppendNoOp(op_id, size));
    }
    return Status::OK();
  }

  Status RollLog() {
    RETURN_NOT_OK(log_->AsyncAllocateSegment());
    return log_->RollOver();
  }

  std::string DumpSegmentsToString(const SegmentSequence& segments) {
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
  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<MetricRegistry> metric_registry_;
  std::shared_ptr<MetricEntity> metric_entity_;
  std::shared_ptr<Log> log_;
  int64_t current_index_;
  LogOptions options_;
  // Reusable entries vector that deletes the entries on destruction.
  LogEntries entries_;
  std::shared_ptr<LogAnchorRegistry> log_anchor_registry_;
  std::shared_ptr<clock::Clock> clock_;
};

} // namespace log
} // namespace kudu

#endif
