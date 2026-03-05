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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/clock/clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/port.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(num_writer_threads, 4, "Number of threads writing to the log");
DEFINE_int32(
    num_reader_threads,
    1,
    "Number of threads accessing the log while writes are ongoing");
DEFINE_int32(num_batches_per_thread, 2000, "Number of batches per thread");
DEFINE_int32(
    num_ops_per_batch_avg,
    5,
    "Target average number of ops per batch");
DEFINE_bool(
    verify_log,
    true,
    "Whether to verify the log by reading it after the writes complete");

DECLARE_int32(log_thread_idle_threshold_ms);
DECLARE_int32(log_inject_thread_lifecycle_latency_ms);

namespace kudu {
namespace log {

using consensus::makeScopedRefptrReplicate;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::ReplicateRefPtr;
using consensus::WRITE_OP;
using std::shared_ptr;
using std::vector;

namespace {

class CustomLatchCallback
    : public std::enable_shared_from_this<CustomLatchCallback> {
 public:
  CustomLatchCallback(CountDownLatch* latch, vector<Status>* errors)
      : latch_(latch), errors_(errors) {}

  void statusCb(const Status& s) {
    if (!s.ok()) {
      errors_->push_back(s);
    }
    latch_->countDown();
  }

  StatusCallback asStatusCallback() {
    return Bind(&CustomLatchCallback::statusCb, this);
  }

 private:
  CountDownLatch* latch_;
  vector<Status>* errors_;
};

} // anonymous namespace

class MultiThreadedLogTest : public LogTestBase {
 public:
  MultiThreadedLogTest() : random_(SeedRandom()) {}

  virtual void SetUp() override {
    LogTestBase::SetUp();
  }

  vector<consensus::ReplicateRefPtr> createRandomBatch() {
    int numOps = static_cast<int>(
        random_.Normal(static_cast<double>(FLAGS_num_ops_per_batch_avg), 1.0));
    DVLOG(1) << numOps << " ops in this batch";
    numOps = std::max(numOps, 1);
    vector<consensus::ReplicateRefPtr> ret;
    for (int j = 0; j < numOps; j++) {
      ReplicateRefPtr replicate =
          makeScopedRefptrReplicate(new ReplicateMsg, Source::Memory);
      replicate->get()->set_op_type(WRITE_OP);
      replicate->get()->set_timestamp(clock_->now().toUint64());
      tserver::WriteRequestPB* request =
          replicate->get()->mutable_write_request();
      addTestRowToPb(
          RowOperationsPB::INSERT,
          schema_,
          12345,
          0,
          "this is a test insert",
          request->mutable_row_operations());
      request->set_tablet_id(kTestTablet);
      ret.push_back(replicate);
    }
    return ret;
  }

  void assignIndexes(vector<consensus::ReplicateRefPtr>* batch) {
    for (auto& rep : *batch) {
      OpId* opId = rep->get()->mutable_id();
      opId->set_term(0);
      opId->set_index(current_index_++);
    }
  }

  void logWriterThread(int threadId) {
    CountDownLatch latch(FLAGS_num_batches_per_thread);
    vector<Status> errors;
    for (int i = 0; i < FLAGS_num_batches_per_thread; i++) {
      // Do the expensive allocation outside the lock.
      vector<consensus::ReplicateRefPtr> batchReplicates = createRandomBatch();
      auto cb = std::make_shared<CustomLatchCallback>(&latch, &errors);
      // Assign indexes and append inside the lock, so that the index order and
      // log order match up.
      {
        std::lock_guard<simple_spinlock> l(lock_);
        assignIndexes(&batchReplicates);
        ASSERT_OK(log_->AsyncAppendReplicates(
            batchReplicates, cb->asStatusCallback()));
      }
      MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
    }
    latch.wait();
    for (const Status& status : errors) {
      WARN_NOT_OK(status, "Unexpected failure during AsyncAppend");
    }
    CHECK_EQ(0, errors.size());
  }

  void run() {
    for (int i = 0; i < FLAGS_num_writer_threads; i++) {
      std::shared_ptr<kudu::Thread> newThread;
      CHECK_OK(
          kudu::Thread::Create(
              "test",
              "inserter",
              &MultiThreadedLogTest::logWriterThread,
              this,
              i,
              &newThread));
      threads_.push_back(newThread);
    }

    // Start a thread which calls some read-only methods on the log
    // to check for races against writers.
    std::atomic<bool> stopReader(false);
    vector<std::thread> readerThreads;
    for (int i = 0; i < FLAGS_num_reader_threads; i++) {
      readerThreads.emplace_back([&]() {
        std::map<int64_t, int64_t> map;
        while (!stopReader) {
          log_->GetReplaySizeMap(&map);
          log_->GetGCableDataSize(
              RetentionIndexes(FLAGS_num_batches_per_thread));
        }
      });
    }

    // Wait for the writers to finish.
    for (std::shared_ptr<kudu::Thread>& thread : threads_) {
      ASSERT_OK(ThreadJoiner(thread.get()).Join());
    }

    // Then stop the reader and join on it as well.
    stopReader = true;
    for (auto& t : readerThreads) {
      t.join();
    }
  }

  void verifyLog() {
    shared_ptr<LogReader> reader;
    ASSERT_OK(
        LogReader::Open(
            fs_manager_.get(), nullptr, kTestTablet, nullptr, &reader));
    SegmentSequence segments;
    ASSERT_OK(reader->getSegmentsSnapshot(&segments));

    for (const SegmentSequence::value_type& entry : segments) {
      ASSERT_OK(entry->readEntries(&entries_));
    }
    vector<uint32_t> ids;
    EntriesToIdList(&ids);
    DVLOG(1) << "Wrote total of " << current_index_ - kStartIndex << " ops";
    ASSERT_EQ(current_index_ - kStartIndex, ids.size());
    ASSERT_TRUE(std::is_sorted(ids.begin(), ids.end()));
  }

 private:
  ThreadSafeRandom random_;
  simple_spinlock lock_;
  vector<std::shared_ptr<kudu::Thread>> threads_;
};

TEST_F(MultiThreadedLogTest, TestAppends) {
  // Roll frequently to stress related code paths, unless overridden
  // on the command line.
  if (gflags::GetCommandLineFlagInfoOrDie("log_segment_size_mb").is_default) {
    options_.segment_size_mb = 1;
  }

  ASSERT_OK(BuildLog());
  LOG_TIMING(
      INFO,
      fmt::format(
          "inserting {} batches({} threads, {} per-thread)",
          FLAGS_num_writer_threads * FLAGS_num_batches_per_thread,
          FLAGS_num_writer_threads,
          FLAGS_num_batches_per_thread)) {
    ASSERT_NO_FATAL_FAILURE(run());
  }
  ASSERT_OK(log_->Close());
  if (FLAGS_verify_log) {
    ASSERT_NO_FATAL_FAILURE(verifyLog());
  }
}

// The lifecycle of the appender task starting and stopping is a bit complicated
// (see Log::AppendThread::GoIdle for details). This injects some latency in key
// points of that lifecycle to ensure that the different potential interleavings
// are triggered. It also injects latency into the writes done by the tests, so
// that sometimes writes are spaced out enough to allow the thread to go idle.
TEST_F(MultiThreadedLogTest, TestAppendThreadStartStopRaces) {
  FLAGS_log_thread_idle_threshold_ms = 1;
  FLAGS_log_inject_thread_lifecycle_latency_ms = 2;
  ASSERT_OK(BuildLog());
  logWriterThread(1);
  ASSERT_OK(log_->Close());
  ASSERT_NO_FATAL_FAILURE(verifyLog());
}

} // namespace log
} // namespace kudu
