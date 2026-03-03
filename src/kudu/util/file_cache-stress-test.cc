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

#include "kudu/util/file_cache.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fmt/core.h>
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/file_cache-test-util.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(test_num_producer_threads, 1, "Number of producer threads");
DEFINE_int32(test_num_consumer_threads, 4, "Number of consumer threads");
DEFINE_int32(test_duration_secs, 2, "Number of seconds to run the test");

DECLARE_bool(cache_force_single_shard);

using std::deque;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {

// FD limit to enforce during the test.
static const int kTestMaxOpenFiles = 100;

template <class FileType>
class FileCacheStressTest : public KuduTest {
// Like CHECK_OK(), but dumps the contents of the cache before failing.
//
// The output of ToDebugString() tends to be long enough that LOG() truncates
// it, so we must split it ourselves before logging.
#define TEST_CHECK_OK(to_call)                                                 \
  do {                                                                         \
    const Status& _s = (to_call);                                              \
    if (!_s.ok()) {                                                            \
      LOG(INFO) << "Dumping cache contents";                                   \
      vector<string> lines =                                                   \
          strings::Split(cache_->toDebugString(), "\n", strings::SkipEmpty()); \
      for (const auto& l : lines) {                                            \
        LOG(INFO) << l;                                                        \
      }                                                                        \
    }                                                                          \
    CHECK(_s.ok()) << "Bad status: " << _s.ToString();                         \
  } while (0);

 public:
  using MetricMap = unordered_map<string, unordered_map<string, int>>;

  FileCacheStressTest() : rand_(SeedRandom()), running_(1) {
    // Use a single shard. Otherwise, the cache can be a little bit "sloppy"
    // depending on the number of CPUs on the system.
    FLAGS_cache_force_single_shard = true;
    cache_.reset(new FileCache<FileType>(
        "test", env_, kTestMaxOpenFiles, std::shared_ptr<MetricEntity>()));
  }

  void SetUp() override {
    ASSERT_OK(cache_->init());
  }

  void producerThread() {
    Random rand(rand_.Next32());
    ObjectIdGenerator oidGenerator;
    MetricMap metrics;

    do {
      // Create a new file with some (0-32k) random data in it.
      string nextFileName = GetTestPath(oidGenerator.next());
      {
        unique_ptr<WritableFile> nextFile;
        CHECK_OK(env_->NewWritableFile(nextFileName, &nextFile));
        uint8_t buf[rand.Uniform((32 * 1024) - 1) + 1];
        CHECK_OK(
            nextFile->Append(generateRandomChunk(buf, sizeof(buf), &rand)));
        CHECK_OK(nextFile->Close());
      }
      {
        std::lock_guard<simple_spinlock> l(lock_);
        auto [it, inserted] = availableFiles_.insert({nextFileName, 0});
        CHECK(inserted);
      }
      metrics[BaseName(nextFileName)]["create"] = 1;
    } while (!running_.WaitFor(MonoDelta::FromMilliseconds(1)));

    // Update the global metrics map.
    mergeNewMetrics(std::move(metrics));
  }

  void consumerThread() {
    // Each thread has its own PRNG to minimize contention on the main one.
    Random rand(rand_.Next32());

    // Active opened files in this thread.
    deque<shared_ptr<FileType>> files;

    // Metrics generated by this thread. They will be merged into the main
    // metrics map when the thread is done.
    MetricMap metrics;

    do {
      // Pick an action to perform. Distribution:
      // 20% open
      // 15% close
      // 35% read
      // 20% write
      // 10% delete
      int nextAction = rand.Uniform(100);

      if (nextAction < 20) {
        // Open an existing file.
        string toOpen;
        if (!getRandomFile(kOpen, &rand, &toOpen)) {
          continue;
        }
        shared_ptr<FileType> newFile;
        TEST_CHECK_OK(cache_->openExistingFile(toOpen, &newFile));
        finishedOpen(toOpen);
        metrics[BaseName(toOpen)]["open"]++;
        files.emplace_back(newFile);
      } else if (nextAction < 35) {
        // Close a file.
        if (files.empty()) {
          continue;
        }
        shared_ptr<FileType> file = files.front();
        files.pop_front();
        metrics[BaseName(file->filename())]["close"]++;
      } else if (nextAction < 70) {
        // Read a random chunk from a file.
        TEST_CHECK_OK(readRandomChunk(files, &metrics, &rand));
      } else if (nextAction < 90) {
        // Write a random chunk to a file.
        TEST_CHECK_OK(writeRandomChunk(files, &metrics, &rand));
      } else if (nextAction < 100) {
        // Delete a file.
        string toDelete;
        if (!getRandomFile(kDelete, &rand, &toDelete)) {
          continue;
        }
        TEST_CHECK_OK(cache_->deleteFile(toDelete));
        metrics[BaseName(toDelete)]["delete"]++;
      }
    } while (!running_.WaitFor(MonoDelta::FromMilliseconds(1)));

    // Update the global metrics map.
    mergeNewMetrics(std::move(metrics));
  }

 protected:
  void notifyThreads() {
    running_.CountDown();
  }

  const MetricMap& metrics() const {
    return metrics_;
  }

 private:
  enum GetMode { kOpen, kDelete };

  // Retrieve a random file name to be either opened or deleted. If deleting,
  // the file name is made inaccessible to future operations.
  bool getRandomFile(GetMode mode, Random* rand, string* out) {
    std::lock_guard<simple_spinlock> l(lock_);
    if (availableFiles_.empty()) {
      return false;
    }

    // This is linear time, but it's simpler than managing multiple data
    // structures.
    auto it = availableFiles_.begin();
    std::advance(it, rand->Uniform(availableFiles_.size()));

    // It's unsafe to delete a file that is still being opened.
    if (mode == kDelete && it->second > 0) {
      return false;
    }

    *out = it->first;
    if (mode == kOpen) {
      it->second++;
    } else {
      availableFiles_.erase(it);
    }
    return true;
  }

  // Signal that a previously in-progress open has finished, allowing the file
  // in question to be deleted.
  void finishedOpen(const string& opened) {
    std::lock_guard<simple_spinlock> l(lock_);
    auto it = availableFiles_.find(opened);
    CHECK(it != availableFiles_.end()) << "Map key not found: " << opened;
    int& openers = it->second;
    openers--;
  }

  // Reads a random chunk of data from a random file in 'files'. On success,
  // writes to 'metrics'.
  static Status readRandomChunk(
      const deque<shared_ptr<FileType>>& files,
      MetricMap* metrics,
      Random* rand) {
    if (files.empty()) {
      return Status::OK();
    }
    const shared_ptr<FileType>& file = files[rand->Uniform(files.size())];

    uint64_t fileSize;
    RETURN_NOT_OK(file->Size(&fileSize));
    uint64_t off = fileSize > 0 ? rand->Uniform(fileSize) : 0;
    size_t len = fileSize > 0 ? rand->Uniform(fileSize - off) : 0;
    unique_ptr<uint8_t[]> scratch(new uint8_t[len]);
    RETURN_NOT_OK(file->Read(off, Slice(scratch.get(), len)));

    (*metrics)[BaseName(file->filename())]["read"]++;
    return Status::OK();
  }

  // Writes a random chunk of data to a random file in 'files'. On success,
  // updates 'metrics'.
  //
  // No-op for file implementations that don't support writing.
  static Status writeRandomChunk(
      const deque<shared_ptr<FileType>>& files,
      MetricMap* metrics,
      Random* rand);

  static Slice
  generateRandomChunk(uint8_t* buffer, size_t maxLength, Random* rand) {
    size_t len = rand->Uniform(maxLength);
    len -= len % sizeof(uint32_t);
    for (int i = 0; i < (len / sizeof(uint32_t)); i += sizeof(uint32_t)) {
      reinterpret_cast<uint32_t*>(buffer)[i] = rand->Next32();
    }
    return Slice(buffer, len);
  }

  // Merge the metrics in 'newMetrics' into the global metric map.
  void mergeNewMetrics(MetricMap newMetrics) {
    std::lock_guard<simple_spinlock> l(lock_);
    for (const auto& fileActionPair : newMetrics) {
      for (const auto& actionCountPair : fileActionPair.second) {
        metrics_[fileActionPair.first][actionCountPair.first] +=
            actionCountPair.second;
      }
    }
  }

  unique_ptr<FileCache<FileType>> cache_;

  // Used to seed per-thread PRNGs.
  ThreadSafeRandom rand_;

  // Drops to zero when the test ends.
  CountDownLatch running_;

  // Protects 'availableFiles_' and 'metrics_'.
  simple_spinlock lock_;

  // Contains files produced by producer threads and ready for consumption by
  // consumer threads.
  //
  // Each entry is a file name and the number of in-progress openers. To delete
  // a file, there must be no openers.
  unordered_map<string, int> availableFiles_;

  // For each file name, tracks the count of consumer actions performed.
  //
  // Only updated at test end.
  MetricMap metrics_;
};

template <>
Status FileCacheStressTest<RWFile>::writeRandomChunk(
    const deque<shared_ptr<RWFile>>& files,
    MetricMap* metrics,
    Random* rand) {
  if (files.empty()) {
    return Status::OK();
  }
  const shared_ptr<RWFile>& file = files[rand->Uniform(files.size())];

  uint64_t fileSize;
  RETURN_NOT_OK(file->Size(&fileSize));
  uint64_t off = fileSize > 0 ? rand->Uniform(fileSize) : 0;
  uint8_t buf[64];
  RETURN_NOT_OK(file->Write(off, generateRandomChunk(buf, sizeof(buf), rand)));
  (*metrics)[BaseName(file->filename())]["write"]++;
  return Status::OK();
}

template <>
Status FileCacheStressTest<RandomAccessFile>::writeRandomChunk(
    const deque<shared_ptr<RandomAccessFile>>& /* unused */,
    MetricMap* /* unused */,
    Random* /* unused */) {
  return Status::OK();
}

using FileTypes = ::testing::Types<RWFile, RandomAccessFile>;
TYPED_TEST_CASE(FileCacheStressTest, FileTypes);

TYPED_TEST(FileCacheStressTest, TestStress) {
  OverrideFlagForSlowTests("test_num_producer_threads", "2");
  OverrideFlagForSlowTests("test_num_consumer_threads", "8");
  OverrideFlagForSlowTests("test_duration_secs", "30");

  // Start the threads.
  PeriodicOpenFdChecker checker(
      this->env_,
      this->GetTestPath("*"), // only count within our test dir
      kTestMaxOpenFiles + // cache capacity
          FLAGS_test_num_producer_threads + // files being written
          FLAGS_test_num_consumer_threads); // files being opened
  checker.start();
  vector<thread> producers;
  for (int i = 0; i < FLAGS_test_num_producer_threads; i++) {
    producers.emplace_back(
        &FileCacheStressTest<TypeParam>::producerThread, this);
  }
  vector<thread> consumers;
  for (int i = 0; i < FLAGS_test_num_consumer_threads; i++) {
    consumers.emplace_back(
        &FileCacheStressTest<TypeParam>::consumerThread, this);
  }

  // Let the test run.
  SleepFor(MonoDelta::FromSeconds(FLAGS_test_duration_secs));

  // Stop the threads.
  this->notifyThreads();
  checker.stop();
  for (auto& p : producers) {
    p.join();
  }
  for (auto& c : consumers) {
    c.join();
  }

  // Log the metrics.
  unordered_map<string, int> actionCounts;
  for (const auto& fileActionPair : this->metrics()) {
    for (const auto& actionCountPair : fileActionPair.second) {
      VLOG(2) << fmt::format(
          "{}: {}: {}",
          fileActionPair.first,
          actionCountPair.first,
          actionCountPair.second);
      actionCounts[actionCountPair.first] += actionCountPair.second;
    }
  }
  for (const auto& actionCountPair : actionCounts) {
    LOG(INFO) << fmt::format(
        "{}: {}", actionCountPair.first, actionCountPair.second);
  }
}

} // namespace kudu
