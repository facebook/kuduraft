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

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/barrier.h"
#include "kudu/util/locks.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

#include <folly/ScopeGuard.h>

using std::atomic;
using std::lock_guard;
using std::string;
using std::thread;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace consensus {

static constexpr const int64_t kInitialTerm = 1;

using LockTable = unordered_map<string, string>;

// Multithreaded stress tests for the cmeta manager.
class ConsensusMetadataManagerStressTest : public KuduTest {
 public:
  ConsensusMetadataManagerStressTest()
      : rng_(SeedRandom()),
        fs_manager_(env_, GetTestPath("fs_root")),
        cmeta_manager_(
            std::make_shared<ConsensusMetadataManager>(&fs_manager_)) {}

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(fs_manager_.CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_.Open());

    // Initialize test configuration.
    config_.set_opid_index(kInvalidOpIdIndex);
    RaftPeerPB* peer = config_.add_peers();
    peer->set_permanent_uuid(fs_manager_.uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

 protected:
  enum OpType {
    kCreate,
    kLoad,
    kDelete,
    kNumOpTypes, // Must come last.
  };

  ThreadSafeRandom rng_;
  FsManager fs_manager_;
  std::shared_ptr<ConsensusMetadataManager> cmeta_manager_;
  RaftConfigPB config_;

  // Lock used by tests.
  simple_spinlock lock_;
};

// Concurrency test to check whether TSAN will flag unsafe concurrent
// operations for simultaneous access to the cmeta manager by different threads
// on different tablet ids. For a given tablet id, a lock table is used as
// external synchronization to ensure exclusive access by a single thread.
TEST_F(ConsensusMetadataManagerStressTest, CreateLoadDeleteTSANTest) {
  static const int kNumTablets = 26;
  static const int kNumThreads = 8;
  static const int kNumOpsPerThread = 1000;

  // Set of tablets we are operating on.
  vector<string> tabletIds;

  // Map of tabletId -> cmeta existence.
  unordered_map<string, bool> tabletCmetaExists;

  // Each entry in 'lockTable' protects each value of
  // 'tabletCmetaExists[tabletId]'. We never resize 'tabletCmetaExists'.
  LockTable lockTable;

  for (int i = 0; i < kNumTablets; i++) {
    string tabletId = string(1, 'a' + i);
    // None of the cmetas have been created yet.
    auto [it, inserted] = tabletCmetaExists.insert({tabletId, false});
    CHECK(inserted);
    tabletIds.push_back(std::move(tabletId));
  }

  // Eventually exit if the test hangs.
  alarm(60);
  auto c = folly::makeGuard([&] { alarm(0); });

  atomic<int64_t> opsPerformed(0);
  Barrier barrier(kNumThreads);
  vector<thread> threads;
  for (int threadNum = 0; threadNum < kNumThreads; threadNum++) {
    threads.emplace_back([&] {
      barrier.Wait();
      for (int opNum = 0; opNum < kNumOpsPerThread; opNum++) {
        const string& tabletId = tabletIds[rng_.Uniform(kNumTablets)];
        auto unlocker = folly::makeGuard([&] {
          lock_guard<simple_spinlock> l(lock_);
          CHECK(lockTable.erase(tabletId));
        });
        // Acquire lock in lock table or bail.
        {
          // 'lock_' protects 'lockTable'.
          lock_guard<simple_spinlock> l(lock_);
          if (lockTable.contains(tabletId)) {
            // Another thread has access to this tablet id. Bail.
            unlocker.dismiss(); // Don't unlock what we didn't lock.
            continue;
          }
          auto [it, inserted] = lockTable.insert({tabletId, "lock for test"});
          CHECK(inserted);
        }
        OpType type = static_cast<OpType>(rng_.Uniform(kNumOpTypes));
        switch (type) {
          case kCreate: {
            Status s =
                cmeta_manager_->createCMeta(tabletId, config_, kInitialTerm);
            if (tabletCmetaExists[tabletId]) {
              CHECK(s.IsAlreadyPresent()) << s.ToString();
            } else {
              CHECK(s.ok()) << s.ToString();
              opsPerformed.fetch_add(1, std::memory_order_relaxed);
            }
            tabletCmetaExists[tabletId] = true;
            break;
          }
          case kLoad: {
            std::shared_ptr<ConsensusMetadata> cmeta;
            Status s = cmeta_manager_->loadCMeta(tabletId, &cmeta);
            if (tabletCmetaExists[tabletId]) {
              CHECK(s.ok()) << s.ToString();
              opsPerformed.fetch_add(1, std::memory_order_relaxed);
            } else {
              CHECK(s.IsNotFound()) << tabletId << ": " << s.ToString();
            }
            // Load() does not change 'tabletCmetaExists' status.
            break;
          }
          case kDelete: {
            Status s = cmeta_manager_->deleteCMeta(tabletId);
            if (tabletCmetaExists[tabletId]) {
              CHECK(s.ok()) << s.ToString();
              opsPerformed.fetch_add(1, std::memory_order_relaxed);
            } else {
              CHECK(s.IsNotFound()) << s.ToString();
            }
            tabletCmetaExists[tabletId] = false;
            break;
          }
          default:
            LOG(FATAL) << type;
        }
      }
    });
  }

  for (int threadNum = 0; threadNum < kNumThreads; threadNum++) {
    threads[threadNum].join();
  }

  LOG(INFO) << "Ops performed: "
            << opsPerformed.load(std::memory_order_relaxed);
}

} // namespace consensus
} // namespace kudu
