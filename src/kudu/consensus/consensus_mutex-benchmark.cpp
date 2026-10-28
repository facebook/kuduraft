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

// The cost of a lock/unlock pair of consensusStateLock_ as a plain
// simple_mutexlock, as a ConsensusMutex without a hook, with the status
// publication hook when nothing changed (its steady state), and with a hook
// that publishes at every unlock (its worst case), uncontended and with
// several threads.

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/concurrency/AtomicSharedPtr.h>
#include <folly/init/Init.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/util/locks.h"

namespace kudu::consensus {
namespace {

RaftConfigPB fiveVoterConfig() {
  RaftConfigPB config;
  config.set_opid_index(42);
  for (int i = 0; i < 5; i++) {
    auto* peer = config.add_peers();
    peer->set_permanent_uuid(std::string(32, static_cast<char>('a' + i)));
    peer->set_member_type(RaftPeerPB::VOTER);
    peer->mutable_last_known_addr()->set_host("benchmark-host.example.com");
    peer->mutable_last_known_addr()->set_port(3306);
    peer->mutable_attrs()->set_quorum_id("quorum-region-0");
    peer->mutable_attrs()->set_region("region");
  }
  return config;
}

// What RaftConsensus's unlock hook touches: the two generation counters, their
// values at the last publication, and the published snapshot.
// Only the hook uses it, and the hook runs with the mutex held, so the plain
// members need no other synchronization, as in RaftConsensus.
class StatusPublisher {
 public:
  StatusPublisher() : config_(fiveVoterConfig()) {}

  // The steady state: nothing changed since the last publication.
  void publishIfChanged() {
    const Generations generations{
        .cmeta = cmetaGeneration_.load(std::memory_order_relaxed),
        .local = localGeneration_};
    if (publishedGenerations_ == generations) {
      return;
    }
    publish(generations);
  }

  // The worst case: a new snapshot at every unlock.
  void publishAlways() {
    publish(
        {.cmeta = cmetaGeneration_.fetch_add(1, std::memory_order_relaxed) + 1,
         .local = localGeneration_});
  }

 private:
  // Mirrors RaftConsensus::StatusGenerations, which is private.
  struct Generations {
    int64_t cmeta{0};
    int64_t local{0};
    bool operator==(const Generations&) const = default;
  };

  void publish(Generations generations) {
    auto snapshot = std::make_shared<RaftConsensus::StatusSnapshot>();
    snapshot->role = RaftPeerPB::LEADER;
    snapshot->currentTerm = 7;
    snapshot->leaderUuid = config_.peers(0).permanent_uuid();
    snapshot->leaderHostPort = {"benchmark-host.example.com", 3306};
    snapshot->committedConfig = config_;
    snapshot->peerQuorumId = "quorum-region-0";
    snapshot->voterDistribution = {{"quorum-region-0", 5}};
    snapshot->voterDistributionValid = true;
    snapshot->proxyPolicy = "SIMPLE_REGION_POLICY";
    published_.store(std::move(snapshot));
    publishedGenerations_ = generations;
  }

  const RaftConfigPB config_;
  std::atomic<int64_t> cmetaGeneration_{0};
  int64_t localGeneration_{0};
  std::optional<Generations> publishedGenerations_;
  folly::atomic_shared_ptr<const RaftConsensus::StatusSnapshot> published_;
};

// Runs 'iters' lock/unlock pairs of 'mutex', split across 'threads' threads.
template <typename Mutex>
void lockUnlock(Mutex& mutex, uint32_t iters, size_t threads) {
  folly::BenchmarkSuspender suspender;
  std::atomic<bool> go{false};
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (size_t t = 0; t < threads; t++) {
    // The first iters % threads threads run one extra pair, so all 'iters' run.
    const uint32_t count = iters / threads + (t < iters % threads ? 1 : 0);
    workers.emplace_back([&, count] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (uint32_t i = 0; i < count; i++) {
        mutex.lock();
        folly::doNotOptimizeAway(&mutex);
        mutex.unlock();
      }
    });
  }
  suspender.dismiss();
  go.store(true, std::memory_order_release);
  for (auto& worker : workers) {
    worker.join();
  }
}

void simpleMutexlock(uint32_t iters, size_t threads) {
  simple_mutexlock mutex;
  lockUnlock(mutex, iters, threads);
}

void consensusMutexNoHook(uint32_t iters, size_t threads) {
  ConsensusMutex mutex;
  lockUnlock(mutex, iters, threads);
}

void consensusMutexHookNothingChanged(uint32_t iters, size_t threads) {
  StatusPublisher publisher;
  ConsensusMutex mutex;
  mutex.lock();
  mutex.setUnlockHook([&publisher] { publisher.publishIfChanged(); });
  mutex.unlock();
  lockUnlock(mutex, iters, threads);
}

void consensusMutexHookPublishesAlways(uint32_t iters, size_t threads) {
  StatusPublisher publisher;
  ConsensusMutex mutex;
  mutex.lock();
  mutex.setUnlockHook([&publisher] { publisher.publishAlways(); });
  mutex.unlock();
  lockUnlock(mutex, iters, threads);
}

BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(simpleMutexlock, 1)
BENCHMARK_RELATIVE_PARAM(consensusMutexNoHook, 1)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookNothingChanged, 1)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookPublishesAlways, 1)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(simpleMutexlock, 2)
BENCHMARK_RELATIVE_PARAM(consensusMutexNoHook, 2)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookNothingChanged, 2)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookPublishesAlways, 2)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(simpleMutexlock, 4)
BENCHMARK_RELATIVE_PARAM(consensusMutexNoHook, 4)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookNothingChanged, 4)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookPublishesAlways, 4)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(simpleMutexlock, 8)
BENCHMARK_RELATIVE_PARAM(consensusMutexNoHook, 8)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookNothingChanged, 8)
BENCHMARK_RELATIVE_PARAM(consensusMutexHookPublishesAlways, 8)

} // namespace
} // namespace kudu::consensus

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
