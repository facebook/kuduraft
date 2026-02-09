// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <folly/Benchmark.h>
#include <glog/logging.h>

#include "common/init/Init.h"

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/routing.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace consensus {

namespace {

constexpr const char* kLeaderUuid = "peer-0";
constexpr const char* kTestTablet = "test-tablet";
// constexpr const char* kLeaderQuorumId = "r0";

// Returns RaftPeerPB with given UUID and obviously-fake hostname / port combo.
inline RaftPeerPB fakeRaftPeerPb(const std::string& uuid) {
  RaftPeerPB peerPb;
  peerPb.set_permanent_uuid(uuid);
  peerPb.set_member_type(RaftPeerPB::VOTER);
  peerPb.mutable_last_known_addr()->set_host("benchmark-fake-hostname");
  peerPb.mutable_last_known_addr()->set_port(0);
  // Large simulate large RaftPeerPb in production
  peerPb.mutable_attrs()->set_quorum_id(std::string(5000, 'q'));
  return peerPb;
}

class ConsensusQueueBenchmark {
 public:
  ConsensusQueueBenchmark()
      : env_(Env::Default()),
        metricEntity_(
            METRIC_ENTITY_server.Instantiate(&metricRegistry_, "queue-bench")),
        registry_(new log::LogAnchorRegistry) {
    SetUmask();
  }

  void setUp() {
    // Create a unique temporary directory for this benchmark run
    // Add random component to avoid collisions if running multiple benchmarks
    testDir_ = "/tmp/consensus_queue_bench_" + std::to_string(getpid()) + "_" +
        std::to_string(env_->NowMicros()) + "_" + std::to_string(rand());

    // Forcibly remove any leftover directory using system commands
    // This handles permission issues that env_->DeleteRecursively might fail on
    string cleanupCmd = "rm -rf " + testDir_ + " 2>/dev/null || true";
    ignoreResult(system(cleanupCmd.c_str()));

    CHECK_OK(env_->CreateDir(testDir_));

    fsManager_.reset(new FsManager(env_, testDir_ + "/fs_root"));
    CHECK_OK(fsManager_->CreateInitialFileSystemLayout());
    CHECK_OK(fsManager_->Open());

    log_ = std::make_shared<StatefulMockLog>(
        log::LogOptions(), fsManager_.get(), "", kTestTablet, nullptr);

    RaftConfigPB raftConfig = BuildRaftConfigPBForTests(50, 50);
    CHECK_OK(
        DurableRoutingTable::create(
            fsManager_.get(), kTestTablet, raftConfig, {}, &routingTable_));

    persistentVarsManager_ =
        std::make_shared<PersistentVarsManager>(fsManager_.get());
    CHECK_OK(persistentVarsManager_->CreatePersistentVars(kTestTablet));

    routingTableContainer_ = std::make_shared<RoutingTableContainer>(
        ProxyPolicy::DURABLE_ROUTING_POLICY,
        fakeRaftPeerPb(kLeaderUuid),
        raftConfig,
        routingTable_,
        std::vector<std::unordered_set<std::string>>());

    clock_ = std::make_shared<clock::HybridClock>();
    CHECK_OK(clock_->Init());

    CHECK_OK(ThreadPoolBuilder("raft").Build(&raftPool_));
    closeAndReopenQueue(MinimumOpId(), MinimumOpId());

    // Set leader mode and track peers once during initialization
    queue_->SetLeaderMode(1, 1, raftConfig);
    for (auto& peer : raftConfig.peers()) {
      if (peer.permanent_uuid() == kLeaderUuid) {
        continue;
      }
      queue_->TrackPeer(peer);
    }
  }

  void closeAndReopenQueue(
      const OpId& replicatedOpId,
      const OpId& committedOpId) {
    std::shared_ptr<clock::Clock> clock =
        std::make_shared<clock::HybridClock>();
    CHECK_OK(clock->Init());
    std::shared_ptr<ITimeManager> timeManager =
        std::make_shared<TimeManager>(clock, Timestamp::kMin);

    queue_.reset(new PeerMessageQueue(
        metricEntity_,
        log_,
        timeManager,
        persistentVarsManager_,
        fakeRaftPeerPb(kLeaderUuid),
        routingTableContainer_,
        kTestTablet,
        raftPool_->NewToken(ThreadPool::ExecutionMode::Serial),
        replicatedOpId,
        committedOpId));
  }

  void tearDown() {
    if (queue_) {
      queue_->Close();
    }
    // Clean up test directory
    if (!testDir_.empty()) {
      ignoreResult(env_->DeleteRecursively(testDir_));
    }
  }

  PeerMessageQueue* queue() {
    return queue_.get();
  }
  std::shared_ptr<clock::Clock> clock() {
    return clock_;
  }
  std::shared_ptr<log::Log> log() {
    return log_;
  }

 private:
  Env* env_;
  string testDir_;
  unique_ptr<FsManager> fsManager_;
  MetricRegistry metricRegistry_;
  std::shared_ptr<MetricEntity> metricEntity_;
  std::shared_ptr<log::Log> log_;
  unique_ptr<ThreadPool> raftPool_;
  shared_ptr<DurableRoutingTable> routingTable_;
  std::shared_ptr<PersistentVarsManager> persistentVarsManager_;
  shared_ptr<RoutingTableContainer> routingTableContainer_;
  unique_ptr<PeerMessageQueue> queue_;
  std::shared_ptr<log::LogAnchorRegistry> registry_;
  std::shared_ptr<clock::Clock> clock_;
};

ConsensusQueueBenchmark* benchmark = nullptr;

void initBenchmark() {
  if (benchmark == nullptr) {
    benchmark = new ConsensusQueueBenchmark();
    benchmark->setUp();
  }
}

void cleanupBenchmark() {
  if (benchmark != nullptr) {
    benchmark->tearDown();
    delete benchmark;
    benchmark = nullptr;
  }
}

} // namespace

// Benchmark appending operations with various payload sizes
BENCHMARK(ResponseFromPeerBenchmark, n) {
  initBenchmark();
  auto* queue = benchmark->queue();
  auto clock = benchmark->clock();

  RaftPeerPB peerPb;
  peerPb.set_permanent_uuid("peer-1");
  peerPb.set_member_type(RaftPeerPB::VOTER);

  ConsensusResponsePB response;
  response.set_responder_uuid("peer-1");
  response.mutable_status()->mutable_last_received()->CopyFrom(MakeOpId(1, 2));
  response.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(1, 1));
  response.mutable_status()->set_last_committed_idx(1);
  for (int i = 0; i < n; i++) {
    folly::doNotOptimizeAway(queue->ResponseFromPeer("peer-1", response));
  }
}

BENCHMARK(MultiThreadResponseFromPeerBenchmark, n) {
  std::vector<std::thread> threads;

  initBenchmark();
  auto* queue = benchmark->queue();
  auto clock = benchmark->clock();

  ConsensusResponsePB response;
  response.set_responder_uuid("peer-1");
  response.mutable_status()->mutable_last_received()->CopyFrom(MakeOpId(1, 2));
  response.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(1, 1));
  response.mutable_status()->set_last_committed_idx(1);

  std::mutex startMutex;
  std::condition_variable startCv;
  bool startFlag = false;
  std::atomic<int> doneCount{0};
  folly::BenchmarkSuspender suspender; // Suspend timing for setup

  threads.reserve(49);
  for (int t = 0; t < 49; t++) {
    threads.emplace_back([&startMutex,
                          &startCv,
                          &startFlag,
                          &doneCount,
                          queue,
                          &response,
                          n,
                          t]() {
      // Wait for the main thread to signal start
      {
        std::unique_lock<std::mutex> lock(startMutex);
        startCv.wait(lock, [&] { return startFlag; });
      }
      std::string peerId = "peer-" + std::to_string(t);
      for (int i = 0; i < n; i++) {
        folly::doNotOptimizeAway(queue->ResponseFromPeer(peerId, response));
      }
      doneCount.fetch_add(1, std::memory_order_release);
    });
  }

  suspender.dismiss(); // Start timing
  {
    std::lock_guard<std::mutex> lock(startMutex);
    startFlag = true;
  }
  startCv.notify_all();
  // Wait for all threads to finish work
  while (doneCount.load(std::memory_order_acquire) < 49) {
    std::this_thread::yield();
  }
  suspender.rehire(); // Stop timing
  // Suspend timing again to join threads (not measured)
  {
    folly::BenchmarkSuspender joinSuspender;
    for (auto& t : threads) {
      t.join();
    }
  }
}

BENCHMARK_DRAW_LINE();

} // namespace consensus
} // namespace kudu

int main(int argc, char** argv) {
  facebook::initFacebook(&argc, &argv);

  // Suppress logs during benchmark runs(only show ERROR and FATAL)
  FLAGS_minloglevel = 2;
  FLAGS_stderrthreshold = 2;

  folly::runBenchmarks();

  // Cleanup
  kudu::consensus::cleanupBenchmark();

  return 0;
}
