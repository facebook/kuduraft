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
#include "kudu/util/threadpool.h"

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
inline RaftPeerPB FakeRaftPeerPB(const std::string& uuid) {
  RaftPeerPB peer_pb;
  peer_pb.set_permanent_uuid(uuid);
  peer_pb.set_member_type(RaftPeerPB::VOTER);
  peer_pb.mutable_last_known_addr()->set_host("benchmark-fake-hostname");
  peer_pb.mutable_last_known_addr()->set_port(0);
  // Large simulate large RaftPeerPb in production
  peer_pb.mutable_attrs()->set_quorum_id(std::string(5000, 'q'));
  return peer_pb;
}

class ConsensusQueueBenchmark {
 public:
  ConsensusQueueBenchmark()
      : env_(Env::Default()),
        metric_entity_(
            METRIC_ENTITY_server.Instantiate(&metric_registry_, "queue-bench")),
        registry_(new log::LogAnchorRegistry) {
    SetUmask();
  }

  void SetUp() {
    // Create a unique temporary directory for this benchmark run
    // Add random component to avoid collisions if running multiple benchmarks
    test_dir_ = "/tmp/consensus_queue_bench_" + std::to_string(getpid()) + "_" +
        std::to_string(env_->NowMicros()) + "_" + std::to_string(rand());

    // Forcibly remove any leftover directory using system commands
    // This handles permission issues that env_->DeleteRecursively might fail on
    string cleanup_cmd = "rm -rf " + test_dir_ + " 2>/dev/null || true";
    ignore_result(system(cleanup_cmd.c_str()));

    CHECK_OK(env_->CreateDir(test_dir_));

    fs_manager_.reset(new FsManager(env_, test_dir_ + "/fs_root"));
    CHECK_OK(fs_manager_->CreateInitialFileSystemLayout());
    CHECK_OK(fs_manager_->Open());
    CHECK_OK(
        log::Log::Open(
            log::LogOptions(), fs_manager_.get(), kTestTablet, nullptr, &log_));

    RaftConfigPB raft_config = BuildRaftConfigPBForTests(50, 50);
    CHECK_OK(
        DurableRoutingTable::Create(
            fs_manager_.get(), kTestTablet, raft_config, {}, &routing_table_));

    persistent_vars_manager_ = new PersistentVarsManager(fs_manager_.get());
    CHECK_OK(persistent_vars_manager_->CreatePersistentVars(kTestTablet));

    routing_table_container_ = std::make_shared<RoutingTableContainer>(
        ProxyPolicy::DURABLE_ROUTING_POLICY,
        FakeRaftPeerPB(kLeaderUuid),
        raft_config,
        routing_table_,
        std::vector<std::unordered_set<std::string>>());

    clock_ = std::make_shared<clock::HybridClock>();
    CHECK_OK(clock_->Init());

    CHECK_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));
    CloseAndReopenQueue(MinimumOpId(), MinimumOpId());

    // Set leader mode and track peers once during initialization
    queue_->SetLeaderMode(1, 1, raft_config);
    for (auto& peer : raft_config.peers()) {
      if (peer.permanent_uuid() == kLeaderUuid) {
        continue;
      }
      queue_->TrackPeer(peer);
    }
  }

  void CloseAndReopenQueue(
      const OpId& replicated_opid,
      const OpId& committed_opid) {
    std::shared_ptr<clock::Clock> clock =
        std::make_shared<clock::HybridClock>();
    CHECK_OK(clock->Init());
    std::shared_ptr<ITimeManager> time_manager =
        std::make_shared<TimeManager>(clock, Timestamp::kMin);

    queue_.reset(new PeerMessageQueue(
        metric_entity_,
        log_.get(),
        time_manager,
        persistent_vars_manager_,
        FakeRaftPeerPB(kLeaderUuid),
        routing_table_container_,
        kTestTablet,
        raft_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL),
        replicated_opid,
        committed_opid));
  }

  void TearDown() {
    if (queue_) {
      log_->WaitUntilAllFlushed();
      queue_->Close();
    }
    // Clean up test directory
    if (!test_dir_.empty()) {
      ignore_result(env_->DeleteRecursively(test_dir_));
    }
  }

  PeerMessageQueue* queue() {
    return queue_.get();
  }
  std::shared_ptr<clock::Clock> clock() {
    return clock_;
  }
  scoped_refptr<log::Log> log() {
    return log_.get();
  }

 private:
  Env* env_;
  string test_dir_;
  unique_ptr<FsManager> fs_manager_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<log::Log> log_;
  unique_ptr<ThreadPool> raft_pool_;
  shared_ptr<DurableRoutingTable> routing_table_;
  scoped_refptr<PersistentVarsManager> persistent_vars_manager_;
  shared_ptr<RoutingTableContainer> routing_table_container_;
  unique_ptr<PeerMessageQueue> queue_;
  std::shared_ptr<log::LogAnchorRegistry> registry_;
  std::shared_ptr<clock::Clock> clock_;
};

ConsensusQueueBenchmark* benchmark = nullptr;

void InitBenchmark() {
  if (benchmark == nullptr) {
    benchmark = new ConsensusQueueBenchmark();
    benchmark->SetUp();
  }
}

void CleanupBenchmark() {
  if (benchmark != nullptr) {
    benchmark->TearDown();
    delete benchmark;
    benchmark = nullptr;
  }
}

} // namespace

// Benchmark appending operations with various payload sizes
BENCHMARK(ResponseFromPeerBenchmark, n) {
  InitBenchmark();
  auto* queue = benchmark->queue();
  auto clock = benchmark->clock();

  RaftPeerPB peer_pb;
  peer_pb.set_permanent_uuid("peer-1");
  peer_pb.set_member_type(RaftPeerPB::VOTER);

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

  InitBenchmark();
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
      std::string peer_id = "peer-" + std::to_string(t);
      for (int i = 0; i < n; i++) {
        folly::doNotOptimizeAway(queue->ResponseFromPeer(peer_id, response));
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
  kudu::consensus::CleanupBenchmark();

  return 0;
}
