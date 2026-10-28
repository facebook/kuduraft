// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/dataloss_bound_guard.h"

#include <cstdint>
#include <limits>
#include <string>

#include <fb303/ServiceData.h>
#include <fb303/ThreadCachedServiceData.h>
#include <gtest/gtest.h>

#include "kudu/consensus/opid_util.h"

namespace kudu::consensus {
namespace {

using Input = DatalossBoundGuard::Input;
using RemoteEvidence = DatalossBoundGuard::RemoteEvidence;

constexpr int64_t kTailIndex = 1000;
constexpr int64_t kTailBytes = 100000;

// A leader measuring against 100 ops and 10,000 bytes with no margin.
Input leaderInput(std::vector<RemoteEvidence> remotes) {
  return Input{
      .isMeasuring = true,
      .isLeader = true,
      .maxOps = 100,
      .maxBytes = 10000,
      .marginPercent = 0,
      .tailIndex = kTailIndex,
      .tailBytes = kTailBytes,
      .remotes = std::move(remotes)};
}

// A remote whose watermark is 'opsBehind' entries and 'bytesBehind' bytes
// behind the tail.
RemoteEvidence remoteBehind(int64_t opsBehind, int64_t bytesBehind) {
  const int64_t index = kTailIndex - opsBehind;
  return RemoteEvidence{
      .confirmedPrefixIndex = index,
      .watermark = RemoteWatermark{
          .opId = MakeOpId(1, index), .bytes = kTailBytes - bytesBehind}};
}

int64_t readCounter(const std::string& name) {
  facebook::fb303::ThreadCachedServiceData::get()->publishStats();
  facebook::fb303::ServiceData::get()->flushAllData();
  const auto counters = facebook::fb303::ServiceData::get()->getCounters();
  const auto it = counters.find(name);
  return it == counters.end() ? 0 : it->second;
}

TEST(DatalossBoundGuardTest, GatesComeBeforeAnyPeerIsConsidered) {
  Input input = leaderInput({remoteBehind(1, 1)});

  input.isMeasuring = false;
  auto health = DatalossBoundGuard::evaluate(input);
  EXPECT_EQ(health.status, DatalossBoundStatus::Off);
  EXPECT_EQ(health.reason, DatalossBoundReason::None);

  input.isMeasuring = true;
  input.isLeader = false;
  health = DatalossBoundGuard::evaluate(input);
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::NotLeader);

  input.isLeader = true;
  input.maxOps = 0;
  health = DatalossBoundGuard::evaluate(input);
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::InternalError);
}

TEST(DatalossBoundGuardTest, NoEligibleRemoteIsNoRemote) {
  const auto health = DatalossBoundGuard::evaluate(leaderInput({}));
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::NoRemote);
  EXPECT_FALSE(health.hasOperationExposure);
}

// Without a watermark, the confirmed prefix still gives the op exposure.
TEST(DatalossBoundGuardTest, RemoteWithoutWatermarkIsBootstrapOrOpLimit) {
  RemoteEvidence remote{.confirmedPrefixIndex = kTailIndex - 40};
  auto health = DatalossBoundGuard::evaluate(leaderInput({remote}));
  EXPECT_EQ(health.reason, DatalossBoundReason::LeaderBootstrap);
  EXPECT_TRUE(health.hasOperationExposure);
  EXPECT_EQ(health.bestOpsBehind, 40);
  EXPECT_FALSE(health.hasWatermark);

  remote.confirmedPrefixIndex = kTailIndex - 100;
  health = DatalossBoundGuard::evaluate(leaderInput({remote}));
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::OpLimit);
}

TEST(DatalossBoundGuardTest, RemoteWithinBothLimitsIsOk) {
  const auto health =
      DatalossBoundGuard::evaluate(leaderInput({remoteBehind(25, 5000)}));
  EXPECT_EQ(health.status, DatalossBoundStatus::Ok);
  EXPECT_EQ(health.reason, DatalossBoundReason::None);
  EXPECT_EQ(health.qualifyingPeers, 1);
  EXPECT_EQ(health.bestOpsBehind, 25);
  EXPECT_EQ(health.bestBytesBehind, 5000);
  // Bytes are the tighter dimension: half the byte budget is left.
  EXPECT_DOUBLE_EQ(health.headroomFraction, 0.5);
}

// A remote exactly at a limit is a violation; one under it qualifies.
TEST(DatalossBoundGuardTest, LimitsAreExclusive) {
  EXPECT_EQ(
      DatalossBoundGuard::evaluate(leaderInput({remoteBehind(99, 9999)}))
          .status,
      DatalossBoundStatus::Ok);

  auto health =
      DatalossBoundGuard::evaluate(leaderInput({remoteBehind(100, 0)}));
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::OpLimit);

  health = DatalossBoundGuard::evaluate(leaderInput({remoteBehind(0, 10000)}));
  EXPECT_EQ(health.reason, DatalossBoundReason::ByteLimit);

  // Past both limits, bytes is the reason reported.
  health =
      DatalossBoundGuard::evaluate(leaderInput({remoteBehind(200, 20000)}));
  EXPECT_EQ(health.reason, DatalossBoundReason::ByteLimit);
}

// Both reported dimensions come from the remote with the most headroom.
TEST(DatalossBoundGuardTest, BestRemoteSuppliesBothDimensions) {
  const auto health = DatalossBoundGuard::evaluate(leaderInput(
      {remoteBehind(10, 9000), remoteBehind(50, 1000), remoteBehind(5, 9500)}));
  EXPECT_EQ(health.status, DatalossBoundStatus::Ok);
  EXPECT_EQ(health.qualifyingPeers, 3);
  EXPECT_EQ(health.bestOpsBehind, 50);
  EXPECT_EQ(health.bestBytesBehind, 1000);
}

TEST(DatalossBoundGuardTest, MarginShrinksTheUsableBudget) {
  Input input = leaderInput({remoteBehind(60, 0)});
  input.marginPercent = 50;
  const auto health = DatalossBoundGuard::evaluate(input);
  EXPECT_EQ(health.status, DatalossBoundStatus::Violation);
  EXPECT_EQ(health.reason, DatalossBoundReason::OpLimit);

  EXPECT_EQ(DatalossBoundGuard::limitAfterMargin(100, 5), 95);
  EXPECT_EQ(DatalossBoundGuard::limitAfterMargin(100, -1), 100);
  EXPECT_EQ(DatalossBoundGuard::limitAfterMargin(100, 150), 1);
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(
      DatalossBoundGuard::limitAfterMargin(kMax, 5), kMax - kMax / 100 * 5);
}

TEST(DatalossBoundGuardTest, RecordStateReportsTransitionsPerPeriod) {
  DatalossBoundGuard guard;
  DatalossBoundHealth health;
  health.status = DatalossBoundStatus::Violation;
  health.reason = DatalossBoundReason::LeaderBootstrap;

  EXPECT_TRUE(guard.recordState(health));
  EXPECT_FALSE(guard.recordState(health));
  health.reason = DatalossBoundReason::NoRemote;
  EXPECT_TRUE(guard.recordState(health));

  // A new period logs its first state even if it matches the last one.
  guard.startPeriod(MonoTime::Now());
  EXPECT_TRUE(guard.recordState(health));
}

TEST(DatalossBoundGuardTest, BootstrapEndIsRecordedOncePerPeriod) {
  const std::string kSamples =
      "consensus_queue.dataloss_bound_bootstrap_ms.count.60";
  const int64_t before = readCounter(kSamples);
  DatalossBoundGuard guard;
  const MonoTime start = MonoTime::Now();

  guard.startPeriod(start);
  guard.recordBootstrapEnd(start + MonoDelta::FromMilliseconds(5));
  guard.recordBootstrapEnd(start + MonoDelta::FromMilliseconds(9));
  EXPECT_EQ(readCounter(kSamples), before + 1);

  guard.startPeriod(start + MonoDelta::FromMilliseconds(10));
  guard.recordBootstrapEnd(start + MonoDelta::FromMilliseconds(12));
  EXPECT_EQ(readCounter(kSamples), before + 2);
}

// Metric names keep the consensus_queue prefix.
TEST(DatalossBoundGuardTest, EmitStatsUsesConsensusQueueNames) {
  const std::string kOk = "consensus_queue.dataloss_bound_ticks.ok.sum";
  const std::string kNone =
      "consensus_queue.dataloss_bound_reason_ticks.none.sum";
  const int64_t okBefore = readCounter(kOk);
  const int64_t noneBefore = readCounter(kNone);

  DatalossBoundGuard::emitStats(
      DatalossBoundGuard::evaluate(leaderInput({remoteBehind(1, 1)})));

  EXPECT_EQ(readCounter(kOk), okBefore + 1);
  EXPECT_EQ(readCounter(kNone), noneBefore + 1);
}

} // namespace
} // namespace kudu::consensus
