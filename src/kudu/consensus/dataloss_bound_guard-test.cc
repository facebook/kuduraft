// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/dataloss_bound_guard.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include <fb303/ServiceData.h>
#include <fb303/ThreadCachedServiceData.h>
#include <gtest/gtest.h>

namespace kudu::consensus {
namespace {

using Config = DatalossBoundGuard::Config;

constexpr int64_t kTailIndex = 1000;
constexpr int64_t kTailBytes = 100000;
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

// Measuring against 100 ops and 10,000 bytes with no margin.
Config failOpen() {
  return Config{
      .isMeasuring = true,
      .failClosed = false,
      .maxOps = 100,
      .maxBytes = 10000,
      .marginPercent = 0};
}

Config failClosed() {
  Config config = failOpen();
  config.failClosed = true;
  return config;
}

int64_t readCounter(const std::string& name) {
  facebook::fb303::ThreadCachedServiceData::get()->publishStats();
  facebook::fb303::ServiceData::get()->flushAllData();
  const auto counters = facebook::fb303::ServiceData::get()->getCounters();
  const auto it = counters.find(name);
  return it == counters.end() ? 0 : it->second;
}

std::string decisionCounter(const std::string& outcome, const char* reason) {
  return "consensus_queue.dataloss_bound_decisions." + outcome + "." + reason +
      ".sum";
}

// An accepted reply from 'uuid' whose log ends at 'index'.
void ack(DatalossBoundGuard& guard, const std::string& uuid, int64_t index) {
  guard.onPeerResponse(uuid, LogMatchResult::Match, index, /*accepted=*/true);
}

// A measuring leader whose only eligible remote, peer-1, has confirmed a
// watermark 'opsBehind' entries and 'bytesBehind' bytes behind a log that
// ends at kTailIndex after kTailBytes.
std::unique_ptr<DatalossBoundGuard> remoteBehind(
    int64_t opsBehind,
    int64_t bytesBehind) {
  auto guard = std::make_unique<DatalossBoundGuard>();
  guard->setMeasuring(true, MonoTime::Now());
  guard->onAppend(kTailIndex - opsBehind, kTailBytes - bytesBehind);
  guard->becomeLeader(MonoTime::Now());
  guard->setEligibleRemotes({"peer-1"}, {"peer-1"});
  // The first reply records a watermark at the tail; the second confirms it.
  ack(*guard, "peer-1", kTailIndex - opsBehind);
  ack(*guard, "peer-1", kTailIndex - opsBehind);
  guard->onAppend(kTailIndex, bytesBehind);
  return guard;
}

DatalossBoundReason reasonOf(const DatalossBoundGuard& guard) {
  return guard.health(failOpen()).reason;
}

TEST(DatalossBoundGuardTest, GatesComeBeforeTheLag) {
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  Config off = failOpen();
  off.isMeasuring = false;
  EXPECT_EQ(guard.health(off).status, DatalossBoundStatus::Off);
  EXPECT_EQ(guard.health(off).reason, DatalossBoundReason::None);

  // No role has been reported.
  EXPECT_EQ(guard.health(failOpen()).status, DatalossBoundStatus::Violation);
  EXPECT_EQ(reasonOf(guard), DatalossBoundReason::LeaderBootstrap);

  guard.becomeFollower();
  EXPECT_EQ(reasonOf(guard), DatalossBoundReason::NotLeader);

  const auto leader = remoteBehind(1, 1);
  Config noBudget = failOpen();
  noBudget.maxOps = 0;
  EXPECT_EQ(
      leader->health(noBudget).reason, DatalossBoundReason::InternalError);
  EXPECT_EQ(leader->health(failOpen()).status, DatalossBoundStatus::Ok);
}

// Recorded byte counts stop describing the log on a role change, a
// truncation, or a measurement toggle.
TEST(DatalossBoundGuardTest, GenerationMarksEachNewByteCoordinate) {
  DatalossBoundGuard guard;
  int64_t generation = guard.generation();
  const auto expectNewGeneration = [&] {
    EXPECT_EQ(guard.generation(), generation + 1);
    generation = guard.generation();
  };

  guard.setMeasuring(true, MonoTime::Now());
  expectNewGeneration();
  guard.setMeasuring(true, MonoTime::Now());
  EXPECT_EQ(guard.generation(), generation);
  guard.becomeLeader(MonoTime::Now());
  expectNewGeneration();
  guard.onTruncate(kTailIndex - 1);
  expectNewGeneration();
  guard.becomeFollower();
  expectNewGeneration();
  guard.setMeasuring(false, MonoTime::Now());
  expectNewGeneration();
}

TEST(DatalossBoundGuardTest, RoleChangesForgetRemoteEvidence) {
  auto guard = remoteBehind(1, 1);
  guard->forgetPeer("peer-2");
  EXPECT_EQ(reasonOf(*guard), DatalossBoundReason::None);
  guard->forgetPeer("peer-1");
  EXPECT_EQ(reasonOf(*guard), DatalossBoundReason::LeaderBootstrap);

  guard = remoteBehind(1, 1);
  guard->becomeLeader(MonoTime::Now());
  EXPECT_FALSE(guard->peerEvidenceForTests("peer-1").has_value());
  EXPECT_EQ(reasonOf(*guard), DatalossBoundReason::NoRemote);

  guard = remoteBehind(1, 1);
  guard->becomeFollower();
  EXPECT_FALSE(guard->peerEvidenceForTests("peer-1").has_value());
}

// After a truncation, indexes past the new tail name different entries, so no
// remote's position or watermark can be trusted.
TEST(DatalossBoundGuardTest, TruncationForgetsRemoteEvidence) {
  auto guard = remoteBehind(10, 100);
  ack(*guard, "peer-1", kTailIndex - 5);
  ASSERT_TRUE(
      guard->peerEvidenceForTests("peer-1")->pendingWatermark.has_value());
  const int64_t generation = guard->generation();

  guard->onTruncate(kTailIndex - 20);
  EXPECT_FALSE(guard->peerEvidenceForTests("peer-1").has_value());
  EXPECT_EQ(reasonOf(*guard), DatalossBoundReason::LeaderBootstrap);
  EXPECT_FALSE(guard->admit(failClosed(), 1, 1).admitted);

  // A scan started before the truncation cannot install its baseline.
  ack(*guard, "peer-1", kTailIndex - 20);
  EXPECT_FALSE(guard->installReconstructedWatermark(
      "peer-1", RemoteWatermark{.index = kTailIndex - 20}, generation));
}

// Evidence is kept for every member, but only eligible remotes count, from
// the moment they become eligible.
TEST(DatalossBoundGuardTest, OnlyEligibleRemotesCount) {
  auto guard = remoteBehind(1, 1);
  ack(*guard, "peer-2", kTailIndex);
  ack(*guard, "peer-2", kTailIndex);
  EXPECT_EQ(guard->health(failOpen()).bestOpsLag, 1);

  guard->setEligibleRemotes({"peer-2"}, {"peer-1", "peer-2"});
  EXPECT_EQ(guard->health(failOpen()).bestOpsLag, 0);
  EXPECT_TRUE(guard->peerEvidenceForTests("peer-1").has_value());

  guard->setEligibleRemotes({"peer-2"}, {"peer-2"});
  EXPECT_FALSE(guard->peerEvidenceForTests("peer-1").has_value());
}

// A reply the leader cannot check against its log can only take back what the
// remote had confirmed.
TEST(DatalossBoundGuardTest, UnavailableReplyKeepsOnlyWhatWasConfirmed) {
  auto guard = remoteBehind(10, 0);
  ack(*guard, "peer-1", kTailIndex - 5);
  ASSERT_EQ(
      guard->peerEvidenceForTests("peer-1")->confirmedPrefixIndex,
      kTailIndex - 5);

  guard->onPeerResponse(
      "peer-1", LogMatchResult::Unavailable, kTailIndex - 7, true);
  auto evidence = guard->peerEvidenceForTests("peer-1");
  EXPECT_EQ(evidence->confirmedPrefixIndex, kTailIndex - 10);
  ASSERT_TRUE(evidence->confirmedWatermark.has_value());

  guard->onPeerResponse(
      "peer-1", LogMatchResult::Unavailable, kTailIndex - 11, true);
  EXPECT_FALSE(guard->peerEvidenceForTests("peer-1").has_value());
}

TEST(DatalossBoundGuardTest, ReasonsWithoutAWatermark) {
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  guard.onAppend(kTailIndex, kTailBytes);
  guard.becomeLeader(MonoTime::Now());
  auto health = guard.health(failOpen());
  EXPECT_EQ(health.reason, DatalossBoundReason::NoRemote);
  EXPECT_FALSE(health.hasOpsLag);

  guard.setEligibleRemotes({"peer-1"}, {"peer-1"});
  EXPECT_EQ(reasonOf(guard), DatalossBoundReason::LeaderBootstrap);

  // A confirmed position gives the ops lag before any watermark exists.
  ack(guard, "peer-1", kTailIndex - 40);
  health = guard.health(failOpen());
  EXPECT_EQ(health.reason, DatalossBoundReason::LeaderBootstrap);
  EXPECT_TRUE(health.hasOpsLag);
  EXPECT_EQ(health.bestOpsLag, 40);
  EXPECT_FALSE(health.hasWatermark);

  ack(guard, "peer-1", kTailIndex - 100);
  EXPECT_EQ(reasonOf(guard), DatalossBoundReason::OpLimit);
}

TEST(DatalossBoundGuardTest, WithinBothLimitsIsOk) {
  const auto health = remoteBehind(25, 5000)->health(failOpen());
  EXPECT_EQ(health.status, DatalossBoundStatus::Ok);
  EXPECT_EQ(health.reason, DatalossBoundReason::None);
  EXPECT_EQ(health.bestOpsLag, 25);
  EXPECT_EQ(health.bestBytesLag, 5000);
  // Bytes are the tighter dimension: half the byte budget is left.
  EXPECT_DOUBLE_EQ(health.headroomFraction, 0.5);
}

// A remote exactly at a limit is a violation; one under it is not.
TEST(DatalossBoundGuardTest, LimitsAreExclusive) {
  EXPECT_EQ(reasonOf(*remoteBehind(99, 9999)), DatalossBoundReason::None);
  EXPECT_EQ(reasonOf(*remoteBehind(100, 0)), DatalossBoundReason::OpLimit);
  EXPECT_EQ(reasonOf(*remoteBehind(0, 10000)), DatalossBoundReason::ByteLimit);
  // Past both limits, bytes is the reason reported.
  EXPECT_EQ(
      reasonOf(*remoteBehind(200, 20000)), DatalossBoundReason::ByteLimit);
}

TEST(DatalossBoundGuardTest, MarginShrinksTheUsableBudget) {
  Config halved = failOpen();
  halved.marginPercent = 50;
  EXPECT_EQ(
      remoteBehind(60, 0)->health(halved).reason, DatalossBoundReason::OpLimit);

  const auto opsAfterMargin = [](int64_t maxOps, int32_t marginPercent) {
    return DatalossBoundGuard::usableBudget(
               Config{
                   .maxOps = maxOps,
                   .maxBytes = 1,
                   .marginPercent = marginPercent})
        ->ops;
  };
  EXPECT_EQ(opsAfterMargin(100, 5), 95);
  EXPECT_EQ(opsAfterMargin(100, -1), 100);
  EXPECT_EQ(opsAfterMargin(100, 150), 1);
  EXPECT_EQ(opsAfterMargin(kMax, 5), kMax - kMax / 100 * 5);
  EXPECT_FALSE(DatalossBoundGuard::usableBudget(Config{.maxOps = 1}));
}

TEST(DatalossBoundGuardTest, QualifyingMeansWithinBothLimits) {
  EXPECT_EQ(remoteBehind(99, 9999)->health(failOpen()).qualifyingRemotes, 1);
  EXPECT_EQ(remoteBehind(100, 0)->health(failOpen()).qualifyingRemotes, 0);
  EXPECT_EQ(remoteBehind(0, 10000)->health(failOpen()).qualifyingRemotes, 0);
  Config noBudget = failOpen();
  noBudget.maxBytes = 0;
  EXPECT_EQ(remoteBehind(0, 0)->health(noBudget).qualifyingRemotes, 0);

  // Reserved writes count as already appended.
  const auto guard = remoteBehind(99, 0);
  EXPECT_EQ(guard->health(failOpen()).qualifyingRemotes, 1);
  const auto decision = guard->admit(failOpen(), 1, 0);
  EXPECT_EQ(guard->health(failOpen()).qualifyingRemotes, 0);
  guard->release(decision);
}

TEST(DatalossBoundGuardTest, ReconstructionInstallsOnlyWhileValid) {
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  guard.onAppend(kTailIndex, kTailBytes);
  guard.becomeLeader(MonoTime::Now());
  guard.setEligibleRemotes({"peer-1", "peer-2"}, {"peer-1", "peer-2"});
  ack(guard, "peer-1", kTailIndex - 20);
  ack(guard, "peer-2", kTailIndex - 10);

  // Only the newest position is a candidate, unless it is past the tail the
  // scan stops at.
  const auto candidate = guard.reconstructionCandidate(kTailIndex);
  ASSERT_TRUE(candidate.has_value());
  EXPECT_EQ(candidate->uuid, "peer-2");
  EXPECT_EQ(candidate->index, kTailIndex - 10);
  EXPECT_EQ(guard.reconstructionCandidate(kTailIndex - 15)->uuid, "peer-1");

  const RemoteWatermark rebuilt{.index = kTailIndex - 10, .bytes = 5};
  const int64_t generation = guard.generation();
  EXPECT_FALSE(
      guard.installReconstructedWatermark("peer-2", rebuilt, generation + 1));
  EXPECT_FALSE(guard.installReconstructedWatermark(
      "peer-1", RemoteWatermark{.index = kTailIndex - 15}, generation));
  EXPECT_TRUE(
      guard.installReconstructedWatermark("peer-2", rebuilt, generation));
  EXPECT_FALSE(
      guard.installReconstructedWatermark("peer-2", rebuilt, generation));
  // peer-1 is behind the newest confirmed watermark and could not improve it.
  EXPECT_FALSE(guard.reconstructionCandidate(kTailIndex).has_value());
}

TEST(DatalossBoundGuardTest, AdmissionReservesUntilReleased) {
  const auto guard = remoteBehind(90, 0);
  const std::string kAdmitted = decisionCounter("admitted", "none");
  const int64_t admittedBefore = readCounter(kAdmitted);

  const auto first = guard->admit(failClosed(), 5, 10);
  EXPECT_EQ(readCounter(kAdmitted), admittedBefore + 1);
  EXPECT_TRUE(first.admitted);
  ASSERT_TRUE(first.reserved);
  EXPECT_EQ(first.reservedOps, 5);
  EXPECT_EQ(first.reservedBytes, 10);
  EXPECT_EQ(first.reason, DatalossBoundReason::None);
  auto health = guard->health(failClosed());
  EXPECT_EQ(health.reservedOps, 5);
  EXPECT_EQ(health.reservedBytes, 10);
  EXPECT_EQ(health.bestOpsLag, 95);

  // 90 behind, 5 reserved, and 5 more reaches the limit.
  const auto second = guard->admit(failClosed(), 5, 10);
  EXPECT_FALSE(second.admitted);
  EXPECT_FALSE(second.reserved);
  EXPECT_EQ(second.reason, DatalossBoundReason::OpLimit);

  guard->release(first);
  const auto third = guard->admit(failClosed(), 5, 10);
  EXPECT_TRUE(third.admitted);
  guard->release(third);
  health = guard->health(failClosed());
  EXPECT_EQ(health.reservedOps, 0);
  EXPECT_EQ(health.reservedBytes, 0);
}

TEST(DatalossBoundGuardTest, ReservationsAreReportedDuringBootstrap) {
  // No role is reported yet. FAIL_OPEN still admits and reserves, and the
  // health reports the reservation alongside the bootstrap reason.
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  const auto decision = guard.admit(failOpen(), 2, 20);
  ASSERT_TRUE(decision.reserved);
  const auto health = guard.health(failOpen());
  EXPECT_EQ(health.reason, DatalossBoundReason::LeaderBootstrap);
  EXPECT_EQ(health.reservedOps, 2);
  EXPECT_EQ(health.reservedBytes, 20);
  guard.release(decision);
}

TEST(DatalossBoundGuardTest, FailOpenAdmitsWhatFailClosedRejects) {
  const auto guard = remoteBehind(100, 0);
  const std::string kWouldReject = decisionCounter("would_reject", "op_limit");
  const std::string kRejected = decisionCounter("rejected", "op_limit");
  const int64_t wouldRejectBefore = readCounter(kWouldReject);
  const int64_t rejectedBefore = readCounter(kRejected);

  const auto open = guard->admit(failOpen(), 1, 1);
  EXPECT_TRUE(open.admitted);
  EXPECT_TRUE(open.reserved);
  EXPECT_EQ(open.reason, DatalossBoundReason::OpLimit);
  guard->release(open);

  const auto closed = guard->admit(failClosed(), 1, 1);
  EXPECT_FALSE(closed.admitted);
  EXPECT_FALSE(closed.reserved);
  EXPECT_EQ(closed.reason, DatalossBoundReason::OpLimit);

  EXPECT_EQ(readCounter(kWouldReject), wouldRejectBefore + 1);
  EXPECT_EQ(readCounter(kRejected), rejectedBefore + 1);
}

TEST(DatalossBoundGuardTest, FollowerAdmitsWithoutReservation) {
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  guard.becomeFollower();
  const auto decision = guard.admit(failClosed(), 1, 1);
  EXPECT_TRUE(decision.admitted);
  EXPECT_FALSE(decision.reserved);
  EXPECT_EQ(decision.reason, DatalossBoundReason::None);
}

TEST(DatalossBoundGuardTest, NegativeProposalIsAnInternalError) {
  const auto guard = remoteBehind(1, 1);
  const auto closed = guard->admit(failClosed(), -1, 1);
  EXPECT_FALSE(closed.admitted);
  EXPECT_EQ(closed.reason, DatalossBoundReason::InternalError);

  const auto open = guard->admit(failOpen(), 1, -1);
  EXPECT_TRUE(open.admitted);
  EXPECT_FALSE(open.reserved);
  EXPECT_EQ(open.reason, DatalossBoundReason::InternalError);
}

TEST(DatalossBoundGuardTest, ReservationIsClampedToTheLimit) {
  const auto guard = remoteBehind(0, 0);
  const auto decision = guard->admit(failOpen(), 1000, 1000000);
  EXPECT_EQ(decision.reservedOps, 100);
  EXPECT_EQ(decision.reservedBytes, 10000);
  guard->release(decision);
}

TEST(DatalossBoundGuardTest, LagSaturatesInsteadOfOverflowing) {
  const Config unlimited{.isMeasuring = true, .maxOps = kMax, .maxBytes = kMax};
  DatalossBoundGuard guard;
  guard.setMeasuring(true, MonoTime::Now());
  guard.onAppend(1, 0);
  guard.becomeLeader(MonoTime::Now());
  guard.setEligibleRemotes({"peer-1"}, {"peer-1"});
  ack(guard, "peer-1", 1);
  ack(guard, "peer-1", 1);
  guard.onAppend(kMax, 0);

  const auto decision = guard.admit(unlimited, 5, 0);
  EXPECT_TRUE(decision.reserved);
  const auto health = guard.health(unlimited);
  EXPECT_EQ(health.bestOpsLag, kMax);
  EXPECT_EQ(health.reason, DatalossBoundReason::OpLimit);
  guard.release(decision);
}

// A reservation carries its guard's owner token, and only that guard releases
// it.
TEST(DatalossBoundGuardTest, ReleaseOfAnotherGuardsReservationIsIgnored) {
  const auto guard = remoteBehind(0, 0);
  const auto reservation = guard->admit(failOpen(), 3, 30);
  ASSERT_TRUE(reservation.reserved);

  const auto other = remoteBehind(0, 0);
  const auto otherReservation = other->admit(failOpen(), 1, 1);
  EXPECT_NE(otherReservation.reservationOwner, reservation.reservationOwner);
  other->release(otherReservation);

  const std::string kMismatch =
      decisionCounter("release_mismatch", "internal_error");
  const int64_t mismatchesBefore = readCounter(kMismatch);
  other->release(reservation);
  EXPECT_EQ(readCounter(kMismatch), mismatchesBefore + 1);
  EXPECT_EQ(guard->health(failOpen()).reservedOps, 3);

  guard->release(reservation);
  EXPECT_EQ(guard->health(failOpen()).reservedOps, 0);
}

TEST(DatalossBoundGuardTest, BootstrapEndIsRecordedOncePerPeriod) {
  const std::string kSamples =
      "consensus_queue.dataloss_bound_bootstrap_ms.count.60";
  const int64_t before = readCounter(kSamples);
  DatalossBoundGuard guard;
  guard.onAppend(kTailIndex, kTailBytes);
  guard.becomeLeader(MonoTime::Now());
  guard.setEligibleRemotes({"peer-1"}, {"peer-1"});

  // Not measuring: replies confirm positions but record no watermark.
  ack(guard, "peer-1", kTailIndex);
  ack(guard, "peer-1", kTailIndex);
  EXPECT_EQ(readCounter(kSamples), before);

  guard.setMeasuring(true, MonoTime::Now());
  ack(guard, "peer-1", kTailIndex);
  EXPECT_EQ(readCounter(kSamples), before);
  ack(guard, "peer-1", kTailIndex);
  ack(guard, "peer-1", kTailIndex);
  EXPECT_EQ(readCounter(kSamples), before + 1);

  guard.becomeLeader(MonoTime::Now());
  guard.setEligibleRemotes({"peer-1"}, {"peer-1"});
  ack(guard, "peer-1", kTailIndex);
  ack(guard, "peer-1", kTailIndex);
  EXPECT_EQ(readCounter(kSamples), before + 2);
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
  guard.becomeLeader(MonoTime::Now());
  EXPECT_TRUE(guard.recordState(health));
}

// Metric names keep the consensus_queue prefix.
TEST(DatalossBoundGuardTest, EmitStatsUsesConsensusQueueNames) {
  const std::string kOk = "consensus_queue.dataloss_bound_ticks.ok.sum";
  const std::string kNone =
      "consensus_queue.dataloss_bound_reason_ticks.none.sum";
  const int64_t okBefore = readCounter(kOk);
  const int64_t noneBefore = readCounter(kNone);

  DatalossBoundGuard::emitStats(remoteBehind(1, 1)->health(failOpen()));

  EXPECT_EQ(readCounter(kOk), okBefore + 1);
  EXPECT_EQ(readCounter(kNone), noneBefore + 1);
}

} // namespace
} // namespace kudu::consensus
