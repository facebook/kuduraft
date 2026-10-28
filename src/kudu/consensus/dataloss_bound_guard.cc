// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/dataloss_bound_guard.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <string_view>

#include <folly/ConstexprMath.h>
#include <glog/logging.h>

#include "kudu/util/Stats.h"
#include "kudu/util/logging.h"

namespace kudu::consensus {

namespace {
// Dashboards and the ODS allowlist read these metrics under the
// consensus_queue prefix.
constexpr std::string_view kStatsTag = "consensus_queue";

std::atomic<uint64_t> nextReservationOwner{1};

// 'limit' less 'marginPercent' of it, clamped to [0, 99] percent.
int64_t limitAfterMargin(int64_t limit, int32_t marginPercent) {
  const int64_t marginPct = std::clamp<int64_t>(marginPercent, 0, 99);
  // Multiplying first preserves precision for small limits; divide first only
  // where multiplication could overflow.
  const int64_t margin = limit <= std::numeric_limits<int64_t>::max() / 100
      ? limit * marginPct / 100
      : limit / 100 * marginPct;
  return limit - margin;
}
} // namespace

const char* datalossBoundStatusName(DatalossBoundStatus s) {
  switch (s) {
    case DatalossBoundStatus::Off:
      return "off";
    case DatalossBoundStatus::Ok:
      return "ok";
    case DatalossBoundStatus::Violation:
      return "violation";
  }
  return "unknown";
}

const char* datalossBoundReasonName(DatalossBoundReason r) {
  switch (r) {
    case DatalossBoundReason::None:
      return "none";
    case DatalossBoundReason::NoRemote:
      return "no_remote";
    case DatalossBoundReason::OpLimit:
      return "op_limit";
    case DatalossBoundReason::ByteLimit:
      return "byte_limit";
    case DatalossBoundReason::LeaderBootstrap:
      return "leader_bootstrap";
    case DatalossBoundReason::NotLeader:
      return "not_leader";
    case DatalossBoundReason::InternalError:
      return "internal_error";
  }
  return "unknown";
}

std::optional<DatalossBoundGuard::UsableBudget>
DatalossBoundGuard::usableBudget(const Config& config) {
  // Margins are held inside the advertised limits, so the budget a remote may
  // actually consume is smaller than the number quoted externally.
  const UsableBudget budget{
      .ops = limitAfterMargin(config.maxOps, config.marginPercent),
      .bytes = limitAfterMargin(config.maxBytes, config.marginPercent)};
  if (budget.ops <= 0 || budget.bytes <= 0) {
    return std::nullopt;
  }
  return budget;
}

DatalossBoundGuard::DatalossBoundGuard()
    : reservationOwner_(
          nextReservationOwner.fetch_add(1, std::memory_order_relaxed)) {}

void DatalossBoundGuard::onAppend(int64_t lastIndex, int64_t payloadBytes) {
  tailIndex_.store(lastIndex, std::memory_order_relaxed);
  cumulativePayloadBytes_.fetch_add(payloadBytes, std::memory_order_relaxed);
}

void DatalossBoundGuard::onTruncate(int64_t lastIndex) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  tailIndex_.store(lastIndex, std::memory_order_relaxed);
  // Indexes past 'lastIndex' will be reused for different entries, so a
  // remote's position there proves nothing about this log.
  evidence_.clear();
  // A reconstruction that recorded the old tail fails the generation check;
  // one that records the new tail never reads the discarded entries.
  generation_.fetch_add(1, std::memory_order_relaxed);
}

int64_t DatalossBoundGuard::cumulativePayloadBytes() const {
  return cumulativePayloadBytes_.load(std::memory_order_relaxed);
}

int64_t DatalossBoundGuard::generation() const {
  return generation_.load(std::memory_order_relaxed);
}

void DatalossBoundGuard::becomeLeader(MonoTime now) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  // Each leader records watermarks against its own byte counter, and indexes
  // return to the same values across a re-election, so nothing learned under
  // an earlier role describes this one.
  role_ = Role::Leader;
  evidence_.clear();
  eligible_.clear();
  generation_.fetch_add(1, std::memory_order_relaxed);
  startPeriodUnlocked(now);
}

void DatalossBoundGuard::becomeFollower() {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  role_ = Role::Follower;
  evidence_.clear();
  eligible_.clear();
  generation_.fetch_add(1, std::memory_order_relaxed);
}

void DatalossBoundGuard::setMeasuring(bool measuring, MonoTime now) {
  // The switch rarely changes, so check it before taking the lock.
  if (measuring_.load(std::memory_order_relaxed) == measuring) {
    return;
  }
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  if (measuring_.load(std::memory_order_relaxed) == measuring) {
    return;
  }
  generation_.fetch_add(1, std::memory_order_relaxed);
  for (auto& [uuid, evidence] : evidence_) {
    evidence.confirmedWatermark.reset();
    evidence.pendingWatermark.reset();
  }
  if (measuring) {
    // The cumulative counter is only a coordinate. A new watermark captures
    // the current counter with its log position, so bytes from a disabled
    // interval are not lag after that position.
    startPeriodUnlocked(now);
  }
  measuring_.store(measuring, std::memory_order_relaxed);
}

bool DatalossBoundGuard::isMeasuring() const {
  return measuring_.load(std::memory_order_relaxed);
}

void DatalossBoundGuard::setEligibleRemotes(
    std::vector<std::string> eligibleUuids,
    const std::vector<std::string>& memberUuids) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  eligible_ = std::move(eligibleUuids);
  std::erase_if(evidence_, [&](const auto& entry) {
    return std::find(memberUuids.begin(), memberUuids.end(), entry.first) ==
        memberUuids.end();
  });
  maybeRecordBootstrapUnlocked();
}

void DatalossBoundGuard::onPeerResponse(
    const std::string& uuid,
    LogMatchResult match,
    int64_t lastReceivedIndex,
    bool accepted) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  if (role_ != Role::Leader) {
    return;
  }
  if (match == LogMatchResult::Mismatch) {
    evidence_.erase(uuid);
    return;
  }

  if (match == LogMatchResult::Unavailable) {
    // The leader no longer retains this index, so the reply cannot establish
    // a new prefix. It can still show that the remote no longer reaches a
    // position it had previously confirmed.
    const auto it = evidence_.find(uuid);
    if (it == evidence_.end() ||
        lastReceivedIndex >= it->second.confirmedPrefixIndex) {
      return;
    }
    const auto& watermark = it->second.confirmedWatermark;
    if (watermark.has_value() && watermark->index <= lastReceivedIndex) {
      it->second.confirmedPrefixIndex = watermark->index;
    } else {
      evidence_.erase(it);
    }
    return;
  }

  PeerEvidence& evidence = evidence_[uuid];
  // A matching reply reports the remote's actual last entry even when it
  // rejects the preceding entry in the request. Lower the confirmed prefix at
  // once if the remote has lost a suffix. Only an accepted reply may move the
  // prefix forward.
  if (lastReceivedIndex < evidence.confirmedPrefixIndex) {
    if (evidence.confirmedWatermark.has_value() &&
        lastReceivedIndex < evidence.confirmedWatermark->index) {
      evidence.confirmedWatermark.reset();
    }
    evidence.confirmedPrefixIndex = lastReceivedIndex;
  }
  if (accepted) {
    evidence.confirmedPrefixIndex = lastReceivedIndex;
    if (measuring_.load(std::memory_order_relaxed)) {
      advanceWatermarkUnlocked(evidence);
      maybeRecordBootstrapUnlocked();
    }
  }
}

void DatalossBoundGuard::forgetPeer(const std::string& uuid) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  evidence_.erase(uuid);
}

void DatalossBoundGuard::advanceWatermarkUnlocked(PeerEvidence& evidence) {
  DCHECK(admissionLock_.is_locked());
  // Once the remote has confirmed the pending watermark's position, it becomes
  // the confirmed watermark.
  if (evidence.pendingWatermark.has_value() &&
      evidence.confirmedPrefixIndex >= evidence.pendingWatermark->index) {
    evidence.confirmedWatermark = evidence.pendingWatermark;
    evidence.pendingWatermark.reset();
  }

  // Keep one target fixed until it is confirmed.
  const int64_t confirmedIndex = evidence.confirmedWatermark.has_value()
      ? evidence.confirmedWatermark->index
      : 0;
  const int64_t tailIndex = tailIndex_.load(std::memory_order_relaxed);
  if (!evidence.pendingWatermark.has_value() && tailIndex > confirmedIndex) {
    evidence.pendingWatermark =
        RemoteWatermark{.index = tailIndex, .bytes = cumulativePayloadBytes()};
  }
}

const DatalossBoundGuard::PeerEvidence* DatalossBoundGuard::evidenceUnlocked(
    const std::string& uuid) const {
  DCHECK(admissionLock_.is_locked());
  const auto it = evidence_.find(uuid);
  return it == evidence_.end() ? nullptr : &it->second;
}

std::optional<DatalossBoundGuard::ReconstructionCandidate>
DatalossBoundGuard::reconstructionCandidate(int64_t tailIndex) const {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  // Evaluation uses only the newest confirmed watermark, so a candidate must
  // be newer than it.
  int64_t newestIndex = 0;
  for (const auto& uuid : eligible_) {
    const PeerEvidence* evidence = evidenceUnlocked(uuid);
    if (evidence != nullptr && evidence->confirmedWatermark.has_value()) {
      newestIndex = std::max(newestIndex, evidence->confirmedWatermark->index);
    }
  }

  std::optional<ReconstructionCandidate> candidate;
  for (const auto& uuid : eligible_) {
    const PeerEvidence* evidence = evidenceUnlocked(uuid);
    if (evidence == nullptr || evidence->confirmedWatermark.has_value()) {
      continue;
    }
    // A position past 'tailIndex' is skipped: a remote can acknowledge
    // entries in the log cache before the queue publishes them as its tail.
    const int64_t index = evidence->confirmedPrefixIndex;
    if (index > newestIndex && index <= tailIndex) {
      candidate = ReconstructionCandidate{.uuid = uuid, .index = index};
      newestIndex = index;
    }
  }
  return candidate;
}

bool DatalossBoundGuard::installReconstructedWatermark(
    const std::string& uuid,
    const RemoteWatermark& watermark,
    int64_t generation) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  // A scan that spans a role change, truncation or measurement toggle summed
  // a backlog that may not be this log's.
  if (generation_.load(std::memory_order_relaxed) != generation ||
      role_ != Role::Leader || !measuring_.load(std::memory_order_relaxed)) {
    return false;
  }
  const auto it = evidence_.find(uuid);
  if (it == evidence_.end() ||
      std::find(eligible_.begin(), eligible_.end(), uuid) == eligible_.end()) {
    return false;
  }
  PeerEvidence& evidence = it->second;
  // A watermark confirmed while the scan ran is newer, so it is kept. A remote
  // that advanced during the scan gets the older watermark, which overstates
  // its lag until the ordinary path catches up.
  if (evidence.confirmedWatermark.has_value() ||
      evidence.confirmedPrefixIndex < watermark.index) {
    return false;
  }
  evidence.confirmedWatermark = watermark;
  maybeRecordBootstrapUnlocked();
  return true;
}

DatalossBoundHealth DatalossBoundGuard::health(const Config& config) const {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  return evaluateUnlocked(config, /*proposedOps=*/0, /*proposedBytes=*/0);
}

DatalossBoundHealth DatalossBoundGuard::evaluateUnlocked(
    const Config& config,
    int64_t proposedOps,
    int64_t proposedBytes) const {
  DCHECK(admissionLock_.is_locked());
  DatalossBoundHealth health;

  // Read reservations before the log tail. A release is sequenced after the
  // corresponding append, so observing it through the acquire below makes
  // that append visible to the later tail read. The opposite overlap can only
  // double-count. Every verdict reports them, so outstanding reservations stay
  // visible while the guard cannot evaluate, for example during bootstrap.
  const int64_t reservedOps = reservedOps_.load(std::memory_order_acquire);
  const int64_t reservedBytes = reservedBytes_.load(std::memory_order_acquire);
  health.reservedOps = reservedOps;
  health.reservedBytes = reservedBytes;

  if (!config.isMeasuring) {
    return health; // Off / None.
  }
  if (role_ == Role::Unknown) {
    health.status = DatalossBoundStatus::Violation;
    health.reason = DatalossBoundReason::LeaderBootstrap;
    return health;
  }
  if (role_ == Role::Follower) {
    // Reported rather than failed: a follower originates no protected writes,
    // so there is no lag to bound and admission does not consult this.
    health.status = DatalossBoundStatus::Violation;
    health.reason = DatalossBoundReason::NotLeader;
    return health;
  }

  const auto budget = usableBudget(config);
  if (!budget.has_value() || reservedOps < 0 || reservedBytes < 0) {
    health.status = DatalossBoundStatus::Violation;
    health.reason = DatalossBoundReason::InternalError;
    return health;
  }

  // A remote can acknowledge entries that are in the log cache but not yet
  // published as this log's tail, so its position can be briefly past
  // tailIndex_; ops lag is clamped at zero. Bytes lag cannot go negative: the
  // byte total only grows, and every watermark's bytes come from an earlier
  // reading of it. The sums saturate because the limits accept values up to
  // INT64_MAX, and a wrapped sum would read as within the budget.
  const int64_t tailIndex = tailIndex_.load(std::memory_order_relaxed);
  const int64_t tailBytes = cumulativePayloadBytes();
  const auto opsLagFrom = [&](int64_t index) {
    return folly::constexpr_add_overflow_clamped(
        folly::constexpr_add_overflow_clamped(
            std::max<int64_t>(0, tailIndex - index), reservedOps),
        proposedOps);
  };
  const auto bytesLagFrom = [&](int64_t bytes) {
    DCHECK_GE(tailBytes, bytes);
    return folly::constexpr_add_overflow_clamped(
        folly::constexpr_add_overflow_clamped(tailBytes - bytes, reservedBytes),
        proposedBytes);
  };

  // All watermarks refer to the same log prefix, so the highest one has no
  // greater ops or bytes lag than any lower one. The same holds for positions.
  int64_t bestPrefixIndex = 0;
  std::optional<RemoteWatermark> best;
  for (const auto& uuid : eligible_) {
    const PeerEvidence* evidence = evidenceUnlocked(uuid);
    if (evidence == nullptr) {
      continue;
    }
    bestPrefixIndex = std::max(bestPrefixIndex, evidence->confirmedPrefixIndex);
    const auto& watermark = evidence->confirmedWatermark;
    if (!watermark.has_value()) {
      continue;
    }
    if (opsLagFrom(watermark->index) < budget->ops &&
        bytesLagFrom(watermark->bytes) < budget->bytes) {
      health.qualifyingRemotes++;
    }
    if (!best.has_value() || watermark->index > best->index) {
      best = watermark;
    }
  }

  if (bestPrefixIndex > 0) {
    health.hasOpsLag = true;
    health.bestOpsLag = opsLagFrom(bestPrefixIndex);
  }

  if (!best.has_value()) {
    health.status = DatalossBoundStatus::Violation;
    // Distinguish "no eligible remote exists to measure against" from "one
    // exists but has not proven a position yet". Only the second resolves on
    // its own, so conflating them would hide a topology problem behind a
    // reason that looks transient.
    if (eligible_.empty()) {
      health.reason = DatalossBoundReason::NoRemote;
    } else if (health.hasOpsLag && health.bestOpsLag >= budget->ops) {
      health.reason = DatalossBoundReason::OpLimit;
    } else {
      health.reason = DatalossBoundReason::LeaderBootstrap;
    }
    return health;
  }

  const int64_t bestOps = opsLagFrom(best->index);
  const int64_t bestBytes = bytesLagFrom(best->bytes);
  const double opsHeadroom = static_cast<double>(budget->ops - bestOps) /
      static_cast<double>(budget->ops);
  const double byteHeadroom = static_cast<double>(budget->bytes - bestBytes) /
      static_cast<double>(budget->bytes);

  health.hasOpsLag = true;
  health.hasWatermark = true;
  health.bestOpsLag = bestOps;
  health.bestBytesLag = bestBytes;
  health.headroomFraction = std::min(opsHeadroom, byteHeadroom);

  if (bestOps < budget->ops && bestBytes < budget->bytes) {
    health.status = DatalossBoundStatus::Ok;
    health.reason = DatalossBoundReason::None;
    return health;
  }

  health.status = DatalossBoundStatus::Violation;
  // Report whichever dimension the best remote ran out of first. When both
  // are exhausted, bytes is the more actionable number to show an operator.
  health.reason = bestBytes >= budget->bytes ? DatalossBoundReason::ByteLimit
                                             : DatalossBoundReason::OpLimit;
  return health;
}

AdmissionDecision
DatalossBoundGuard::admit(const Config& config, int64_t ops, int64_t bytes) {
  AdmissionDecision decision;
  DatalossBoundHealth health;
  const auto budget = usableBudget(config);
  const bool invalidProposal = ops < 0 || bytes < 0;
  // Reserve at most the budget. A write at or past it is a violation
  // whatever its size, since the limits are exclusive, and the cap keeps
  // the reservation counters from overflowing.
  const int64_t reservedOps =
      budget.has_value() && !invalidProposal ? std::min(ops, budget->ops) : 0;
  const int64_t reservedBytes = budget.has_value() && !invalidProposal
      ? std::min(bytes, budget->bytes)
      : 0;
  {
    std::lock_guard<simple_mutexlock> lock(admissionLock_);
    if (invalidProposal) {
      health.status = DatalossBoundStatus::Violation;
      health.reason = DatalossBoundReason::InternalError;
    } else {
      health = evaluateUnlocked(config, reservedOps, reservedBytes);
    }

    if (health.reason == DatalossBoundReason::NotLeader) {
      return decision;
    }

    decision.reason = health.reason;
    decision.admitted =
        !(config.failClosed && health.status == DatalossBoundStatus::Violation);
    if (decision.admitted && !invalidProposal) {
      reservedOps_.fetch_add(reservedOps, std::memory_order_relaxed);
      reservedBytes_.fetch_add(reservedBytes, std::memory_order_relaxed);
      decision.reserved = true;
      decision.reservedOps = reservedOps;
      decision.reservedBytes = reservedBytes;
      decision.reservationOwner = reservationOwner_;
    }
  }

  const char* outcome = nullptr;
  if (decision.admitted && health.status == DatalossBoundStatus::Ok) {
    outcome = "admitted";
  } else if (decision.admitted) {
    // Would have been rejected under FAIL_CLOSED. This is the signal that
    // sizes the limits before anything depends on them.
    outcome = "would_reject";
  } else {
    outcome = "rejected";
  }
  STATS_datalossBoundDecisions.add(
      1, kStatsTag, outcome, datalossBoundReasonName(decision.reason));
  return decision;
}

void DatalossBoundGuard::release(const AdmissionDecision& reservation) {
  if (!reservation.reserved) {
    return;
  }
  if (reservation.reservationOwner != reservationOwner_) {
    // Only reachable when Raft is turned off and back on in a running mysqld
    // (rpl_raft_on) while this write was in flight: another queue admitted
    // it, and its counters went away with that queue. Subtracting here would
    // drive this guard's counters negative.
    STATS_datalossBoundDecisions.add(
        1,
        kStatsTag,
        "release_mismatch",
        datalossBoundReasonName(DatalossBoundReason::InternalError));
    KLOG_EVERY_N_SECS(WARNING, 60)
        << "Ignoring a bounded-data-loss reservation owned by a different "
           "queue";
    return;
  }
  // The append is sequenced before this release. An admission evaluation that
  // observes the reservation disappear must also observe the log accounting
  // that replaced it; the opposite overlap merely double-counts and is safe.
  reservedOps_.fetch_sub(reservation.reservedOps, std::memory_order_release);
  reservedBytes_.fetch_sub(
      reservation.reservedBytes, std::memory_order_release);
}

void DatalossBoundGuard::maybeRecordBootstrapUnlocked() {
  DCHECK(admissionLock_.is_locked());
  if (bootstrapCompleted_ || role_ != Role::Leader ||
      !measuring_.load(std::memory_order_relaxed)) {
    return;
  }
  for (const auto& uuid : eligible_) {
    const PeerEvidence* evidence = evidenceUnlocked(uuid);
    if (evidence != nullptr && evidence->confirmedWatermark.has_value()) {
      bootstrapCompleted_ = true;
      STATS_datalossBoundBootstrapMs.addValue(
          (MonoTime::Now() - periodStart_).ToMilliseconds(), kStatsTag);
      return;
    }
  }
}

void DatalossBoundGuard::startPeriodUnlocked(MonoTime now) {
  DCHECK(admissionLock_.is_locked());
  periodStart_ = now;
  bootstrapCompleted_ = false;
  lastState_.reset();
}

bool DatalossBoundGuard::recordState(const DatalossBoundHealth& health) {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  const auto state = std::make_pair(health.status, health.reason);
  const bool changed = lastState_ != state;
  lastState_ = state;
  return changed;
}

void DatalossBoundGuard::emitStats(const DatalossBoundHealth& health) {
  // One tick counted against the leader's current state, so a rate of time
  // spent in each state is recoverable.
  STATS_datalossBoundTicks.add(
      1, kStatsTag, datalossBoundStatusName(health.status));
  STATS_datalossBoundReasonTicks.add(
      1, kStatsTag, datalossBoundReasonName(health.reason));
  // When this is 1, losing that one remote leaves no replica inside the bound.
  STATS_datalossBoundQualifyingPeers.addValue(
      health.qualifyingRemotes, kStatsTag);

  // Only meaningful when some remote supplied them. Publishing zeros otherwise
  // would read as "fully caught up" -- the opposite of "we cannot tell".
  if (health.hasOpsLag) {
    STATS_datalossBoundOpsBehind.addValue(health.bestOpsLag, kStatsTag);
  }
  if (health.hasWatermark) {
    STATS_datalossBoundBytesBehind.addValue(health.bestBytesLag, kStatsTag);
    STATS_datalossBoundHeadroomPct.addValue(
        static_cast<int64_t>(std::floor(health.headroomFraction * 100.0)),
        kStatsTag);
  }
}

std::optional<DatalossBoundGuard::PeerEvidence>
DatalossBoundGuard::peerEvidenceForTests(const std::string& uuid) const {
  std::lock_guard<simple_mutexlock> lock(admissionLock_);
  const auto it = evidence_.find(uuid);
  if (it == evidence_.end()) {
    return std::nullopt;
  }
  return it->second;
}

} // namespace kudu::consensus
