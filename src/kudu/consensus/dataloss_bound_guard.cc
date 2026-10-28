// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/consensus/dataloss_bound_guard.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <string_view>

#include "kudu/util/Stats.h"

namespace kudu::consensus {

namespace {
// Dashboards and the ODS allowlist read these metrics under the
// consensus_queue prefix.
constexpr std::string_view kStatsTag = "consensus_queue";
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

int64_t DatalossBoundGuard::limitAfterMargin(
    int64_t limit,
    int32_t marginPercent) {
  const int64_t marginPct = std::clamp<int64_t>(marginPercent, 0, 99);
  // Multiplying first preserves precision for small limits; divide first only
  // where multiplication could overflow.
  const int64_t margin = limit <= std::numeric_limits<int64_t>::max() / 100
      ? limit * marginPct / 100
      : limit / 100 * marginPct;
  return limit - margin;
}

DatalossBoundHealth DatalossBoundGuard::evaluate(const Input& input) {
  DatalossBoundHealth health;

  if (!input.isMeasuring) {
    return health; // Off / None.
  }
  if (!input.isLeader) {
    health.status = DatalossBoundStatus::Violation;
    health.reason = DatalossBoundReason::NotLeader;
    return health;
  }

  // Margins are held inside the advertised limits, so the budget a peer may
  // actually consume is smaller than the number quoted externally.
  const int64_t usableOps = limitAfterMargin(input.maxOps, input.marginPercent);
  const int64_t usableBytes =
      limitAfterMargin(input.maxBytes, input.marginPercent);
  if (usableOps <= 0 || usableBytes <= 0) {
    health.status = DatalossBoundStatus::Violation;
    health.reason = DatalossBoundReason::InternalError;
    return health;
  }

  int64_t bestKnownPosition = 0;
  for (const RemoteEvidence& remote : input.remotes) {
    bestKnownPosition =
        std::max(bestKnownPosition, remote.confirmedPrefixIndex);
    if (!remote.watermark.has_value()) {
      continue;
    }

    const auto& watermark = *remote.watermark;
    const int64_t opsBehind =
        std::max<int64_t>(0, input.tailIndex - watermark.opId.index());
    const int64_t bytesBehind =
        std::max<int64_t>(0, input.tailBytes - watermark.bytes);
    const double opsHeadroom = static_cast<double>(usableOps - opsBehind) /
        static_cast<double>(usableOps);
    const double byteHeadroom = static_cast<double>(usableBytes - bytesBehind) /
        static_cast<double>(usableBytes);
    const double headroom = std::min(opsHeadroom, byteHeadroom);

    if (!health.hasWatermark || headroom > health.headroomFraction) {
      health.hasWatermark = true;
      health.hasOperationExposure = true;
      health.bestOpsBehind = opsBehind;
      health.bestBytesBehind = bytesBehind;
      health.headroomFraction = headroom;
    }
    if (opsBehind < usableOps && bytesBehind < usableBytes) {
      health.qualifyingPeers++;
    }
  }

  if (!health.hasWatermark) {
    health.status = DatalossBoundStatus::Violation;
    if (bestKnownPosition > 0) {
      health.hasOperationExposure = true;
      health.bestOpsBehind =
          std::max<int64_t>(0, input.tailIndex - bestKnownPosition);
    }
    if (health.hasOperationExposure && health.bestOpsBehind >= usableOps) {
      health.reason = DatalossBoundReason::OpLimit;
    } else {
      health.reason = input.remotes.empty()
          ? DatalossBoundReason::NoRemote
          : DatalossBoundReason::LeaderBootstrap;
    }
    return health;
  }

  if (health.qualifyingPeers > 0) {
    health.status = DatalossBoundStatus::Ok;
    health.reason = DatalossBoundReason::None;
    return health;
  }

  health.status = DatalossBoundStatus::Violation;
  // Report whichever dimension the best peer ran out of first. When both are
  // exhausted, bytes is the more actionable number to show an operator.
  health.reason = health.bestBytesBehind >= usableBytes
      ? DatalossBoundReason::ByteLimit
      : DatalossBoundReason::OpLimit;
  return health;
}

void DatalossBoundGuard::startPeriod(MonoTime now) {
  periodStart_ = now;
  bootstrapCompleted_ = false;
  lastState_.reset();
}

void DatalossBoundGuard::recordBootstrapEnd(MonoTime now) {
  if (bootstrapCompleted_) {
    return;
  }
  bootstrapCompleted_ = true;
  STATS_datalossBoundBootstrapMs.addValue(
      (now - periodStart_).ToMilliseconds(), kStatsTag);
}

bool DatalossBoundGuard::recordState(const DatalossBoundHealth& health) {
  const auto state = std::make_pair(health.status, health.reason);
  const bool changed = lastState_ != state;
  lastState_ = state;
  return changed;
}

void DatalossBoundGuard::emitStats(const DatalossBoundHealth& health) {
  STATS_datalossBoundTicks.add(
      1, kStatsTag, datalossBoundStatusName(health.status));
  STATS_datalossBoundReasonTicks.add(
      1, kStatsTag, datalossBoundReasonName(health.reason));
  STATS_datalossBoundQualifyingPeers.addValue(
      health.qualifyingPeers, kStatsTag);

  // Only meaningful when some peer supplied them. Publishing zeros otherwise
  // would read as "fully caught up" -- the opposite of "we cannot tell".
  if (health.hasOperationExposure) {
    STATS_datalossBoundOpsBehind.addValue(health.bestOpsBehind, kStatsTag);
  }
  if (health.hasWatermark) {
    STATS_datalossBoundBytesBehind.addValue(health.bestBytesBehind, kStatsTag);
    STATS_datalossBoundHeadroomPct.addValue(
        static_cast<int64_t>(std::floor(health.headroomFraction * 100.0)),
        kStatsTag);
  }
}

} // namespace kudu::consensus
