// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "kudu/consensus/opid.pb.h"
#include "kudu/util/monotime.h"

namespace kudu::consensus {

// Off when the bound is configured OFF. Ok when an eligible remote is within
// both limits. Violation otherwise; the reason says why.
enum class DatalossBoundStatus : uint8_t {
  Off,
  Ok,
  Violation,
};

// Why an evaluation failed. Low cardinality on purpose -- this is a metric
// dimension, not a message.
enum class DatalossBoundReason : uint8_t {
  None,
  // No eligible remote voter exists.
  NoRemote,
  // The best eligible remote is past the operation or byte limit.
  OpLimit,
  ByteLimit,
  // Watermarks are being re-established: a new leader term, a configuration
  // change, or measurement was just enabled.
  LeaderBootstrap,
  NotLeader,
  InternalError,
};

// Stable lowercase names shared by logs and metrics.
[[nodiscard]] const char* datalossBoundStatusName(DatalossBoundStatus status);
[[nodiscard]] const char* datalossBoundReasonName(DatalossBoundReason reason);

// A log position and the leader's cumulative payload bytes through it.
struct RemoteWatermark {
  OpId opId;
  // May be negative: a reconstructed watermark subtracts the peer's backlog
  // from the counter, and that backlog can include entries this queue never
  // counted. Only differences between readings on this queue are meaningful.
  int64_t bytes = 0;
};

struct DatalossBoundHealth {
  DatalossBoundStatus status = DatalossBoundStatus::Off;
  DatalossBoundReason reason = DatalossBoundReason::None;

  // Eligible remote voters with a watermark and room left in both dimensions.
  int32_t qualifyingPeers = 0;

  // The best eligible peer's exposure, and the fraction of the usable budget
  // still available to it. Both dimensions come from that one peer: mixing the
  // best ops from one peer with the best bytes from another would describe a
  // replica that does not exist.
  int64_t bestOpsBehind = 0;
  int64_t bestBytesBehind = 0;

  // min(remaining ops, remaining bytes) as a fraction of the usable budget.
  // Zero or negative when no peer is within the limits: how far past them the
  // best peer is.
  double headroomFraction = 0.0;

  // Operation exposure can be known before byte reconstruction completes.
  bool hasOperationExposure = false;
  // Some eligible peer has a confirmed watermark, so both exposures are known
  // for the best one.
  bool hasWatermark = false;
};

// Evaluates the bounded-data-loss limits and reports the result. It works on
// values the queue passes in and never calls back into the queue.
//
// The owning queue's queueLock_ protects the per-period state.
class DatalossBoundGuard {
 public:
  // An eligible remote voter's confirmed prefix and, once known, its
  // watermark.
  struct RemoteEvidence {
    int64_t confirmedPrefixIndex = 0;
    std::optional<RemoteWatermark> watermark;
  };

  struct Input {
    // Measurement is enabled and the mode is not OFF.
    bool isMeasuring = false;
    bool isLeader = false;
    int64_t maxOps = 0;
    int64_t maxBytes = 0;
    int32_t marginPercent = 0;
    int64_t tailIndex = 0;
    int64_t tailBytes = 0;
    // Eligible remote voters. Only read when measuring as the leader.
    std::vector<RemoteEvidence> remotes;
  };

  // Exposure of the best eligible remote against the limits.
  [[nodiscard]] static DatalossBoundHealth evaluate(const Input& input);

  // 'limit' less 'marginPercent' of it, clamped to [0, 99] percent.
  [[nodiscard]] static int64_t limitAfterMargin(
      int64_t limit,
      int32_t marginPercent);

  // Starts a measurement period, when this node becomes leader or measurement
  // is enabled. Bootstrap time and logged transitions restart from here.
  void startPeriod(MonoTime now);

  // Bootstrap ends when the first eligible remote of the period has a
  // confirmed watermark; from then on the exposure is measurable. Records the
  // time since the period started, once per period.
  void recordBootstrapEnd(MonoTime now);

  // Returns true if the health's (status, reason) differs from the last one
  // recorded in this period, so callers log transitions only.
  [[nodiscard]] bool recordState(const DatalossBoundHealth& health);

  // Publishes the per-tick status and reason counts and the exposure stats.
  static void emitStats(const DatalossBoundHealth& health);

 private:
  MonoTime periodStart_{MonoTime::Min()};
  bool bootstrapCompleted_ = false;
  std::optional<std::pair<DatalossBoundStatus, DatalossBoundReason>> lastState_;
};

} // namespace kudu::consensus
