// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"

namespace kudu::consensus {

// Off when the bound is configured OFF. Ok when an eligible remote is within
// both limits. Violation otherwise; the reason says why. Only FAIL_CLOSED
// rejects writes on a violation.
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

// Stable lowercase names shared by logs, metrics, and SHOW STATUS.
[[nodiscard]] const char* datalossBoundStatusName(DatalossBoundStatus status);
[[nodiscard]] const char* datalossBoundReasonName(DatalossBoundReason reason);

// Where an OpId stands relative to this leader's log.
enum class LogMatchResult {
  // The local log has the same OpId at that index.
  Match,
  // The local log has a different term at that index, or the index is past
  // its end, so the OpId cannot be on this leader's prefix.
  Mismatch,
  // The index is no longer retained locally, so there is no evidence either
  // way.
  Unavailable,
};

// A log position and the leader's cumulative payload bytes through it.
struct RemoteWatermark {
  int64_t index = 0;
  // May be negative: a reconstructed watermark subtracts the peer's backlog
  // from the counter, and that backlog can include entries this queue never
  // counted. Only differences between readings on this queue are meaningful.
  int64_t bytes = 0;
};

struct DatalossBoundHealth {
  DatalossBoundStatus status = DatalossBoundStatus::Off;
  DatalossBoundReason reason = DatalossBoundReason::None;

  // The best eligible remote's lag behind the leader's tail. Reserved writes
  // count as already appended.
  int64_t bestOpsLag = 0;
  int64_t bestBytesLag = 0;
  int64_t reservedOps = 0;
  int64_t reservedBytes = 0;

  // Eligible remotes with a confirmed watermark within both usable limits,
  // counting reserved writes. When this is 1, losing that remote leaves no
  // replica inside the bound.
  int64_t qualifyingRemotes = 0;

  // min(remaining ops, remaining bytes) as a fraction of the usable budget.
  // Zero or negative when no remote is within the limits: how far past them
  // the best remote is.
  double headroomFraction = 0.0;

  // Ops lag can be known before byte reconstruction completes.
  bool hasOpsLag = false;
  // Some eligible remote has a confirmed watermark, so both lags are known for
  // the best one.
  bool hasWatermark = false;
};

// Whether a proposed protected write may enter Raft.
struct AdmissionDecision {
  bool admitted = true;
  bool reserved = false;
  int64_t reservedOps = 0;
  int64_t reservedBytes = 0;
  // Identifies the guard whose counters hold this reservation.
  uint64_t reservationOwner = 0;
  // Why it was, or under FAIL_OPEN would have been, rejected.
  DatalossBoundReason reason = DatalossBoundReason::None;
};

// The bounded-data-loss guard measures how far the leader has run ahead of
// eligible remote voters. A disconnected remote continues to count at its last
// position confirmed to be on the leader's log prefix. Ops lag is the distance
// from that position to the leader's current tail.
//
// Bytes lag uses a leader-local watermark: the cumulative payload-byte count
// recorded at a log position and confirmed when a remote acknowledges that
// position. After an election, normal replication responses establish remote
// positions and the retained log index reconstructs bytes lag. The remote does
// not receive or store watermark state.
//
// FAIL_OPEN reports limit violations without rejecting writes. FAIL_CLOSED
// also rejects new protected writes on any violation: when a budget is
// exhausted, or when the guard cannot show it is not (no eligible remote,
// leader bootstrap, internal error).
//
// The guard owns the evidence it measures: each remote's confirmed position and
// watermarks, keyed by UUID, and the set of eligible remotes. The owning queue
// reports its own log's tail, its role, configuration changes, and remote
// replies as they happen. The guard never calls back into the queue.
//
// Locking. The queue reports events while holding its queueLock_, so the guard
// sees the tail and remote replies in the order the queue applies them. The
// guard's admissionLock_ protects its evidence and makes an admission's
// evaluation and reservation one step. It is taken inside queueLock_ and is
// never held while queueLock_ is acquired. Three writers skip a lock: onAppend
// writes the tail atomics without admissionLock_; installReconstructedWatermark
// runs without queueLock_ and rechecks everything it depends on under
// admissionLock_; and release() subtracts from the reservation atomics without
// either lock, which evaluateUnlocked's acquire read pairs with. Admission and
// health take only admissionLock_ and read the tail without queueLock_.
class DatalossBoundGuard {
 public:
  // The configured bound, built by the queue from flags.
  struct Config {
    // Measurement is enabled and the mode is not OFF.
    bool isMeasuring = false;
    // Reject protected writes on a violation.
    bool failClosed = false;
    int64_t maxOps = 0;
    int64_t maxBytes = 0;
    int32_t marginPercent = 0;
  };

  // The limits less the configured margin.
  struct UsableBudget {
    int64_t ops = 0;
    int64_t bytes = 0;
  };

  // What the guard knows about one remote's log.
  struct PeerEvidence {
    // The highest index the remote acknowledged that is known to be on this
    // leader's log prefix, or 0. Unverified catch-up hints never move it, and
    // it moves back when a matching reply shows the remote lost a suffix.
    int64_t confirmedPrefixIndex = 0;
    // The pending watermark stays fixed until the remote confirms it; using the
    // byte count at confirmation time would understate lag. The confirmed one
    // remains usable meanwhile.
    std::optional<RemoteWatermark> confirmedWatermark;
    std::optional<RemoteWatermark> pendingWatermark;
  };

  // An eligible remote whose confirmed watermark can be rebuilt by summing the
  // log from 'index' to the tail.
  struct ReconstructionCandidate {
    std::string uuid;
    int64_t index = 0;
  };

  // The usable budget, or nullopt if the margin leaves none.
  [[nodiscard]] static std::optional<UsableBudget> usableBudget(
      const Config& config);

  DatalossBoundGuard();

  // This node's log now ends at 'lastIndex', 'payloadBytes' longer than
  // before. Reported in every role, so the tail and byte total are current the
  // moment this node becomes leader. Pass zero bytes when not measuring, and
  // when the queue first opens its log. Called in the same critical section
  // that sets the queue's tail, and before any reservation covering the
  // appended entries is released.
  void onAppend(int64_t lastIndex, int64_t payloadBytes);

  // This node's log now ends at 'lastIndex' after discarding later entries (a
  // follower discarding a divergent suffix). Forgets all remote evidence,
  // which may describe the discarded entries, and starts a new generation: a
  // byte count recorded past 'lastIndex' no longer describes this log.
  void onTruncate(int64_t lastIndex);

  // Running total of replicated payload bytes appended while measuring.
  // Subtracting the value captured with a remote's watermark gives its bytes
  // lag without rescanning the log.
  //
  // Counts the write payload of every appended entry, not only client
  // transactions: the bootstrap scan reconstructs this from the log index,
  // which carries payload length but not op type, and the two must measure
  // the same quantity. Other entries therefore count whatever payload they
  // carry, which is the conservative direction.
  [[nodiscard]] int64_t cumulativePayloadBytes() const;

  // Changes whenever recorded byte counts stop describing this log: a new
  // role, a truncation, or measurement turned on or off. A reader that works
  // without queueLock_, such as a reconstruction scan, records it with its
  // view of the tail and discards its result if it changed.
  [[nodiscard]] int64_t generation() const;

  // This node now leads a new term. Forgets all remote evidence and the
  // eligible set, and starts a new generation and measurement period.
  void becomeLeader(MonoTime now);

  // This node no longer leads. Forgets all remote evidence and the eligible
  // set, and starts a new generation.
  void becomeFollower();

  // Applies the measurement switch. Turning it either way drops all
  // watermarks, which cannot span an interval in which bytes were not
  // counted, and starts a new generation. Turning it on also starts a
  // measurement period. Confirmed positions stay valid.
  void setMeasuring(bool measuring, MonoTime now);
  [[nodiscard]] bool isMeasuring() const;

  // Replaces the remotes admission measures against with 'eligibleUuids', and
  // forgets evidence for any remote not in 'memberUuids'. Both hold permanent
  // UUIDs.
  void setEligibleRemotes(
      std::vector<std::string> eligibleUuids,
      const std::vector<std::string>& memberUuids);

  // Applies a reply from remote 'uuid' whose last entry is at
  // 'lastReceivedIndex' and matched this leader's log as 'match'. Only a reply
  // that accepted the request ('accepted') moves the confirmed position
  // forward or advances a watermark.
  void onPeerResponse(
      const std::string& uuid,
      LogMatchResult match,
      int64_t lastReceivedIndex,
      bool accepted);

  // Forgets everything known about remote 'uuid'. It must confirm a new
  // position before it can reduce the reported lag.
  void forgetPeer(const std::string& uuid);

  // The eligible remote with the newest confirmed position at or before
  // 'tailIndex' that has no confirmed watermark, if that position is newer
  // than every confirmed watermark. Evaluation uses only the newest watermark,
  // so no other remote's reconstruction could lower the lag.
  [[nodiscard]] std::optional<ReconstructionCandidate> reconstructionCandidate(
      int64_t tailIndex) const;

  // Installs a rebuilt watermark for remote 'uuid' if 'generation' is still
  // current, this node leads and measures, and the remote is still eligible,
  // has no confirmed watermark, and still holds 'watermark.index'. Returns
  // whether it was installed.
  [[nodiscard]] bool installReconstructedWatermark(
      const std::string& uuid,
      const RemoteWatermark& watermark,
      int64_t generation);

  // Lag against the leader's last appended log entries, plus admitted writes
  // not yet in the log.
  [[nodiscard]] DatalossBoundHealth health(const Config& config) const;

  // Decides whether a protected write of 'ops' Raft entries and 'bytes' of
  // replicated payload may enter Raft, and reserves that capacity if admitted.
  // FAIL_OPEN always admits and reserves, so it measures the decision
  // FAIL_CLOSED would have made under concurrency.
  //
  // Admits without a reservation when this node does not lead: a follower
  // originates no protected writes, and blocking one would stall the apply
  // path this guard is not about.
  [[nodiscard]] AdmissionDecision
  admit(const Config& config, int64_t ops, int64_t bytes);

  // Releases exactly what admit() reserved for 'reservation'; a no-op unless
  // reservation.reserved. Call exactly once per reservation, after the write
  // reaches the log or definitively aborts. A second release drives the
  // counters negative, which evaluates as internal_error.
  void release(const AdmissionDecision& reservation);

  // Returns true if the health's (status, reason) differs from the last one
  // recorded in this period, so callers log transitions only.
  [[nodiscard]] bool recordState(const DatalossBoundHealth& health);

  // Publishes the per-tick status and reason counts, the lag stats, and the
  // number of eligible remotes within both limits.
  static void emitStats(const DatalossBoundHealth& health);

  [[nodiscard]] std::optional<PeerEvidence> peerEvidenceForTests(
      const std::string& uuid) const;

 private:
  enum class Role : uint8_t {
    // No role has been reported yet. This must not read as "not the leader,
    // so nothing to bound": that is the one role that waves writes through,
    // and a newly elected leader would then admit without limit.
    Unknown,
    Leader,
    Follower,
  };

  // The single evaluator. Health passes a zero proposal and admission passes
  // the write it is deciding on, so the two cannot disagree about what the
  // limits mean. Caller holds admissionLock_.
  [[nodiscard]] DatalossBoundHealth evaluateUnlocked(
      const Config& config,
      int64_t proposedOps,
      int64_t proposedBytes) const;

  // Promotes a confirmed pending watermark, then records the next one at the
  // tail. Caller holds admissionLock_, and queueLock_ so the tail and byte
  // total describe the same point in the log.
  void advanceWatermarkUnlocked(PeerEvidence& evidence);

  // The evidence for 'uuid', or nullptr. Caller holds admissionLock_.
  [[nodiscard]] const PeerEvidence* evidenceUnlocked(
      const std::string& uuid) const;

  // Records the time to the period's first usable watermark, once. Caller
  // holds admissionLock_.
  void maybeRecordBootstrapUnlocked();

  // Caller holds admissionLock_.
  void startPeriodUnlocked(MonoTime now);

  mutable simple_mutexlock admissionLock_;
  Role role_ = Role::Unknown;
  std::unordered_map<std::string, PeerEvidence> evidence_;
  std::vector<std::string> eligible_;
  std::atomic<bool> measuring_{false};

  std::atomic<int64_t> reservedOps_{0};
  std::atomic<int64_t> reservedBytes_{0};
  const uint64_t reservationOwner_;

  // The queue's last appended index and cumulative payload bytes, written
  // together under queueLock_. A reader without queueLock_ can see them one
  // append batch apart. Admission is such a reader: a protected write stays
  // reserved in reservedBytes_ until its append is published, and entries
  // that bypass admission are covered by the configured margin.
  std::atomic<int64_t> tailIndex_{0};
  std::atomic<int64_t> cumulativePayloadBytes_{0};
  std::atomic<int64_t> generation_{0};

  // Per measurement period.
  MonoTime periodStart_{MonoTime::Min()};
  bool bootstrapCompleted_ = false;
  std::optional<std::pair<DatalossBoundStatus, DatalossBoundReason>> lastState_;
};

} // namespace kudu::consensus
