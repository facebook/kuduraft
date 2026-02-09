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

#ifndef KUDU_CONSENSUS_LEADER_ELECTION_H
#define KUDU_CONSENSUS_LEADER_ELECTION_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu::consensus {

using ConsensusTerm = int64_t;

class FlexibleVoteCounterTest;

// The vote a peer has given.
enum ElectionVote {
  VOTE_DENIED = 0,
  VOTE_GRANTED = 1,
};

/**
 * A decision for an election or for a vote counting mechanism.
 *
 * FlexiRaft for example has multiple counting mechanisms. Some may indicate
 * "LOST" but others may indicate "WON". If the later is a valid mechanism, the
 * overall result is still "WON".
 */
enum ElectionDecision {
  WON = 0,
  UNDECIDED = 1,
  LOST = 2,
};

// How the election was decided
enum class ElectionDecisionMethod {
  SIMPLE_MAJORITY,
  CONTINUOUS_LKL_QUORUM,
  PESSIMISTIC_QUORUM,
  VOTER_HISTORY,
  INVALIDATED_BY_HIGHER_TERM,
  JOINT_CONSENSUS_LEADER_ELECTION,
};

// Details of the vote received from a peer.
struct VoteInfo {
  ElectionVote vote;

  // Previous voting history of this voter.
  std::vector<PreviousVotePB> previous_vote_history;
  int64_t last_pruned_term;

  // Was this candidate peer removed from the voter's committed config
  bool is_candidate_removed = false;
};

// Internal structure to denote the optimizer's computation of the
// next potential leader.
struct PotentialNextLeadersResponse {
  enum Status {
    ERROR = 0,
    POTENTIAL_NEXT_LEADERS_DETECTED = 1,
    WAITING_FOR_MORE_VOTES = 2,
    ALL_INTERMEDIATE_TERMS_SCANNED = 3
  };
  PotentialNextLeadersResponse(Status);
  PotentialNextLeadersResponse(Status, const std::set<std::string>&, int64_t);
  PotentialNextLeadersResponse(
      Status,
      const std::set<std::string>&,
      int64_t,
      bool);
  Status status;
  std::set<std::string> potential_leader_regions;
  // term of the potential leader
  int64_t next_term;
  // If we used votes that have not arrived in a conservative way to include
  // potential leaders
  bool used_unreceived_votes = false;
};

/**
 * A struct to encompasss the current state of quorum statisfaction, and how
 * we arrived at that state.
 */
struct ElectionDecisionState {
  /**
   * Creates a new decision state with only the decision mechanism.
   *
   * The state of the election is defaulted to undecided.
   *
   * @param mechanism The mechanism used to arrive at the decision
   */
  explicit ElectionDecisionState(ElectionDecisionMethod mechanism);

  /**
   * Creates a new decisions state based on the mechanism and if the election is
   * won or can be won.
   *
   * @param decision The decision for the election
   * @param mechanism The mechanism used to arrive at the decision
   * @param consideredQuorumIds The regions that were considered when making the
   * decision
   */
  ElectionDecisionState(
      ElectionDecision decision,
      ElectionDecisionMethod mechanism,
      std::set<std::string> consideredQuorumIds = {});

  /**
   * The current decision for the election or for a vote counting mechanism.
   */
  ElectionDecision decision = ElectionDecision::UNDECIDED;
  /**
   * How we arrived at the decision for achievedMajority.
   * Note that canAchieveMajority might be different from what the mechanism
   * specifies. For example, we might determine we cannot win via pessimistic
   * quorum, but that doesn't mean we can't win via other mechanisms.
   */
  ElectionDecisionMethod decisionMechanism;

  /**
   * Quorums that were considered when making the decision.
   * For example, CONTINUOUS_LKL_QUORUM will only consider LKL region.
   */
  std::set<std::string> consideredQuorumIds = {};

  /**
   * Returns true if we can say the election is definitively won or lost.
   *
   * @return true if election is decided
   */
  bool decided() const;
};

// Simple class to count votes (in-memory, not persisted to disk).
// This class is not thread safe and requires external synchronization.
class VoteCounter {
 public:
  // Create new VoteCounter with the given majority size.
  VoteCounter(int num_voters, int majority_size);
  virtual ~VoteCounter() = default;

  // Register a peer's vote.
  //
  // If the voter already has a vote recorded, but it has a different value than
  // the vote specified, returns Status::IllegalArgument.
  //
  // If the same vote is duplicated, 'is_duplicate' is set to true.
  // Otherwise, it is set to false.
  // If an OK status is not returned, the value in 'is_duplicate' is undefined.
  virtual Status RegisterVote(
      const std::string& voter_uuid,
      const VoteInfo& vote_info,
      bool* is_duplicate);

  /**
   * Returns the state of decision of the current election.
   *
   * This contains information if the election has been decided, if it can still
   * be won, as well as metadata about how the state was reached. The state can
   * change as more votes come in.
   *
   * @return The election decision state
   */
  virtual ElectionDecisionState GetDecision() const;

  bool IsCandidateRemoved() const;

  // Return the total of "Yes" and "No" votes.
  int GetTotalVotesCounted() const;

  // Return total number of expected votes.
  int GetTotalExpectedVotes() const {
    return num_voters_;
  }

  // Return true iff GetTotalVotesCounted() == num_voters_;
  bool AreAllVotesIn() const;

  /**
   * Returns a string representation for the vote tally.
   *
   * Prints the tally in (Y/N/A|R/T) => (Yes/No/Absent|Required/Total) form.
   *
   * Useful for logging the current votes.
   *
   * @param ElectionDecisionState The metadata containing the election decision
   * (VoterCounter itself doesn't need anything from this so this is unused)
   * @return A string showing the vote tally.
   */
  virtual std::string printableVoteTally(const ElectionDecisionState&) const;

 protected:
  int num_voters_;

  using VoteMap = std::map<std::string, VoteInfo>;
  VoteMap votes_; // Voting record.

  // Set to true when any voters respond with a 'no' vote and also indicate that
  // this candidate peer is removed from the committed config on the voter node
  bool is_candidate_removed_;

 private:
  friend class VoteCounterTest;

  const int majority_size_;
  int yes_votes_; // Accumulated yes votes, for quick counting.
  int no_votes_; // Accumulated no votes.
  DISALLOW_COPY_AND_ASSIGN(VoteCounter);
};

// Class to enable FlexiRaft vote counting for different quorum modes.
class FlexibleVoteCounter : public VoteCounter {
 public:
  FlexibleVoteCounter(
      std::string candidate_uuid,
      int64_t election_term,
      LastKnownLeaderPB last_known_leader,
      RaftConfigPB config,
      bool adjust_voter_distribution);

  // Synchronization is done by the LeaderElection class. Therefore, VoteCounter
  // class doesn't need to take care of thread safety of its book-keeping
  // variables.
  Status RegisterVote(
      const std::string& voter_uuid,
      const VoteInfo& vote,
      bool* is_duplicate) override;

  /**
   * Returns the state of decision of the current election.
   *
   * Since flexiraft contains different mechanisms for evaluating quorum
   * statisfcation, the state shows the last method used to arrive at the state.
   * The state, as well as the mechanism itself can change as more votes come
   * in, since certain mechanisms can be more decisive once more votes are in.
   *
   * @return The election decision state
   */
  virtual ElectionDecisionState GetDecision() const override;

  /**
   * Returns a string representation for the vote tally.
   *
   * Outputs one per quorum in Y/N/A|R/T form. (see
   * VoteCounter::printableVoteTally).
   *
   * Useful for logging the current votes.
   *
   * @param state The metadata containg the election decision
   * @return A string showing the vote tally.
   */
  virtual std::string printableVoteTally(
      const ElectionDecisionState& state) const override;

 private:
  friend class FlexibleVoteCounterTest;
  FRIEND_TEST(FlexibleVoteCounterTest, FetchRegionalPrunedCounts);
  // A safeguard max iteration count to prevent against future bugs.
  static const int64_t kQuorumOptimizationIterationCountMax = 10000;

  // Mapping from region to set of voter UUIDs that have responded in that
  // region.
  using RegionToVoterSet = std::map<std::string, std::set<std::string>>;

  // UUID and term pair for which a vote was given.
  using UUIDTermPair = std::pair<std::string, int64_t>;

  // A mapping from UUID term pair to maps from region to voter sets in those
  // regions that have voted for the UUID term pair.
  using VoteHistoryCollation = std::map<UUIDTermPair, RegionToVoterSet>;

  // Fetches topology information required by the flexible vote counter.
  void FetchTopologyInfo();

  // Fetches the number of votes that still haven't arrived in this election
  // cycle from the given `region`.
  // If use_vd is true, we consider total votes based on voter_distribution_
  // instead of the config
  int FetchVotesRemainingInRegion(const std::string& region, bool use_vd) const;

  // Populates the number of servers per region that have pruned voting
  // history >= `term`.
  void FetchRegionalPrunedCounts(
      int64_t term,
      std::map<std::string, int32_t>* region_pruned_counts) const;

  // Populates the number of servers per region that have not pruned voting
  // history beyond `term`, i.e. have a full voting history > `term`.
  // Note: the result can't not be used to determine voting history for
  // `term` itself but only for `term + 1` and beyond.
  void FetchRegionalUnprunedCounts(
      int64_t term,
      std::map<std::string, int32_t>* region_unpruned_counts) const;

  // Fetch the region corresponding to server UUID. Returns an empty string
  // if UUID is not found. This could happen during a configuration change,
  // if a server was removed from the configuration.
  std::string DetermineQuorumIdForUUID(const std::string& uuid) const;

  /**
   * Returns a vector of election decision states for each quorum id.
   *
   * @param regions The list of quorum ids
   * @return A vector in order of input for the decision for each quorum id
   */
  std::vector<ElectionDecision> IsMajoritySatisfiedInRegions(
      const std::vector<std::string>& regions) const;

  /**
   * Returns if the majority is satisfied in all quorum_ids.
   *
   * This pessimistically combines the results for all the quorum ids. If all of
   * the says WON, then the combined result is WON. If there are UNDECIDED cases
   * but no LOST cases, then it's UNDECIDED. If there are any LOST cases, then
   * the combined result is LOST.
   *
   * @param regions The list of quorum ids
   * @return The decision (WON|UNDECIDED|LOST)
   */
  ElectionDecision IsMajoritySatisfiedInAllRegions(
      const std::set<std::string>& leader_regions) const;

  /**
   * Returns the quorum satisfaction decision for a given quorum id.
   *
   * @param region The quorum id
   * @return The decision (WON|UNDECIDED|LOST)
   */
  ElectionDecision IsMajoritySatisfiedInRegion(const std::string& region) const;

  /**
   * Returns if we satisfied quorum in a majorirty of quorum ids
   *
   * @return The decision (WON|UNDECIDED|LOST)
   */
  ElectionDecision IsMajoritySatisfiedInMajorityOfRegions() const;

  /**
   * Returns true if the majority is satisfied for both the last known leader
   * quorum ids as well as the candidate quorum id if specified.
   *
   * Which majority is needed is controlled by gflags.
   *
   * @param last_known_leader_regions The last known leader quorum ids
   * @param candidate_region The candidate quorum id
   * @return The decision (WON|UNDECIDED|LOST)
   */
  ElectionDecision AreMajoritiesSatisfied(
      const std::set<std::string>& last_known_leader_regions,
      const std::string& candidate_region) const;

  // Returns if we're able to get a majority from all quorums, and if it's still
  // possible to do so.
  ElectionDecisionState GetPessimisticQuorumDecision() const;

  // Figure out if `vote_count` satisfies the majority
  // in `region`.
  // `vote_count` is computed from the voting history.
  // Returns a pair of booleans representing:
  // 1. if the quorum is satisfied in the current state
  // 2. if the quorum can still be satisfied in the current state
  // Please note the underlying assumption that the configuration for
  // leader election quorum remains immutable.
  // `pruned_count` is the number of voters in the `region` which
  // have pruned their voting histories beyond the last known leader's
  // election term.
  std::tuple<bool, bool, bool> DoHistoricalVotesSatisfyMajorityInRegion(
      const std::string& region,
      const RegionToVoterSet& region_to_voter_set,
      const std::map<std::string, int32_t>& region_pruned_counts) const;

  // Return the last known leader.
  void GetLastKnownLeader(LastKnownLeaderPB* last_known_leader) const;

  // Extends the `next_leader_regions` set to include the regions of the UUIDs
  // in `next_leader_uuids`. Returns an error status if the UUID does not
  // belong to the configuration.
  Status ExtendNextLeaderRegions(
      const std::set<std::string>& next_leader_uuids,
      std::set<std::string>* next_leader_regions) const;

  // Iterates over all the votes that have come in so far and collates them
  // corresponding to each UUID term pair for the purpose of determining if
  // one of those UUIDs could have won an election after the `term` provided.
  // It also computes the `min_term` greater than `term` in which one of the
  // servers from the `leader_regions` had voted.
  void ConstructRegionWiseVoteCollation(
      int64_t term,
      VoteHistoryCollation* vote_collation,
      int64_t* min_term) const;

  // Helper function which determines if there are enough votes from each
  // potential leader region and if the majority has prior voting history
  // available. Returns true if both conditions are met and false otherwise.
  bool EnoughVotesWithSufficientHistories(
      int64_t term,
      const std::set<std::string>& leader_regions) const;

  // Helper function to go over all the `leader_regions` and see if the
  // `region_to_voter_set` representing the historical votes that a
  // `candidate_uuid` got in the past was enough for it to have won an
  // election in that term. The side effects of this function include updating
  // `potential_leader_uuids` to include `candidate_uuid` if it could have won
  // an election in the past.
  void AppendPotentialLeaderUUID(
      const std::string& candidate_uuid,
      const std::set<std::string>& leader_regions,
      const RegionToVoterSet& region_to_voter_set,
      const std::map<std::string, int32_t>& region_pruned_counts,
      std::set<std::string>* potential_leader_uuids,
      bool* used_unreceived_votes) const;

  // Optimizer function which tries to recursively figure out the next leader
  // regions since the last term provided and the potential set of leaders
  // regions in that term. It returns the set of potential leader regions and
  // the next term to consider. It is possible that the next possible leader
  // regions cannot be determined in which case, the situation is reflected in
  // the status.
  PotentialNextLeadersResponse GetPotentialNextLeaders(
      int64_t term,
      const std::set<std::string>& leader_regions) const;

  // Given information about the last known leader, this helper function
  // utilizes the voting histories sent by the voters to determine the outcome
  // of the leader election. History is used to determine a list of regions
  // which could potentially have the current leader.
  ElectionDecisionState ComputeElectionDecisionFromVotingHistory(
      const LastKnownLeaderPB& last_known_leader,
      const std::string& last_known_leader_region,
      const std::string& candidate_region) const;

  // Returns if we're able to statisfy the quorum and if it's still possible to
  // do so for a dynamic quorum
  ElectionDecisionState GetDynamicQuorumDecision() const;

  // Generic log prefix.
  std::string LogPrefix() const;

  const std::string candidate_uuid_;

  // Term of this election.
  const int64_t election_term_;

  // Mapping from each region to number of active voters.
  std::map<std::string, int> voter_distribution_;

  // Should we adjust voter distribution based on current config?
  const bool adjust_voter_distribution_;

  // Vote count per region.
  std::map<std::string, int> yes_vote_count_, no_vote_count_;

  // Last known leader properties.
  const LastKnownLeaderPB last_known_leader_;

  // Config at the beginning of the leader election.
  const RaftConfigPB config_;

  // Number of voters in each quorum
  std::unordered_map<std::string, size_t> num_voters_per_quorum_id_;

  // UUID to quorum_id map derived from RaftConfigPB.
  std::map<std::string, std::string> uuid_to_quorum_id_;

  // UUID to last term pruned mapping.
  std::map<std::string, int64_t> uuid_to_last_term_pruned_;

  // Time when vote counter object was created
  std::chrono::time_point<std::chrono::system_clock> creation_time_;

  // The quorum ids considered by the last voter history run
  std::unordered_set<std::string> voter_history_quorum_ids_;

  DISALLOW_COPY_AND_ASSIGN(FlexibleVoteCounter);
};

// Class to enable joint-consensus vote counting, which considers the voters
// from the new config (C_new) and the voters from the old config (C_old).
class JointConsensusVoteCounter : public VoteCounter {
 public:
  // Creates a new vote counter for joint-consensus given the active
  // transitional config. It creates two vote counters, one for the old config
  // and one for the new config, then returns the joint-consensus vote counter.
  static std::unique_ptr<JointConsensusVoteCounter> Create(
      const RaftConfigPB& active_transitional_config);

  // Constructor, given the active transitional config, and the two non-null
  // vote counters from that active config. The vote counters *must* be created
  // for the peers in the old and new config from `active_transitional_config`.
  JointConsensusVoteCounter(
      const RaftConfigPB& active_transitional_config,
      std::unique_ptr<VoteCounter> old_conf_vote_counter,
      std::unique_ptr<VoteCounter> new_conf_vote_counter);

  /**
   * Registers a peer's vote, either in the old config or the new config.
   * Registering vote from unknown peer will return an error.
   *
   * @param voter_uuid UUID of the voter, either old or new config's peer.
   * @param vote_info The vote info, either VOTE_GRANTED or VOTE_DENIED.
   * @param is_duplicate Pointer to a bool, which will be set to true iff the
   *        vote is a duplicate. It is undefined if the return status is error.
   *
   * @return Status::OK if the vote is registered successfully internally for
   *         the old config's counter, new config's counter, or both.
   */
  Status RegisterVote(
      const std::string& voter_uuid,
      const VoteInfo& vote_info,
      bool* is_duplicate) override;

  /**
   * Returns the decision's state given the current votes so far.
   *
   * @return The election decision state, either WON, LOST, or UNDECIDED, along
   * with metadata indicating decision from JOINT_CONSENSUS_LEADER_ELECTION.
   */
  ElectionDecisionState GetDecision() const override;

  /**
   * Return a string representation of the vote tally so far. The format is
   * roughly the following:
   *  Old Config: (Y/N/A|R/T)
   *  New Config: (Y/N/A|R/T)
   * where (Y/N/A|R/T) => (Yes/No/Absent|Required/Total).
   *
   * @param decision The decision obtained from GetDecision(), which will be
   *        passed to the underlying vote counters.
   *
   * @return A string representation of the vote tally.
   */
  std::string printableVoteTally(
      const ElectionDecisionState& decision) const override;

 private:
  /**
   * Combines the election decision from peers in the old config and peers in
   * the new config into a single decision. This is used for leader election
   * during joint-consensus where we have two configs to consider (C_old and
   * C_new). The decision WON is generated iff both configs' decision are WON.
   * The decision is LOST if at least one of the config's decision is LOST.
   *
   * @param old_conf_decision Decision obtained from vote counter of old config.
   * @param old_conf_decision Decision obtained from vote counter of new config.
   * @return The combined decision for election during joint-consensus.
   */
  static ElectionDecisionState CombineElectionDecisionState(
      const ElectionDecisionState& old_conf_decision,
      const ElectionDecisionState& new_conf_decision);

  // Helper enum to clasify Voter membership for leader-election during
  // joint-consensus. A voter can be part of old config, new config, or both.
  enum class VoterConfigMembership {
    OLD_CONFIG_ONLY,
    NEW_CONFIG_ONLY,
    OLD_AND_NEW_CONFIG,
  };
  using VoterConfigMap = std::unordered_map<std::string, VoterConfigMembership>;

  /**
   * Generates a mapping from UUID to VoterConfigMembership for all voters
   * in the config. The mapping is used to consider the correct VoteCounter as a
   * peer can be present in the OLD_CONFIG_ONLY, NEW_CONFIG_ONLY, or
   * OLD_AND_NEW_CONFIG.
   *
   * @param config The Raft config, having `peers` and `next_config_peers`.
   * @return A mapping from UUID to VoterConfigMembership.
   */
  static VoterConfigMap PopulateVoterConfigMapping(
      const RaftConfigPB& active_transitional_config);

  // Mapping from UUID to VoterConfigMembership.
  // Initialized in constructor using `PopulateVoterConfigMapping(..)`.
  const VoterConfigMap voter_map_;

  // Vote counters for voter in the old config and voter in the new config.
  const std::unique_ptr<VoteCounter> old_conf_vote_counter_;
  const std::unique_ptr<VoteCounter> new_conf_vote_counter_;
};

// The result of a leader election.
struct ElectionResult {
 public:
  ElectionResult(
      VoteRequestPB vote_request,
      ElectionVote decision,
      ConsensusTerm highest_term,
      const std::string& message,
      bool is_candidate_removed,
      ElectionDecisionMethod decision_method);

  // The vote request that was sent to the voters for this election.
  const VoteRequestPB vote_request;

  // The overall election GRANTED/DENIED decision of the configuration.
  const ElectionVote decision;

  // The highest term seen from any voter.
  const ConsensusTerm highest_voter_term;

  // Human-readable explanation of the vote result, if any.
  const std::string message;

  // Set to true when the decision is VOTE_DENIED and atleast one voter
  // responded with a 'no' vote and indicated that the candidate has been
  // removed from the voter's committed config
  const bool is_candidate_removed;

  // How we arrived the at result of the election
  const ElectionDecisionMethod decision_method;
};

class VoteLoggerInterface {
 public:
  virtual ~VoteLoggerInterface() = default;

  virtual void logElectionStarted(
      const VoteRequestPB& voteRequest,
      const RaftConfigPB& config) = 0;
  virtual void logVoteReceived(const VoteResponsePB& voteResponse) = 0;
  virtual void logElectionDecided(const ElectionResult& electionResult) = 0;
  virtual void advanceEpoch(int64_t epoch) = 0;
};

// Driver class to run a leader election.
//
// The caller must pass a callback to the driver, which will be called exactly
// once when a Yes/No decision has been made, except in case of Shutdown()
// on the Messenger or test ThreadPool, in which case no guarantee of a
// callback is provided. In that case, we should not care about the election
// result, because the server is ostensibly shutting down.
//
// For a "Yes" decision, a majority of voters must grant their vote.
//
// A "No" decision may be caused by either one of the following:
// - One of the peers replies with a higher term before a decision is made.
// - A majority of the peers votes "No".
//
// Any votes that come in after a decision has been made and the callback has
// been invoked are logged but ignored. Note that this somewhat strays from the
// letter of the Raft paper, in that replies that come after a "Yes" decision
// do not immediately cause the candidate/leader to step down, but this keeps
// our implementation and API simple, and the newly-minted leader will soon
// discover that it must step down when it attempts to replicate its first
// message to the peers.
//
// This class is thread-safe.
class LeaderElection : public std::enable_shared_from_this<LeaderElection> {
 public:
  using ElectionDecisionCallback = std::function<void(const ElectionResult&)>;

  // Set up a new leader election driver.
  //
  // 'proxy_factory' must not go out of scope while LeaderElection is alive.
  //
  // The 'vote_counter' must be initialized with the candidate's own yes vote.
  LeaderElection(
      RaftConfigPB config,
      PeerProxyFactory* proxy_factory,
      VoteRequestPB request,
      std::unique_ptr<VoteCounter> vote_counter,
      MonoDelta timeout,
      ElectionDecisionCallback decision_callback,
      std::shared_ptr<VoteLoggerInterface> vote_logger);

  // Run the election: send the vote request to followers.
  void Run();

  ~LeaderElection();

 private:
  struct VoterState {
    std::string peer_uuid;
    std::shared_ptr<PeerProxy> proxy;

    // If constructing the proxy failed (e.g. due to a DNS resolution issue)
    // then 'proxy' will be NULL, and 'proxy_status' will contain the error.
    Status proxy_status;

    rpc::RpcController rpc;
    VoteRequestPB request;
    VoteResponsePB response;

    std::string PeerInfo() const;
  };

  using VoterStateMap = std::unordered_map<std::string, VoterState*>;
  using Lock = simple_spinlock;

  // Check to see if a decision has been made. If so, invoke decision callback.
  // Calls the callback outside of holding a lock.
  void CheckForDecision();

  // Callback called when the RPC responds.
  void VoteResponseRpcCallback(const std::string& voter_uuid);

  // Record vote from specified peer.
  void RecordVoteUnlocked(const VoterState& state, ElectionVote vote);

  // Handle a peer that reponded with a term greater than the election term.
  void HandleHigherTermUnlocked(const VoterState& state);

  // Log and record a granted vote.
  void HandleVoteGrantedUnlocked(const VoterState& state);

  // Log the reason for a denied vote and record it.
  void HandleVoteDeniedUnlocked(const VoterState& state);

  // Returns a string to be prefixed to all log entries.
  // This method accesses const members and is thread safe.
  std::string LogPrefix() const;

  // Helper to reference the term we are running the election for.
  ConsensusTerm election_term() const {
    return request_.candidate_term();
  }

  // All non-const fields are protected by 'lock_'.
  Lock lock_;

  // The result returned by the ElectionDecisionCallback.
  // NULL if not yet known.
  std::unique_ptr<ElectionResult> result_;

  // Whether we have responded via the callback yet.
  bool has_responded_;

  // Active Raft configuration at election start time.
  const RaftConfigPB config_;

  // Factory used in the creation of new proxies.
  PeerProxyFactory* proxy_factory_;

  // Election request to send to voters.
  const VoteRequestPB request_;

  // Object to count the votes.
  const std::unique_ptr<VoteCounter> vote_counter_;

  // Timeout for sending RPCs.
  const MonoDelta timeout_;

  // Callback invoked to notify the caller of an election decision.
  const ElectionDecisionCallback decision_callback_;

  // Map of UUID -> VoterState.
  VoterStateMap voter_state_;

  // The highest term seen from a voter so far (or 0 if no votes).
  int64_t highest_voter_term_;

  // The time when the election started
  MonoTime start_time_;

  std::shared_ptr<VoteLoggerInterface> vote_logger_;

  // Flag indicating that the election is held in joint-consensus phase.
  const bool is_joint_consensus_election_;
};

} // namespace kudu::consensus

#endif /* KUDU_CONSENSUS_LEADER_ELECTION_H */
