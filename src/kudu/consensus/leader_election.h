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

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

// The vote a peer has given.
enum ElectionVote {
  VOTE_DENIED = 0,
  VOTE_GRANTED = 1,
};

// Details of the vote received from a peer.
struct VoteInfo {
  ElectionVote vote;

  // Previous voting history of this voter.
  std::vector<PreviousVotePB> previous_vote_history;
};

// Internal structure to denote the optimizer's computation of the
// next potential leader.
// TODO(ritwikyadav) : Add a link to the post describing the algorithm.
struct PotentialNextLeaderResponse {
  enum Status {
    POTENTIAL_NEXT_LEADER_DETECTED = 0,
    WAITING_FOR_MORE_VOTES = 1,
    ALL_INTERMEDIATE_TERMS_DEFUNCT = 2
  };
  PotentialNextLeaderResponse(Status);
  PotentialNextLeaderResponse(Status, const std::string&, const int64_t);
  Status status;
  std::string potential_leader_uuid;
  int64_t potential_leader_term;
};

// Simple class to count votes (in-memory, not persisted to disk).
// This class is not thread safe and requires external synchronization.
class VoteCounter {
 public:
  // Create new VoteCounter with the given majority size.
  VoteCounter(int num_voters, int majority_size);
  virtual ~VoteCounter() {}

  // Register a peer's vote.
  //
  // If the voter already has a vote recorded, but it has a different value than
  // the vote specified, returns Status::IllegalArgument.
  //
  // If the same vote is duplicated, 'is_duplicate' is set to true.
  // Otherwise, it is set to false.
  // If an OK status is not returned, the value in 'is_duplicate' is undefined.
  virtual Status RegisterVote(
      const std::string& voter_uuid, const VoteInfo& vote_info,
      bool* is_duplicate);

  // Return whether the vote is decided yet.
  virtual bool IsDecided() const;

  // Return decision iff IsDecided() returns true.
  // If vote is not yet decided, returns Status::IllegalState().
  virtual Status GetDecision(ElectionVote* decision) const;

  // Return the total of "Yes" and "No" votes.
  int GetTotalVotesCounted() const;

  // Return total number of expected votes.
  int GetTotalExpectedVotes() const { return num_voters_; }

  // Return true iff GetTotalVotesCounted() == num_voters_;
  bool AreAllVotesIn() const;

 protected:
  int num_voters_;

  typedef std::map<std::string, VoteInfo> VoteMap;
  VoteMap votes_; // Voting record.

 private:
  friend class VoteCounterTest;

  const int majority_size_;
  int yes_votes_; // Accumulated yes votes, for quick counting.
  int no_votes_;  // Accumulated no votes.

  DISALLOW_COPY_AND_ASSIGN(VoteCounter);
};

class FlexibleVoteCounter : public VoteCounter {
 public:
  FlexibleVoteCounter(
      int64_t election_term, const LastKnownLeader& last_known_leader,
      RaftConfigPB config);

  // Synchronization is done by the LeaderElection class. Therefore, VoteCounter
  // class doesn't need to take care of thread safety of its book-keeping
  // variables.
  Status RegisterVote(
      const std::string& voter_uuid, const VoteInfo& vote,
      bool* is_duplicate) override;
  bool IsDecided() const override;
  Status GetDecision(ElectionVote* decision) const override;
 private:
  // Mapping from UUID to an iterator in its voter history.
  typedef std::map<std::string, std::vector<PreviousVotePB>::const_iterator>
  VoteHistoryIteratorMap;

  // Mapping from region to set of voter UUIDs that have responded in that
  // region.
  typedef std::map<std::string, std::set<std::string> > RegionToVoterSet;

  // UUID and term pair for which a vote was given.
  typedef std::pair<std::string, int64_t> UUIDTermPair;

  // A mapping from UUID term pair to maps from region to voter sets in those
  // regions that have voted for the UUID term pair.
  typedef std::map<UUIDTermPair, RegionToVoterSet> VoteHistoryCollation;

  // Fetches the number of votes that still haven't arrived in this election
  // cycle from the given `region`. Will throw, if the region is not part of
  // the configuration.
  int FetchVotesRemainingInRegion(const std::string& region) const;

  // Given a leader region, returns the resolved quorum.
  // Only to be called when the quorum is a function of the leader region,
  // otherwise it throws an exception.
  std::map<std::string, int> ResolveQuorum(
      const std::string& leader_region) const;

  // Given a quorum (mapping from region to number of votes required in the
  // region), returns a pair of booleans representing:
  // 1. if the quorum is satisfied in the current state
  // 2. if the quorum can still be satisfied in the current state
  std::pair<bool, bool> IsQuorumSatisfied(
      const std::map<std::string, int>& resolved_quorum) const;

  // Figure out if `region_to_voter_set` satisfies the quorum requirement.
  // `region_to_voter_set` is constructed from the voting history.
  // Please note the underlying assumption that the configuration for
  // leader election quorum remains immutable.
  // Returns a pair of booleans representing:
  // 1. if the quorum is satisfied in the current state
  // 2. if the quorum can still be satisfied in the current state
  std::pair<bool, bool> FetchQuorumSatisfactionInfoFromVoteHistory(
      const std::string& leader_region,
      const RegionToVoterSet& region_to_voter_set) const;

  // Iterates over all the votes that have come in so far and collates them
  // corresponding to each UUID term pair for the purpose of determining if
  // one of those UUIDs could have won an election after the `term` provided
  // when the last known leader's region was `leader_region`. It also computes
  // the min_term greater than `term` in which one of the UUIDs from the
  // `leader_region` had voted. This function only considers votes that are
  // relevant as per the leader election quorum requirement.
  void ConstructRegionWiseVoteCollation(
      const int64_t term,
      const std::string& leader_region,
      VoteHistoryCollation* vote_collation,
      VoteHistoryIteratorMap* it_map,
      int64_t* min_term) const;

  // Updates the region-wise vote collation represented by `vote_collation`
  // to consider votes only after `term` provided. It also advances the
  // iterators in `it_map` such that they represent a point in voting history
  // after `term`. It also computes the min_term greater than `term` in which
  // one of the UUIDs from the `leader_region` had voted.
  void UpdateRegionWiseVoteCollation(
      const int64_t term,
      const std::string& leader_region,
      VoteHistoryCollation* vote_collation,
      VoteHistoryIteratorMap* it_map,
      int64_t* min_term) const;

  // Optimizer function which tries to recursively figure out the next leader
  // region since the last known leader. It accepts two parameters, term and
  // region of the last known leader. It returns the UUID and term of a node
  // that could potentially be the leader in a subsequent term that the
  // candidate might not be aware of. It is possible that the next possible
  // leader cannot be determined in which case, the situation is reflected in
  // the status.
  PotentialNextLeaderResponse GetPotentialNextLeader(
      const int64_t term, const std::string& leader_region) const;

  // Returns a couple of booleans - the first denotes if the election quorum
  // has been satisfied and the second denotes if it can still be satisfied.
  std::pair<bool, bool> GetQuorumState() const;

  // Fetches topology information required by the flexible vote counter.
  void FetchTopologyInfo();

  // Computes a pessimistic leader election quorum.
  void ComputePessimisticLeaderElectionQuorum();

  // Generic log prefix.
  std::string LogPrefix() const;

  // Term of this election.
  const int64_t election_term_;

  // Mapping from each region to number of active voters.
  std::map<std::string, int> voter_distribution_;

  // Mapping from each region to number of yes votes required.
  std::map<std::string, int> le_quorum_requirement_;

  // Worst case quorum requirement for leader election.
  std::map<std::string, int> pessimistic_le_quorum_;

  // Vote count per region.
  std::map<std::string, int> yes_vote_count_, no_vote_count_;

  // Last known leader properties.
  bool depends_on_last_leader_;
  const LastKnownLeader last_known_leader_;
  std::string last_known_leader_region_;

  // Config at the beginning of the leader election.
  const RaftConfigPB config_;

  // UUID to region map derived from RaftConfigPB
  std::map<std::string, std::string> uuid_to_region_;

  DISALLOW_COPY_AND_ASSIGN(FlexibleVoteCounter);
};

// The result of a leader election.
struct ElectionResult {
 public:
  ElectionResult(VoteRequestPB vote_request, ElectionVote decision,
                 ConsensusTerm highest_term, const std::string& message);

  // The vote request that was sent to the voters for this election.
  const VoteRequestPB vote_request;

  // The overall election GRANTED/DENIED decision of the configuration.
  const ElectionVote decision;

  // The highest term seen from any voter.
  const ConsensusTerm highest_voter_term;

  // Human-readable explanation of the vote result, if any.
  const std::string message;
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
class LeaderElection : public RefCountedThreadSafe<LeaderElection> {
 public:
  typedef std::function<void(const ElectionResult&)> ElectionDecisionCallback;

  // Set up a new leader election driver.
  //
  // 'proxy_factory' must not go out of scope while LeaderElection is alive.
  //
  // The 'vote_counter' must be initialized with the candidate's own yes vote.
  LeaderElection(RaftConfigPB config,
                 PeerProxyFactory* proxy_factory,
                 VoteRequestPB request,
                 gscoped_ptr<VoteCounter> vote_counter,
                 MonoDelta timeout,
                 ElectionDecisionCallback decision_callback);

  // Run the election: send the vote request to followers.
  void Run();

 private:
  friend class RefCountedThreadSafe<LeaderElection>;

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

  typedef std::unordered_map<std::string, VoterState*> VoterStateMap;
  typedef simple_spinlock Lock;

  // This class is refcounted.
  ~LeaderElection();

  // Check to see if a decision has been made. If so, invoke decision callback.
  // Calls the callback outside of holding a lock.
  void CheckForDecision();

  // Callback called when the RPC responds.
  void VoteResponseRpcCallback(const std::string& voter_uuid);

  // Record vote from specified peer.
  void RecordVoteUnlocked(
    const VoterState& state, ElectionVote vote);

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
  ConsensusTerm election_term() const { return request_.candidate_term(); }

  // All non-const fields are protected by 'lock_'.
  Lock lock_;

  // The result returned by the ElectionDecisionCallback.
  // NULL if not yet known.
  gscoped_ptr<ElectionResult> result_;

  // Whether we have responded via the callback yet.
  bool has_responded_;

  // Active Raft configuration at election start time.
  const RaftConfigPB config_;

  // Factory used in the creation of new proxies.
  PeerProxyFactory* proxy_factory_;

  // Election request to send to voters.
  const VoteRequestPB request_;

  // Object to count the votes.
  const gscoped_ptr<VoteCounter> vote_counter_;

  // Timeout for sending RPCs.
  const MonoDelta timeout_;

  // Callback invoked to notify the caller of an election decision.
  const ElectionDecisionCallback decision_callback_;

  // Map of UUID -> VoterState.
  VoterStateMap voter_state_;

  // The highest term seen from a voter so far (or 0 if no votes).
  int64_t highest_voter_term_;
};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LEADER_ELECTION_H */
