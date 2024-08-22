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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/consensus/leader_election.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
// #include "kudu/tserver/tserver.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DEFINE_bool(
    voter_history_consider_candidate_quorum,
    false,
    "Whether to consider candidate quorums when looking for potential leaders in "
    "voter history logic. include_candidate_region must be on for this to have "
    "an effect");
DEFINE_bool(
    srd_strict_leader_election_quorum,
    false,
    "Use majority of majorities for leader election quorum (LEQ) "
    "in SINGLE_REGION_DYNAMIC (SRD) mode.");
DEFINE_bool(
    include_candidate_region,
    true,
    "In flexiraft for availability, always wait for majority "
    "in candidate region");
DEFINE_bool(
    trust_last_leader_entries,
    true,
    "In flexiraft assume that a voter will always send its last known leader"
    " if it has sent votes and had ever received AppendEntries from that leader"
    " So if a CANDIDATE has heard from all voters, it can make decisions on the last known"
    " leader from the ring");
DEFINE_int32(
    wait_for_pessimistic_quorum_secs,
    10,
    "Secs to wait for pessimistic quorum to be satisfied before "
    "trying the voter history method");

DEFINE_bool(
    use_voting_history_as_last_resort,
    true,
    "Whether to fallback on using Voting History mechanism to find potential leader regions");

namespace kudu::consensus {

using std::vector;
using strings::Substitute;

namespace {

// Comparator for PreviousVotePB
bool compare_PreviousVotePB(const PreviousVotePB& a, const PreviousVotePB& b) {
  return a.election_term() < b.election_term();
}

// Comparator for binary search in a sorted list of PreviousVotePB
bool compareTerm_PreviousVotePB(int64_t a, const PreviousVotePB& b) {
  return a < b.election_term();
}

std::string uuid2hostport(const std::string& uuid, const RaftConfigPB& config) {
  for (const RaftPeerPB& peer : config.peers()) {
    if (peer.has_last_known_addr()) {
      if (uuid == peer.permanent_uuid()) {
        return Substitute(
            "$0:$1($2)", peer.hostname(), peer.last_known_addr().port(), uuid);
      }
    }
  }
  return "";
}

auto electionDecisionMethodToString(ElectionDecisionMethod method) {
  switch (method) {
    case ElectionDecisionMethod::SIMPLE_MAJORITY:
      return "SIMPLE_MAJORITY";
    case ElectionDecisionMethod::STATIC_QUORUM:
      return "STATIC_QUORUM";
    case ElectionDecisionMethod::CONTINUOUS_LKL_QUORUM:
      return "CONTINUOUS_LKL_QUORUM";
    case ElectionDecisionMethod::PESSIMISTIC_QUORUM:
      return "PESSIMISTIC_QUORUM";
    case ElectionDecisionMethod::VOTER_HISTORY:
      return "VOTER_HISTORY";
    case ElectionDecisionMethod::INVALIDATED_BY_HIGHER_TERM:
      return "INVALIDATED_BY_HIGHER_TERM";
      // Do not add default case, compiler will complain if new enum added
  }
}
} // namespace

///////////////////////////////////////////////////
// VoteCounter & FlexibleVoteCounter
///////////////////////////////////////////////////

VoteCounter::VoteCounter(int num_voters, int majority_size)
    : num_voters_(num_voters),
      is_candidate_removed_(false),
      majority_size_(majority_size),
      yes_votes_(0),
      no_votes_(0) {
  CHECK_LE(majority_size, num_voters);
  CHECK_GT(num_voters_, 0);
  CHECK_GT(majority_size_, 0);
}

Status VoteCounter::RegisterVote(
    const std::string& voter_uuid,
    const VoteInfo& vote_info,
    bool* is_duplicate) {
  // Handle repeated votes.
  if (PREDICT_FALSE(ContainsKey(votes_, voter_uuid))) {
    // Detect changed votes.
    const VoteInfo& prior_vote_info = votes_.at(voter_uuid);
    if (PREDICT_FALSE(prior_vote_info.vote != vote_info.vote)) {
      std::string msg = Substitute(
          "Peer $0 voted a different way twice in the same election. "
          "First vote: $1, second vote: $2.",
          voter_uuid,
          prior_vote_info.vote,
          vote_info.vote);
      return Status::InvalidArgument(msg);
    }

    // This was just a duplicate. Allow the caller to log it but don't change
    // the voting record.
    *is_duplicate = true;
    return Status::OK();
  }

  // Sanity check to ensure we did not exceed the allowed number of voters.
  if (PREDICT_FALSE(yes_votes_ + no_votes_ == num_voters_)) {
    // More unique voters than allowed!
    return Status::InvalidArgument(Substitute(
        "Vote from peer $0 would cause the number of votes to exceed the expected number of "
        "voters, which is $1. Votes already received from the following peers: {$2}",
        voter_uuid,
        num_voters_,
        JoinKeysIterator(votes_.begin(), votes_.end(), ", ")));
  }

  // This is a valid vote, so store it.
  InsertOrDie(&votes_, voter_uuid, vote_info);
  switch (vote_info.vote) {
    case VOTE_GRANTED:
      ++yes_votes_;
      break;
    case VOTE_DENIED:
      is_candidate_removed_ =
          is_candidate_removed_ || vote_info.is_candidate_removed;
      ++no_votes_;
      break;
  }
  *is_duplicate = false;
  return Status::OK();
}

bool VoteCounter::IsDecided() const {
  return yes_votes_ >= majority_size_ ||
      no_votes_ > num_voters_ - majority_size_;
}

Status VoteCounter::GetDecision(
    ElectionVote* decision,
    ElectionDecisionMethod* decision_method) const {
  if (yes_votes_ >= majority_size_) {
    *decision = VOTE_GRANTED;
    *decision_method = ElectionDecisionMethod::SIMPLE_MAJORITY;
    return Status::OK();
  }
  if (no_votes_ > num_voters_ - majority_size_) {
    *decision = VOTE_DENIED;
    *decision_method = ElectionDecisionMethod::SIMPLE_MAJORITY;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

bool VoteCounter::IsCandidateRemoved() const {
  return is_candidate_removed_;
}

int VoteCounter::GetTotalVotesCounted() const {
  return yes_votes_ + no_votes_;
}

bool VoteCounter::AreAllVotesIn() const {
  return GetTotalVotesCounted() == num_voters_;
}

PotentialNextLeadersResponse::PotentialNextLeadersResponse(
    PotentialNextLeadersResponse::Status s) {
  status = s;
  next_term = -1;
}

PotentialNextLeadersResponse::PotentialNextLeadersResponse(
    PotentialNextLeadersResponse::Status s,
    const std::set<std::string>& leader_regions,
    int64_t term) {
  status = s;
  potential_leader_regions.insert(leader_regions.begin(), leader_regions.end());
  next_term = term;
}

PotentialNextLeadersResponse::PotentialNextLeadersResponse(
    PotentialNextLeadersResponse::Status s,
    const std::set<std::string>& leader_regions,
    int64_t term,
    bool unreceived_votes) {
  status = s;
  potential_leader_regions.insert(leader_regions.begin(), leader_regions.end());
  next_term = term;
  used_unreceived_votes = unreceived_votes;
}

void FlexibleVoteCounter::FetchTopologyInfo() {
  CHECK(config_.has_commit_rule());

  // Step 1: Populate number of voters in each region.
  GetVoterDistributionForQuorumId(config_, &voter_distribution_);

  // Step 2: Populate mapping from UUID to quorum_id.
  bool use_quorum_id = IsUseQuorumId(config_.commit_rule());
  for (const RaftPeerPB& peer : config_.peers()) {
    if (peer.member_type() == RaftPeerPB::VOTER) {
      uuid_to_quorum_id_.emplace(
          peer.permanent_uuid(), GetQuorumId(peer, use_quorum_id));
    }
  }

  // adjust_voter_distribution_ is set to false on in cases where we want to
  // perform an election forcefully i.e. unsafe config change
  if (PREDICT_TRUE(adjust_voter_distribution_)) {
    // Step 1: Compute number of voters in each region in the active config.
    // As voter distribution provided in topology config can lag,
    // we need to take into account the active voters as well due to
    // membership changes.
    AdjustVoterDistributionWithCurrentVoters(config_, &voter_distribution_);

    // We assume that there are no voters in config_.peers() who
    // are in present in regions not covered by voter_distribution_
    // That is enforced via bootstrap and add-member
    // The reverse is not true and has been handled in
    // AdjustVoterDistributionWithCurrentVoters
  }
}

FlexibleVoteCounter::FlexibleVoteCounter(
    std::string candidate_uuid,
    int64_t election_term,
    LastKnownLeaderPB last_known_leader,
    RaftConfigPB config,
    bool adjust_voter_distribution)
    : VoteCounter(1, 1),
      candidate_uuid_(std::move(candidate_uuid)),
      election_term_(election_term),
      adjust_voter_distribution_(adjust_voter_distribution),
      last_known_leader_(std::move(last_known_leader)),
      config_(std::move(config)),
      creation_time_(std::chrono::system_clock::now()) {
  num_voters_ = 0;

  // Computes voter distribution and uuid to region map.
  FetchTopologyInfo();

  for (const std::pair<const std::string, int>& regional_voter_count :
       voter_distribution_) {
    // When instances are being removed from ring, the voter distribution
    // can have extra regions, but we have taken them out in
    // FetchTopology. So this should never happen
    if (adjust_voter_distribution_ && regional_voter_count.second <= 0) {
      continue;
    }
    // num_voters_ += regional_voter_count.second;
    yes_vote_count_.emplace(regional_voter_count.first, 0);
    no_vote_count_.emplace(regional_voter_count.first, 0);
  }

  // Its critical that we count num_voters_ based on current voter list
  // as voter_distribution_ can be greater or less than current voter list
  num_voters_ = uuid_to_quorum_id_.size();
  for (const auto& [uuid, quorum_id] : uuid_to_quorum_id_) {
    auto quorum_id_itr = num_voters_per_quorum_id_.find(quorum_id);
    if (quorum_id_itr == num_voters_per_quorum_id_.end()) {
      num_voters_per_quorum_id_.emplace(quorum_id, 1);
    } else {
      quorum_id_itr->second++;
    }
  }

  CHECK_GT(num_voters_, 0);
}

Status FlexibleVoteCounter::RegisterVote(
    const std::string& voter_uuid,
    const VoteInfo& vote_info,
    bool* is_duplicate) {
  // The base function returns error if a voter has changed his
  // mind. We return error in that case and return early
  // in case this vote is a duplicate
  Status s = VoteCounter::RegisterVote(voter_uuid, vote_info, is_duplicate);
  RETURN_NOT_OK(s);

  // No book-keeping required for duplicate votes.
  if (*is_duplicate) {
    return s;
  }

  // In Flexi-Raft all voters are expected to have region tag
  if (!ContainsKey(uuid_to_quorum_id_, voter_uuid)) {
    // This is never expected to happen
    return Status::InvalidArgument(
        Substitute("UUID {$0} not present in config.", voter_uuid));
  }

  // In Flexi-Raft we never allow voters without voter distribution
  // to be in Ring. Hence yes_vote_count_ and no_vote_count_ will
  // have the same number of voting regions as voter_distribution_
  const std::string& quorum_id = uuid_to_quorum_id_.at(voter_uuid);
  switch (vote_info.vote) {
    case VOTE_GRANTED:
      InsertIfNotPresent(&yes_vote_count_, quorum_id, 0);
      yes_vote_count_[quorum_id]++;
      break;
    case VOTE_DENIED:
      InsertIfNotPresent(&no_vote_count_, quorum_id, 0);
      no_vote_count_[quorum_id]++;
      break;
  }

  // TODO - explain this more
  InsertOrUpdate(
      &uuid_to_last_term_pruned_, voter_uuid, vote_info.last_pruned_term);
  return s;
}

int FlexibleVoteCounter::FetchVotesRemainingInRegion(
    const std::string& region,
    bool use_vd) const {
  // All the following must at least be initialized to zero in the
  // constructor.
  int regional_yes_count = FindOrDie(yes_vote_count_, region);
  int regional_no_count = FindOrDie(no_vote_count_, region);
  int total_region_count = use_vd
      ? FindOrDie(voter_distribution_, region)
      : FindOrDie(num_voters_per_quorum_id_, region);
  return std::max(
      0, total_region_count - regional_yes_count - regional_no_count);
}

void FlexibleVoteCounter::FetchRegionalPrunedCounts(
    int64_t term,
    std::map<std::string, int32_t>* region_pruned_counts) const {
  CHECK(region_pruned_counts);
  region_pruned_counts->clear();
  for (const std::pair<const std::string, int64_t>& uuid_pruned_term_pair :
       uuid_to_last_term_pruned_) {
    const std::string& uuid = uuid_pruned_term_pair.first;
    int64_t lpt = uuid_pruned_term_pair.second;
    if (lpt > term) {
      const std::string& region = uuid_to_quorum_id_.at(uuid);
      int32_t& region_count = LookupOrInsert(region_pruned_counts, region, 0);
      region_count++;
    }
  }
}

void FlexibleVoteCounter::FetchRegionalUnprunedCounts(
    int64_t term,
    std::map<std::string, int32_t>* region_unpruned_counts) const {
  CHECK(region_unpruned_counts);
  region_unpruned_counts->clear();
  for (const std::pair<const std::string, int64_t>& uuid_pruned_term_pair :
       uuid_to_last_term_pruned_) {
    const std::string& uuid = uuid_pruned_term_pair.first;
    int64_t lpt = uuid_pruned_term_pair.second;
    if (lpt <= term) {
      const std::string& region = uuid_to_quorum_id_.at(uuid);
      int32_t& region_count = LookupOrInsert(region_unpruned_counts, region, 0);
      region_count++;
    }
  }
}

std::string FlexibleVoteCounter::DetermineQuorumIdForUUID(
    const std::string& uuid) const {
  std::map<std::string, std::string>::const_iterator reg_it =
      uuid_to_quorum_id_.find(uuid);
  if (reg_it == uuid_to_quorum_id_.end()) {
    return "";
  } else {
    return reg_it->second;
  }
}

std::vector<std::pair<bool, bool>>
FlexibleVoteCounter::IsMajoritySatisfiedInRegions(
    const std::vector<std::string>& regions) const {
  CHECK(!regions.empty());

  VLOG_WITH_PREFIX(1) << "Number of regions: " << regions.size();

  std::vector<std::pair<bool, bool>> results;

  for (const std::string& region : regions) {
    if (region.empty()) {
      results.emplace_back(false, false);
      continue;
    }

    bool quorum_satisfied = true;
    bool quorum_satisfaction_possible = true;

    // All the following must at least be initialized to zero in the
    // constructor.
    int regional_yes_count = FindOrDie(yes_vote_count_, region);
    int regional_no_count = FindOrDie(no_vote_count_, region);
    int regional_quorum_count = FindOrDie(voter_distribution_, region);
    size_t regional_total_count = FindOrDie(num_voters_per_quorum_id_, region);

    VLOG_WITH_PREFIX(3) << "Region: " << region
                        << " Total voters: " << regional_quorum_count
                        << " Votes granted count: " << regional_yes_count
                        << " Votes denied count: " << regional_no_count;

    const int region_majority_size = MajoritySize(regional_quorum_count);

    if (regional_yes_count < region_majority_size) {
      VLOG_WITH_PREFIX(2) << "Yes votes in region: " << region
                          << " are: " << regional_yes_count
                          << " but majority requirement is: "
                          << region_majority_size;
      quorum_satisfied = false;
    }
    if ((regional_yes_count + regional_no_count) >= regional_total_count) {
      DCHECK_EQ(regional_yes_count + regional_no_count, regional_total_count);
      VLOG_WITH_PREFIX(2) << "All votes are in. Quorum "
                          << (quorum_satisfied ? "" : "not")
                          << " statisfied in region " << region
                          << ". Yes votes: " << regional_yes_count
                          << ", no votes: " << regional_no_count
                          << ", total members: " << regional_total_count
                          << ", majority requirement: " << region_majority_size;
      quorum_satisfaction_possible = quorum_satisfied;
    } else if (
        !quorum_satisfied &&
        regional_no_count + region_majority_size > regional_quorum_count) {
      VLOG_WITH_PREFIX(2) << "Quorum satisfaction not possible in region: "
                          << region << " because of excessive no votes: "
                          << regional_no_count
                          << " Majority requirement: " << region_majority_size;
      quorum_satisfaction_possible = false;
    }
    results.emplace_back(quorum_satisfied, quorum_satisfaction_possible);
  }
  return results;
}

std::pair<bool, bool> FlexibleVoteCounter::IsMajoritySatisfiedInRegion(
    const std::string& region) const {
  // We piggyback on the general implementation that takes a vector of
  // regions and then provides quorum satisfaction information corresponding
  // to each region. Each pair of booleans represent if the quorum is already
  // satisfied and if it can be specified in a given region.
  const std::vector<std::pair<bool, bool>>& results =
      IsMajoritySatisfiedInRegions({region});
  CHECK_EQ(1, results.size());
  return results.at(0);
}

FlexibleVoteCounter::QuorumState FlexibleVoteCounter::IsStaticQuorumSatisfied()
    const {
  CHECK(
      config_.commit_rule().mode() == QuorumMode::STATIC_DISJUNCTION ||
      config_.commit_rule().mode() == QuorumMode::STATIC_CONJUNCTION);
  CHECK(config_.commit_rule().rule_predicates_size() > 0);
  const auto& rule_predicates = config_.commit_rule().rule_predicates();
  bool quorum_satisfied = true;
  bool quorum_satisfaction_possible = true;

  VLOG_WITH_PREFIX(2)
      << "Checking leader election quorum satisfaction in static mode. "
      << "Number of predicates: "
      << config_.commit_rule().rule_predicates_size();

  for (const CommitRulePredicatePB& rule_predicate : rule_predicates) {
    int regions_subset_size = rule_predicate.regions_size() + 1 -
        rule_predicate.regions_subset_size();

    VLOG_WITH_PREFIX(2)
        << "Checking satisfaction of leader election quorum predicate with "
        << rule_predicate.regions_size() << " regions."
        << " Number of majorities required: " << regions_subset_size;

    int num_regions_satisfied = 0;
    int num_regions_impossible_to_satisfy = 0;
    for (const std::string& region : rule_predicate.regions()) {
      std::pair<bool, bool> result = IsMajoritySatisfiedInRegion(region);
      if (result.first) {
        VLOG_WITH_PREFIX(3) << "Majority satisfied in region: " << region;
        num_regions_satisfied++;
      }
      if (!result.second) {
        VLOG_WITH_PREFIX(3)
            << "Majority cannot be satisfied in region: " << region;
        num_regions_impossible_to_satisfy++;
      }
    }
    if (num_regions_satisfied < regions_subset_size) {
      VLOG_WITH_PREFIX(3) << "Quorum not satisfied. Regions with majorities: "
                          << num_regions_satisfied
                          << ". Number of majorities needed: "
                          << regions_subset_size;
      quorum_satisfied = false;
    }
    if (rule_predicate.regions_size() - num_regions_impossible_to_satisfy <
        regions_subset_size) {
      VLOG_WITH_PREFIX(3)
          << "Quorum cannot be satisfied. "
          << "Number of regions where majority can't be achieved: "
          << num_regions_satisfied
          << ". Number of majorities needed: " << regions_subset_size;
      quorum_satisfaction_possible = false;
    }
  }
  return {
      quorum_satisfied,
      quorum_satisfaction_possible,
      ElectionDecisionMethod::STATIC_QUORUM};
}

// Flexible Paxos/Raft says that for a quorum of N if Datapath quorum is
// M then leader election quorum is (N+1) - M
// In the case of region based quorums, if M = 1 (single region dynamic)
// The leader election quorum should include all regions.
// In the case that all regions are healthy, this pessimistic quorum would
// suffice. We use this first because this is the most comprehensive check,
// however in case regions are down, this would not work. In those cases
// we use other heuristics like intersecting with last leader region or
// a majority of regions + last leader region.
FlexibleVoteCounter::QuorumState
FlexibleVoteCounter::IsPessimisticQuorumSatisfied() const {
  VLOG_WITH_PREFIX(3) << "Checking if pessimistic quorum is satisfied.";

  // Fetching all regions.
  std::set<std::string> regions;
  for (const std::pair<const std::string, int>& region_count_pair :
       voter_distribution_) {
    regions.insert(region_count_pair.first);
  }
  auto [achievedMajority, canAchieveMajority] =
      IsMajoritySatisfiedInAllRegions(regions);
  return {
      achievedMajority,
      canAchieveMajority,
      ElectionDecisionMethod::PESSIMISTIC_QUORUM};
}

std::pair<bool, bool>
FlexibleVoteCounter::IsMajoritySatisfiedInMajorityOfRegions() const {
  std::vector<std::string> regions_vector;
  for (const std::pair<const std::string, int32_t>& regional_count :
       voter_distribution_) {
    regions_vector.push_back(regional_count.first);
  }
  int32_t num_regions = regions_vector.size();

  const std::vector<std::pair<bool, bool>>& results =
      IsMajoritySatisfiedInRegions(regions_vector);
  CHECK_EQ(results.size(), num_regions);

  int32_t num_majority_regions = MajoritySize(num_regions);

  int32_t satisfied_count = 0;
  int32_t satisfaction_possible_count = 0;
  for (const std::pair<bool, bool>& result : results) {
    if (result.first) {
      satisfied_count++;
    }
    if (result.second) {
      satisfaction_possible_count++;
    }
  }
  VLOG_WITH_PREFIX(2) << "Number of regions: " << num_regions
                      << " Satisfied count: " << satisfied_count
                      << " Satisfaction possible count: "
                      << satisfaction_possible_count;
  return std::make_pair(
      satisfied_count >= num_majority_regions,
      satisfaction_possible_count >= num_majority_regions);
}

std::pair<bool, bool> FlexibleVoteCounter::IsMajoritySatisfiedInAllRegions(
    const std::set<std::string>& regions) const {
  CHECK(!regions.empty());
  std::vector<std::string> region_vector(regions.begin(), regions.end());
  const std::vector<std::pair<bool, bool>>& results =
      IsMajoritySatisfiedInRegions(region_vector);
  CHECK_EQ(results.size(), regions.size());

  bool quorum_satisfied = true;
  bool quorum_satisfaction_possible = true;

  for (const std::pair<bool, bool>& result : results) {
    quorum_satisfied = quorum_satisfied && result.first;
    quorum_satisfaction_possible =
        quorum_satisfaction_possible && result.second;
  }
  return std::make_pair(quorum_satisfied, quorum_satisfaction_possible);
}

std::tuple<bool, bool, bool>
FlexibleVoteCounter::DoHistoricalVotesSatisfyMajorityInRegion(
    const std::string& region,
    const RegionToVoterSet& region_to_voter_set,
    const std::map<std::string, int32_t>& region_pruned_counts) const {
  VLOG_WITH_PREFIX(1) << "Fetching quorum satisfaction info from "
                      << "vote history. Region: " << region;
  int32_t pruned_count = FindWithDefault(region_pruned_counts, region, 0);
  int32_t votes_received =
      FindWithDefault(region_to_voter_set, region, std::set<std::string>())
          .size();

  bool quorum_satisfied = false;
  bool quorum_satisfaction_possible = false;
  bool used_unreceived_votes = false;

  int total_voters = FindOrDie(voter_distribution_, region);
  DCHECK(total_voters >= 1 || !adjust_voter_distribution_);
  int commit_requirement = MajoritySize(total_voters);
  int votes_remaining = FetchVotesRemainingInRegion(region, false);
  VLOG_WITH_PREFIX(3) << "Region: " << region
                      << " , Votes granted: " << votes_received
                      << " , Votes remaining: " << votes_remaining
                      << " , Voters with pruned history: " << pruned_count
                      << " , Commit Requirement: " << commit_requirement;
  if (votes_received >= commit_requirement) {
    quorum_satisfied = true;
  }
  if (votes_received + pruned_count >= commit_requirement) {
    quorum_satisfaction_possible = true;
  } else if (
      votes_received + votes_remaining + pruned_count >= commit_requirement) {
    quorum_satisfaction_possible = true;
    used_unreceived_votes = true;
  }

  return std::make_tuple(
      quorum_satisfied, quorum_satisfaction_possible, used_unreceived_votes);
}

Status FlexibleVoteCounter::ExtendNextLeaderRegions(
    const std::set<std::string>& next_leader_uuids,
    std::set<std::string>* next_leader_quorum_ids) const {
  CHECK(next_leader_quorum_ids);
  for (const std::string& leader_uuid : next_leader_uuids) {
    // Check next leader quorum to explore is within the list of quorums
    // voters are present in for this replicaset.
    std::string leader_quorum_id = DetermineQuorumIdForUUID(leader_uuid);
    if (leader_quorum_id.empty()) {
      // This should never happen, i.e. we are exploring a region which
      // is not in our configuration. In such a case, we return loss of
      // election.
      VLOG_WITH_PREFIX(1) << "Potential next leader: " << leader_uuid
                          << " is not a part " << "of the configuration.";
      return Status::IllegalState("Potential next leader not in configuration");
    }
    next_leader_quorum_ids->insert(leader_quorum_id);
    LOG_WITH_PREFIX(INFO) << "Potential next leader: " << leader_uuid
                          << " in quorum:  " << leader_quorum_id;
  }
  return Status::OK();
}

/**
 * Collating voter history into votes received for instance in term.
 * { (uuid, term) => { quorum => votes } }
 */
void FlexibleVoteCounter::ConstructRegionWiseVoteCollation(
    int64_t term,
    VoteHistoryCollation* vote_collation,
    int64_t* min_term) const {
  CHECK(vote_collation);
  CHECK(min_term);

  vote_collation->clear();
  *min_term = std::numeric_limits<int64_t>::max();

  for (const std::pair<const std::string, VoteInfo>& it : votes_) {
    const std::string& uuid = it.first;
    const VoteInfo& vote_info = it.second;
    const std::vector<PreviousVotePB>& pvh = vote_info.previous_vote_history;

    const std::string quorum_id = DetermineQuorumIdForUUID(uuid);
    if (quorum_id.empty()) {
      continue;
    }

    // Find the voting record immediately after the term of the last known
    // leader. Skip if there is no history beyond the last known leader.
    std::vector<PreviousVotePB>::const_iterator vhi = std::upper_bound(
        pvh.begin(), pvh.end(), term, compareTerm_PreviousVotePB);
    if (vhi == pvh.end()) {
      continue;
    }
    const UUIDTermPair utp =
        std::make_pair(vhi->candidate_uuid(), vhi->election_term());

    // Update minimum term seen so far.
    *min_term = std::min(*min_term, utp.second);

    // Insert the iterator into the map and update the collation.
    // The collation is a map from (UUID, term) -> [region -> set(UUID)].
    // For each key (UUID - term pair), it represents all servers
    // (corresponding UUIDs) which voted for the key.
    RegionToVoterSet& rtvs =
        LookupOrInsert(vote_collation, utp, RegionToVoterSet());
    std::set<std::string>& uuid_set =
        LookupOrInsert(&rtvs, quorum_id, std::set<std::string>());
    uuid_set.insert(uuid);
  }
}

bool FlexibleVoteCounter::EnoughVotesWithSufficientHistories(
    int64_t term,
    const std::set<std::string>& leader_regions) const {
  // Figure out the total number of voters and votes not received so far for
  // each region. Return early if majority vote in some region is not
  // registered.
  for (const std::string& leader_region : leader_regions) {
    int total_voters = FindOrDie(voter_distribution_, leader_region);
    int votes_not_received = FetchVotesRemainingInRegion(leader_region, true);

    // If we haven't received enough votes from one potential leader region,
    // there is no point proceeding. We need to wait for more votes.
    DCHECK(total_voters >= 1 || !adjust_voter_distribution_);
    if (votes_not_received >= MajoritySize(total_voters)) {
      LOG(INFO) << "Not enough votes have arrived in region: " << leader_region
                << ". Votes not received: " << votes_not_received
                << ". Total number of voters in the region: " << total_voters;
      return false;
    }
  }

  std::map<std::string, int32_t> region_unpruned_counts;
  FetchRegionalUnprunedCounts(term, &region_unpruned_counts);

  for (const std::string& leader_region : leader_regions) {
    int total_voters = FindOrDie(voter_distribution_, leader_region);
    int unpruned_count =
        FindWithDefault(region_unpruned_counts, leader_region, 0);

    // There is no point in proceeding if voting history is not available
    // on majority of the servers in one of the possible leader regions.
    DCHECK(total_voters >= 1 || !adjust_voter_distribution_);
    if (unpruned_count < MajoritySize(total_voters)) {
      LOG(INFO)
          << "Not enough voters have sufficient voting history in region: "
          << leader_region << ". Unpruned count: " << unpruned_count
          << ". Total number of voters in the region: " << total_voters;
      return false;
    }
  }
  return true;
}

void FlexibleVoteCounter::AppendPotentialLeaderUUID(
    const std::string& candidate_uuid,
    const std::set<std::string>& leader_regions,
    const RegionToVoterSet& region_to_voter_set,
    const std::map<std::string, int32_t>& region_pruned_counts,
    std::set<std::string>* potential_leader_uuids,
    bool* used_unreceived_votes) const {
  CHECK(potential_leader_uuids);

  if (FLAGS_include_candidate_region &&
      FLAGS_voter_history_consider_candidate_quorum) {
    const std::string candidate_region =
        DetermineQuorumIdForUUID(candidate_uuid);
    const std::tuple<bool, bool, bool> candidate_quorum_satisfaction_info =
        DoHistoricalVotesSatisfyMajorityInRegion(
            candidate_region, region_to_voter_set, region_pruned_counts);
    if (!std::get<0>(candidate_quorum_satisfaction_info) &&
        !std::get<1>(candidate_quorum_satisfaction_info)) {
      VLOG_WITH_PREFIX(3) << "Not adding candidate UUID: " << candidate_uuid
                          << " due to lack of candidate quorum";
      return;
    }
    *used_unreceived_votes = *used_unreceived_votes ||
        std::get<2>(candidate_quorum_satisfaction_info);
  }

  for (const std::string& leader_region : leader_regions) {
    std::tuple<bool, bool, bool> quorum_satisfaction_info =
        DoHistoricalVotesSatisfyMajorityInRegion(
            leader_region, region_to_voter_set, region_pruned_counts);
    if (std::get<0>(quorum_satisfaction_info) ||
        std::get<1>(quorum_satisfaction_info)) {
      potential_leader_uuids->insert(candidate_uuid);
      *used_unreceived_votes =
          *used_unreceived_votes || std::get<2>(quorum_satisfaction_info);

      VLOG_WITH_PREFIX(3) << "Added potential leader UUID: " << candidate_uuid;
      return;
    }
  }
}

PotentialNextLeadersResponse FlexibleVoteCounter::GetPotentialNextLeaders(
    int64_t term,
    const std::set<std::string>& leader_regions) const {
  // Return waiting for more votes if there aren't enough votes or if a
  // majority isn't available with sufficient voting histories.
  if (!EnoughVotesWithSufficientHistories(term, leader_regions)) {
    VLOG_WITH_PREFIX(1)
        << "Either not enough votes have arrived or a majority do not "
        << "have sufficient vote histories yet.";
    return PotentialNextLeadersResponse(
        PotentialNextLeadersResponse::WAITING_FOR_MORE_VOTES);
  }

  // We limit the number of iterations performed even though the algorithm
  // guarantees termination to prevent against any future bugs.
  int64_t iteration_count = 0;

  // Iterate over the voting histories of potential leader regions.
  int64_t min_term;

  // Mapping from UUID term pair to a set of UUIDs that voted for it
  // grouped by their region.
  VoteHistoryCollation vote_collation;
  ConstructRegionWiseVoteCollation(term, &vote_collation, &min_term);
  // Set of regions that could possibly serve as leaders in the subsequent
  // terms.
  std::set<std::string> next_leader_regions = leader_regions;

  // For each term greater than the term of the last known leader,
  // compute if some server could have won an election in that term. If not,
  // we consider the next available term from the voting histories and repeat
  // until all the history is exhausted.
  while (!vote_collation.empty() && min_term < election_term_ &&
         iteration_count++ < QUORUM_OPTIMIZATION_ITERATION_COUNT_MAX) {
    std::map<std::string, int32_t> region_pruned_counts;
    FetchRegionalPrunedCounts(min_term, &region_pruned_counts);

    std::set<std::string> potential_leader_uuids;
    bool used_unreceived_votes = false;
    for (const std::pair<const UUIDTermPair, RegionToVoterSet>&
             collation_entry : vote_collation) {
      const std::string& uuid = collation_entry.first.first;
      int64_t vc_term = collation_entry.first.second;
      const RegionToVoterSet& region_to_voter_set = collation_entry.second;

      // Skip if the next highest term that this server voted in is not the
      // min_term.
      if (vc_term != min_term) {
        continue;
      }

      AppendPotentialLeaderUUID(
          uuid,
          leader_regions,
          region_to_voter_set,
          region_pruned_counts,
          &potential_leader_uuids,
          &used_unreceived_votes);
    }

    if (!potential_leader_uuids.empty()) {
      Status s =
          ExtendNextLeaderRegions(potential_leader_uuids, &next_leader_regions);
      if (!s.ok()) {
        return PotentialNextLeadersResponse(
            PotentialNextLeadersResponse::ERROR);
      }
      return PotentialNextLeadersResponse(
          PotentialNextLeadersResponse::POTENTIAL_NEXT_LEADERS_DETECTED,
          next_leader_regions,
          min_term,
          used_unreceived_votes);
    }

    // No UUID could have won an election in min_term, recompute vote
    // collations. This function advances the min_term.
    int64_t old_min_term = min_term;
    ConstructRegionWiseVoteCollation(old_min_term, &vote_collation, &min_term);

    // The next iteration should always consider a higher term.
    DCHECK_GT(min_term, old_min_term);
  }

  // Voting history suggests all intervening terms between the last known
  // leader's term and the current election's terms are defunct.
  return PotentialNextLeadersResponse(
      PotentialNextLeadersResponse::ALL_INTERMEDIATE_TERMS_SCANNED,
      next_leader_regions,
      -1);
}

FlexibleVoteCounter::QuorumState
FlexibleVoteCounter::ComputeElectionResultFromVotingHistory(
    const LastKnownLeaderPB& last_known_leader,
    const std::string& last_known_leader_region,
    const std::string& candidate_region) const {
  VLOG_WITH_PREFIX(3)
      << "Attempting to compute election result from voting history.";
  int64_t term_it = last_known_leader.election_term();
  std::set<std::string> next_leader_regions{last_known_leader_region};

  // We limit the number of iterations performed even though the algorithm
  // guarantees termination to prevent against any future bugs.
  int64_t iteration_count = 0;
  bool used_unreceived_votes = false;

  while (next_leader_regions.size() < voter_distribution_.size() &&
         iteration_count++ < QUORUM_OPTIMIZATION_ITERATION_COUNT_MAX) {
    const PotentialNextLeadersResponse& r =
        GetPotentialNextLeaders(term_it, next_leader_regions);
    used_unreceived_votes = used_unreceived_votes || r.used_unreceived_votes;
    switch (r.status) {
      case PotentialNextLeadersResponse::POTENTIAL_NEXT_LEADERS_DETECTED: {
        // Next term to consider should always be higher.
        DCHECK_GT(r.next_term, term_it);
        term_it = r.next_term;
        next_leader_regions = std::move(r.potential_leader_regions);
        LOG_WITH_PREFIX(INFO)
            << "Computed new potential leaders in the next term: " << term_it
            << ". Current election term: " << election_term_
            << ". Potential leader regions: "
            << JoinStringsIterator(
                   next_leader_regions.begin(),
                   next_leader_regions.end(),
                   ", ");
        break;
      }
      case PotentialNextLeadersResponse::ALL_INTERMEDIATE_TERMS_SCANNED: {
        LOG_WITH_PREFIX(INFO)
            << "All intermediate terms since the last known leader: "
            << last_known_leader.uuid()
            << " in term: " << last_known_leader.election_term()
            << " were explored. " << "Current election term: " << election_term_
            << ". Potential leader regions: "
            << JoinStringsIterator(
                   r.potential_leader_regions.begin(),
                   r.potential_leader_regions.end(),
                   ", ")
            << ". Used unreceived votes to include regions: "
            << used_unreceived_votes;

        auto [achievedMajority, canAchieveMajority] = AreMajoritiesSatisfied(
            r.potential_leader_regions, candidate_region);
        return {
            achievedMajority,
            used_unreceived_votes || canAchieveMajority,
            ElectionDecisionMethod::VOTER_HISTORY};
      }
      case PotentialNextLeadersResponse::ERROR:
        // Declare undecided election in case of an error.
        LOG_WITH_PREFIX(INFO)
            << "Encountered an error during computing election result "
            << "from vote history. Falling back on pessimistic quorum. "
            << "Election term: " << election_term_;
        return {false, true, ElectionDecisionMethod::VOTER_HISTORY};
      case PotentialNextLeadersResponse::WAITING_FOR_MORE_VOTES:
      default:
        LOG_WITH_PREFIX(INFO)
            << "Waiting for more votes. Election result hasn't been "
            << "determined. Election term: " << election_term_;
        return {false, !AreAllVotesIn(), ElectionDecisionMethod::VOTER_HISTORY};
    }
  }

  // We have converged to the most pessimistic quorum which hasn't
  // been satisfied yet.
  VLOG_WITH_PREFIX(3)
      << "Converged to the most pessimistic quorum. Could not reach "
      << "a result using vote histories. Election term: " << election_term_;
  return {false, true, ElectionDecisionMethod::VOTER_HISTORY};
}

void FlexibleVoteCounter::GetLastKnownLeader(
    LastKnownLeaderPB* last_known_leader) const {
  last_known_leader->CopyFrom(last_known_leader_);
  if (!last_known_leader->uuid().empty()) {
    LOG_WITH_PREFIX(INFO) << "Candidates own Last Known Leader: "
                          << last_known_leader->uuid()
                          << " term: " << last_known_leader->election_term()
                          << " quorum_id: "
                          << DetermineQuorumIdForUUID(
                                 last_known_leader->uuid());
  } else {
    LOG(INFO) << "Candidates own Last Known Leader is unset";
  }
}

std::pair<bool, bool> FlexibleVoteCounter::AreMajoritiesSatisfied(
    const std::set<std::string>& last_known_leader_regions,
    const std::string& candidate_region) const {
  std::pair<bool, bool> result =
      IsMajoritySatisfiedInAllRegions(last_known_leader_regions);

  // Case: We require majority of majority to win the election
  if (FLAGS_srd_strict_leader_election_quorum) {
    std::pair<bool, bool> majority_result =
        IsMajoritySatisfiedInMajorityOfRegions();
    result = std::make_pair(
        result.first && majority_result.first,
        result.second && majority_result.second);
  }

  // Case: We require majority from candidate region to win the election
  if (FLAGS_include_candidate_region &&
      last_known_leader_regions.find(candidate_region) ==
          last_known_leader_regions.end()) {
    std::pair<bool, bool> candidate_result =
        IsMajoritySatisfiedInRegion(candidate_region);
    result = std::make_pair(
        result.first && candidate_result.first,
        result.second && candidate_result.second);
  }

  return result;
}

FlexibleVoteCounter::QuorumState FlexibleVoteCounter::IsDynamicQuorumSatisfied()
    const {
  CHECK(config_.commit_rule().mode() == QuorumMode::SINGLE_REGION_DYNAMIC);

  LastKnownLeaderPB last_known_leader;
  GetLastKnownLeader(&last_known_leader);

  // Declare loss early upon discovering leader in higher term.
  if (election_term_ <= last_known_leader.election_term()) {
    LOG_WITH_PREFIX(INFO) << "Declaring election loss because a new leader "
                          << "has been found in the crowd sourcing phase.";
    return {false, false, ElectionDecisionMethod::INVALIDATED_BY_HIGHER_TERM};
  }
  bool all_votes_are_in = AreAllVotesIn();
  if (all_votes_are_in) {
    LOG_WITH_PREFIX(INFO) << "All expected number of votes are in";
  }

  // This could be empty as a Last known leader could no more
  // be a voter or in the ring.
  std::string last_known_leader_quorum_id(
      DetermineQuorumIdForUUID(last_known_leader.uuid()));

  // Step 1: Check if pessimistic quorum is satisfied.
  QuorumState pessimistic_result = IsPessimisticQuorumSatisfied();

  if (all_votes_are_in &&
      pessimistic_result.achievedMajority !=
          pessimistic_result.canAchieveMajority) {
    // when all_votes_are_in, it should not be the case that
    // pessimistic_result.second is different from pessimistic_result.first,
    // because it should be a clear VOTE_GRANTED or VOTE_DENIED case
    // (decideable)
    LOG_WITH_PREFIX(DFATAL)
        << "UNEXPECTED VOTING: All votes are in but Pessimistic quorum is "
        << "not decideable. Acheived majority: "
        << pessimistic_result.achievedMajority
        << ", can achieve majority: " << pessimistic_result.canAchieveMajority;
  }

  // Return pessimistic quorum result if the pessimistic quorum is satisfied
  // or if the pessimistic quorum cannot be satisfied and we depend on the
  // knowledge of the last leader without having it (eg. during bootstrap), we
  // should declare having lost the election or having insufficient votes to
  // make a decision.
  if (pessimistic_result.achievedMajority ||
      last_known_leader_quorum_id.empty()) {
    LOG_WITH_PREFIX(INFO)
        << "Election status returned from pessimistic quorum check. "
        << "Last known leader quorum_id: " << last_known_leader_quorum_id;
    return pessimistic_result;
  }

  // candidate_region is expected to be valid, because Raft Consensus
  // only starts elections in voters which have valid region
  std::string candidate_quorum_id(DetermineQuorumIdForUUID(candidate_uuid_));

  // Step 2: Check if last known leader's quorum is satisfied and we directly
  // succeed term.
  //
  // Since pessimistic quorum is not satisfied, we have to intersect with last
  // known leader region to guarantee longest log in next LEADER.
  // However if there is a period of confusion after there was a stable
  // LEADER, the CANDIDATE might not be able to know which region to intersect
  // with.
  bool is_continuous = election_term_ == last_known_leader.election_term() + 1;

  QuorumState result;

  if (is_continuous) {
    CHECK(!last_known_leader.uuid().empty());
    CHECK(!last_known_leader_quorum_id.empty());
    VLOG_WITH_PREFIX(1)
        << "Election term immediately succeeds term of the last known leader"
        << " or continuity in terms is not required."
        << " Election term: " << election_term_
        << " lkl term: " << last_known_leader.election_term()
        << " lkl uuid: " << last_known_leader.uuid()
        << " lkl quorum_id: " << last_known_leader_quorum_id
        << " is_continuous: " << is_continuous;
    auto [achievedMajority, canAchieveMajority] = AreMajoritiesSatisfied(
        {last_known_leader_quorum_id}, candidate_quorum_id);
    // Log the heuristic used if the election is successful or
    // if the election is definitely lost. We don't want to log
    // for each vote received as it might be 15-20 lines.
    result.achievedMajority = achievedMajority;
    result.canAchieveMajority = canAchieveMajority;
    result.latest_decision_mechanism =
        ElectionDecisionMethod::CONTINUOUS_LKL_QUORUM;
    if (result.achievedMajority || !result.canAchieveMajority) {
      LOG_WITH_PREFIX(INFO)
          << "Final decision: Flexiraft Heuristic Info. Election term immediately succeeds term of the last known leader"
          << " or continuity in terms is not required."
          << " Election term: " << election_term_
          << " lkl term: " << last_known_leader.election_term()
          << " lkl uuid: " << last_known_leader.uuid()
          << " lkl quorum_id: " << last_known_leader_quorum_id
          << " is_continuous: " << is_continuous;
    }
  } else {
    // Case: If we're here there is discontinuity in election terms
    // It is rare case (since we run pre-elections for Dead Primary Promotions
    // before term bump). Today this can happen for 3 reasons
    // 1. TransferLeadership Promotions which fail for corner cases.
    // 2. Race of 2 CANDIDATEs/split votes
    // 3. Operator introduced Promotions

    // Step 4.1: If pessimistic quorum satisfaction is possible we wait for
    // FLAGS_wait_for_pessimistic_quorum_secs secs for it to be satisfied
    auto now = std::chrono::system_clock::now();
    long time_elapsed_secs =
        std::chrono::duration_cast<std::chrono::seconds>(now - creation_time_)
            .count();
    if (pessimistic_result.canAchieveMajority &&
        time_elapsed_secs < FLAGS_wait_for_pessimistic_quorum_secs) {
      LOG_WITH_PREFIX(INFO)
          << "Pausing for Pessimistic quorum to help decide election";
      return pessimistic_result;
    }

    // Set this to false if we don't want to fallback to Voting history,
    // which has some known gaps in its logic.
    // Risk with that is when there is a single region failure, pessimistic
    // quorum will return failure but the election can still be won by finding
    // a smaller set of previous leader regions to intersect with.
    // In these cases operator intervention will be required.
    if (!FLAGS_use_voting_history_as_last_resort) {
      LOG_WITH_PREFIX(INFO)
          << "Pessimistic quorum did not help decide election but voting history is disabled";
      return pessimistic_result;
    }

    LOG_WITH_PREFIX(INFO)
        << "Using Voting History Fallback due to term discontinuity"
        << " Election term: " << election_term_
        << " lkl term: " << last_known_leader.election_term()
        << " lkl uuid: " << last_known_leader.uuid()
        << " lkl quorum_id: " << last_known_leader_quorum_id
        << " is_continuous: " << is_continuous;

    // Step 4.2: We come here if pessimistic quorum satisfaction is not
    // possible or we were not able to decide with pessimistic quorum
    // We also should have waited ror FLAGS_wait_for_pessimistic_quorum_secs
    // to give pessimistic quorum and other peers a chance to win the
    // election.

    // Find possible leader regions at every term greater than last known
    // leader's term. Computes possible successor regions until next term is
    // the current election's term or the quorum converges to pessimistic
    // quorum.
    //
    // If we come here, it is guaranteed that none of the current votes that
    // we have received (including our own) help us figure out last known
    // LEADER, or we are running an election where there has been a period of
    // confusion, i.e. we are running an election at a term which does not
    // immediately follow Last Known Leader. This is not expected to happen
    // due to PreVote being used all the time, but in some cases
    // ForcedElections (StartElection) with failure can lead to analysis
    // paralysis term.
    //
    // So our hail mary is: If by analyzing the votes, we can find potential
    // leader regions, we can intersect with those potential regions in the
    // hope that it will be less than pessimistic quorum.
    result = ComputeElectionResultFromVotingHistory(
        last_known_leader, last_known_leader_quorum_id, candidate_quorum_id);
  }

  if (all_votes_are_in &&
      result.achievedMajority != result.canAchieveMajority) {
    // when all_votes_are_in, it should not be the case that
    // result.second is different from result.first,
    // because it should be a clear VOTE_GRANTED or VOTE_DENIED case
    // (decideable)
    LOG_WITH_PREFIX(DFATAL)
        << "UNEXPECTED VOTING: All votes are in but quorum is "
        << "not decideable. Acheived majority: " << result.achievedMajority
        << ", can achieve majority: " << result.canAchieveMajority;
  }

  return result;
}

FlexibleVoteCounter::QuorumState FlexibleVoteCounter::GetQuorumState() const {
  // If the quorum is not a function of the last leader's region,
  // return early.
  if (config_.commit_rule().mode() == QuorumMode::STATIC_DISJUNCTION ||
      config_.commit_rule().mode() == QuorumMode::STATIC_CONJUNCTION) {
    return IsStaticQuorumSatisfied();
  }
  return IsDynamicQuorumSatisfied();
}

bool FlexibleVoteCounter::IsDecided() const {
  const QuorumState quorum_state = GetQuorumState();
  return quorum_state.achievedMajority || !quorum_state.canAchieveMajority;
}

Status FlexibleVoteCounter::GetDecision(
    ElectionVote* decision,
    ElectionDecisionMethod* decision_method) const {
  const QuorumState quorum_state = GetQuorumState();
  if (quorum_state.achievedMajority) {
    *decision = VOTE_GRANTED;
    *decision_method = quorum_state.latest_decision_mechanism;
    return Status::OK();
  }
  if (!quorum_state.canAchieveMajority) {
    *decision = VOTE_DENIED;
    *decision_method = quorum_state.latest_decision_mechanism;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

std::string FlexibleVoteCounter::LogPrefix() const {
  return Substitute(
      "[Flexible Vote Counter] Election term: $0 ", election_term_);
}

///////////////////////////////////////////////////
// ElectionResult
///////////////////////////////////////////////////

ElectionResult::ElectionResult(
    VoteRequestPB vote_request,
    ElectionVote decision,
    ConsensusTerm highest_voter_term,
    const std::string& message,
    bool is_candidate_removed,
    ElectionDecisionMethod decision_method)
    : vote_request(std::move(vote_request)),
      decision(decision),
      highest_voter_term(highest_voter_term),
      message(message),
      is_candidate_removed(is_candidate_removed),
      decision_method(decision_method) {
  DCHECK(!message.empty());
}

///////////////////////////////////////////////////
// LeaderElection::VoterState
///////////////////////////////////////////////////

std::string LeaderElection::VoterState::PeerInfo() const {
  std::string info = peer_uuid;
  if (proxy) {
    strings::SubstituteAndAppend(&info, " ($0)", proxy->PeerName());
  }
  return info;
}

///////////////////////////////////////////////////
// LeaderElection
///////////////////////////////////////////////////

LeaderElection::LeaderElection(
    RaftConfigPB config,
    PeerProxyFactory* proxy_factory,
    VoteRequestPB request,
    std::unique_ptr<VoteCounter> vote_counter,
    MonoDelta timeout,
    ElectionDecisionCallback decision_callback,
    std::shared_ptr<VoteLoggerInterface> vote_logger)
    : has_responded_(false),
      config_(std::move(config)),
      proxy_factory_(proxy_factory),
      request_(std::move(request)),
      vote_counter_(std::move(vote_counter)),
      timeout_(timeout),
      decision_callback_(std::move(decision_callback)),
      highest_voter_term_(0),
      start_time_(MonoTime::Now()),
      vote_logger_(std::move(vote_logger)) {}

LeaderElection::~LeaderElection() {
  std::lock_guard<Lock> guard(lock_);
  STLDeleteValues(&voter_state_);
}

void LeaderElection::Run() {
  VLOG_WITH_PREFIX(1) << "Running leader election.";

  // Initialize voter state tracking.
  vector<std::string> other_voter_uuids;
  voter_state_.clear();
  for (const RaftPeerPB& peer : config_.peers()) {
    if (request_.candidate_uuid() == peer.permanent_uuid()) {
      DCHECK_EQ(peer.member_type(), RaftPeerPB::VOTER) << Substitute(
          "non-voter member $0 tried to start an election; "
          "Raft config {$1}",
          peer.permanent_uuid(),
          pb_util::SecureShortDebugString(config_));
      continue;
    }
    if (peer.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    other_voter_uuids.emplace_back(peer.permanent_uuid());

    std::unique_ptr<VoterState> state(new VoterState());
    state->peer_uuid = peer.permanent_uuid();
    state->proxy_status = proxy_factory_->NewProxy(peer, &state->proxy);
    InsertOrDie(&voter_state_, peer.permanent_uuid(), state.release());
  }

  // Ensure that the candidate has already voted for itself.
  CHECK_EQ(1, vote_counter_->GetTotalVotesCounted())
      << "Candidate must vote for itself first";

  // Ensure that existing votes + future votes add up to the expected total.
  CHECK_EQ(
      vote_counter_->GetTotalVotesCounted() + other_voter_uuids.size(),
      vote_counter_->GetTotalExpectedVotes())
      << "Expected different number of voters. Voter UUIDs: ["
      << JoinStringsIterator(
             other_voter_uuids.begin(), other_voter_uuids.end(), ", ")
      << "]; RaftConfig: {" << pb_util::SecureShortDebugString(config_) << "}";

  // Check if we have already won the election (relevant if this is a
  // single-node configuration, since we always pre-vote for ourselves).
  CheckForDecision();

  std::string msg;
  msg.reserve(100 * other_voter_uuids.size());
  size_t pnum = 0;
  // The rest of the code below is for a typical multi-node configuration.
  for (const auto& voter_uuid : other_voter_uuids) {
    VoterState* state = nullptr;
    {
      std::lock_guard<Lock> guard(lock_);
      state = FindOrDie(voter_state_, voter_uuid);
      // Safe to drop the lock because voter_state_ is not mutated outside of
      // the constructor / destructor. We do this to avoid deadlocks below.
    }

    // If we failed to construct the proxy, just record a 'NO' vote with the
    // status that indicates why it failed.
    if (!state->proxy_status.ok()) {
      LOG_WITH_PREFIX(WARNING)
          << "Was unable to construct an RPC proxy to peer "
          << state->PeerInfo() << ": " << state->proxy_status.ToString()
          << ". Counting it as a 'NO' vote.";
      {
        std::lock_guard<Lock> guard(lock_);
        RecordVoteUnlocked(*state, VOTE_DENIED);
      }
      CheckForDecision();
      continue;
    }

    // Create a single message with comma separated peers
    if (pnum != 0) {
      msg.append(", ");
    }
    pnum++;
    msg.append(uuid2hostport(state->peer_uuid, config_));

    state->rpc.set_timeout(timeout_);

    state->request = request_;
    state->request.set_dest_uuid(voter_uuid);

    state->proxy->RequestConsensusVoteAsync(
        &state->request,
        &state->response,
        &state->rpc,
        // We use gutil Bind() for the refcounting and boost::bind to adapt
        // the gutil Callback to a thunk.
        boost::bind(
            &Closure::Run,
            Bind(&LeaderElection::VoteResponseRpcCallback, this, voter_uuid)));
  }
  // Send the RPC request.
  LOG_WITH_PREFIX(INFO) << "Requesting " << ElectionMode_Name(request_.mode())
                        << "-vote from peers: " << msg;
  if (vote_logger_) {
    vote_logger_->logElectionStarted(request_, config_);
  }
}

void LeaderElection::CheckForDecision() {
  bool to_respond = false;
  {
    std::lock_guard<Lock> guard(lock_);
    // Check if the vote has been newly decided.
    if (!result_ && vote_counter_->IsDecided()) {
      ElectionVote decision;
      ElectionDecisionMethod decision_method;
      CHECK_OK(vote_counter_->GetDecision(&decision, &decision_method));
      MonoTime end = MonoTime::Now();
      MonoDelta election_duration = end.GetDeltaSince(start_time_);

      LOG_WITH_PREFIX(INFO)
          << "Election decided. Result: candidate "
          << ((decision == VOTE_GRANTED) ? "won." : "lost.")
          << " duration: " << election_duration.ToString()
          << ", mechanism: " << electionDecisionMethodToString(decision_method);
      std::string msg = (decision == VOTE_GRANTED)
          ? "achieved majority votes"
          : "could not achieve majority";

      bool is_candidate_removed = false;
      if (decision == VOTE_DENIED) {
        is_candidate_removed = vote_counter_->IsCandidateRemoved();
      }

      result_.reset(new ElectionResult(
          request_,
          decision,
          highest_voter_term_,
          msg,
          is_candidate_removed,
          decision_method));
      if (vote_logger_) {
        vote_logger_->logElectionDecided(*result_);
      }
    }
    // Check whether to respond. This can happen as a result of either getting
    // a majority vote or of something invalidating the election, like
    // observing a higher term.
    if (result_ && !has_responded_) {
      has_responded_ = true;
      to_respond = true;
    }
  }

  // Respond outside of the lock.
  if (to_respond) {
    // This is thread-safe since result_ is write-once.
    decision_callback_(*result_);
  }
}

void LeaderElection::VoteResponseRpcCallback(const std::string& voter_uuid) {
  {
    std::lock_guard<Lock> guard(lock_);
    VoterState* state = FindOrDie(voter_state_, voter_uuid);

    // Check for RPC errors.
    if (!state->rpc.status().ok()) {
      LOG_WITH_PREFIX(WARNING)
          << "RPC error from VoteRequest() call to peer " << state->PeerInfo()
          << ": " << state->rpc.status().ToString();
      RecordVoteUnlocked(*state, VOTE_DENIED);

      // Check for tablet errors.
    } else if (state->response.has_error()) {
      RecordVoteUnlocked(*state, VOTE_DENIED);

      // If the peer changed their IP address, we shouldn't count this vote
      // since our knowledge of the configuration is in an inconsistent state.
    } else if (PREDICT_FALSE(voter_uuid != state->response.responder_uuid())) {
      LOG_WITH_PREFIX(DFATAL)
          << "Received vote response from peer " << state->PeerInfo() << ": "
          << "we thought peer had UUID " << voter_uuid
          << " but its actual UUID is " << state->response.responder_uuid();
      RecordVoteUnlocked(*state, VOTE_DENIED);

    } else {
      // No error: count actual votes.
      highest_voter_term_ =
          std::max(highest_voter_term_, state->response.responder_term());
      if (state->response.vote_granted()) {
        HandleVoteGrantedUnlocked(*state);
      } else {
        HandleVoteDeniedUnlocked(*state);
      }
    }
    if (vote_logger_) {
      vote_logger_->logVoteReceived(state->response);
    }
  }

  // Check for a decision outside the lock.
  CheckForDecision();
}

void LeaderElection::RecordVoteUnlocked(
    const VoterState& state,
    ElectionVote vote) {
  DCHECK(lock_.is_locked());

  // Construct vote information struct.
  VoteInfo vote_info;
  vote_info.vote = vote;
  if (state.response.has_last_pruned_term()) {
    vote_info.last_pruned_term = state.response.last_pruned_term();
  } else {
    vote_info.last_pruned_term = election_term();
  }
  if (state.response.has_voter_context()) {
    vote_info.is_candidate_removed =
        state.response.voter_context().is_candidate_removed();
  }

  for (int i = 0; i < state.response.previous_vote_history_size(); i++) {
    vote_info.previous_vote_history.push_back(
        state.response.previous_vote_history(i));
  }

  // Sorting according to election_term.
  std::sort(
      vote_info.previous_vote_history.begin(),
      vote_info.previous_vote_history.end(),
      compare_PreviousVotePB);

  // Record the vote.
  bool duplicate;
  Status s =
      vote_counter_->RegisterVote(state.peer_uuid, vote_info, &duplicate);
  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Error registering vote for peer "
                             << state.PeerInfo() << ": " << s.ToString();
    return;
  }
  if (duplicate) {
    // Note: This is DFATAL because at the time of writing we do not support
    // retrying vote requests, so this should be impossible. It may be valid
    // to receive duplicate votes in the future if we implement retry.
    LOG_WITH_PREFIX(DFATAL)
        << "Duplicate vote received from peer " << state.PeerInfo();
  }
}

void LeaderElection::HandleHigherTermUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  DCHECK_GT(state.response.responder_term(), election_term());

  std::string msg = Substitute(
      "Vote denied by peer $0 with higher term. Message: $1",
      state.PeerInfo(),
      StatusFromPB(state.response.consensus_error().status()).ToString());
  LOG_WITH_PREFIX(WARNING) << msg;

  if (!result_) {
    bool is_candidate_removed = false;
    LOG_WITH_PREFIX(INFO)
        << "Cancelling election due to peer responding with higher term";
    if (state.response.has_voter_context() &&
        state.response.voter_context().is_candidate_removed()) {
      is_candidate_removed = true;
    }
    result_.reset(new ElectionResult(
        request_,
        VOTE_DENIED,
        state.response.responder_term(),
        msg,
        is_candidate_removed,
        ElectionDecisionMethod::INVALIDATED_BY_HIGHER_TERM));
  }
}

void LeaderElection::HandleVoteGrantedUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  ElectionMode mode = request_.mode();
  if (mode != ElectionMode::PRE_ELECTION &&
      mode != ElectionMode::MOCK_ELECTION) {
    DCHECK_EQ(state.response.responder_term(), election_term());
  }
  DCHECK(state.response.vote_granted());

  LOG_WITH_PREFIX(INFO) << "Vote granted by peer "
                        << uuid2hostport(state.peer_uuid, config_);
  RecordVoteUnlocked(state, VOTE_GRANTED);
}

void LeaderElection::HandleVoteDeniedUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  DCHECK(!state.response.vote_granted());

  // If one of the voters responds with a greater term than our own, and we
  // have not yet triggered the decision callback, it cancels the election.
  if (state.response.responder_term() > election_term()) {
    return HandleHigherTermUnlocked(state);
  }

  LOG_WITH_PREFIX(INFO)
      << "Vote denied by peer " << uuid2hostport(state.peer_uuid, config_)
      << ". Message: "
      << StatusFromPB(state.response.consensus_error().status()).ToString();
  RecordVoteUnlocked(state, VOTE_DENIED);
}

std::string LeaderElection::LogPrefix() const {
  return Substitute(
      "T $0 P $1 [CANDIDATE]: Term $2 $3: ",
      request_.tablet_id(),
      request_.candidate_uuid(),
      request_.candidate_term(),
      ElectionMode_Name(request_.mode()));
}

} // namespace kudu::consensus
