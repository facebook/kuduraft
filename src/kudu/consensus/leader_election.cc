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
//#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {
namespace {

// Comparator for PreviousVotePB
bool compare_PreviousVotePB(
    const PreviousVotePB& a, const PreviousVotePB& b) {
  return a.election_term() < b.election_term();
}

// Comparator for binary search in a sorted list of PreviousVotePB
bool compareTerm_PreviousVotePB(
    const int64_t a, const PreviousVotePB& b) {
  return a < b.election_term();
}
}

using std::string;
using std::vector;
using strings::Substitute;

///////////////////////////////////////////////////
// VoteCounter & FlexibleVoteCounter
///////////////////////////////////////////////////

VoteCounter::VoteCounter(int num_voters, int majority_size)
  : num_voters_(num_voters),
    majority_size_(majority_size),
    yes_votes_(0),
    no_votes_(0) {
  CHECK_LE(majority_size, num_voters);
  CHECK_GT(num_voters_, 0);
  CHECK_GT(majority_size_, 0);
}

Status VoteCounter::RegisterVote(
    const std::string& voter_uuid, const VoteInfo& vote_info,
    bool* is_duplicate) {
  // Handle repeated votes.
  if (PREDICT_FALSE(ContainsKey(votes_, voter_uuid))) {
    // Detect changed votes.
    const VoteInfo& prior_vote_info = votes_.at(voter_uuid);
    if (PREDICT_FALSE(prior_vote_info.vote != vote_info.vote)) {
      string msg = Substitute(
          "Peer $0 voted a different way twice in the same election. "
          "First vote: $1, second vote: $2.",
          voter_uuid, prior_vote_info.vote, vote_info.vote);
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

Status VoteCounter::GetDecision(ElectionVote* decision) const {
  if (yes_votes_ >= majority_size_) {
    *decision = VOTE_GRANTED;
    return Status::OK();
  }
  if (no_votes_ > num_voters_ - majority_size_) {
    *decision = VOTE_DENIED;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

int VoteCounter::GetTotalVotesCounted() const {
  return yes_votes_ + no_votes_;
}

bool VoteCounter::AreAllVotesIn() const {
  return GetTotalVotesCounted() == num_voters_;
}

PotentialNextLeaderResponse::PotentialNextLeaderResponse(
    PotentialNextLeaderResponse::Status s) {
  status = s;
  potential_leader_uuid = "";
  potential_leader_term = -1;
}

PotentialNextLeaderResponse::PotentialNextLeaderResponse(
    PotentialNextLeaderResponse::Status s, const std::string& uuid,
    const int64_t term) {
  status = s;
  potential_leader_uuid = uuid;
  potential_leader_term = term;
}

void FlexibleVoteCounter::FetchTopologyInfo() {
  // Step 1: Compute number of voters in each region.
  // Also, computes the leader's region from its UUID.
  GetRegionalCountsFromConfig(
      config_, last_known_leader_.uuid, &voter_distribution_,
      &last_known_leader_region_);

  CHECK(config_.has_commit_rule());

  // Step 2: Compute the leader election quorum from the corresponding
  // data commit quorum. The requirement is for it to intersect with all the
  // data commit quorums and all the leader election quorums.
  for (const RegionRuleSpecPB& rule_spec :
      config_.commit_rule().rule_conjunctions()) {
    std::string region = rule_spec.region_identifier();

    if (region == "master") {
      depends_on_last_leader_ = true;

      // Note: We have populated the commit requirement for the master region
      // and are not resolving it now.
      InsertIfNotPresent(&le_quorum_requirement_, region, -1);
      continue;
    }

    // This will throw if the configuration is not properly defined.
    int commit_req_num = ParseCommitRequirement(
        rule_spec.commit_requirement());

    // At this point region is resolved and we should be able to
    // fetch its cardinalities.
    // TODO(ritwikyadav): It is possible that all voters in a region
    // have left the replicaset. Handle that case gracefully in a subsequent diff.
    int total_voters = FindOrDie(voter_distribution_, region);
    int region_majority = MajoritySize(total_voters);

    // commit_req_num is -1 in case of majority requirement.
    if (commit_req_num == -1) {
      commit_req_num = region_majority;
    }

    // We chose the maximum of the two conditions required for intersection
    // of leader election quorum with data commit quorum and for any two
    // leader election quorums to intersect with one another.
    int le_req_num = std::max(
        total_voters + 1 - commit_req_num, region_majority);

    InsertIfNotPresent(&le_quorum_requirement_, region, le_req_num);
  }

  ComputePessimisticLeaderElectionQuorum();
}

void FlexibleVoteCounter::ComputePessimisticLeaderElectionQuorum() {
  // If master region cannot be resolved, we choose the most pessimistic quorum.
  pessimistic_le_quorum_ = le_quorum_requirement_;

  if (!depends_on_last_leader_) {
    // If the quorum is not a function of the current master region, then it
    // is exactly the same as le_quorum_requirement_
    return;
  } else {
    // Removing unresolved master specification.
    pessimistic_le_quorum_.erase("master");
  }

  // Here we enforce the same requirements that were needed in the leader region in
  // all the regions which have voters.
  for (const std::pair<std::string, int>& region_count_pair :
      voter_distribution_) {
    int& pessimistic_le_count =
        LookupOrInsert(&pessimistic_le_quorum_, region_count_pair.first, 0);
    int region_majority_count = MajoritySize(region_count_pair.second);
    // It is possible that the region already had more stringent requirements.
    pessimistic_le_count = std::max(pessimistic_le_count, region_majority_count);
  }
}

FlexibleVoteCounter::FlexibleVoteCounter(
    int64_t election_term, const LastKnownLeader& last_known_leader,
    RaftConfigPB config)
  : VoteCounter(0, 0),
    election_term_(election_term),
    depends_on_last_leader_(false),
    last_known_leader_(last_known_leader),
    config_(std::move(config)) {
  num_voters_ = 0;

  FetchTopologyInfo();
  for (
      const std::pair<std::string, int>& regional_voter_count :
      voter_distribution_) {
    CHECK_GT(regional_voter_count.second, 0);
    num_voters_ += regional_voter_count.second;
    yes_vote_count_.emplace(regional_voter_count.first, 0);
    no_vote_count_.emplace(regional_voter_count.first, 0);
  }

  CHECK_GT(num_voters_, 0);

  // Flag to check if the quorum specification is non-empty for all
  // specifications.
  bool empty_quorum = false;
  for (const std::pair<std::string, int>& regional_quorum_count :
      le_quorum_requirement_) {
    if (regional_quorum_count.first == "master") {
      continue;
    }
    const std::map<std::string, int>::const_iterator total_voters =
        voter_distribution_.find(regional_quorum_count.first);
    // Make sure that voters are present in the region specified by quorum
    // requirement and their number is enough.
    CHECK(total_voters != voter_distribution_.end());
    CHECK_LE(regional_quorum_count.second, total_voters->second);
    if (regional_quorum_count.second <= 0) {
      empty_quorum = true;
    }
  }
  CHECK(!empty_quorum);

  for (const RaftPeerPB& peer : config_.peers()) {
    if (peer.member_type() == RaftPeerPB::VOTER) {
      uuid_to_region_.emplace(peer.permanent_uuid(), peer.attrs().region());
    }
  }
}

Status FlexibleVoteCounter::RegisterVote(
    const std::string& voter_uuid, const VoteInfo& vote_info,
    bool* is_duplicate) {
  Status s = VoteCounter::RegisterVote(
      voter_uuid, vote_info, is_duplicate);

  // No book-keeping required for duplicate votes.
  if (*is_duplicate) {
    return s;
  }

  if (!ContainsKey(uuid_to_region_, voter_uuid)) {
    return Status::InvalidArgument(
        Substitute("UUID {$0} not present in config.", voter_uuid));
  }

  const std::string& region = uuid_to_region_[voter_uuid];
  switch (vote_info.vote) {
    case VOTE_GRANTED:
      InsertIfNotPresent(&yes_vote_count_, region, 0);
      yes_vote_count_[region]++;
      break;
    case VOTE_DENIED:
      InsertIfNotPresent(&no_vote_count_, region, 0);
      no_vote_count_[region]++;
      break;
  }

  return s;
}

int FlexibleVoteCounter::FetchVotesRemainingInRegion(
    const std::string& region) const {
  const std::map<std::string, int>::const_iterator regional_yes_count =
      yes_vote_count_.find(region);
  const std::map<std::string, int>::const_iterator regional_no_count =
      no_vote_count_.find(region);
  const std::map<std::string, int>::const_iterator total_region_count =
      voter_distribution_.find(region);

  // All the following must at least be initialized to zero in the
  // constructor.
  CHECK(total_region_count != voter_distribution_.end());
  CHECK(regional_yes_count != yes_vote_count_.end());
  CHECK(regional_no_count != no_vote_count_.end());

  return total_region_count->second - regional_yes_count->second -
      regional_no_count->second;
}

std::map<string, int> FlexibleVoteCounter::ResolveQuorum(
    const std::string& leader_region) const {
  bool master_requirement_found = false;
  std::map<string, int> resolved_quorum;
  for (const std::pair<string, int>& quorum_spec : le_quorum_requirement_) {
    if (quorum_spec.first == "master") {
      master_requirement_found = true;
      int commit_requirement = quorum_spec.second;

      // We only allow "majority" tag with "master" region since
      // the complexity to support an arbitrary integer as the master's
      // requirement is high. For instance, if the number of votes required
      // in the master region is 5 but the master region has 3 voters, we will
      // never be able to promote out of that region (trapped master).
      CHECK(commit_requirement == -1);
      int total_voters = FindOrDie(voter_distribution_, leader_region);
      commit_requirement = MajoritySize(total_voters);

      resolved_quorum.insert(
          std::make_pair<>(leader_region, commit_requirement));
    } else {
      resolved_quorum.insert(quorum_spec);
    }
  }
  CHECK(master_requirement_found);
  return resolved_quorum;
}

std::pair<bool, bool> FlexibleVoteCounter::IsQuorumSatisfied(
    const std::map<std::string, int>& resolved_quorum) const {
  CHECK(!resolved_quorum.empty());

  bool quorum_satisfied = true;
  bool quorum_satisfaction_possible = true;

  std::map<std::string, int>::const_iterator it = resolved_quorum.begin();
  for (; it != resolved_quorum.end(); it++) {
    const string& region = it->first;
    const std::map<std::string, int>::const_iterator regional_yes_count =
        yes_vote_count_.find(region);
    const std::map<std::string, int>::const_iterator regional_no_count =
        no_vote_count_.find(region);
    const std::map<std::string, int>::const_iterator total_region_count =
        voter_distribution_.find(region);

    // All the following must at least be initialized to zero in the
    // constructor.
    CHECK(total_region_count != voter_distribution_.end());
    CHECK(regional_yes_count != yes_vote_count_.end());
    CHECK(regional_no_count != no_vote_count_.end());

    VLOG_WITH_PREFIX(3) << "Region: " << region
                        << " Total voters: " << total_region_count->second
                        << " Votes granted count: "
                        << regional_yes_count->second
                        << " Votes denied count: " << regional_no_count->second
                        << " Quorum requirement: " << it->second;

    if (total_region_count->second < it->second) {
      // This should never happen in practice because of pre-checks.
      VLOG_WITH_PREFIX(1) << "Total number of voters in region: " << region
                          << " is: " << total_region_count->second
                          << " but quorum requirement is: " << it->second;
      quorum_satisfied = false;
      quorum_satisfaction_possible = false;
      break;
    }

    if (regional_yes_count->second < it->second) {
      VLOG_WITH_PREFIX(2) << "Yes votes in region: " << region
                    << " are: " << regional_yes_count->second
                    << " but quorum requirement is: " << it->second;
      quorum_satisfied = false;
    }
    if (regional_no_count->second +
        it->second > total_region_count->second) {
      VLOG_WITH_PREFIX(2) << "Quorum satisfaction not possible in region: "
                          << region << " because of excessive no votes: "
                          << regional_no_count->second
                          << " Quorum requirement: " << it->second;
      quorum_satisfaction_possible = false;
    }
  }
  return std::make_pair<>(quorum_satisfied, quorum_satisfaction_possible);
}

std::pair<bool, bool>
FlexibleVoteCounter::FetchQuorumSatisfactionInfoFromVoteHistory(
    const std::string& leader_region,
    const RegionToVoterSet& region_to_voter_set) const {
  VLOG_WITH_PREFIX(1) << "Fetching quorum satisfaction info from "
                      << "vote history. Leader region: " << leader_region;
  bool quorum_satisfied = true;
  bool quorum_satisfaction_possible = true;
  const std::map<std::string, int>& resolved_quorum =
      ResolveQuorum(leader_region);
  std::map<std::string, int>::const_iterator quorum_it =
      resolved_quorum.begin();
  for ( ; quorum_it != resolved_quorum.end(); quorum_it++) {
    const std::string& region = quorum_it->first;
    int votes_received = 0;
    const RegionToVoterSet::const_iterator& rtvs_it =
        region_to_voter_set.find(region);
    if (rtvs_it != region_to_voter_set.end()) {
      votes_received = rtvs_it->second.size();
    }
    int commit_requirement = quorum_it->second;
    int votes_remaining = FetchVotesRemainingInRegion(region);
    VLOG_WITH_PREFIX(3) << "Region: " << region << " , Votes granted: "
                        << votes_received << " , Votes remaining: "
                        << votes_remaining << " , Commit Requirement: "
                        << commit_requirement;
    if (votes_received < commit_requirement) {
      quorum_satisfied = false;
    }
    if (votes_received + votes_remaining < commit_requirement) {
      quorum_satisfaction_possible = false;
    }
  }
  return std::make_pair<>(quorum_satisfied, quorum_satisfaction_possible);
}

void FlexibleVoteCounter::ConstructRegionWiseVoteCollation(
    const int64_t term,
    const std::string& leader_region,
    VoteHistoryCollation* vote_collation,
    VoteHistoryIteratorMap* it_map,
    int64_t* min_term) const {
  CHECK(vote_collation);
  CHECK(it_map);
  CHECK(min_term);

  vote_collation->clear();
  it_map->clear();
  *min_term = 0;

  VoteMap::const_iterator it = votes_.begin();
  while (it != votes_.end()) {
    const std::string& uuid = it->first;

    // Skip servers that are not in the region of the last known leader or
    // one of the regions that feature in the leader election quorum.
    std::map<std::string, std::string>::const_iterator reg_it =
        uuid_to_region_.find(uuid);
    if (reg_it == uuid_to_region_.end() ||
        reg_it->second != leader_region ||
        le_quorum_requirement_.find(reg_it->second) ==
            le_quorum_requirement_.end()) {
      continue;
    }
    const std::vector<PreviousVotePB>& pvh = it->second.previous_vote_history;

    // Find the voting record immediately after the term of the last known
    // leader. Skip if there is no history beyond the last known leader.
    std::vector<PreviousVotePB>::const_iterator vhi = std::upper_bound(
        pvh.begin(), pvh.end(), term, compareTerm_PreviousVotePB);
    if (vhi == pvh.end()) {
      continue;
    }
    const UUIDTermPair utp = std::make_pair<>(
        vhi->candidate_uuid(), vhi->election_term());

    // Update minimum term seen so far in the last known leader's region.
    if (reg_it->second == leader_region) {
      *min_term = std::min(*min_term, utp.second);
    }

    // Insert the iterator into the map and update the collation.
    // The collation is a map from (UUID, term) -> [region -> set(UUID)].
    // For each key (UUID - term pair), it represents all servers
    // (corresponding UUIDs) which voted for the key.
    InsertIfNotPresent(it_map, uuid, vhi);
    RegionToVoterSet& rtvs =
        LookupOrInsert(vote_collation, utp, RegionToVoterSet());
    std::set<std::string>& uuid_set =
        LookupOrInsert(&rtvs, reg_it->second, std::set<std::string>());
    uuid_set.insert(uuid);

    it++;
  }
}

void FlexibleVoteCounter::UpdateRegionWiseVoteCollation(
    const int64_t term,
    const std::string& leader_region,
    VoteHistoryCollation* vote_collation,
    VoteHistoryIteratorMap* it_map,
    int64_t* min_term) const {
  CHECK(vote_collation);
  CHECK(it_map);
  CHECK(min_term);

  // Advance all the iterators to after the term provided.
  VoteHistoryIteratorMap::iterator vhim_it = it_map->begin();
  while (vhim_it != it_map->end()) {
    VoteMap::const_iterator it = votes_.find(vhim_it->first);
    CHECK(it != votes_.end());
    const std::vector<PreviousVotePB>& pvh = it->second.previous_vote_history;
    vhim_it->second = std::upper_bound(
        vhim_it->second, pvh.end(), term, compareTerm_PreviousVotePB);
    if (vhim_it->second == pvh.end()) {
      vhim_it = it_map->erase(vhim_it);
    } else {
      vhim_it++;
    }
  }

  // Recreate the vote collation from scratch.
  // The collation is a map from (UUID, term) -> [region -> set(UUID)].
  vote_collation->clear();
  vhim_it = it_map->begin();
  while (vhim_it != it_map->end()) {
    const std::string& region = FindOrDie(uuid_to_region_, vhim_it->first);
    const UUIDTermPair utp = std::make_pair<>(
        vhim_it->second->candidate_uuid(),
        vhim_it->second->election_term());
    RegionToVoterSet& rtvs =
        LookupOrInsert(vote_collation, utp, RegionToVoterSet());
    std::set<std::string>& uuid_set =
        LookupOrInsert(&rtvs, region, std::set<std::string>());
    uuid_set.insert(vhim_it->first);

    // Update the min term.
    // Note: min_term should actually be max(min_term_per_region) but
    // we use min_term in the leader region only for simplicity. It does
    // not affect correctness.
    if (region == leader_region) {
      *min_term = std::min(*min_term, vhim_it->second->election_term());
    }
    vhim_it++;
  }
}

PotentialNextLeaderResponse FlexibleVoteCounter::GetPotentialNextLeader(
    const int64_t term, const std::string& leader_region) const {
  // Collate a mapping from UUID, term pair to an iterator in the
  // voting history.
  int64_t min_term;

  // Mapping from UUID term pair to a set of UUIDs that voted for it
  // grouped by their region.
  VoteHistoryCollation vote_collation;

  // Map from UUID to an iterator in its vote history.
  VoteHistoryIteratorMap it_map;
  ConstructRegionWiseVoteCollation(
      term, leader_region, &vote_collation, &it_map, &min_term);

  // Figure out the total number of voters, commit requirement and votes not
  // received so far.
  int total_voters_leader_region =
      FindOrDie(voter_distribution_, leader_region);
  int commit_requirement_leader_region =
      MajoritySize(total_voters_leader_region);
  int votes_not_received_leader_region =
      FetchVotesRemainingInRegion(leader_region);

  // If we haven't received enough votes from the leader region, there is no point
  // proceeding. We need to wait for more votes.
  if (votes_not_received_leader_region >= commit_requirement_leader_region) {
    return PotentialNextLeaderResponse(
        PotentialNextLeaderResponse::WAITING_FOR_MORE_VOTES);
  }

  // For each term greater than the term of the last known leader,
  // compute if some server could have won an election in that term. If not,
  // we consider the next available term from the voting histories and repeat
  // until all the history is exhausted.
  while (!vote_collation.empty()) {
    VoteHistoryCollation::iterator vhc_it = vote_collation.begin();
    for ( ; vhc_it != vote_collation.end(); vhc_it++) {
      if (vhc_it->first.second == min_term) {
        std::pair<bool, bool> quorum_satisfaction_info =
            FetchQuorumSatisfactionInfoFromVoteHistory(
                leader_region, vhc_it->second);
        if (quorum_satisfaction_info.first) {
          // Case 1: Some UUID, term pair has the requisite votes.
          // Return new potential leader UUID for this case.
          return PotentialNextLeaderResponse(
              PotentialNextLeaderResponse::POTENTIAL_NEXT_LEADER_DETECTED,
              vhc_it->first.first, min_term);
        } else if (quorum_satisfaction_info.second) {
          // This UUID, term pair could still have won. Need more data.
          return PotentialNextLeaderResponse(
              PotentialNextLeaderResponse::WAITING_FOR_MORE_VOTES);
        }
      }
    }

    // No UUID could have won an election in min_term, recompute vote
    // collations and min term.
    UpdateRegionWiseVoteCollation(
        min_term, leader_region, &vote_collation, &it_map, &min_term);
  }

  // Return victory for the current election term, if it satisfies the quorum.
  // Voting history suggests all intervening terms between the last known
  // leader's term and the current election's terms are defunct.
  return PotentialNextLeaderResponse(
      PotentialNextLeaderResponse::ALL_INTERMEDIATE_TERMS_DEFUNCT);
}

std::pair<bool, bool> FlexibleVoteCounter::GetQuorumState() const {
  // Step 0: If the quorum is not a function of the last leader's region,
  // return early.
  if (!depends_on_last_leader_) {
    return IsQuorumSatisfied(le_quorum_requirement_);
  }

  // Step 1: Check if pessimistic quorum is satisfied.
  std::pair<bool, bool> presult = IsQuorumSatisfied(pessimistic_le_quorum_);

  // Return pessimistic quorum result if the pessimistic quorum is satisfied or
  // if the pessimistic quorum cannot be satisfied and we depend on the
  // knowledge of the last leader without having it (eg. during bootstrap), we
  // should declare having lost the election or having insufficient votes to make
  // a decision.
  if (presult.first || last_known_leader_region_.empty()) {
    return presult;
  }

  // Step 2: Check if last known leader's quorum is satisfied and we directly
  // succeed term.
  if (election_term_ == last_known_leader_.election_term + 1) {
    CHECK(!last_known_leader_.uuid.empty());
    CHECK(!last_known_leader_region_.empty());
    return IsQuorumSatisfied(ResolveQuorum(last_known_leader_region_));
  }

  // Step 3: Find a possible successor at every stage or a determination that
  // there was no leader in this term.
  // Return possible successor, next term to consider.
  // Repeat step 3 until next term is the current election's term or the
  // quorum converges to pessimistic quorum.
  std::set<string> explored_leader_regions;

  std::string possible_leader_region = last_known_leader_region_;
  int64_t term_it = last_known_leader_.election_term;

  explored_leader_regions.insert(possible_leader_region);
  while (explored_leader_regions.size() < pessimistic_le_quorum_.size()) {
    PotentialNextLeaderResponse r =
        GetPotentialNextLeader(term_it, possible_leader_region);
    switch (r.status) {
      case PotentialNextLeaderResponse::POTENTIAL_NEXT_LEADER_DETECTED: {
        term_it = r.potential_leader_term;

        // Check next leader region to explore is within the list of regions
        // voters are present in for this replicaset.
        const std::map<std::string, std::string>::const_iterator
        uuid_to_region_it =
            uuid_to_region_.find(r.potential_leader_uuid);
        if (uuid_to_region_it == uuid_to_region_.end()) {
          // This should never happen, i.e. we are exploring a region which
          // is not in our configuration. In such a case, we return loss of
          // election.
          VLOG_WITH_PREFIX(1) << "Potential next leader: "
                              << r.potential_leader_uuid << " is not a part "
                              << "of the configuration.";
          return std::make_pair<>(false, false);
        } else {
          possible_leader_region = uuid_to_region_it->second;
        }
        explored_leader_regions.insert(possible_leader_region);
        VLOG_WITH_PREFIX(3)
            << "Potential next leader: "
            << r.potential_leader_uuid << " in region:  "
            << possible_leader_region
            << " detected with term: " << term_it;
        break;
      }
      case PotentialNextLeaderResponse::ALL_INTERMEDIATE_TERMS_DEFUNCT:
        VLOG_WITH_PREFIX(3)
            << "All intermediate terms since the last known leader: "
            << last_known_leader_.uuid << " in term: "
            << last_known_leader_.election_term << " are defunct. "
            << "Current election term: " << election_term_;
        return IsQuorumSatisfied(ResolveQuorum(possible_leader_region));
      case PotentialNextLeaderResponse::WAITING_FOR_MORE_VOTES:
      // TODO(ritwikyadav): Implement the case with only majority vote
      // as the requirement.
      default:
        VLOG_WITH_PREFIX(3)
            << "Waiting for more votes. Election result hasn't been "
            << "determined. Election term: " << election_term_;
        return std::make_pair<>(false, true);
    }
  }

  // Step 4: We have converged to the most pessimistic quorum which hasn't
  // been satisfied yet.
  return presult;
}

bool FlexibleVoteCounter::IsDecided() const {
  const std::pair<bool, bool> quorum_state = GetQuorumState();
  return quorum_state.first || !quorum_state.second;
}

Status FlexibleVoteCounter::GetDecision(ElectionVote* decision) const {
  const std::pair<bool, bool> quorum_state = GetQuorumState();
  if (quorum_state.first) {
    *decision = VOTE_GRANTED;
    return Status::OK();
  }
  if (!quorum_state.second) {
    *decision = VOTE_DENIED;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

std::string FlexibleVoteCounter::LogPrefix() const {
  return Substitute(
      "[Flexible Vote Counter] Election term: $0 "
      "Last known leader region: $1 ",
      election_term_, last_known_leader_region_);
}

///////////////////////////////////////////////////
// ElectionResult
///////////////////////////////////////////////////

ElectionResult::ElectionResult(VoteRequestPB vote_request, ElectionVote decision,
                               ConsensusTerm highest_voter_term, const std::string& message)
  : vote_request(std::move(vote_request)),
    decision(decision),
    highest_voter_term(highest_voter_term),
    message(message) {
  DCHECK(!message.empty());
}

///////////////////////////////////////////////////
// LeaderElection::VoterState
///////////////////////////////////////////////////

string LeaderElection::VoterState::PeerInfo() const {
  std::string info = peer_uuid;
  if (proxy) {
    strings::SubstituteAndAppend(&info, " ($0)", proxy->PeerName());
  }
  return info;
}

///////////////////////////////////////////////////
// LeaderElection
///////////////////////////////////////////////////

LeaderElection::LeaderElection(RaftConfigPB config,
                               PeerProxyFactory* proxy_factory,
                               VoteRequestPB request,
                               gscoped_ptr<VoteCounter> vote_counter,
                               MonoDelta timeout,
                               ElectionDecisionCallback decision_callback)
    : has_responded_(false),
      config_(std::move(config)),
      proxy_factory_(proxy_factory),
      request_(std::move(request)),
      vote_counter_(std::move(vote_counter)),
      timeout_(timeout),
      decision_callback_(std::move(decision_callback)),
      highest_voter_term_(0) {
}

LeaderElection::~LeaderElection() {
  std::lock_guard<Lock> guard(lock_);
  DCHECK(has_responded_); // We must always call the callback exactly once.
  STLDeleteValues(&voter_state_);
}

void LeaderElection::Run() {
  VLOG_WITH_PREFIX(1) << "Running leader election.";

  // Initialize voter state tracking.
  vector<string> other_voter_uuids;
  voter_state_.clear();
  for (const RaftPeerPB& peer : config_.peers()) {
    if (request_.candidate_uuid() == peer.permanent_uuid()) {
      DCHECK_EQ(peer.member_type(), RaftPeerPB::VOTER)
          << Substitute("non-voter member $0 tried to start an election; "
                        "Raft config {$1}",
                        peer.permanent_uuid(),
                        pb_util::SecureShortDebugString(config_));
      continue;
    }
    if (peer.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    other_voter_uuids.emplace_back(peer.permanent_uuid());

    gscoped_ptr<VoterState> state(new VoterState());
    state->peer_uuid = peer.permanent_uuid();
    state->proxy_status = proxy_factory_->NewProxy(peer, &state->proxy);
    InsertOrDie(&voter_state_, peer.permanent_uuid(), state.release());
  }

  // Ensure that the candidate has already voted for itself.
  CHECK_EQ(1, vote_counter_->GetTotalVotesCounted()) << "Candidate must vote for itself first";

  // Ensure that existing votes + future votes add up to the expected total.
  CHECK_EQ(vote_counter_->GetTotalVotesCounted() + other_voter_uuids.size(),
           vote_counter_->GetTotalExpectedVotes())
      << "Expected different number of voters. Voter UUIDs: ["
      << JoinStringsIterator(other_voter_uuids.begin(), other_voter_uuids.end(), ", ")
      << "]; RaftConfig: {" << pb_util::SecureShortDebugString(config_) << "}";

  // Check if we have already won the election (relevant if this is a
  // single-node configuration, since we always pre-vote for ourselves).
  CheckForDecision();

  // The rest of the code below is for a typical multi-node configuration.
  for (const auto& voter_uuid : other_voter_uuids) {
    VoterState* state = nullptr;
    {
      std::lock_guard<Lock> guard(lock_);
      state = FindOrDie(voter_state_, voter_uuid);
      // Safe to drop the lock because voter_state_ is not mutated outside of
      // the constructor / destructor. We do this to avoid deadlocks below.
    }

    // If we failed to construct the proxy, just record a 'NO' vote with the status
    // that indicates why it failed.
    if (!state->proxy_status.ok()) {
      LOG_WITH_PREFIX(WARNING) << "Was unable to construct an RPC proxy to peer "
                               << state->PeerInfo() << ": " << state->proxy_status.ToString()
                               << ". Counting it as a 'NO' vote.";
      {
        std::lock_guard<Lock> guard(lock_);
        RecordVoteUnlocked(*state, VOTE_DENIED);
      }
      CheckForDecision();
      continue;
    }

    // Send the RPC request.
    LOG_WITH_PREFIX(INFO) << "Requesting "
                          << (request_.is_pre_election() ? "pre-" : "")
                          << "vote from peer " << state->PeerInfo();
    state->rpc.set_timeout(timeout_);

    state->request = request_;
    state->request.set_dest_uuid(voter_uuid);

    state->proxy->RequestConsensusVoteAsync(
        &state->request,
        &state->response,
        &state->rpc,
        // We use gutil Bind() for the refcounting and boost::bind to adapt the
        // gutil Callback to a thunk.
        boost::bind(&Closure::Run,
                    Bind(&LeaderElection::VoteResponseRpcCallback, this, voter_uuid)));
  }
}

void LeaderElection::CheckForDecision() {
  bool to_respond = false;
  {
    std::lock_guard<Lock> guard(lock_);
    // Check if the vote has been newly decided.
    if (!result_ && vote_counter_->IsDecided()) {
      ElectionVote decision;
      CHECK_OK(vote_counter_->GetDecision(&decision));
      LOG_WITH_PREFIX(INFO) << "Election decided. Result: candidate "
                << ((decision == VOTE_GRANTED) ? "won." : "lost.");
      string msg = (decision == VOTE_GRANTED) ?
          "achieved majority votes" : "could not achieve majority";
      result_.reset(new ElectionResult(request_, decision, highest_voter_term_, msg));
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
      LOG_WITH_PREFIX(WARNING) << "RPC error from VoteRequest() call to peer "
                               << state->PeerInfo() << ": "
                               << state->rpc.status().ToString();
      RecordVoteUnlocked(*state, VOTE_DENIED);

    // Check for tablet errors.
    } else if (state->response.has_error()) {
#ifdef FB_DO_NOT_REMOVE
      LOG_WITH_PREFIX(WARNING) << "Tablet error from VoteRequest() call to peer "
                               << state->PeerInfo() << ": "
                               << StatusFromPB(state->response.error().status()).ToString();
#endif
      RecordVoteUnlocked(*state, VOTE_DENIED);

    // If the peer changed their IP address, we shouldn't count this vote since
    // our knowledge of the configuration is in an inconsistent state.
    } else if (PREDICT_FALSE(voter_uuid != state->response.responder_uuid())) {
      LOG_WITH_PREFIX(DFATAL) << "Received vote response from peer "
                              << state->PeerInfo() << ": "
                              << "we thought peer had UUID " << voter_uuid
                              << " but its actual UUID is "
                              << state->response.responder_uuid();
      RecordVoteUnlocked(*state, VOTE_DENIED);

    } else {
      // No error: count actual votes.
      highest_voter_term_ = std::max(highest_voter_term_, state->response.responder_term());
      if (state->response.vote_granted()) {
        HandleVoteGrantedUnlocked(*state);
      } else {
        HandleVoteDeniedUnlocked(*state);
      }
    }
  }

  // Check for a decision outside the lock.
  CheckForDecision();
}

void LeaderElection::RecordVoteUnlocked(
    const VoterState& state, ElectionVote vote) {
  DCHECK(lock_.is_locked());

  // Construct vote information struct.
  VoteInfo vote_info;
  vote_info.vote = vote;
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
  Status s = vote_counter_->RegisterVote(
      state.peer_uuid, vote_info, &duplicate);
  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Error registering vote for peer "
                             << state.PeerInfo() << ": " << s.ToString();
    return;
  }
  if (duplicate) {
    // Note: This is DFATAL because at the time of writing we do not support
    // retrying vote requests, so this should be impossible. It may be valid to
    // receive duplicate votes in the future if we implement retry.
    LOG_WITH_PREFIX(DFATAL) << "Duplicate vote received from peer " << state.PeerInfo();
  }
}

void LeaderElection::HandleHigherTermUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  DCHECK_GT(state.response.responder_term(), election_term());

  string msg = Substitute("Vote denied by peer $0 with higher term. Message: $1",
                          state.PeerInfo(),
                          StatusFromPB(state.response.consensus_error().status()).ToString());
  LOG_WITH_PREFIX(WARNING) << msg;

  if (!result_) {
    LOG_WITH_PREFIX(INFO) << "Cancelling election due to peer responding with higher term";
    result_.reset(new ElectionResult(request_, VOTE_DENIED,
                                     state.response.responder_term(), msg));
  }
}

void LeaderElection::HandleVoteGrantedUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  if (!request_.is_pre_election()) {
    DCHECK_EQ(state.response.responder_term(), election_term());
  }
  DCHECK(state.response.vote_granted());

  LOG_WITH_PREFIX(INFO) << "Vote granted by peer " << state.PeerInfo();
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

  LOG_WITH_PREFIX(INFO) << "Vote denied by peer " << state.PeerInfo() << ". Message: "
            << StatusFromPB(state.response.consensus_error().status()).ToString();
  RecordVoteUnlocked(state, VOTE_DENIED);
}

std::string LeaderElection::LogPrefix() const {
  return Substitute("T $0 P $1 [CANDIDATE]: Term $2 $3election: ",
                    request_.tablet_id(),
                    request_.candidate_uuid(),
                    request_.candidate_term(),
                    request_.is_pre_election() ? "pre-" : "");
}

} // namespace consensus
} // namespace kudu
