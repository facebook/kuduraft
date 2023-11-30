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

// ===========================================================================
//  Consensus Metadata
// ===========================================================================

namespace cpp2 facebook.raft
namespace py3 facebook.py3

include "kudu/common/common.thrift"

typedef common.HostPort HostPort

// Per-replica attributes.
struct RaftPeerAttrs {
  // Whether to promote a replica when it has caught up with the leader,
  // changing its membership type from NON_VOTER to VOTER. This field is
  // applicable only for NON_VOTER replicas.
  1: optional bool promote;

  // If set to 'true', the replica needs to be replaced regardless of
  // its health report.
  2: optional bool replace;

  // Denotes if the server is backed by a database. If not, it only has the
  // logs.
  3: optional bool backing_db_present;

  // Geographic region of the raft server.
  4: optional string region;

  // The port on which the logtailer server for a BLS peer is running
  5: optional i32 logtailer_server_port;

  // The mysql/bls server id
  6: optional i32 server_id;

  // Define which quorum the peer belongs to in Flexiraft when using
  // QuorumType::QUORUM_ID
  7: optional string quorum_id;
}

// HealthStatus respresents a fully-connected state machine, where
// transitions between any of the states are allowed.
enum HealthStatus {
  // No information on the health status.
  UNKNOWN = 999,

  // Replica has failed and needs replacement. The failure might be a
  // transient one, so replica may return to a healthy state soon.
  FAILED = 0,

  // Replica is functioning properly.
  HEALTHY = 1,

  // Replica has failed in an irreversible and unrecoverable way and needs
  // replacement. The failure is permanent and the replica definitely cannot
  // return to a healthy state.
  FAILED_UNRECOVERABLE = 2,
}

// Report on a replica's (peer's) health.
struct HealthReport {
  // Overall health status of a replica. Reflects at least the responsiveness
  // (i.e. time of last contact) and the lag of the replica's WAL
  // behind the leader's.
  1: optional HealthStatus overall_health;
}

// The following structs `CommitRulePredicatePB`, `CommitRulePB` &
// `ReplicationTopologyPB` need to be kept in sync with the plugin.

enum QuorumMode {
  STATIC_DISJUNCTION = 0,
  STATIC_CONJUNCTION = 1,
  SINGLE_REGION_DYNAMIC = 2,
}

enum QuorumType {
  REGION = 0,
  QUORUM_ID = 1,
}

// Analogue of CommitRulePredicate on the plugin.
// Defines a single predicate in the static quorum modes.
// The predicates are of the form:
// 3 out of the 5 regions in {region1, region2, ... , region5}
// `regions` denotes the set {region1, region2, ... , region5}
// The subset size 3 is represented by `regions_subset_size`.
struct CommitRulePredicate {
  1: list<string> regions;
  2: i32 regions_subset_size;
}

// Struct specifying commit requirements. Analogue of CommitRule on the plugin.
struct CommitRule {
  1: QuorumMode mode;
  2: list<CommitRulePredicate> rule_predicates;
  // Use quorum_id instead of region for flexiraft quorum
  3: optional QuorumType quorum_type;
}

// Represents the arrangement of master and its slaves for a MySQL replica set.
// Borrowed from `dba/prod_config.thrift` with some amendments. Analogue of
// ReplicationTopology on the plugin.
struct ReplicationTopology {
  // master region
  // This is where MySQL Infra automation will prefer to have the master
  // located for this replica set. If a working replica exists in this region
  // and the master is not in this region, automation will (eventually)
  // do a promotion to this region.
  1: optional string master_region;

  // fallback regions
  // When there are no more working replicas in the preferred master region, a
  // replica from one of these regions will be selected (in order) as the new
  // master. If this is not provided and you run out of replicas in the master
  // region, your replica set may break.
  2: list<string> fallback_regions;

  // followers that exist per region
  // This should include the replicas for the master, fallback regions and
  // include LBU counts.
  3: map<string, i32> follower_distribution;
}

// Analogue of RaftTopology on the plugin.
struct RaftTopology {
  // Server properties of each member in the replicaset's raft ring.
  1: list<RaftPeer> raft_server_properties;

  // Replication topology of the replicaset
  2: optional ReplicationTopology replication_topology;
}

// Analogue of TopologyConfig on the plugin.
struct TopologyConfig {
  // Data commit rule defined for the replicaset.
  2: optional CommitRule commit_rule;

  // Master ban to be updated once the protocol is finalized.
  // optional bool master_ban_info = 3;
  // optional string master_ban_instance = 4;

  // Name of the replicaset being configured: eg. mysql.replicaset.10001
  5: optional string replicaset_name;

  // Server properties of the instance being configured.
  6: optional RaftPeer server_config;

  // Replicaset topology information.
  7: optional RaftTopology raft_topology;

  // Voters that exist per region.
  // This should include all voters including the LBUs.
  // Caution: This should NOT be modified by automation when
  // servers are being removed for maintenance.
  8: map<string, i32> voter_distribution;

  // The initial raft rpc token used to prove we're part of the ring.
  // This will be populated if we don't already have a rpc token from
  // persistent vars
  9: optional string initial_raft_rpc_token;
}

// The possible roles for peers.
enum Role {
  UNKNOWN_ROLE = 999,

  // Indicates this node is a follower in the configuration, i.e. that it participates
  // in majorities and accepts Consensus::Update() calls.
  FOLLOWER = 0,

  // Indicates this node is the current leader of the configuration, i.e. that it
  // participates in majorities and accepts Consensus::Append() calls.
  LEADER = 1,

  // Indicates that this node participates in the configuration in a passive role,
  // i.e. that it accepts Consensus::Update() calls but does not participate
  // in elections or majorities.
  LEARNER = 2,

  // Indicates that this node is not a participant of the configuration, i.e. does
  // not accept Consensus::Update() or Consensus::Update() and cannot
  // participate in elections or majorities. This is usually the role of a node
  // that leaves the configuration.
  NON_PARTICIPANT = 3,
}

enum MemberType {
  UNKNOWN = 999,
  NON_VOTER = 0,
  VOTER = 1,
}

// A peer in a configuration.
struct RaftPeer {
  // Permanent uuid is optional: RaftPeer/RaftConfig instances may
  // be created before the permanent uuid is known (e.g., when
  // manually specifying a configuration for Master/CatalogManager);
  // permanent uuid can be retrieved at a later time through RPC.
  1: optional binary permanent_uuid;
  2: optional MemberType member_type;
  3: optional HostPort last_known_addr;

  // Replica attributes.
  4: optional RaftPeerAttrs attrs;

  // Replica's health report, as seen by the leader. This is a run-time
  // only field, it should not be persisted or read from the persistent storage.
  5: optional HealthReport health_report;

  6: string hostname = "";
}

// A set of peers, serving a single tablet.
struct RaftConfig {
  // The index of the operation which serialized this RaftConfig through
  // consensus. It is set when the operation is consensus-committed (replicated
  // to a majority of voters) and before the consensus metadata is updated.
  // It is left undefined if the operation isn't committed.
  1: optional i64 opid_index;

  // Obsolete. This parameter has been retired.
  // 2: optional bool OBSOLETE_local

  // Flag to allow unsafe config change operations.
  2: bool unsafe_config_change = false;

  // The set of peers in the configuration.
  3: list<RaftPeer> peers;

  // Name of the replicaset.
  6: optional string replicaset_name;

  // Commit rules defined for FlexiRaft.
  7: optional CommitRule commit_rule;

  // Voters that exist per region.
  // This should include all voters including the LBUs.
  // Caution: This should NOT be modified by automation when
  // servers are being removed for maintenance.
  8: map<string, i32> voter_distribution;
}

// A single directed edge on the request proxying graph.
struct ProxyEdge {
  1: optional string peer_uuid; // Peer getting its requests proxied.
  2: optional string proxy_from_uuid; // Peer to proxy from.
}

// A directed graph representation of the proxy topology.
struct ProxyTopology {
  1: list<ProxyEdge> proxy_edges;
}

// Represents a snapshot of a configuration at a given moment in time.
struct ConsensusState {
  // A configuration is always guaranteed to have a known term.
  1: i64 current_term = -1;

  // There may not always be a leader of a configuration at any given time.
  // An empty string indicates the leader doesn't exist or no known.
  //
  // The node that the local peer considers to be leader changes based on rules
  // defined in the Raft specification. Roughly, this corresponds either to
  // being elected leader (in the case that the local peer is the leader), or
  // when an update is accepted from another node, which basically just amounts
  // to a term check on the UpdateConsensus() RPC request.
  //
  // Whenever the local peer sees a new term, the leader flag is cleared until
  // a new leader is acknowledged based on the above criteria. Simply casting a
  // vote for a peer is not sufficient to assume that the peer has won the
  // election, so we do not update this field based on our vote.
  //
  // The leader may be a part of the committed or the pending configuration (or both).
  2: optional string leader_uuid;

  // The committed peers. Initial peership is set on tablet start, so this
  // field should always be present.
  3: RaftConfig committed_config;

  // The peers in the pending configuration, if there is one.
  4: optional RaftConfig pending_config;
}

// This PB is used to serialize all of the persistent state needed for
// Consensus that is not in the WAL, such as leader election and
// communication on startup.
struct ConsensusMeta {
  // Last-committed peership.
  1: RaftConfig committed_config;

  // Latest term this server has seen.
  // When a configuration is first created, initialized to 0.
  //
  // Whenever a new election is started, the candidate increments this by one
  // and requests votes from peers.
  //
  // If any RPC request or response is received from another node containing a term higher
  // than this one, the server should step down to FOLLOWER and set its current_term to
  // match the caller's term.
  //
  // If a follower receives an UpdateConsensus RPC with a term lower than this
  // term, then that implies that the RPC is coming from a former LEADER who has
  // not realized yet that its term is over. In that case, we will reject the
  // UpdateConsensus() call with ConsensusErrorPB::INVALID_TERM.
  //
  // If a follower receives a RequestConsensusVote() RPC with an earlier term,
  // the vote is denied.
  2: i64 current_term;

  // Permanent UUID of the candidate voted for in 'current_term', or not present
  // if no vote was made in the current term.
  3: optional string voted_for;

  // Persistence of the following metadata is NOT strictly necessary
  // but is done as an optimization.

  // Last known leader of the server.
  9: optional LastKnownLeader last_known_leader;

  // Voting history of the server.
  10: optional i64 last_pruned_term;
  11: map<i64, PreviousVote> previous_vote_history;
}

// Information about previously granted vote.
struct PreviousVote {
  1: string candidate_uuid;
  2: i64 election_term;
}

// Information about the last known leader.
struct LastKnownLeader {
  1: string uuid;
  2: i64 election_term;
}
