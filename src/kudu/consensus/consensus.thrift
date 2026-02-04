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

package "facebook.com/raft"

namespace cpp2 facebook.raft
namespace py3 facebook.py3

include "kudu/common/wire_protocol.thrift"
include "kudu/consensus/metadata.thrift"
include "kudu/consensus/opid.thrift"
include "thrift/annotation/cpp.thrift"

typedef opid.OpId OpId
typedef wire_protocol.AppStatus AppStatus
typedef wire_protocol.NodeInstance NodeInstance
typedef metadata.RaftConfig RaftConfig
typedef metadata.RaftPeer RaftPeer
typedef metadata.ConsensusState ConsensusState
typedef metadata.PreviousVote PreviousVote
typedef metadata.LastKnownLeader LastKnownLeader
typedef metadata.ProxyTopology ProxyTopology
typedef metadata.StateMachineMetrics StateMachineMetrics
typedef metadata.ReplicaManagementInfo ReplicaManagementInfo

// ===========================================================================
//  Consensus Error Codes
// ===========================================================================

// The codes for consensus responses. These are set in the status when
// some consensus internal error occurs and require special handling
// by the caller. A generic error code is purposefully absent since
// generic errors should use ServerError.
enum ConsensusErrorCode {
  UNKNOWN = 0,

  // Invalid term.
  // Sent by peers in response to leader RPCs whenever the term
  // of one of the messages sent in a batch is lower than the
  // the term the peer is expecting.
  INVALID_TERM = 2,

  // For leader election.
  // The last OpId logged by the candidate is older than the last OpId logged
  // by the local peer.
  LAST_OPID_TOO_OLD = 3,

  // For leader election.
  // The local replica has already voted for another candidate in this term.
  ALREADY_VOTED = 4,

  // The replica does not recognize the caller's request as coming from a
  // member of the configuration.
  NOT_IN_QUORUM = 5,

  // The responder's last entry didn't match the caller's preceding entry.
  PRECEDING_ENTRY_DIDNT_MATCH = 6,

  // The local replica is either a leader, or has heard from a valid leader
  // more recently than the election timeout, so believes the leader to be
  // alive.
  LEADER_IS_ALIVE = 7,

  // The local replica is in the middle of servicing either another vote
  // or an update from a valid leader.
  CONSENSUS_BUSY = 8,

  // The local replica was unable to prepare a single transaction.
  CANNOT_PREPARE = 9,
}

// Consensus-specific errors use this struct
struct ConsensusError {
  // The error code.
  1: ConsensusErrorCode code;

  // The Status object for the error. This will include a textual
  // message that may be more useful to present in log messages, etc,
  // though its error code is less specific.
  2: AppStatus status;
}

// Tablet-server specific error codes
enum ServerErrorCode {
  // An error which has no more specific error code.
  UNKNOWN_ERROR = 1,

  // The provided configuration was not well-formed and/or
  // had a sequence number that was below the current config.
  INVALID_CONFIG = 9,

  // The consensus is hosted on this server, but not in RUNNING state.
  CONSENSUS_NOT_RUNNING = 12,

  // This tserver is not the leader of the consensus configuration.
  NOT_THE_LEADER = 15,

  // The destination UUID in the request does not match this server.
  WRONG_SERVER_UUID = 16,

  // The compare-and-swap specified by an atomic RPC operation failed.
  CAS_FAILED = 17,

  // The requested operation is already inprogress, e.g. TabletCopy.
  ALREADY_INPROGRESS = 18,

  // The provided raft_rpc_token does not match with the token of the server
  RING_TOKEN_MISMATCH = 19,

  // Client request error. For example, not providing a
  // mock_election_snapshot_op_id when requesting a Mock Election.
  INVALID_CLIENT_REQUEST = 20,

  // Service is not able to service the request due to thread pools either at
  // capacity or shutdown
  SERVICE_UNAVAILABLE = 21,

  // An attempt to start an election on a non-voter peer.
  NOT_VOTER = 22,

  // The proxying instance does not have the log entries for the proxy request
  PROXY_MISSING_LOG_ENTRIES = 23,
}

// Tablet-server specific errors use this struct.
struct ServerError {
  // The error code.
  1: ServerErrorCode code = ServerErrorCode.UNKNOWN_ERROR;

  // The Status object for the error. This will include a textual
  // message that may be more useful to present in log messages, etc,
  // though its error code is less specific.
  2: AppStatus status;
}

// ===========================================================================
//  External Consensus Messages
// ===========================================================================

// The types of operations that need a commit message, i.e. those that require
// at least one round of the consensus algorithm.
enum OperationType {
  UNKNOWN_OP = 0,
  NO_OP = 1,
  // These are higher level than RAFT
  // WRITE_OP = 3;
  // ALTER_SCHEMA_OP = 4;
  CHANGE_CONFIG_OP = 5,
  WRITE_OP_EXT = 6,
  PROXY_OP = 7,
  ROTATE_OP = 8,
}

// A configuration change request for the tablet with 'tablet_id'.
// This message is dynamically generated by the leader when AddServer() or
// RemoveServer() is called, and is what gets replicated to the log.
struct ChangeConfigRecord {
  // The old committed configuration config for verification purposes.
  1: RaftConfig old_config;

  // The new configuration to set the configuration to.
  2: RaftConfig new_config;
}

enum ChangeConfigType {
  UNKNOWN_CHANGE = 0,
  ADD_PEER = 1,
  REMOVE_PEER = 2,
  MODIFY_PEER = 3,
}

enum CompressionType {
  DEFAULT_COMPRESSION = 0,
  NO_COMPRESSION = 1,
  SNAPPY = 2,
  LZ4 = 3,
  ZLIB = 4,
  UNKNOWN_COMPRESSION = 999,
}

struct ConfigExternalVersion {
  // CAS: compare input current_version with existing version in Raft
  // config. Reject the config change if two do not match
  1: optional i64 current_version;

  // Set Raft config external version. It has to be larger than current
  // Raft config external version. Otherwise config change is rejected.
  2: optional i64 next_version;

  // Allow next version to be smaller than current external version.
  // This should be rarely used.
  3: optional bool backdoor_allow_arbitrary_next_version;
}

// Payload for replicate message (for write requests)
struct WritePayload {
  1: optional binary payload;

  // Compression codec used to compress payload
  2: CompressionType compression_codec = CompressionType.NO_COMPRESSION;

  // Uncompressed size of payload. Should be present when
  // compression_codec != NO_COMPRESSION
  3: optional i64 uncompressed_size;

  // crc32 checksum of the payload. If the payload is compressed, then the
  // checksum is computed _after_ compression
  4: i32 crc32 = 0;
}

// A Replicate message, sent to replicas by leader to indicate this operation
// must be stored in the WAL/SM log, as part of the first phase of the two phase
// commit.
struct ReplicateMsg {
  // The Raft operation ID (term and index) being replicated.
  1: OpId id;
  // The (hybrid or logical) timestamp assigned to this message.
  2: i64 timestamp;
  3: OperationType op_type;

  4: optional ChangeConfigRecord change_config_record;

  // The payload for a write request (present if op_type=WRITE_OP_EXT)
  7: optional WritePayload write_payload;
}

// A commit message for a previous operation.
// This is a commit in the consensus sense and may abort/commit any operation
// that required a consensus round.
struct CommitMsg {
  1: OperationType op_type;
  // the id of the message this commit pertains to
  2: optional OpId commited_op_id;
}

// ===========================================================================
//  Internal Consensus Messages and State
// ===========================================================================

// Status message received in the peer responses.
struct ConsensusStatus {
  // The last message received (and replicated) by the peer.
  1: OpId last_received;

  // The id of the last op that was replicated by the current leader.
  2: optional OpId last_received_current_leader;

  // The last committed index that is known to the peer.
  3: optional i64 last_committed_idx;

  // When the last request failed for some consensus related (internal) reason.
  4: optional ConsensusError error;
}

// The candidate populates this field and sends it along with the RequestVote
// RPC. Current usage is mainly for logging to improve debugging leader
// elections
struct CandidateContext {
  // Candidate peer information
  1: optional RaftPeer candidate_peer_pb;
}

enum ElectionMode {
  UNKNOWN_ELECTION_MODE = 0,

  // A normal leader election. Peers will not vote for this node
  // if they believe that a leader is alive.
  NORMAL_ELECTION = 1,

  // A "pre-election". Peers will vote as they would for a normal
  // election, except that the votes will not be "binding". In other
  // words, they will not durably record their vote.
  PRE_ELECTION = 2,

  // In this mode, peers will vote for this candidate even if they
  // think a leader is alive. This can be used for a faster hand-off
  // between a leader and one of its replicas.
  ELECT_EVEN_IF_LEADER_IS_ALIVE = 3,

  // Similar to a pre-election, where votes are not durably recorded. The
  // difference is that a specific snapshot op id is passed in as consensus
  // state for a candidate to determine whether a node is caught up enough to be
  // a leader. The motivation for introducing this mode is that we want to have
  // confidence that leadership can be transferred before we give up leadership
  // and go into a read-only mode, which can incur some downtime.
  MOCK_ELECTION = 4,
}

// A request from a candidate peer that wishes to become leader of
// the configuration serving tablet with 'tablet_id'.
// See RAFT sec. 5.2.
struct VoteRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  // Identifies the tablet configuration a the vote is being requested for.
  2: string tablet_id;

  // The uuid of the sending peer.
  3: string candidate_uuid;

  // The term we are requesting a vote for.
  4: i64 candidate_term;

  // The candidate node status so that the voter node can
  // decide whether to vote for it as LEADER.
  5: ConsensusStatus candidate_status;

  // Additional candidate context that is passed by the candidate
  8: optional CandidateContext candidate_context;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  9: optional string raft_rpc_token;

  10: ElectionMode mode = ElectionMode.UNKNOWN_ELECTION_MODE;

  // See RunLeaderElectionRequest.mock_election_snapshot_op_id for definition.
  // Must be set if mode is MOCK_ELECTION. Value is ignored in any other mode.
  11: optional OpId mock_election_snapshot_op_id;
}

// Additional context that a voter sends back in the response to RequestVote()
// rpc
struct VoterContext {
  // Candidate was removed from the voter's committed config and is currently
  // tracked in the voter's 'removed_peers_' list. This is used by the candidate
  // to perform aggressive backoffs
  1: bool is_candidate_removed = false;
}

// A response from a replica to a leader election request.
struct VoteResponse {
  // The uuid of the node sending the reply.
  1: optional string responder_uuid;

  // The term of the node sending the reply.
  // Allows the candidate to update itself if it is behind.
  2: optional i64 responder_term;

  // True if this peer voted for the caller, false otherwise.
  3: optional bool vote_granted;

  // Previously granted votes by this server.
  4: list<PreviousVote> previous_vote_history;

  // The greatest term that has been pruned from previous_vote_history.
  5: optional i64 last_pruned_term;

  // Last known leader as per the responding voter.
  6: optional LastKnownLeader last_known_leader;

  // Additional context sent back by the voter
  7: optional VoterContext voter_context;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  8: optional string raft_rpc_token;

  // Error message from the consensus implementation.
  9: optional ConsensusError consensus_error;

  // A generic error message (such as tablet not found).
  10: optional ServerError error;
}

// A consensus request struct, the basic unit of a consensus round.
struct ConsensusRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  // UUID of server that will proxy this request to the eventual 'dest_uuid'.
  // Must be set if this request is intended to be proxied.
  2: optional string proxy_dest_uuid;

  3: string tablet_id;

  // UUID of the leader peer making the call.
  4: string caller_uuid;

  // UUID of server proxying this request.
  // Must be set if this request was proxied on behalf of the leader.
  5: optional string proxy_caller_uuid;

  // Hop count / TTL field for proxy requests.
  6: optional i32 proxy_hops_remaining;

  // The caller's term. As only leaders can send messages,
  // replicas will accept all messages as long as the term
  // is equal to or higher than the last term they know about.
  7: i64 caller_term;

  // The id of the operation immediately preceding the first
  // operation in 'ops'. If the replica is receiving 'ops' for
  // the first time 'preceding_id' must match the replica's
  // last operation.
  8: optional OpId preceding_id;

  // The index of the last committed operation in the configuration.
  // Raft calls this field 'leaderCommit'.
  9: optional i64 committed_index;

  // Sequence of operations to be replicated by this peer.
  11: list<ReplicateMsg> ops;

  // The highest index that is known to be replicated by all members of
  // the configuration.
  12: optional i64 all_replicated_index;

  // The safe timestamp on the leader.
  13: optional i64 safe_timestamp;

  // The index of the most recent operation appended to the leader.
  14: optional i64 last_idx_appended_to_leader;

  // The index that is deemed to have been 'region-durable'.
  15: optional i64 region_durable_index;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  16: optional string raft_rpc_token;

  // Dictionary to use for decompression when dictionary compression is used
  17: optional string compression_dictionary;

  // Leader requesting the lease duration for Followers to ACK on
  18: optional i32 requested_lease_duration;
}

struct ConsensusResponse {
  // The uuid of the peer making the response.
  1: optional string responder_uuid;

  // The current term of the peer making the response.
  2: optional i64 responder_term;

  // The current consensus status of the receiver peer.
  3: optional ConsensusStatus status;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  4: optional string raft_rpc_token;

  // True if the follower had accepted the lease renewal
  5: optional bool lease_granted;

  // Time of the server processes this request. Can be used to calculate
  // rtt time between leader and follower.
  6: i64 server_process_time_us = 0;

  7: optional StateMachineMetrics state_machine_metrics;

  // A generic error message (such as tablet not found).
  8: optional ServerError error;
}

struct GetNodeInstanceRequest {}

struct GetNodeInstanceResponse {
  1: NodeInstance node_instance;
}

struct LeaderElectionContext {
  // Time when the original server was promoted away from. We can use this to
  // measure the total time of a chain of promotions
  // Should be specified as nanoseconds since epoch
  1: i64 original_start_time;

  // UUID of original server that was promoted away from, used when current
  // server cannot be leader
  2: string original_uuid;

  // True if the original promotion was due to the original leader specified in
  // original_uuid being dead/unreachable
  3: bool is_origin_dead_promotion = false;
}

// Message that makes the local peer run leader election to be elected leader.
// Assumes that a tablet with 'tablet_id' exists.
struct RunLeaderElectionRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  // the id of the tablet
  2: string tablet_id;

  3: optional LeaderElectionContext election_context;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  4: optional string raft_rpc_token;

  // Whether to return response after a decision has been made. When set true,
  // RunLeaderElectionResponse.vote_granted will be populated.
  5: bool wait_for_decision = false;

  // Snapshot op id taken on the leader. Used in the mock election to compare
  // how ahead or behind a voter is to a candidate. If this is set, we assume
  // that the election is a mock election.
  6: optional OpId mock_election_snapshot_op_id;
}

struct RunLeaderElectionResponse {
  // A generic error message (such as tablet not found).
  1: optional ServerError error;

  // Whether request's dest_uuid was elected as leader. Is only populated if
  // RunLeaderElectionRequest.wait_for_decision is set to true.
  2: optional bool election_won;
}

enum LeaderStepDownMode {
  // The leader will immediately step down.
  ABRUPT = 1,
  // The leader will attempt to arrange for a successor to be elected ASAP.
  // If it cannot do so, it remains leader.
  GRACEFUL = 2,
}

struct LeaderStepDownRequest {
  // UUID of the server this request is addressed to.
  1: optional string dest_uuid;

  // The id of the tablet.
  2: string tablet_id;

  // How the leader will attempt to relinquish its leadership.
  3: optional LeaderStepDownMode mode;

  // The UUID of the peer that should be promoted to leader in GRACEFUL mode.
  // If unset, the leader will select a successor.
  // In ABRUPT mode, it is illegal to set this field.
  4: optional string new_leader_uuid;
}

struct LeaderStepDownResponse {
  1: optional ServerError error;
}

enum OpIdType {
  UNKNOWN_OPID_TYPE = 0,
  RECEIVED_OPID = 1,
  COMMITTED_OPID = 2,
}

struct GetLastOpIdRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  // the id of the tablet
  2: string tablet_id;

  // Whether to return the last-received or last-committed OpId.
  3: OpIdType opid_type = OpIdType.RECEIVED_OPID;
}

struct GetLastOpIdResponse {
  1: optional OpId opid;
  // A generic error message (such as tablet not found).
  2: optional ServerError error;
}

enum IncludeHealthReport {
  UNSPECIFIED_HEALTH_REPORT = 0,
  EXCLUDE_HEALTH_REPORT = 1,
  INCLUDE_HEALTH_REPORT = 2,
}

struct GetConsensusStateRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  // The ids of the tablets.
  // An empty list means return info for all tablets known to the tablet server.
  2: list<string> tablet_ids;

  // Include a health report inline in the consensus state PB if
  // 'report_health' is set to INCLUDE_HEALTH_REPORT. Even in that case, only
  // the leader replica will return a health report for the members of the
  // config.
  3: IncludeHealthReport report_health = IncludeHealthReport.UNSPECIFIED_HEALTH_REPORT;
}

struct TabletConsensusInfo {
  1: string tablet_id;
  2: optional ConsensusState cstate;
}

struct GetConsensusStateResponse {
  1: list<TabletConsensusInfo> tablets;

  2: optional ReplicaManagementInfo replica_management_info;

  3: optional ServerError error;
}

enum JointConsensusPhase {
  START_JOINT_CONSENSUS = 1, // Transition from C_old => C_old_new
  FINISH_JOINT_CONSENSUS = 2, // Transition from C_old_new => C_new
  ROLLBACK_JOINT_CONSENSUS = 3, // Transition back from C_old_new => C_old
}

struct JointConsensusConfigChangeRequest {
  1: string tablet_id;
  2: list<RaftPeer> new_peers;
}

struct JointConsensusConfigChangeResponse {
  1: optional ServerError error;
}

struct ChangeProxyTopologyRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;
  2: optional string tablet_id;

  // Sender identification, it could be a static string as well.
  3: optional string caller_id;

  // The new proxy topology to use.
  4: optional ProxyTopology new_config;
}

struct ChangeProxyTopologyResponse {
  1: optional ServerError error;
}

// ===========================================================================
//  Consensus Service
// ===========================================================================

// A Raft implementation.
service ConsensusService {
  // Analogous to AppendEntries in Raft, but only used for followers.
  // This is the main replication RPC.
  @cpp.ProcessInEbThreadUnsafe
  ConsensusResponse UpdateConsensus(1: ConsensusRequest req);

  // RequestVote() from Raft.
  @cpp.ProcessInEbThreadUnsafe
  VoteResponse RequestConsensusVote(1: VoteRequest req);

  // Change the routing graph that defines how requests are proxied.
  ChangeProxyTopologyResponse ChangeProxyTopology(
    1: ChangeProxyTopologyRequest req,
  );

  GetNodeInstanceResponse GetNodeInstance(1: GetNodeInstanceRequest req);

  // Force this node to run a leader election.
  RunLeaderElectionResponse RunLeaderElection(1: RunLeaderElectionRequest req);

  // Force this node to step down as leader.
  LeaderStepDownResponse LeaderStepDown(1: LeaderStepDownRequest req);

  GetLastOpIdResponse GetLastOpId(1: GetLastOpIdRequest req);

  // Returns the consensus state for a set of tablets.
  // Does not return information for tombstoned tablets.
  GetConsensusStateResponse GetConsensusState(1: GetConsensusStateRequest req);
}
