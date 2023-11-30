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

namespace cpp2 facebook.raft
namespace py3 facebook.py3

include "kudu/common/wire_protocol.thrift"
include "kudu/consensus/metadata.thrift"
include "kudu/consensus/opid.thrift"
include "thrift/annotation/cpp.thrift"

typedef opid.OpId OpId
typedef wire_protocol.AppStatus AppStatus
typedef metadata.RaftConfig RaftConfig
typedef metadata.RaftPeer RaftPeer
typedef metadata.PreviousVote PreviousVote
typedef metadata.LastKnownLeader LastKnownLeader

// The codes for consensus responses. These are set in the status when
// some consensus internal error occurs and require special handling
// by the caller. A generic error code is purposefully absent since
// generic errors should use ServerError.
enum ErrorCode {
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
  1: ErrorCode code;

  // The Status object for the error. This will include a textual
  // message that may be more useful to present in log messages, etc,
  // though its error code is less specific.
  2: AppStatus status;
}

enum Code {
  // An error which has no more specific error code.
  // The code and message in 'status' may reveal more details.
  //
  // RPCs should avoid returning this, since callers will not be
  // able to easily parse the error.
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
}

// Tablet-server specific errors use this protobuf.
struct ServerError {
  // The error code.
  1: Code code = UNKNOWN_ERROR;

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

  3: string tablet_id;
}

struct ProxyRecord {
  // The destination server intended to receive this message.
  1: optional string dest_server;
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

// A configuration change request for the tablet with 'tablet_id'.
// These requests are restricted to one-by-one operations, as specified in
// Diego Ongaro's Raft PhD thesis.
// This is the RPC request, but it does not end up in the log.
// See also ChangeConfigRecordPB.
struct ChangeConfigRequest {
  // UUID of server this request is addressed to.
  1: optional string dest_uuid;

  2: string tablet_id;

  // The type of config change requested.
  // This field must be specified, but is left as optional due to being an enum.
  3: ChangeConfigType type = UNKNOWN_CHANGE;

  // The peer to add or remove.
  // When 'type' == ADD_PEER, both the permanent_uuid and last_known_addr
  // fields must be set. Otherwise, only the permanent_uuid field is required.
  4: optional RaftPeer server;

  // The OpId index of the committed config to replace.
  // This optional parameter is here to provide an atomic (compare-and-swap)
  // ChangeConfig operation. The ChangeConfig() operation will fail if this
  // parameter is specified and the committed config does not have a matching
  // opid_index. See also the definition of RaftConfigPB.
  5: optional i64 cas_config_opid_index;
}

// The configuration change response. If any immediate error occurred
// the 'error' field is set with it, otherwise 'new_configuration' is set.
struct ChangeConfigResponse {
  1: optional ServerError error;

  // Updated configuration after changing the config.
  2: optional RaftPeer new_config;

  // The timestamp chosen by the server for this change config operation.
  // TODO: At the time of writing, this field is never set in the response.
  // TODO: Propagate signed timestamps. See KUDU-611.
  3: optional i64 timestamp;
}

// Payload for replicate message (for write requests)
struct WritePayload {
  1: optional string payload;

  // Compression codec used to compress payload
  2: optional CompressionType compression_codec;

  // Uncompressed size of payload. Should be present when
  // compression_codec != NO_COMPRESSION
  3: optional i64 uncompressed_size;

  // crc32 checksum of the payload. If the payload is compressed, then the
  // checksum is computed _after_ compression
  4: optional i64 crc32;
}

// A Replicate message, sent to replicas by leader to indicate this operation must
// be stored in the WAL/SM log, as part of the first phase of the two phase
// commit.
struct ReplicateMsg {
  // The Raft operation ID (term and index) being replicated.
  1: OpId id;
  // The (hybrid or logical) timestamp assigned to this message.
  2: i64 timestamp;
  // optional ExternalConsistencyMode external_consistency_mode = 3 [default = NO_CONSISTENCY];
  3: OperationType op_type;

  4: optional ChangeConfigRecord change_config_record;
  5: optional ProxyRecord proxy_record;

  // The payload for a write request (present if op_type=WRITE_OP_EXT)
  7: optional WritePayload write_payload;

  8: optional NoOpRequest noop_request;
// TODO: jaganmaddukuri: Corresponding request-id in thrift
// The client's request id for this message, if it is set.
// 9: optional rpc.RequestId request_id;
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

// NO_OP requests are replicated by a peer after being elected leader.
struct NoOpRequest {
  // Allows to set a dummy payload, for tests.
  1: optional string payload_for_tests;

  // Set to true if the op id for this request is expected to be monotonically
  // increasing with the assigned timestamp. For no-ops that are sent by a
  // leader marking a successful Raft election, this is true. If not set, it is
  // assumed to be true.
  2: optional bool timestamp_in_opid_order;
}

// Status message received in the peer responses.
struct ConsensusStatus {
  // The last message received (and replicated) by the peer.
  1: OpId last_received;

  // The id of the last op that was replicated by the current leader.
  // This doesn't necessarily mean that the term of this op equals the current
  // term, since the current leader may be replicating ops from a prior term.
  // Unset if none currently received.
  //
  // In the case where there is a log matching property error
  // (PRECEDING_ENTRY_DIDNT_MATCH), this field is important and may still be
  // set, since the leader queue uses this field in conjunction with
  // last_received to decide on the next id to send to the follower.
  //
  // NOTE: it might seem that the leader itself could track this based on knowing
  // which batches were successfully sent. However, the follower is free to
  // truncate the batch if an operation in the middle of the batch fails
  // to prepare (eg due to memory limiting). In that case, the leader
  // will get a success response but still need to re-send some operations.
  2: optional OpId last_received_current_leader;

  // The last committed index that is known to the peer.
  3: optional i64 last_committed_idx;

  // When the last request failed for some consensus related (internal) reason.
  // In some cases the error will have a specific code that the caller will
  // have to handle in certain ways.
  4: optional ConsensusError error;
}

// The candidate populates this field and sends it along with the RequestVote
// RPC.Current usage is mainly for logging to improve debugging leader
// elections
struct CandidateContext {
  // Candidate peer information
  1: optional RaftPeer candidate_peer_pb;
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
  // If this term is higher than the callee's term, the callee will update its
  // own term to match, and if it is the current leader it will step down.
  4: i64 candidate_term;

  // The candidate node status so that the voter node can
  // decide whether to vote for it as LEADER.
  //
  // In particular, this includes the last OpId persisted in the candidate's
  // log, which corresponds to the lastLogIndex and lastLogTerm fields in Raft.
  // A replica must vote no for a candidate that has an OpId lower than them.
  5: ConsensusStatus candidate_status;

  // Normally, replicas will deny a vote with a LEADER_IS_ALIVE error if
  // they are a leader or recently heard from a leader. This is to prevent
  // partitioned nodes from disturbing liveness. If this flag is true,
  // peers will vote even if they think a leader is alive. This can be used
  // for example to force a faster leader hand-off rather than waiting for
  // the election timer to expire.
  6: optional bool ignore_live_leader;

  // In a "pre-election", voters should respond how they _would_ have voted
  // but not actually record the vote.
  7: optional bool is_pre_election;

  // Additional candidate context that is passed by the candidate
  8: optional CandidateContext candidate_context;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  9: optional string raft_rpc_token;
}

// Additional context that a voter sends back in the response to RequestVote()
// rpc
struct VoterContext {
  // Candidate was removed from the voter's committed config and is currently
  // tracked in the voter's 'removed_peers_' list. This is used by the candidate
  // to perform aggressive backoffs
  1: optional bool is_candidate_removed;
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

  // TODO: Migrate ConsensusService to the AppStatusPB RPC style and merge these errors.
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
  // If a leader receives a request with a term higher than its own,
  // it will step down and enter FOLLOWER state (see Raft sec. 5.1).
  7: i64 caller_term;

  // The id of the operation immediately preceding the first
  // operation in 'ops'. If the replica is receiving 'ops' for
  // the first time 'preceding_id' must match the replica's
  // last operation.
  //
  // This must be set if 'ops' is non-empty.
  8: optional OpId preceding_id;

  // The index of the last committed operation in the configuration. This is the
  // index of the last operation the leader deemed committed from a consensus
  // standpoint (not the last operation the leader applied).
  //
  // Raft calls this field 'leaderCommit'.
  9: optional i64 committed_index;

  // Deprecated field used in Kudu 0.10.0 and earlier. Remains here to prevent
  // accidental reuse and provide a nicer error message if the user attempts
  // a rolling upgrade.
  10: optional OpId DEPRECATED_committed_index;

  // Sequence of operations to be replicated by this peer.
  // These will be committed when committed_index advances above their
  // respective OpIds. In some cases committed_index can indicate that
  // these operations are already committed, in which case they will be
  // committed during the same request.
  11: list<ReplicateMsg> ops;

  // The highest index that is known to be replicated by all members of
  // the configuration.
  //
  // NOTE: this is not necessarily monotonically increasing. For example, if a node is in
  // the process of being added to the configuration but has not yet copied a snapshot,
  // this value may drop to 0.
  12: optional i64 all_replicated_index;

  // The index of the most recent operation appended to the leader.
  // Followers can use this to determine roughly how far behind they are from the leader.
  13: optional i64 last_idx_appended_to_leader;

  // The index that is deemed to have been 'region-durable'. Region durability
  // is currently defined as the OpId that is replicated to atleast one
  // additional region (other than the leader's region). This is sent by the
  // leader to all followers and they sync their queue state with this index.
  14: optional i64 region_durable_index;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  15: optional string raft_rpc_token;

  // The safe timestamp on the leader.
  // This is only set if the leader has no messages to send to the peer or if the last sent
  // message is already (raft) committed. By setting this the leader allows followers to advance
  // the "safe time" past the timestamp of the last committed message and answer snapshot scans
  // in the present in the absence of writes.
  16: optional i64 safe_timestamp;
}

struct ConsensusResponse {
  // The uuid of the peer making the response.
  1: optional string responder_uuid;

  // The current term of the peer making the response.
  // This is used to update the caller (and make it step down if it is
  // out of date).
  2: optional i64 responder_term;

  // The current consensus status of the receiver peer.
  3: optional ConsensusStatus status;

  // A token stamped to the request to prove to the remote host that we're part
  // of a ring
  4: optional string raft_rpc_token;

  // A generic error message (such as tablet not found), per operation
  // error messages are sent along with the consensus status.
  5: optional ServerError error;
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
}

struct RunLeaderElectionResponse {
  // A generic error message (such as tablet not found).
  1: optional ServerError error;
}

// A Raft implementation.
service ConsensusService {
  // Only used for followers.
  @cpp.ProcessInEbThreadUnsafe
  ConsensusResponse AppendEntries(1: ConsensusRequest req);

  // RequestVote() from Raft.
  @cpp.ProcessInEbThreadUnsafe
  VoteResponse RequestVote(1: VoteRequest req);

  // Force this node to run a leader election.
  RunLeaderElectionResponse RunLeaderElection(1: RunLeaderElectionRequest req);
}
