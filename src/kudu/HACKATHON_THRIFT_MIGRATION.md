# Kuduraft Protobuf to Thrift Migration

This document tracks progress on the hackathon project to migrate kuduraft from its custom protobuf-based RPC to Facebook Thrift.

## Background

kuduraft (located at `fbcode/kudu`) is a consensus library that runs the Raft protocol. It currently uses a home-rolled RPC server with Google protobuf serialization. This creates maintenance burden and doesn't fit with Meta's thrift ecosystem.

## Standing Rules

1. Track all changes in this markdown for context resumption
2. Branch-based development - no need for dual protobuf/thrift support during dev
3. Incremental commits - one task (or chunk) at a time, Meta commit style
4. Bookmark before each commit for human-readable history
5. Amend + rebase when changes touch prior commits

## Testing Requirements

- Each commit must build: `buck build //mysql_raft/... //kudu/... //dbproxy/...`
- Sanity tests:
  - `buck test //mysql_raft/tests:RingTest -- RaftRingTestSuite/RaftRingTest.BringUpAndShutdown1`
  - `buck run //mysql/server:mtr_test -- rpl_raft.basic`

## Task Status

| Task | Description | Status |
|------|-------------|--------|
| T0 | Reference existing thrift handler, then delete and restart | COMPLETE |
| T1.1 | Create thrift file for consensus.proto | COMPLETE |
| T1.2 | Create thrift files for remaining metadata protos | COMPLETE |
| T2.1 | Wrap consensus PBs with interface (protobuf as specialization) | IN PROGRESS |
| T2.2 | Implement thrift specialization of the interface | NOT STARTED |
| T3 | Replace current direct protobuf calls with wrappers | NOT STARTED |
| T3.1 | Start with consensus module (RPC services in protobuf, higher levels use wrappers) | NOT STARTED |
| T3.2 | Do the same for metadata objects (file format serialization) | NOT STARTED |
| T4 | Implement thrift server with ServiceFrameworkLight at rpl_service.h level | NOT STARTED |
| T4.1 | LongUpdateConsensusLoading if possible (no-op fallback) | NOT STARTED |
| T5 | Raw thrift client (NO service router), reconstruct on errors | NOT STARTED |
| T5.1 | Integrate thrift client into RpcPeerProxy | NOT STARTED |
| T6 | (Optional) Delete protobuf stack entirely | NOT STARTED |

## Commit History

1. **T1.1**: Complete consensus.thrift to match consensus.proto
   - Added all missing enums: ElectionMode, OpIdType, IncludeHealthReport, JointConsensusPhase, LeaderStepDownMode
   - Added all missing structs: ConfigExternalVersion, ChangeProxyTopology, GetNodeInstance, LeaderStepDown, GetLastOpId, GetConsensusState, etc.
   - Added service methods: ChangeConfig, ChangeProxyTopology, GetNodeInstance, LeaderStepDown, GetLastOpId, GetConsensusState
   - Updated metadata.thrift with: StateMachineMetrics, ReplicaManagementInfo, RegionGroup, RaftConfig.external_version, RaftConfig.next_config_peers
   - **Dropped deprecated fields**: VoteRequest.ignore_live_leader, VoteRequest.is_pre_election, ConsensusRequest.DEPRECATED_committed_index
   - **Dropped unused features**: BulkChangeConfig (ConfigChangeItem, BulkChangeConfigRequest structs and service method), UnsafeChangeConfig (UnsafeChangeConfigRequest, UnsafeChangeConfigResponse structs and service method) - these will not be implemented in the thrift server

2. **T1.2**: Create thrift files for remaining metadata protos
   - Created persistent_vars.thrift with: PersistentVars struct
   - Added BUCK targets: persistent_vars_thrift

3. **T2.1 - Basic types**: OpId and AppStatus wrapper types
   - Created wrapper design with two interface classes per type:
     - `TypeView` (interface): provides getters and setters
     - `Type` (TypeView): mostly empty but denotes data is owned
   - Created PB implementations in separate subdirectory:
     - `TypePbView`: holds mutable reference to protobuf, implements TypeView
     - `TypePb`: owns protobuf, implements Type
   - Interfaces in `kudu::consensus::types` namespace (to avoid collision with PB-generated classes)
   - PB implementations in `kudu/consensus/types/pb/` subdirectory
   - **OpId wrapper** (`kudu/consensus/types/`):
     - `opid_view.h` - OpIdView interface (term, index)
     - `opid.h` - OpId owning interface
     - `pb/opid_pb.h/cc` - OpIdPb and OpIdPbView implementations
   - **AppStatus wrapper** (`kudu/common/types/`):
     - `app_status_view.h` - AppStatusView interface + AppStatusCode enum
     - `app_status.h` - AppStatus owning interface
     - `pb/app_status_pb.h/cc` - AppStatusPb and AppStatusPbView implementations

4. **T2.1 - Error and status types**: ConsensusError, ServerError, ConsensusStatus wrapper types
   - **ConsensusError wrapper** (`kudu/consensus/types/`):
     - `consensus_error_view.h` - ConsensusErrorView interface with code() and status()
     - `consensus_error.h` - ConsensusError owning interface
     - ConsensusErrorCode enum mirroring protobuf values
     - `pb/consensus_error_pb.h/cc` - ConsensusErrorPb, ConsensusErrorPbView implementations
     - Demonstrates nested type access: status() returns unique_ptr<AppStatusView>
   - **ServerError wrapper** (`kudu/consensus/types/`):
     - `server_error_view.h` - ServerErrorView interface with code() and status()
     - `server_error.h` - ServerError owning interface
     - ServerErrorCode enum mirroring protobuf values
     - `pb/server_error_pb.h/cc` - ServerErrorPb, ServerErrorPbView implementations
   - **ConsensusStatus wrapper** (`kudu/consensus/types/`):
     - `consensus_status_view.h` - ConsensusStatusView with last_received, last_received_current_leader, last_committed_idx, error
     - `consensus_status.h` - ConsensusStatus owning interface
     - `pb/consensus_status_pb.h/cc` - ConsensusStatusPb, ConsensusStatusPbView implementations
     - Uses nested OpIdView and ConsensusErrorView

5. **T2.1 - Vote RPC types**: VoteRequest and VoteResponse wrapper types
   - **VoteRequest wrapper** (`kudu/consensus/types/`):
     - `vote_request_view.h` - VoteRequestView interface with dest_uuid, tablet_id, candidate_uuid, candidate_term, candidate_status, mode, candidate_context
     - `vote_request.h` - VoteRequest owning interface
     - ElectionMode enum: NORMAL_ELECTION, PRE_ELECTION, ELECT_EVEN_IF_LEADER_IS_ALIVE, MOCK_ELECTION
     - `pb/vote_request_pb.h/cc` - VoteRequestPb, VoteRequestPbView implementations
     - Uses nested OpIdView, ConsensusStatusView, and CandidateContextView
   - **CandidateContext wrapper** (`kudu/consensus/types/`):
     - `candidate_context_view.h` - CandidateContextView minimal interface (has_candidate_peer only)
     - `candidate_context.h` - CandidateContext owning interface
     - `pb/candidate_context_pb.h/cc` - CandidateContextPb, CandidateContextPbView implementations
     - Provides raw `candidate_peer_pb()` accessor until RaftPeer wrapper is available (defined later in nested types commit)
   - **VoteResponse wrapper** (`kudu/consensus/types/`):
     - `vote_response_view.h` - VoteResponseView interface with responder_uuid, responder_term, vote_granted, consensus_error, error
     - `vote_response.h` - VoteResponse owning interface
     - `pb/vote_response_pb.h/cc` - VoteResponsePb, VoteResponsePbView implementations
   - **VoteResponse additional types** (for complete voting history support):
     - `previous_vote.h` - PreviousVote simple value type (candidate_uuid, election_term)
     - `last_known_leader.h` - LastKnownLeader simple value type (uuid, election_term)
     - `voter_context_view.h` / `voter_context.h` - VoterContext interface (is_candidate_removed)
     - `pb/voter_context_pb.h/cc` - VoterContextPb, VoterContextPbView implementations
   - **VoteResponseView complete fields**:
     - `previous_vote_history()` / `add_previous_vote()` - voting history list
     - `last_pruned_term()` / `set_last_pruned_term()` - pruned term tracking
     - `last_known_leader()` / `set_last_known_leader()` - leader tracking
     - `voter_context()` - voter context access (mutable nested)

**Next Steps:**
- Continue T2.1: Wrap remaining consensus PBs (ConsensusRequestPB, ConsensusResponsePB)
