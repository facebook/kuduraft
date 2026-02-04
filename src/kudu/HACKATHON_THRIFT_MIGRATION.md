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
| T2.1 | Wrap consensus PBs with interface (protobuf as specialization) | NOT STARTED |
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

**Next Steps:**
- Begin T2.1: Wrap consensus PBs with interface (protobuf as specialization)
