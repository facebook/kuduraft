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

#include "kudu/tserver/simple_tablet_manager.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/clock/clock.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/persistent_vars.h"
#include "kudu/consensus/persistent_vars_manager.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

DECLARE_bool(enable_flexi_raft);

using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::ConsensusOptions;
using consensus::ConsensusRound;
using consensus::ConsensusStatePB;
using consensus::ITimeManager;
using consensus::PeerProxyFactory;
using consensus::PersistentVars;
using consensus::PersistentVarsManager;
using consensus::RaftConfigPB;
using consensus::RaftConsensus;
using consensus::RaftPeerPB;
using consensus::RpcPeerProxyFactory;
using consensus::TimeManager;
using consensus::TimeManagerDummy;
using log::Log;
using log::LogOptions;
using pb_util::SecureDebugString;
using pb_util::SecureShortDebugString;

namespace tserver {

/*static*/ Status TabletManagerIf::CreateConfigFromTserverAddresses(
    const TabletServerOptions& options,
    KC::RaftConfigPB* newConfig) {
  size_t tsIndex = 0;
  // Build the set of followers from our server options.
  for (const HostPort& hostPort : options.tserverAddresses) {
    KC::RaftPeerPB peer;
    HostPortPB peerHostPortPb;
    RETURN_NOT_OK(hostPortToPb(hostPort, &peerHostPortPb));
    peer.mutable_last_known_addr()->CopyFrom(peerHostPortPb);
    peer.set_member_type(RaftPeerPB::VOTER);

    // applications are allowed to not populate bbd
    if (!options.tserverBbd.empty()) {
      peer.mutable_attrs()->set_backing_db_present(options.tserverBbd[tsIndex]);
    }

    // applications are allowed to not populate region, but
    // region specific features like commit rules and LEADER bans
    // will not work in that case
    if (!options.tserverRegions.empty()) {
      peer.mutable_attrs()->set_region(options.tserverRegions[tsIndex]);
    }
    newConfig->add_peers()->CopyFrom(peer);
    tsIndex++;
  }
  return Status::OK();
}

/*static*/ void TabletManagerIf::CreateConfigFromBootstrapPeers(
    const TabletServerOptions& options,
    KC::RaftConfigPB* newConfig) {
  for (const RaftPeerPB& peer : options.bootstrapTservers) {
    newConfig->add_peers()->CopyFrom(peer);
  }
}

const std::string TSTabletManager::kSysCatalogTabletId(
    "00000000000000000000000000000000");

TSTabletManager::TSTabletManager(TabletServer* server)
    : fs_manager_(server->fsManager()),
      cmeta_manager_(std::make_shared<ConsensusMetadataManager>(fs_manager_)),
      persistent_vars_manager_(
          std::make_shared<PersistentVarsManager>(fs_manager_)),
      server_(server),
      metric_registry_(server->metricRegistry()),
      state_(MANAGER_INITIALIZING),
      mark_dirty_clbk_(
          Bind(&TSTabletManager::MarkTabletDirty, Unretained(this))) {}

TSTabletManager::~TSTabletManager() {
  // Close cannot be called from the destructor any more.
  // as Close from Log::~Log will call the base class Close()
  // Another way to think about it is that Init and Close go in
  // pairs. If Init is called virtual, Close should also be
  if (log_) {
    WARN_NOT_OK(log_->Close(), "Error closing Log");
  }
}

Status TSTabletManager::Load(FsManager* /* fs_manager */) {
  if (server_->opts().isDistributed()) {
    LOG(INFO) << "Verifying existing consensus state";
    std::shared_ptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(
        cmeta_manager_->loadCMeta(kSysCatalogTabletId, &cmeta),
        "Unable to load consensus metadata for tablet " + kSysCatalogTabletId);
    ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
    RETURN_NOT_OK(consensus::verifyRaftConfig(cstate.committed_config()));
    CHECK(!cstate.has_pending_config());

    // Make sure the set of masters passed in at start time matches the set in
    // the on-disk cmeta.
    set<string> peerAddrsFromOpts;
    for (const auto& hp : server_->opts().tserverAddresses) {
      peerAddrsFromOpts.insert(hp.ToString());
    }
    if (peerAddrsFromOpts.size() < server_->opts().tserverAddresses.size()) {
      LOG(WARNING) << fmt::format(
          "Found duplicates in --tserver_addresses: "
          "the unique set of addresses is {}",
          JoinStrings(peerAddrsFromOpts, ", "));
    }
    set<string> peerAddrsFromDisk;
    for (const auto& p : cstate.committed_config().peers()) {
      HostPort hp;
      RETURN_NOT_OK(hostPortFromPb(p.last_known_addr(), &hp));
      peerAddrsFromDisk.insert(hp.ToString());
    }
    vector<string> symmDiff;
    std::set_symmetric_difference(
        peerAddrsFromOpts.begin(),
        peerAddrsFromOpts.end(),
        peerAddrsFromDisk.begin(),
        peerAddrsFromDisk.end(),
        std::back_inserter(symmDiff));
    if (!symmDiff.empty()) {
      string msg = fmt::format(
          "on-disk master list ({}) and provided master list ({}) differ. "
          "Their symmetric difference is: {}",
          JoinStrings(peerAddrsFromDisk, ", "),
          JoinStrings(peerAddrsFromOpts, ", "),
          JoinStrings(symmDiff, ", "));
      return Status::InvalidArgument(msg);
    }
  }

  return SetupRaft();
}

Status TSTabletManager::CreateNew(FsManager* fs_manager) {
  RaftConfigPB config;
  if (server_->opts().isDistributed()) {
    LOG(INFO) << "TSTabletManager::CreateNew - Calling CreateDistributedConfig";
    RETURN_NOT_OK_PREPEND(
        CreateDistributedConfig(server_->opts(), &config),
        "Failed to create new distributed Raft config");
  } else {
    LOG(INFO)
        << "TSTabletManager::CreateNew - Setting up single peer local config";
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(fs_manager->uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

  RETURN_NOT_OK_PREPEND(
      cmeta_manager_->createCMeta(
          kSysCatalogTabletId, config, consensus::kMinimumTerm),
      "Unable to persist consensus metadata for tablet " + kSysCatalogTabletId);
  // TODO(mpercy): Provide a way to specify the proxy graph at tablet creation
  // time. For now, we initialize with an empty proxy graph.
  RETURN_NOT_OK_PREPEND(
      cmeta_manager_->createDrt(kSysCatalogTabletId, config, {}),
      "Unable to create new durable routing table for tablet " +
          kSysCatalogTabletId);
  // Note that we are intentionally not creating Persistent Vars here because we
  // do it in SetupRaft() anyway if the file does not exist

  return SetupRaft();
}

Status TSTabletManager::CreateDistributedConfig(
    const TabletServerOptions& options,
    RaftConfigPB* committedConfig) {
  DCHECK(options.isDistributed());

  RaftConfigPB newConfig;
  newConfig.set_opid_index(consensus::kInvalidOpIdIndex);

  // WARN if both are set. Not failing it now, because
  // during the rollout phase, we might be setting both by
  // mistake.
  if (!options.tserverAddresses.empty() && !options.bootstrapTservers.empty()) {
    LOG(WARNING)
        << "Both tserver_addresses and bootstrap_tservers is"
           " being passed during bootstrap. This can create unexpected bahavior."
           " Move to boostrap_tservers as it is more capable.";
  }

  // Give first priority to options.tserverAddresses
  // Over time applications will stop setting this and
  // pass in list of peers. Applications are expected to
  // not use both modes, till we remove support for tserverAddresses
  if (!options.tserverAddresses.empty()) {
    RETURN_NOT_OK(
        TabletManagerIf::CreateConfigFromTserverAddresses(options, &newConfig));
  } else {
    TabletManagerIf::CreateConfigFromBootstrapPeers(options, &newConfig);
  }

  // Now resolve UUIDs.
  // By the time a SysCatalogTable is created and initted, the masters should be
  // starting up, so this should be fine to do.
  DCHECK(server_->messenger());
  RaftConfigPB resolvedConfig = newConfig;
  resolvedConfig.clear_peers();
  for (const RaftPeerPB& peer : newConfig.peers()) {
    if (peer.has_permanent_uuid()) {
      resolvedConfig.add_peers()->CopyFrom(peer);
    } else {
      LOG(INFO) << SecureShortDebugString(peer)
                << " has no permanent_uuid. Determining permanent_uuid...";
      RaftPeerPB newPeer = peer;
      RETURN_NOT_OK_PREPEND(
          consensus::SetPermanentUuidForRemotePeer(
              server_->messenger(), &newPeer),
          fmt::format(
              "Unable to resolve UUID for peer {}",
              SecureShortDebugString(peer)));
      resolvedConfig.add_peers()->CopyFrom(newPeer);
    }
  }

  if (FLAGS_enable_flexi_raft) {
    DCHECK(options.topologyConfig.has_commit_rule());
    resolvedConfig.mutable_commit_rule()->CopyFrom(
        options.topologyConfig.commit_rule());
    resolvedConfig.mutable_voter_distribution()->insert(
        options.topologyConfig.voter_distribution().begin(),
        options.topologyConfig.voter_distribution().end());
  }

  RETURN_NOT_OK(consensus::verifyRaftConfig(resolvedConfig));
  VLOG(1) << "Distributed Raft configuration: "
          << SecureShortDebugString(resolvedConfig);

  *committedConfig = resolvedConfig;
  return Status::OK();
}

Status TSTabletManager::WaitUntilConsensusRunning(const MonoDelta& timeout) {
  MonoTime start(MonoTime::Now());

  int backoffExp = 0;
  const int kMaxBackoffExp = 8;
  while (true) {
    if (consensus_ && consensus_->isRunning()) {
      break;
    }
    MonoTime now(MonoTime::Now());
    MonoDelta elapsed(now - start);
    if (elapsed > timeout) {
      return Status::TimedOut(
          fmt::format(
              "Raft Consensus is not running after waiting for {}:",
              elapsed.ToString()));
    }
    SleepFor(MonoDelta::FromMilliseconds(1L << backoffExp));
    backoffExp = std::min(backoffExp + 1, kMaxBackoffExp);
  }
  return Status::OK();
}

Status TSTabletManager::WaitUntilRunning() {
  TRACE_EVENT0("master", "SysCatalogTable::WaitUntilRunning");
  int secondsWaited = 0;
  while (true) {
    Status status = WaitUntilConsensusRunning(MonoDelta::FromSeconds(1));
    secondsWaited++;
    if (status.ok()) {
      LOG_WITH_PREFIX(INFO)
          << "configured and running, proceeding with master startup.";
      break;
    }
    if (status.IsTimedOut()) {
      LOG_WITH_PREFIX(INFO) << "not online yet (have been trying for "
                            << secondsWaited << " seconds)";
      continue;
    }
    // if the status is not OK or TimedOut return it.
    return status;
  }
  return Status::OK();
}

bool TSTabletManager::IsInitialized() const {
  return state() == MANAGER_INITIALIZED;
}

bool TSTabletManager::isRunning() const {
  return state() == MANAGER_RUNNING;
}

Status TSTabletManager::Init(bool isFirstRun) {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  if (isFirstRun) {
    LOG(INFO)
        << "TSTabletManager::Init: is_first_run detected. Calling CreateNew";
    RETURN_NOT_OK_PREPEND(
        CreateNew(server_->fsManager()),
        "Failed to CreateNew in TabletManager");
  } else {
    LOG(INFO) << "TSTabletManager::Init: existing cmeta dir. Calling Load";
    RETURN_NOT_OK_PREPEND(
        Load(server_->fsManager()), "Failed to Load in TabletManager");
  }

  set_state(MANAGER_INITIALIZED);
  return Status::OK();
}

Status TSTabletManager::Start(bool isFirstRun) {
  CHECK_EQ(state(), MANAGER_INITIALIZED);

  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  std::shared_ptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->loadCMeta(kSysCatalogTabletId, &cmeta);

  std::shared_ptr<PersistentVars> persistent_vars;
  s = persistent_vars_manager_->loadPersistentVars(
      kSysCatalogTabletId, &persistent_vars);

  // We have already captured the ConsensusBootstrapInfo in SetupRaft
  // and saved it locally.
  // consensus::ConsensusBootstrapInfo bootstrap_info;

  TRACE("Starting consensus");
  VLOG(2) << "T " << kSysCatalogTabletId << " P " << consensus_->peer_uuid()
          << ": Peer starting";
  VLOG(2) << "RaftConfig before starting: "
          << SecureDebugString(consensus_->CommittedConfig());

  unique_ptr<PeerProxyFactory> peerProxyFactory;
  std::shared_ptr<ITimeManager> timeManager;

  peerProxyFactory.reset(
      new RpcPeerProxyFactory(server_->messenger(), server_->metricEntity()));

  if (server_->opts().enableTimeManager) {
    // THIS IS OBVIOUSLY NOT CORRECT.
    // ONLY TO MAKE CODE COMPILE [ Anirban ]
    timeManager = std::shared_ptr<ITimeManager>(new TimeManager(
        server_->clock()->shared_from_this(), Timestamp::kInitialTimestamp));
    // timeManager.reset(new TimeManager(server_->clock(),
    // tablet_->mvcc_manager()->GetCleanTimestamp()));
  } else {
    timeManager = std::shared_ptr<ITimeManager>(new TimeManagerDummy());
  }

  ConsensusRoundHandler* roundHandler = this;
  // If round handler comes from server options then override it
  if (server_->opts().roundHandler) {
    roundHandler = server_->opts().roundHandler;
  }

  // We cannot hold 'lock_' while we call RaftConsensus::Start() because it
  // may invoke TabletReplica::StartFollowerTransaction() during startup,
  // causing a self-deadlock. We take a ref to members protected by 'lock_'
  // before unlocking.
  std::shared_ptr<consensus::ConsensusBootstrapInfo> bootstrapInfo =
      log_->getRecoveryInfo();
  RETURN_NOT_OK(consensus_->start(
      bootstrapInfo,
      std::move(peerProxyFactory),
      log_,
      std::move(timeManager),
      roundHandler,
      server_->metricEntity(),
      mark_dirty_clbk_));

  log_->ClearOrphanedReplicates();

  RETURN_NOT_OK_PREPEND(
      WaitUntilRunning(), "Failed waiting for the raft to run");

  set_state(MANAGER_RUNNING);
  return Status::OK();
}

Status TSTabletManager::SetupRaft() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  InitLocalRaftPeerPB();

  // If the persistent vars file does not already exist, create one
  if (!persistent_vars_manager_->persistentVarsFileExists(
          kSysCatalogTabletId)) {
    LOG(INFO) << "Persistent Vars file does not exist for tablet "
              << kSysCatalogTabletId << ". Creating a new one";
    RETURN_NOT_OK_PREPEND(
        persistent_vars_manager_->createPersistentVars(kSysCatalogTabletId),
        "Unable to create persistent vars file for tablet " +
            kSysCatalogTabletId);
  }

  ConsensusOptions options;
  options.tablet_id = kSysCatalogTabletId;
  options.proxy_policy = server_->opts().proxyPolicy;
  options.proxy_region_groups = server_->opts().proxyRegionGroups;
  if (server_->opts().topologyConfig.has_initial_raft_rpc_token()) {
    options.initial_raft_rpc_token =
        server_->opts().topologyConfig.initial_raft_rpc_token();
  }

  shared_ptr<RaftConsensus> consensus;
  TRACE("Creating consensus");
  LOG(INFO) << LogPrefix(kSysCatalogTabletId)
            << "Creating Raft for the system tablet";
  RETURN_NOT_OK(
      RaftConsensus::Create(
          std::move(options),
          local_peer_pb_,
          cmeta_manager_,
          persistent_vars_manager_,
          server_->raftPool(),
          &consensus));
  consensus_ = std::move(consensus);
  if (server_->opts().edcb) {
    consensus_->SetElectionDecisionCallback(server_->opts().edcb);
  }
  if (server_->opts().tacb) {
    consensus_->SetTermAdvancementCallback(server_->opts().tacb);
  }
  if (server_->opts().norcb) {
    consensus_->SetNoOpReceivedCallback(server_->opts().norcb);
  }
  if (server_->opts().ldcb) {
    consensus_->SetLeaderDetectedCallback(server_->opts().ldcb);
  }
  if (server_->opts().disableNoop) {
    consensus_->disableNoOpEntries();
  }
  if (server_->opts().voteLogger) {
    consensus_->SetVoteLogger(server_->opts().voteLogger);
  }
  if (server_->opts().stateMachineMetrics) {
    consensus_->SetStateMachineMetrics(server_->opts().stateMachineMetrics);
  }

  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  // Not sure these 2 lines are required
  std::shared_ptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->loadCMeta(kSysCatalogTabletId, &cmeta);

  // Open the log, while passing in the factory class.
  // Factory could be empty.
  LogOptions logOptions;
  logOptions.logFactory = server_->opts().logFactory;
  Status s1 = Log::Open(
      logOptions,
      fs_manager_,
      kSysCatalogTabletId,
      server_->metricEntity(),
      &log_);

  if (!s1.ok()) {
    LOG(ERROR) << "Failed to open log: " << s1.ToString();
    return s1;
  }

  // Abstracted logs will do their own log recovery
  // during Log::Open->Log::Init (virtual call). bootstrap_info
  // is populated during that step. Capture it so as to pass it
  // to RaftConsensus::Start, in TSTabletManager::Start
  //
  // Skip recovery on "is_first_run" because you are creating a
  // fresh raft instance (the raft metadata directories are new).
  // This would be the equivalent of what kudu has because is_first_run
  // also implies that wal directory is empty in kuduraft.
  //
  // However, for the MySQL case, we allow logs to be copied from a previous
  // instance while this instance is still new (is_first_run) and
  // going to be added to the ring. In that mode, the consensus-metadata files
  // are not copied from the previous instance (this might change in the
  // future). The cmeta is actually built from the options parameters. Using :
  // 1. Term = Term of the last binlog opid term
  // 2. Config opid index, the index of last configuration passed in by
  // bootstrapper.
  // 3. Servers are passed in by options->bootstrap_servers/topology config
  // Since the default term is 0, we need to adjust the term of such
  // an instance to the term of the Last Logged OpId.
  // In the MySQL first_run case, MySQL is expected to pass in
  // logBootstrapOnFirstRun in options.
  if (server_->opts().logFactory &&
      (!server_->is_first_run_ || server_->opts().logBootstrapOnFirstRun)) {
    std::shared_ptr<consensus::ConsensusBootstrapInfo> bootstrapInfo =
        log_->getRecoveryInfo();
    if (bootstrapInfo &&
        bootstrapInfo->last_id.term() > consensus_->CurrentTerm()) {
      consensus_->SetCurrentTermBootstrap(bootstrapInfo->last_id.term());
    }
  }
  return s1;
}

void TSTabletManager::Shutdown() {
  {
    std::lock_guard lock(lock_);
    switch (state_) {
      case MANAGER_QUIESCING: {
        VLOG(1) << "Tablet manager shut down already in progress..";
        return;
      }
      case MANAGER_SHUTDOWN: {
        VLOG(1) << "Tablet manager has already been shut down.";
        return;
      }
      case MANAGER_INITIALIZING:
      case MANAGER_INITIALIZED:
      case MANAGER_RUNNING: {
        LOG(INFO) << "Shutting down tablet manager...";
        state_ = MANAGER_QUIESCING;
        break;
      }
      default: {
        LOG(FATAL) << "Invalid state: " << TSTabletManagerStatePB_Name(state_);
      }
    }
  }

  if (consensus_) {
    consensus_->Shutdown();
  }

  state_ = MANAGER_SHUTDOWN;
}

const NodeInstancePB& TSTabletManager::NodeInstance() const {
  return server_->instancePb();
}

void TSTabletManager::InitLocalRaftPeerPB() {
  DCHECK_EQ(state(), MANAGER_INITIALIZING);
  local_peer_pb_.set_permanent_uuid(fs_manager_->uuid());
  Sockaddr addr = server_->firstRpcAddress();
  HostPort hp;
  CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  CHECK_OK(hostPortToPb(hp, local_peer_pb_.mutable_last_known_addr()));

  // We will make this the default soon, Flexi-raft needs regions
  // attr. We assumed that on plugin side, topologyConfig->server_config
  // is well formed. We use it directly here.
  if (FLAGS_enable_flexi_raft &&
      server_->opts().topologyConfig.has_server_config()) {
    local_peer_pb_ = server_->opts().topologyConfig.server_config();
  }
}

string TSTabletManager::LogPrefix(
    const string& tabletId,
    FsManager* fsManager) {
  DCHECK(fsManager != nullptr);
  return fmt::format("T {} P {}: ", tabletId, fsManager->uuid());
}

string TSTabletManager::LogPrefix() const {
  return LogPrefix(kSysCatalogTabletId);
}

Status TSTabletManager::startConsensusOnlyRound(
    const std::shared_ptr<consensus::ConsensusRound>& /* round */) {
  // this is currently a no-op but other implementations
  // can provide their own version
  return Status::OK();
}

Status TSTabletManager::startFollowerTransaction(
    const std::shared_ptr<ConsensusRound>& round) {
  // THIS IS CURRENTLY A NO-OP
  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  DCHECK(replicate_msg->has_timestamp());
  return Status::OK();
}

void TSTabletManager::finishConsensusOnlyRound(ConsensusRound* round) {
  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  consensus::OperationType op_type = replicate_msg->op_type();
  (void)op_type;
  (void)replicate_msg;
}

bool TSTabletManager::isLeaderEligible() const {
  // Currently no-op
  return true;
}
} // namespace tserver
} // namespace kudu
