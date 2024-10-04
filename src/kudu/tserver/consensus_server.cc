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

#include "kudu/tserver/consensus_server.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <optional>

#include "kudu/clock/clock.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.service.h"
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
#include "kudu/consensus/persistent_vars.pb.h"
#include "kudu/consensus/persistent_vars_manager.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/tserver/consensus_service.h"
#include "kudu/tserver/simple_tablet_manager.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DECLARE_bool(enable_flexi_raft);

using kudu::rpc::ServiceIf;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::ConsensusOptions;
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

RaftConsensusServer::RaftConsensusServer(const ConsensusServerOptions& opts)
    : RaftConsensusServerIf(
          "RaftConsensusServer",
          opts,
          "kudu.consensus_server"),
      initted_(false),
      started_(false),
      opts_(opts),
      consensus_manager_(new RaftConsensusManager(this)) {}

RaftConsensusServer::~RaftConsensusServer() {
  Shutdown();
}

string RaftConsensusServer::ToString() const {
  // TODO: include port numbers, etc.
  return "RaftConsensusServer";
}

Status RaftConsensusServer::Init() {
  if (initted_) {
    LOG(INFO) << "Already initialized, skipping";
    return Status::OK();
  }

  LOG(INFO) << "Initializing RaftConsensusServer";

  // This pool will be used to wait for Raft
  RETURN_NOT_OK(
      ThreadPoolBuilder("init").set_max_threads(1).Build(&init_pool_));

  RETURN_NOT_OK(KuduServer::Init());

  std::unique_ptr<ServiceIf> consensus_service(
      new ConsensusServiceImpl(this, *consensus_manager_));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(KuduServer::Start());

  if (consensus_manager_->IsInitialized()) {
    return Status::IllegalState("Consensus manager is already initialized");
  }

  RETURN_NOT_OK_PREPEND(
      consensus_manager_->Init(is_first_run_),
      "Unable to initialize consensus manager");

  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  initted_ = true;
  return Status::OK();
}

Status RaftConsensusServer::Start() {
  CHECK(initted_);

  if (started_) {
    LOG(INFO) << "Already started, skipping";
    return Status::OK();
  }

  LOG(INFO) << "Starting RaftConsensusServer";

  if (!consensus_manager_->IsInitialized()) {
    return Status::IllegalState("Consensus manager is not initialized");
  }

  RETURN_NOT_OK_PREPEND(
      consensus_manager_->Start(is_first_run_),
      "Unable to start consensus manager");
  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  started_ = true;
  return Status::OK();
}

void RaftConsensusServer::Shutdown() {
  LOG(INFO) << "Shutting down RaftConsensusServer";
  if (!initted_) {
    return;
  }

  const string name = ToString();
  LOG(INFO) << name << " shutting down...";

  // 1. Stop accepting new RPCs.
  UnregisterAllServices();

  // 2. Stop consensus
  consensus_manager_->Shutdown();

  // 3. Shut down generic subsystems.
  KuduServer::Shutdown();
  LOG(INFO) << name << " shutdown complete.";
  initted_ = false;
}

RaftConsensusInstance::RaftConsensusInstance(
    const std::string& id,
    RaftConsensusServer* server,
    scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
    scoped_refptr<consensus::PersistentVarsManager> persistent_vars_manager)
    : id_(id),
      state_(MANAGER_INITIALIZING),
      server_(server),
      fs_manager_(server->fs_manager()),
      cmeta_manager_(cmeta_manager),
      persistent_vars_manager_(persistent_vars_manager) {}

RaftConsensusInstance::~RaftConsensusInstance() {
  // Close cannot be called from the destructor any more.
  // as Close from Log::~Log will call the base class Close()
  // Another way to think about it is that Init and Close go in
  // pairs. If Init is called virtual, Close should also be
  if (log_) {
    WARN_NOT_OK(log_->Close(), "Error closing Log");
  }
}

Status RaftConsensusInstance::Init(bool is_first_run) {
  LOG_WITH_PREFIX(INFO) << "Initializing RaftConsensusInstance";
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  if (is_first_run) {
    LOG_WITH_PREFIX(INFO)
        << "RaftConsensusInstance::Init: is_first_run detected. Calling CreateNew";
    RETURN_NOT_OK_PREPEND(
        CreateNew(server_->fs_manager()),
        "Failed to CreateNew in TabletManager");
  } else {
    LOG_WITH_PREFIX(INFO)
        << "RaftConsensusInstance::Init: existing cmeta dir. Calling Load";
    RETURN_NOT_OK_PREPEND(
        Load(server_->fs_manager()), "Failed to Load in TabletManager");
  }

  set_state(MANAGER_INITIALIZED);
  return Status::OK();
}

Status RaftConsensusInstance::Start(bool /*is_first_run*/) {
  LOG_WITH_PREFIX(INFO) << "Starting RaftConsensusInstance";
  CHECK_EQ(state(), MANAGER_INITIALIZED);

  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->LoadCMeta(id_, &cmeta);

  scoped_refptr<PersistentVars> persistent_vars;
  s = persistent_vars_manager_->LoadPersistentVars(id_, &persistent_vars);

  // We have already captured the ConsensusBootstrapInfo in SetupRaft
  // and saved it locally.
  // consensus::ConsensusBootstrapInfo bootstrap_info;

  TRACE("Starting consensus");
  VLOG_WITH_PREFIX(2) << "T " << id_ << " P " << consensus_->peer_uuid()
                      << ": Peer starting";
  VLOG_WITH_PREFIX(2) << "RaftConfig before starting: "
                      << SecureDebugString(consensus_->CommittedConfig());

  std::unique_ptr<PeerProxyFactory> peer_proxy_factory;
  scoped_refptr<ITimeManager> time_manager;

  peer_proxy_factory.reset(
      new RpcPeerProxyFactory(server_->messenger(), server_->metric_entity()));

  if (server_->opts(id_).enable_time_manager) {
    // THIS IS OBVIOUSLY NOT CORRECT.
    // ONLY TO MAKE CODE COMPILE [ Anirban ]
    time_manager.reset(
        new TimeManager(server_->clock(), Timestamp::kInitialTimestamp));
    // time_manager.reset(new TimeManager(server_->clock(),
    // tablet_->mvcc_manager()->GetCleanTimestamp()));
  } else {
    time_manager.reset(new TimeManagerDummy());
  }

  consensus::ConsensusRoundHandler* round_handler = nullptr;
  // If round handler comes from server options then override it
  if (server_->opts(id_).round_handler) {
    round_handler = server_->opts(id_).round_handler;
  }

  // We cannot hold 'lock_' while we call RaftConsensus::Start() because it
  // may invoke TabletReplica::StartFollowerTransaction() during startup,
  // causing a self-deadlock. We take a ref to members protected by 'lock_'
  // before unlocking.
  auto bootstrap_info = log_->GetRecoveryInfo();
  RETURN_NOT_OK(consensus_->Start(
      bootstrap_info,
      std::move(peer_proxy_factory),
      log_,
      std::move(time_manager),
      round_handler,
      server_->metric_entity(),
      Bind(&RaftConsensusInstance::MarkTabletDirty, Unretained(this))));

  log_->ClearOrphanedReplicates();

  RETURN_NOT_OK_PREPEND(
      WaitUntilRunning(), "Failed waiting for the raft to run");

  set_state(MANAGER_RUNNING);
  return Status::OK();
}

bool RaftConsensusInstance::IsInitialized() const {
  return state() == MANAGER_INITIALIZED;
}

void RaftConsensusInstance::Shutdown() {
  LOG_WITH_PREFIX(INFO) << "Shutting down RaftConsensusInstance";
  {
    const std::lock_guard<RWMutex> lock(lock_);
    switch (state_) {
      case MANAGER_QUIESCING: {
        VLOG_WITH_PREFIX(1)
            << "Raft consensus instance shut down already in progress..";
        return;
      }
      case MANAGER_SHUTDOWN: {
        VLOG_WITH_PREFIX(1)
            << "Raft consensus instance has already been shut down.";
        return;
      }
      case MANAGER_INITIALIZING:
      case MANAGER_INITIALIZED:
      case MANAGER_RUNNING: {
        LOG_WITH_PREFIX(INFO) << "Shutting down raft consensus instance...";
        state_ = MANAGER_QUIESCING;
        break;
      }
      default: {
        LOG_WITH_PREFIX(FATAL)
            << "Invalid state: " << TSTabletManagerStatePB_Name(state_);
      }
    }
  }

  if (consensus_)
    consensus_->Shutdown();

  state_ = MANAGER_SHUTDOWN;
}

std::shared_ptr<consensus::RaftConsensus>
RaftConsensusInstance::shared_consensus() const {
  return consensus_;
}

std::string RaftConsensusInstance::LogPrefix() const {
  DCHECK(fs_manager_ != nullptr);
  return strings::Substitute("[$0] ", id_);
}

Status RaftConsensusInstance::CreateNew(FsManager* fs_manager) {
  RaftConfigPB config;
  if (server_->opts(id_).IsDistributed()) {
    LOG_WITH_PREFIX(INFO)
        << "RaftConsensusInstance::CreateNew - Calling CreateDistributedConfig";
    RETURN_NOT_OK_PREPEND(
        CreateDistributedConfig(server_->opts(id_), &config),
        "Failed to create new distributed Raft config");
  } else {
    LOG_WITH_PREFIX(INFO)
        << "RaftConsensusInstance::CreateNew - Setting up single peer local config";
    config.set_obsolete_local(true);
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(fs_manager->uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

  RETURN_NOT_OK_PREPEND(
      cmeta_manager_->CreateCMeta(id_, config, consensus::kMinimumTerm),
      "Unable to persist consensus metadata for tablet " + id_);
  // TODO(mpercy): Provide a way to specify the proxy graph at tablet creation
  // time. For now, we initialize with an empty proxy graph.
  RETURN_NOT_OK_PREPEND(
      cmeta_manager_->CreateDRT(id_, config, {}),
      "Unable to create new durable routing table for tablet " + id_);
  // Note that we are intentionally not creating Persistent Vars here because we
  // do it in SetupRaft() anyway if the file does not exist

  return SetupRaft();
}

Status RaftConsensusInstance::Load(FsManager* /* fs_manager */) {
  if (server_->opts(id_).IsDistributed()) {
    LOG_WITH_PREFIX(INFO) << "Verifying existing consensus state";
    scoped_refptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(
        cmeta_manager_->LoadCMeta(id_, &cmeta),
        "Unable to load consensus metadata for tablet " + id_);
    const ConsensusStatePB& cstate = cmeta->ToConsensusStatePB();
    RETURN_NOT_OK(consensus::VerifyRaftConfig(cstate.committed_config()));
    CHECK(!cstate.has_pending_config());

    // Make sure the set of masters passed in at start time matches the set in
    // the on-disk cmeta.
    set<string> peer_addrs_from_opts;
    for (const auto& hp : server_->opts(id_).tserver_addresses) {
      peer_addrs_from_opts.insert(hp.ToString());
    }
    if (peer_addrs_from_opts.size() <
        server_->opts(id_).tserver_addresses.size()) {
      LOG_WITH_PREFIX(WARNING) << Substitute(
          "Found duplicates in --tserver_addresses: "
          "the unique set of addresses is $0",
          JoinStrings(peer_addrs_from_opts, ", "));
    }
    set<string> peer_addrs_from_disk;
    for (const auto& p : cstate.committed_config().peers()) {
      HostPort hp;
      RETURN_NOT_OK(HostPortFromPB(p.last_known_addr(), &hp));
      peer_addrs_from_disk.insert(hp.ToString());
    }
    vector<string> symm_diff;
    std::set_symmetric_difference(
        peer_addrs_from_opts.begin(),
        peer_addrs_from_opts.end(),
        peer_addrs_from_disk.begin(),
        peer_addrs_from_disk.end(),
        std::back_inserter(symm_diff));
    if (!symm_diff.empty()) {
      const string msg = Substitute(
          "on-disk master list ($0) and provided master list ($1) differ. "
          "Their symmetric difference is: $2",
          JoinStrings(peer_addrs_from_disk, ", "),
          JoinStrings(peer_addrs_from_opts, ", "),
          JoinStrings(symm_diff, ", "));
      return Status::InvalidArgument(msg);
    }
  }

  return SetupRaft();
}

Status RaftConsensusInstance::CreateDistributedConfig(
    const TabletServerOptions& options,
    RaftConfigPB* committed_config) {
  DCHECK(options.IsDistributed());

  RaftConfigPB new_config;
  new_config.set_obsolete_local(false);
  new_config.set_opid_index(consensus::kInvalidOpIdIndex);

  // WARN if both are set. Not failing it now, because
  // during the rollout phase, we might be setting both by
  // mistake.
  if (!options.tserver_addresses.empty() &&
      !options.bootstrap_tservers.empty()) {
    LOG_WITH_PREFIX(WARNING)
        << "Both tserver_addresses and bootstrap_tservers is"
           " being passed during bootstrap. This can create unexpected behavior."
           " Move to boostrap_tservers as it is more capable.";
  }

  // Give first priority to options.tserver_addresses
  // Over time applications will stop setting this and
  // pass in list of peers. Applications are expected to
  // not use both modes, till we remove support for tserver_addresses
  if (!options.tserver_addresses.empty()) {
    RETURN_NOT_OK(TabletManagerIf::CreateConfigFromTserverAddresses(
        options, &new_config));
  } else {
    TabletManagerIf::CreateConfigFromBootstrapPeers(options, &new_config);
  }

  // Now resolve UUIDs.
  // By the time a SysCatalogTable is created and initted, the masters should be
  // starting up, so this should be fine to do.
  DCHECK(server_->messenger());
  RaftConfigPB resolved_config = new_config;
  resolved_config.clear_peers();
  for (const RaftPeerPB& peer : new_config.peers()) {
    if (peer.has_permanent_uuid()) {
      resolved_config.add_peers()->CopyFrom(peer);
    } else {
      LOG_WITH_PREFIX(INFO)
          << SecureShortDebugString(peer)
          << " has no permanent_uuid. Determining permanent_uuid...";
      RaftPeerPB new_peer = peer;
      RETURN_NOT_OK_PREPEND(
          consensus::SetPermanentUuidForRemotePeer(
              server_->messenger(), &new_peer),
          Substitute(
              "Unable to resolve UUID for peer $0",
              SecureShortDebugString(peer)));
      resolved_config.add_peers()->CopyFrom(new_peer);
    }
  }

  if (FLAGS_enable_flexi_raft) {
    DCHECK(options.topology_config.has_commit_rule());
    resolved_config.mutable_commit_rule()->CopyFrom(
        options.topology_config.commit_rule());
    resolved_config.mutable_voter_distribution()->insert(
        options.topology_config.voter_distribution().begin(),
        options.topology_config.voter_distribution().end());
  }

  RETURN_NOT_OK(consensus::VerifyRaftConfig(resolved_config));
  VLOG_WITH_PREFIX(1) << "Distributed Raft configuration: "
                      << SecureShortDebugString(resolved_config);

  *committed_config = resolved_config;
  return Status::OK();
}

Status RaftConsensusInstance::SetupRaft() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  InitLocalRaftPeerPB();

  // If the persistent vars file does not already exist, create one
  if (!persistent_vars_manager_->PersistentVarsFileExists(id_)) {
    LOG_WITH_PREFIX(INFO) << "Persistent Vars file does not exist for tablet "
                          << id_ << ". Creating a new one";
    RETURN_NOT_OK_PREPEND(
        persistent_vars_manager_->CreatePersistentVars(id_),
        "Unable to create persistent vars file for tablet " + id_);
  }

  const TabletServerOptions& opts = server_->opts(id_);
  ConsensusOptions options;
  options.tablet_id = id_;
  options.proxy_policy = opts.proxy_policy;
  options.proxy_region_groups = opts.proxy_region_groups;
  if (opts.topology_config.has_initial_raft_rpc_token()) {
    options.initial_raft_rpc_token =
        opts.topology_config.initial_raft_rpc_token();
  }

  shared_ptr<RaftConsensus> consensus;
  TRACE("Creating consensus");
  LOG_WITH_PREFIX(INFO) << "Creating Raft for the system tablet";
  RETURN_NOT_OK(RaftConsensus::Create(
      std::move(options),
      local_peer_pb_,
      cmeta_manager_,
      persistent_vars_manager_,
      server_->raft_pool(),
      &consensus));
  consensus_ = std::move(consensus);
  if (opts.edcb) {
    consensus_->SetElectionDecisionCallback(opts.edcb);
  }
  if (opts.tacb) {
    consensus_->SetTermAdvancementCallback(opts.tacb);
  }
  if (opts.norcb) {
    consensus_->SetNoOpReceivedCallback(opts.norcb);
  }
  if (opts.ldcb) {
    consensus_->SetLeaderDetectedCallback(opts.ldcb);
  }
  if (opts.disable_noop) {
    consensus_->DisableNoOpEntries();
  }
  if (opts.vote_logger) {
    consensus_->SetVoteLogger(opts.vote_logger);
  }

  // set_state(INITIALIZED);
  // SetStatusMessage("Initialized. Waiting to start...");

  // Not sure these 2 lines are required
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager_->LoadCMeta(id_, &cmeta));

  // Open the log, while passing in the factory class.
  // Factory could be empty.
  LogOptions log_options;
  log_options.log_factory = opts.log_factory;
  RETURN_NOT_OK(Log::Open(
      log_options, fs_manager_, id_, server_->metric_entity(), &log_));

  // Abstracted logs will do their own log recovery
  // during Log::Open->Log::Init (virtual call). bootstrap_info
  // is populated during that step. Capture it so as to pass it
  // to RaftConsensus::Start, in RaftConsensusInstance::Start
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
  // log_bootstrap_on_first_run in options.
  if (opts.log_factory &&
      (!server_->is_first_run_ || opts.log_bootstrap_on_first_run)) {
    auto bootstrap_info = log_->GetRecoveryInfo();
    if (bootstrap_info &&
        bootstrap_info->last_id.term() > consensus_->CurrentTerm()) {
      consensus_->SetCurrentTermBootstrap(bootstrap_info->last_id.term());
    }
  }
  return Status::OK();
}

void RaftConsensusInstance::InitLocalRaftPeerPB() {
  DCHECK_EQ(state(), MANAGER_INITIALIZING);
  local_peer_pb_.set_permanent_uuid(fs_manager_->uuid());
  const Sockaddr addr = server_->first_rpc_address();
  HostPort hp;
  CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  CHECK_OK(HostPortToPB(hp, local_peer_pb_.mutable_last_known_addr()));

  // We will make this the default soon, Flexi-raft needs regions
  // attr. We assumed that on plugin side, topology_config->server_config
  // is well formed. We use it directly here.
  if (FLAGS_enable_flexi_raft &&
      server_->opts(id_).topology_config.has_server_config()) {
    local_peer_pb_ = server_->opts(id_).topology_config.server_config();
  }
}

Status RaftConsensusInstance::WaitUntilConsensusRunning(
    const MonoDelta& timeout) {
  const MonoTime start(MonoTime::Now());

  int backoff_exp = 0;
  const int kMaxBackoffExp = 8;
  while (true) {
    if (consensus_ && consensus_->IsRunning()) {
      break;
    }
    const MonoTime now(MonoTime::Now());
    const MonoDelta elapsed(now - start);
    if (elapsed > timeout) {
      return Status::TimedOut(Substitute(
          "Raft Consensus is not running after waiting for $0:",
          elapsed.ToString()));
    }
    SleepFor(MonoDelta::FromMilliseconds(1L << backoff_exp));
    backoff_exp = std::min(backoff_exp + 1, kMaxBackoffExp);
  }
  return Status::OK();
}

Status RaftConsensusInstance::WaitUntilRunning() {
  TRACE_EVENT0("master", "SysCatalogTable::WaitUntilRunning");
  int seconds_waited = 0;
  while (true) {
    Status status = WaitUntilConsensusRunning(MonoDelta::FromSeconds(1));
    seconds_waited++;
    if (status.ok()) {
      LOG_WITH_PREFIX(INFO)
          << "configured and running, proceeding with master startup.";
      break;
    }
    if (status.IsTimedOut()) {
      LOG_WITH_PREFIX(INFO) << "not online yet (have been trying for "
                            << seconds_waited << " seconds)";
      continue;
    }
    // if the status is not OK or TimedOut return it.
    return status;
  }
  return Status::OK();
}

RaftConsensusManager::RaftConsensusManager(RaftConsensusServer* server)
    : fs_manager_(server->fs_manager()),
      cmeta_manager_(new ConsensusMetadataManager(fs_manager_)),
      persistent_vars_manager_(new PersistentVarsManager(fs_manager_)),
      server_(server),
      state_(MANAGER_INITIALIZING) {
  const std::unique_lock lock(map_lock_);
  std::vector<std::string> ids;
  server_->opts_.GetIds(ids);
  for (const auto& id : ids) {
    auto instance_manager = std::make_shared<RaftConsensusInstance>(
        id, server_, cmeta_manager_, persistent_vars_manager_);
    map_[id] = instance_manager;
  }
}

const NodeInstancePB& RaftConsensusManager::NodeInstance() const {
  // TODO(abhinav): is this ok?
  return server_->instance_pb();
}

std::shared_ptr<consensus::RaftConsensus>
RaftConsensusManager::shared_consensus(const std::string& id) const {
  const std::shared_lock lock(map_lock_);
  auto itr = map_.find(id);
  if (itr == map_.end()) {
    return nullptr;
  }
  return itr->second->shared_consensus();
}

Status RaftConsensusManager::Init(bool is_first_run) {
  LOG(INFO) << "Initializing RaftConsensusManager";
  const std::shared_lock lock(map_lock_);
  for (const auto& entry : map_) {
    RETURN_NOT_OK(entry.second->Init(is_first_run));
  }
  return Status::OK();
}

Status RaftConsensusManager::Start(bool is_first_run) {
  LOG(INFO) << "Starting RaftConsensusManager";
  const std::shared_lock lock(map_lock_);
  for (const auto& entry : map_) {
    RETURN_NOT_OK(entry.second->Start(is_first_run));
  }
  return Status::OK();
}

bool RaftConsensusManager::IsInitialized() const {
  const std::shared_lock lock(map_lock_);
  for (const auto& entry : map_) {
    if (!entry.second->IsInitialized()) {
      return false;
    }
  }
  return true;
}

void RaftConsensusManager::Shutdown() {
  LOG(INFO) << "Shutting down RaftConsensusManager";
  const std::shared_lock lock(map_lock_);
  for (const auto& entry : map_) {
    entry.second->Shutdown();
  }
}

} // namespace tserver
} // namespace kudu
