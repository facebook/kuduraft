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

#include "kudu/server/server_base.h"

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/init.h"
#include "kudu/server/diagnostics_log.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/spinlock_profiling.h"
#include "kudu/util/thread.h"
#include "kudu/util/user.h"
#include "kudu/util/version_info.h"

DEFINE_int32(
    num_reactor_threads,
    4,
    "Number of libev reactor threads to start.");
TAG_FLAG(num_reactor_threads, advanced);

DEFINE_int32(
    min_negotiation_threads,
    0,
    "Minimum number of connection negotiation threads.");
TAG_FLAG(min_negotiation_threads, advanced);

DEFINE_int32(
    max_negotiation_threads,
    50,
    "Maximum number of connection negotiation threads.");
TAG_FLAG(max_negotiation_threads, advanced);

DEFINE_int64(
    rpc_negotiation_timeout_ms,
    3000,
    "Timeout for negotiating an RPC connection.");
TAG_FLAG(rpc_negotiation_timeout_ms, advanced);
TAG_FLAG(rpc_negotiation_timeout_ms, runtime);

DEFINE_bool(
    webserver_enabled,
    true,
    "Whether to enable the web server on this daemon. "
    "NOTE: disabling the web server is also likely to prevent monitoring systems "
    "from properly capturing metrics.");
TAG_FLAG(webserver_enabled, advanced);

DEFINE_string(
    superuser_acl,
    "",
    "The list of usernames to allow as super users, comma-separated. "
    "A '*' entry indicates that all authenticated users are allowed. "
    "If this is left unset or blank, the default behavior is that the "
    "identity of the daemon itself determines the superuser. If the "
    "daemon is logged in from a Keytab, then the local username from "
    "the Kerberos principal is used; otherwise, the local Unix "
    "username is used.");
TAG_FLAG(superuser_acl, stable);
TAG_FLAG(superuser_acl, sensitive);

DEFINE_string(
    user_acl,
    "*",
    "The list of usernames who may access the cluster, comma-separated. "
    "A '*' entry indicates that all authenticated users are allowed.");
TAG_FLAG(user_acl, stable);
TAG_FLAG(user_acl, sensitive);

DEFINE_string(
    principal,
    "kudu/_HOST",
    "Kerberos principal that this daemon will log in as. The special token "
    "_HOST will be replaced with the FQDN of the local host.");
TAG_FLAG(principal, experimental);
// This is currently tagged as unsafe because there is no way for users to
// configure clients to expect a non-default principal. As such, configuring a
// server to login as a different one would end up with a cluster that can't be
// connected to. See KUDU-1884.
TAG_FLAG(principal, unsafe);

DEFINE_bool(
    allow_world_readable_credentials,
    false,
    "Enable the use of keytab files and TLS private keys with "
    "world-readable permissions.");
TAG_FLAG(allow_world_readable_credentials, unsafe);

DEFINE_string(
    rpc_authentication,
    "optional",
    "Whether to require RPC connections to authenticate. Must be one "
    "of 'disabled', 'optional', or 'required'. If 'optional', "
    "authentication will be used when the remote end supports it. If "
    "'required', connections which are not able to authenticate "
    "(because the remote end lacks support) are rejected. Secure "
    "clusters should use 'required'.");
DEFINE_string(
    rpc_encryption,
    "optional",
    "Whether to require RPC connections to be encrypted. Must be one "
    "of 'disabled', 'optional', or 'required'. If 'optional', "
    "encryption will be used when the remote end supports it. If "
    "'required', connections which are not able to use encryption "
    "(because the remote end lacks support) are rejected. If 'disabled', "
    "encryption will not be used, and RPC authentication "
    "(--rpc_authentication) must also be disabled as well. "
    "Secure clusters should use 'required'.");
TAG_FLAG(rpc_authentication, evolving);
TAG_FLAG(rpc_encryption, evolving);

DEFINE_string(
    rpc_tls_ciphers,
    kudu::security::SecurityDefaults::kDefaultTlsCiphers,
    "The cipher suite preferences to use for TLS-secured RPC connections. "
    "Uses the OpenSSL cipher preference list format. See man (1) ciphers "
    "for more information.");
TAG_FLAG(rpc_tls_ciphers, advanced);

DEFINE_string(
    rpc_tls_min_protocol,
    kudu::security::SecurityDefaults::kDefaultTlsMinVersion,
    "The minimum protocol version to allow when for securing RPC "
    "connections with TLS. May be one of 'TLSv1', 'TLSv1.1', or "
    "'TLSv1.2'.");
TAG_FLAG(rpc_tls_min_protocol, advanced);

DECLARE_string(rpc_certificate_file);
DECLARE_string(rpc_private_key_file);
DECLARE_string(rpc_ca_certificate_file);
DECLARE_string(rpc_private_key_password_cmd);

DEFINE_int32(
    rpc_default_keepalive_time_ms,
    65000,
    "If an RPC connection from a client is idle for this amount of time, the server "
    "will disconnect the client. Setting this to any negative value keeps connections "
    "always alive.");
TAG_FLAG(rpc_default_keepalive_time_ms, advanced);

DECLARE_bool(use_hybrid_clock);

DEFINE_bool(
    write_metrics_to_file,
    false,
    "When enabled, write metrics log periodically");

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class HostPortPB;

namespace server {

namespace {

// Disambiguates between servers when in a minicluster.
AtomicInt<int32_t> mem_tracker_id_counter(-1);

shared_ptr<MemTracker> CreateMemTrackerForServer() {
  int32_t id = mem_tracker_id_counter.increment();
  string id_str = "server";
  if (id != 0) {
    strAppend(&id_str, " ", id);
  }
  return shared_ptr<MemTracker>(MemTracker::createTracker(-1, id_str));
}

} // anonymous namespace

ServerBase::ServerBase(
    string name,
    const ServerBaseOptions& options,
    const string& metric_namespace)
    : name_(std::move(name)),
      memTracker_(CreateMemTrackerForServer()),
      metricRegistry_(new MetricRegistry()),
      metricEntity_(METRIC_ENTITY_server.instantiate(
          metricRegistry_.get(),
          metric_namespace)),
      rpcServer_(new RpcServer(options.rpcOpts)),
      resultTracker_(new rpc::ResultTracker(
          shared_ptr<MemTracker>(
              MemTracker::createTracker(-1, "result-tracker", memTracker_)))),
      isFirstRun_(false),
      options_(options),
      stopBackgroundThreadsLatch_(1) {
  FsManagerOpts fs_opts;
  fs_opts.metricEntity = metricEntity_;
  fs_opts.parentMemTracker = memTracker_;
  fs_opts.walRoot = options.fsOpts.walRoot;
  fsManager_.reset(new FsManager(options.env, std::move(fs_opts)));

  if (FLAGS_use_hybrid_clock) {
    clock_ = std::make_shared<clock::HybridClock>();
  } else {
    clock_ = std::shared_ptr<clock::Clock>(
        clock::LogicalClock::createStartingAt(Timestamp::kInitialTimestamp));
  }
}

ServerBase::~ServerBase() {
  Shutdown();
}

Sockaddr ServerBase::firstRpcAddress() const {
  vector<Sockaddr> addrs;
  WARN_NOT_OK(
      rpcServer_->getBoundAddresses(&addrs), "Couldn't get bound RPC address");
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

const security::TlsContext& ServerBase::tlsContext() const {
  return messenger_->tlsContext();
}

security::TlsContext* ServerBase::mutableTlsContext() {
  return messenger_->mutableTlsContext();
}

const security::TokenVerifier& ServerBase::tokenVerifier() const {
  return messenger_->tokenVerifier();
}

security::TokenVerifier* ServerBase::mutableTokenVerifier() {
  return messenger_->mutableTokenVerifier();
}

const NodeInstancePB& ServerBase::instancePb() const {
  return *DCHECK_NOTNULL(instancePb_.get());
}

void ServerBase::generateInstanceId() {
  instancePb_.reset(new NodeInstancePB);
  instancePb_->set_permanent_uuid(fsManager_->uuid());
  // TODO: maybe actually bump a sequence number on local disk instead of
  // using time.
  instancePb_->set_instance_seqno(Env::Default()->nowMicros());
}

Status ServerBase::Init() {
  registerSpinLockContentionMetrics(metricEntity_);

  initSpinLockContentionProfiling();

  // Initialize the clock immediately. This checks that the clock is
  // synchronized so we're less likely to get into a partially initialized state
  // on disk during startup if we're having clock problems.
  RETURN_NOT_OK_PREPEND(clock_->init(), "Cannot initialize clock");

  fs::FsReport report;
  Status s = fsManager_->Open(&report);
  if (s.IsNotFound()) {
    LOG(INFO) << "Could not load existing FS layout: " << s.ToString();
    LOG(INFO) << "Attempting to create new FS layout instead";
    isFirstRun_ = true;
    std::optional<std::string> uuid;
    if (!options_.appProvidedInstanceUuid.empty()) {
      uuid = options_.appProvidedInstanceUuid;
    }
    s = fsManager_->CreateInitialFileSystemLayout(uuid);
    if (s.isAlreadyPresent()) {
      // The operator is likely trying to start up with an extra entry in their
      // `fs_data_dirs` configuration.
      LOG(INFO) << "To start Kudu with a different FS layout, the `kudu fs "
                   "update_dirs` tool must be run first";
      return s.cloneAndPrepend(
          "FS layout already exists; not overwriting existing layout");
    }
    RETURN_NOT_OK_PREPEND(s, "Could not create new FS layout");
    s = fsManager_->Open(&report);
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");
  RETURN_NOT_OK(report.logAndCheckForFatalErrors());

  RETURN_NOT_OK(initAcls());

  // Create the Messenger.
  rpc::MessengerBuilder builder(name_);

  builder.setNumReactors(FLAGS_num_reactor_threads)
      .setMinNegotiationThreads(FLAGS_min_negotiation_threads)
      .setMaxNegotiationThreads(FLAGS_max_negotiation_threads)
      .setMetricEntity(metricEntity())
      .setConnectionKeepAliveTime(FLAGS_rpc_default_keepalive_time_ms)
      .setRpcNegotiationTimeoutMs(FLAGS_rpc_negotiation_timeout_ms)
      .setRpcAuthentication(FLAGS_rpc_authentication)
      .setRpcEncryption(FLAGS_rpc_encryption)
      .setRpcTlsCiphers(FLAGS_rpc_tls_ciphers)
      .setRpcTlsMinProtocol(FLAGS_rpc_tls_min_protocol)
      .setEpkiCertKeyFiles(
          FLAGS_rpc_certificate_file, FLAGS_rpc_private_key_file)
      .setEpkiCertificateAuthorityFile(FLAGS_rpc_ca_certificate_file)
      .setEpkiPrivatePasswordKeyCmd(FLAGS_rpc_private_key_password_cmd)
      .enableInboundTls();

  // If rpcOpts explicitly specify the number of reactor threads, then use it
  // to override FLAGS_num_reactor_threads
  if (options_.rpcOpts.numReactorThreads != 0) {
    builder.setNumReactors(options_.rpcOpts.numReactorThreads);
  }

  RETURN_NOT_OK(builder.build(&messenger_));
  rpcServer_->setTooBusyHook(
      std::bind(
          &ServerBase::serviceQueueOverflowed, this, std::placeholders::_1));

  RETURN_NOT_OK(rpcServer_->init(messenger_));

  // Bind the RPC server so that the
  // local raft peer can be initialized
  RETURN_NOT_OK(rpcServer_->bind());
  clock_->registerMetrics(metricEntity_);

  RETURN_NOT_OK_PREPEND(
      startMetricsLogging(), "Could not enable metrics logging");

  resultTracker_->startGcThread();
  RETURN_NOT_OK(startExcessLogFileDeleterThread());

  return Status::OK();
}

Status ServerBase::initAcls() {
  string service_user;
  std::optional<string> keytab_user = security::getLoggedInUsernameFromKeytab();
  if (keytab_user) {
    // If we're logged in from a keytab, then everyone should be, and we expect
    // them to use the same mapped username.
    service_user = *keytab_user;
  } else {
    // If we aren't logged in from a keytab, then just assume that the services
    // will be running as the same Unix user as we are.
    RETURN_NOT_OK_PREPEND(
        getLoggedInUser(&service_user), "could not deterine local username");
  }

  // If the user has specified a superuser acl, use that. Otherwise, assume
  // that the same user running the service acts as superuser.
  if (!FLAGS_superuser_acl.empty()) {
    RETURN_NOT_OK_PREPEND(
        superuserAcl_.parseFlag(FLAGS_superuser_acl),
        "could not parse --superuser_acl flag");
  } else {
    superuserAcl_.reset({service_user});
  }

  RETURN_NOT_OK_PREPEND(
      userAcl_.parseFlag(FLAGS_user_acl), "could not parse --user_acl flag");

  // For the "service" ACL, we currently don't allow it to be user-configured,
  // but instead assume that all of the services will be running the same
  // way.
  serviceAcl_.reset({service_user});

  return Status::OK();
}

Status ServerBase::getStatusPb(ServerStatusPB* status) const {
  // Node instance
  status->mutable_node_instance()->CopyFrom(*instancePb_);

  // RPC ports
  {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK_PREPEND(
        rpcServer_->getBoundAddresses(&addrs),
        "could not get bound RPC addresses");
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      RETURN_NOT_OK_PREPEND(
          hostPortFromSockaddrReplaceWildcard(addr, &hp),
          "could not get RPC hostport");
      HostPortPB* pb = status->add_bound_rpc_addresses();
      RETURN_NOT_OK_PREPEND(
          hostPortToPb(hp, pb), "could not convert RPC hostport");
    }
  }

  VersionInfo::getVersionInfoPb(status->mutable_version_info());
  return Status::OK();
}

void ServerBase::logUnauthorizedAccess(rpc::RpcContext* rpc) const {
  LOG(WARNING) << "Unauthorized access attempt to method " << rpc->serviceName()
               << "." << rpc->methodName() << " from "
               << rpc->requestorString();
}

bool ServerBase::authorize(rpc::RpcContext* rpc, uint32_t allowed_roles) {
  if ((allowed_roles & kSuperUser) &&
      superuserAcl_.userAllowed(rpc->remoteUser().username())) {
    return true;
  }

  if ((allowed_roles & kUser) &&
      userAcl_.userAllowed(rpc->remoteUser().username())) {
    return true;
  }

  if ((allowed_roles & kServiceUser) &&
      serviceAcl_.userAllowed(rpc->remoteUser().username())) {
    return true;
  }

  logUnauthorizedAccess(rpc);
  rpc->respondFailure(
      Status::NotAuthorized(
          "unauthorized access to method", rpc->methodName()));
  return false;
}

Status ServerBase::dumpServerInfo(const string& path, const string& format)
    const {
  ServerStatusPB status;
  RETURN_NOT_OK_PREPEND(getStatusPb(&status), "could not get server status");

  if (boost::iequals(format, "json")) {
    string json = JsonWriter::toJson(status, JsonWriter::kPretty);
    RETURN_NOT_OK(writeStringToFile(options_.env, Slice(json), path));
  } else if (boost::iequals(format, "pb")) {
    // TODO: Use PB container format?
    RETURN_NOT_OK(
        pb_util::WritePBToPath(
            options_.env,
            path,
            status,
            pb_util::kNoSync)); // durability doesn't matter
  } else {
    return Status::InvalidArgument("bad format", format);
  }

  LOG(INFO) << "Dumped server information to " << path;
  return Status::OK();
}

Status ServerBase::registerService(unique_ptr<rpc::ServiceIf> rpc_impl) {
  return rpcServer_->registerService(std::move(rpc_impl));
}

Status ServerBase::startMetricsLogging() {
  if (options_.metricsLogIntervalMs <= 0) {
    return Status::OK();
  }
  if (!FLAGS_write_metrics_to_file) {
    LOG(WARNING) << "Not starting metrics log since disabled by gflag";
    return Status::OK();
  }
  std::string log_dir = FLAGS_log_dir;
  if (!options_.metricsLogDir.empty()) {
    log_dir = options_.metricsLogDir;
  }
  if (log_dir.empty()) {
    LOG(INFO)
        << "Not starting metrics log since no log directory was specified.";
    return Status::OK();
  }
  unique_ptr<DiagnosticsLog> l(
      new DiagnosticsLog(std::move(log_dir), metricRegistry_.get()));
  l->setMetricsLogInterval(
      MonoDelta::FromMilliseconds(options_.metricsLogIntervalMs));
  RETURN_NOT_OK(l->start());
  diagLog_ = std::move(l);
  return Status::OK();
}

Status ServerBase::startExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future. Same with minidumps.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(
        deleteExcessLogFiles(options_.env),
        "Unable to delete excess log files");
  }
  return Thread::create(
      "server",
      "excess-log-deleter",
      &ServerBase::excessLogFileDeleterThread,
      this,
      &excessLogDeleterThread_);
}

void ServerBase::excessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog and minidump files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stopBackgroundThreadsLatch_.waitUntil(MonoTime::Now() + kWait)) {
    WARN_NOT_OK(
        deleteExcessLogFiles(options_.env),
        "Unable to delete excess log files");
  }
}

Status ServerBase::Start() {
  generateInstanceId();

  RETURN_NOT_OK(rpcServer_->start());

  if (!options_.dumpInfoPath.empty()) {
    RETURN_NOT_OK_PREPEND(
        dumpServerInfo(options_.dumpInfoPath, options_.dumpInfoFormat),
        "Failed to dump server info to " + options_.dumpInfoPath);
  }

  return Status::OK();
}

void ServerBase::Shutdown() {
  // First, stop accepting incoming requests and wait for any outstanding
  // requests to finish processing.
  //
  // Note: prior to Messenger::Shutdown, it is assumed that any incoming RPCs
  // deferred from reactor threads have already been cleaned up.

  rpcServer_->shutdown();
  if (messenger_) {
    messenger_->Shutdown();
  }

  // Next, shut down remaining server components.
  stopBackgroundThreadsLatch_.countDown();
  if (diagLog_) {
    diagLog_->stop();
  }

  if (excessLogDeleterThread_) {
    excessLogDeleterThread_->join();
  }
}

void ServerBase::unregisterAllServices() {
  messenger_->UnregisterAllServices();
}

void ServerBase::serviceQueueOverflowed(rpc::ServicePool* service) {
  if (!diagLog_) {
    return;
  }

  // Logging all of the stacks is relatively heavy-weight, so if we are in a
  // persistent state of overload, it's probably not a good idea to start
  // compounding the issue with a lot of stack-logging activity. So, we limit
  // the frequency of stack-dumping.
  static logging::LogThrottler throttler;
  const int kStackDumpFrequencySecs = 5;
  int suppressed = 0;
  if (PREDICT_TRUE(
          !throttler.shouldLog(kStackDumpFrequencySecs, "", &suppressed))) {
    return;
  }
}

} // namespace server
} // namespace kudu
