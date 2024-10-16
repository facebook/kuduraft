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
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
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
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/init.h"
#include "kudu/server/diagnostics_log.h"
#include "kudu/server/glog_metrics.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/minidump.h"
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
  int32_t id = mem_tracker_id_counter.Increment();
  string id_str = "server";
  if (id != 0) {
    StrAppend(&id_str, " ", id);
  }
  return shared_ptr<MemTracker>(MemTracker::CreateTracker(-1, id_str));
}

} // anonymous namespace

ServerBase::ServerBase(
    string name,
    const ServerBaseOptions& options,
    const string& metric_namespace)
    : name_(std::move(name)),
      mem_tracker_(CreateMemTrackerForServer()),
      metric_registry_(new MetricRegistry()),
      metric_entity_(METRIC_ENTITY_server.Instantiate(
          metric_registry_.get(),
          metric_namespace)),
      rpc_server_(new RpcServer(options.rpc_opts)),
      result_tracker_(new rpc::ResultTracker(shared_ptr<MemTracker>(
          MemTracker::CreateTracker(-1, "result-tracker", mem_tracker_)))),
      is_first_run_(false),
      options_(options),
      stop_background_threads_latch_(1) {
  FsManagerOpts fs_opts;
  fs_opts.metric_entity = metric_entity_;
  fs_opts.parent_mem_tracker = mem_tracker_;
  fs_opts.block_manager_type = options.fs_opts.block_manager_type;
  fs_opts.wal_root = options.fs_opts.wal_root;
  fs_opts.data_roots = options.fs_opts.data_roots;
  fs_opts.allow_non_empty_root = options.fs_opts.allow_non_empty_root;
  fs_manager_.reset(new FsManager(options.env, std::move(fs_opts)));

  if (FLAGS_use_hybrid_clock) {
    clock_ = new clock::HybridClock();
  } else {
    clock_ =
        clock::LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp);
  }
}

ServerBase::~ServerBase() {
  Shutdown();
}

Sockaddr ServerBase::first_rpc_address() const {
  vector<Sockaddr> addrs;
  WARN_NOT_OK(
      rpc_server_->GetBoundAddresses(&addrs), "Couldn't get bound RPC address");
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

const security::TlsContext& ServerBase::tls_context() const {
  return messenger_->tls_context();
}

security::TlsContext* ServerBase::mutable_tls_context() {
  return messenger_->mutable_tls_context();
}

const security::TokenVerifier& ServerBase::token_verifier() const {
  return messenger_->token_verifier();
}

security::TokenVerifier* ServerBase::mutable_token_verifier() {
  return messenger_->mutable_token_verifier();
}

const NodeInstancePB& ServerBase::instance_pb() const {
  return *DCHECK_NOTNULL(instance_pb_.get());
}

void ServerBase::GenerateInstanceID() {
  instance_pb_.reset(new NodeInstancePB);
  instance_pb_->set_permanent_uuid(fs_manager_->uuid());
  // TODO: maybe actually bump a sequence number on local disk instead of
  // using time.
  instance_pb_->set_instance_seqno(Env::Default()->NowMicros());
}

Status ServerBase::Init() {
  RegisterSpinLockContentionMetrics(metric_entity_);

  InitSpinLockContentionProfiling();

  // Initialize the clock immediately. This checks that the clock is
  // synchronized so we're less likely to get into a partially initialized state
  // on disk during startup if we're having clock problems.
  RETURN_NOT_OK_PREPEND(clock_->Init(), "Cannot initialize clock");

  fs::FsReport report;
  Status s = fs_manager_->Open(&report);
  if (s.IsNotFound()) {
    LOG(INFO) << "Could not load existing FS layout: " << s.ToString();
    LOG(INFO) << "Attempting to create new FS layout instead";
    is_first_run_ = true;
    std::optional<std::string> uuid;
    if (!options_.app_provided_instance_uuid.empty()) {
      uuid = options_.app_provided_instance_uuid;
    }
    s = fs_manager_->CreateInitialFileSystemLayout(uuid);
    if (s.IsAlreadyPresent()) {
      // The operator is likely trying to start up with an extra entry in their
      // `fs_data_dirs` configuration.
      LOG(INFO) << "To start Kudu with a different FS layout, the `kudu fs "
                   "update_dirs` tool must be run first";
      return s.CloneAndPrepend(
          "FS layout already exists; not overwriting existing layout");
    }
    RETURN_NOT_OK_PREPEND(s, "Could not create new FS layout");
    s = fs_manager_->Open(&report);
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");
  RETURN_NOT_OK(report.LogAndCheckForFatalErrors());

  RETURN_NOT_OK(InitAcls());

  // Create the Messenger.
  rpc::MessengerBuilder builder(name_);

  builder.set_num_reactors(FLAGS_num_reactor_threads)
      .set_min_negotiation_threads(FLAGS_min_negotiation_threads)
      .set_max_negotiation_threads(FLAGS_max_negotiation_threads)
      .set_metric_entity(metric_entity())
      .set_connection_keep_alive_time(FLAGS_rpc_default_keepalive_time_ms)
      .set_rpc_negotiation_timeout_ms(FLAGS_rpc_negotiation_timeout_ms)
      .set_rpc_authentication(FLAGS_rpc_authentication)
      .set_rpc_encryption(FLAGS_rpc_encryption)
      .set_rpc_tls_ciphers(FLAGS_rpc_tls_ciphers)
      .set_rpc_tls_min_protocol(FLAGS_rpc_tls_min_protocol)
      .set_epki_cert_key_files(
          FLAGS_rpc_certificate_file, FLAGS_rpc_private_key_file)
      .set_epki_certificate_authority_file(FLAGS_rpc_ca_certificate_file)
      .set_epki_private_password_key_cmd(FLAGS_rpc_private_key_password_cmd)
      .enable_inbound_tls();

  if (options_.rpc_opts.rpc_reuseport) {
    builder.set_reuseport();
  }

  // If rpc_opts explicitly specify the number of reactor threads, then use it
  // to override FLAGS_num_reactor_threads
  if (options_.rpc_opts.num_reactor_threads != 0) {
    builder.set_num_reactors(options_.rpc_opts.num_reactor_threads);
  }

  RETURN_NOT_OK(builder.Build(&messenger_));
  rpc_server_->set_too_busy_hook(std::bind(
      &ServerBase::ServiceQueueOverflowed, this, std::placeholders::_1));

  RETURN_NOT_OK(rpc_server_->Init(messenger_));

  // Bind the RPC server so that the
  // local raft peer can be initialized
  RETURN_NOT_OK(rpc_server_->Bind());
  clock_->RegisterMetrics(metric_entity_);

  RETURN_NOT_OK_PREPEND(
      StartMetricsLogging(), "Could not enable metrics logging");

  result_tracker_->StartGCThread();
  RETURN_NOT_OK(StartExcessLogFileDeleterThread());

  return Status::OK();
}

Status ServerBase::InitAcls() {
  string service_user;
  std::optional<string> keytab_user = security::GetLoggedInUsernameFromKeytab();
  if (keytab_user) {
    // If we're logged in from a keytab, then everyone should be, and we expect
    // them to use the same mapped username.
    service_user = *keytab_user;
  } else {
    // If we aren't logged in from a keytab, then just assume that the services
    // will be running as the same Unix user as we are.
    RETURN_NOT_OK_PREPEND(
        GetLoggedInUser(&service_user), "could not deterine local username");
  }

  // If the user has specified a superuser acl, use that. Otherwise, assume
  // that the same user running the service acts as superuser.
  if (!FLAGS_superuser_acl.empty()) {
    RETURN_NOT_OK_PREPEND(
        superuser_acl_.ParseFlag(FLAGS_superuser_acl),
        "could not parse --superuser_acl flag");
  } else {
    superuser_acl_.Reset({service_user});
  }

  RETURN_NOT_OK_PREPEND(
      user_acl_.ParseFlag(FLAGS_user_acl), "could not parse --user_acl flag");

  // For the "service" ACL, we currently don't allow it to be user-configured,
  // but instead assume that all of the services will be running the same
  // way.
  service_acl_.Reset({service_user});

  return Status::OK();
}

Status ServerBase::GetStatusPB(ServerStatusPB* status) const {
  // Node instance
  status->mutable_node_instance()->CopyFrom(*instance_pb_);

  // RPC ports
  {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK_PREPEND(
        rpc_server_->GetBoundAddresses(&addrs),
        "could not get bound RPC addresses");
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      RETURN_NOT_OK_PREPEND(
          HostPortFromSockaddrReplaceWildcard(addr, &hp),
          "could not get RPC hostport");
      HostPortPB* pb = status->add_bound_rpc_addresses();
      RETURN_NOT_OK_PREPEND(
          HostPortToPB(hp, pb), "could not convert RPC hostport");
    }
  }

  VersionInfo::GetVersionInfoPB(status->mutable_version_info());
  return Status::OK();
}

void ServerBase::LogUnauthorizedAccess(rpc::RpcContext* rpc) const {
  LOG(WARNING) << "Unauthorized access attempt to method "
               << rpc->service_name() << "." << rpc->method_name() << " from "
               << rpc->requestor_string();
}

bool ServerBase::Authorize(rpc::RpcContext* rpc, uint32_t allowed_roles) {
  if ((allowed_roles & SUPER_USER) &&
      superuser_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  if ((allowed_roles & USER) &&
      user_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  if ((allowed_roles & SERVICE_USER) &&
      service_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  LogUnauthorizedAccess(rpc);
  rpc->RespondFailure(Status::NotAuthorized(
      "unauthorized access to method", rpc->method_name()));
  return false;
}

Status ServerBase::DumpServerInfo(const string& path, const string& format)
    const {
  ServerStatusPB status;
  RETURN_NOT_OK_PREPEND(GetStatusPB(&status), "could not get server status");

  if (boost::iequals(format, "json")) {
    string json = JsonWriter::ToJson(status, JsonWriter::PRETTY);
    RETURN_NOT_OK(WriteStringToFile(options_.env, Slice(json), path));
  } else if (boost::iequals(format, "pb")) {
    // TODO: Use PB container format?
    RETURN_NOT_OK(pb_util::WritePBToPath(
        options_.env,
        path,
        status,
        pb_util::NO_SYNC)); // durability doesn't matter
  } else {
    return Status::InvalidArgument("bad format", format);
  }

  LOG(INFO) << "Dumped server information to " << path;
  return Status::OK();
}

Status ServerBase::RegisterService(unique_ptr<rpc::ServiceIf> rpc_impl) {
  return rpc_server_->RegisterService(std::move(rpc_impl));
}

Status ServerBase::StartMetricsLogging() {
  if (options_.metrics_log_interval_ms <= 0) {
    return Status::OK();
  }
  if (!FLAGS_write_metrics_to_file) {
    LOG(WARNING) << "Not starting metrics log since disabled by gflag";
    return Status::OK();
  }
  std::string log_dir = FLAGS_log_dir;
  if (!options_.metrics_log_dir.empty()) {
    log_dir = options_.metrics_log_dir;
  }
  if (log_dir.empty()) {
    LOG(INFO)
        << "Not starting metrics log since no log directory was specified.";
    return Status::OK();
  }
  unique_ptr<DiagnosticsLog> l(
      new DiagnosticsLog(std::move(log_dir), metric_registry_.get()));
  l->SetMetricsLogInterval(
      MonoDelta::FromMilliseconds(options_.metrics_log_interval_ms));
  RETURN_NOT_OK(l->Start());
  diag_log_ = std::move(l);
  return Status::OK();
}

Status ServerBase::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future. Same with minidumps.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(
        DeleteExcessLogFiles(options_.env),
        "Unable to delete excess log files");
  }
  return Thread::Create(
      "server",
      "excess-log-deleter",
      &ServerBase::ExcessLogFileDeleterThread,
      this,
      &excess_log_deleter_thread_);
}

void ServerBase::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog and minidump files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitUntil(MonoTime::Now() + kWait)) {
    WARN_NOT_OK(
        DeleteExcessLogFiles(options_.env),
        "Unable to delete excess log files");
  }
}

Status ServerBase::Start() {
  GenerateInstanceID();

  RETURN_NOT_OK(rpc_server_->Start());

  if (!options_.dump_info_path.empty()) {
    RETURN_NOT_OK_PREPEND(
        DumpServerInfo(options_.dump_info_path, options_.dump_info_format),
        "Failed to dump server info to " + options_.dump_info_path);
  }

  return Status::OK();
}

void ServerBase::Shutdown() {
  // First, stop accepting incoming requests and wait for any outstanding
  // requests to finish processing.
  //
  // Note: prior to Messenger::Shutdown, it is assumed that any incoming RPCs
  // deferred from reactor threads have already been cleaned up.

  rpc_server_->Shutdown();
  if (messenger_) {
    messenger_->Shutdown();
  }

  // Next, shut down remaining server components.
  stop_background_threads_latch_.CountDown();
  if (diag_log_) {
    diag_log_->Stop();
  }

  if (excess_log_deleter_thread_) {
    excess_log_deleter_thread_->Join();
  }
}

void ServerBase::UnregisterAllServices() {
  messenger_->UnregisterAllServices();
}

void ServerBase::ServiceQueueOverflowed(rpc::ServicePool* service) {
  if (!diag_log_)
    return;

  // Logging all of the stacks is relatively heavy-weight, so if we are in a
  // persistent state of overload, it's probably not a good idea to start
  // compounding the issue with a lot of stack-logging activity. So, we limit
  // the frequency of stack-dumping.
  static logging::LogThrottler throttler;
  const int kStackDumpFrequencySecs = 5;
  int suppressed = 0;
  if (PREDICT_TRUE(
          !throttler.ShouldLog(kStackDumpFrequencySecs, "", &suppressed))) {
    return;
  }
}

} // namespace server
} // namespace kudu
