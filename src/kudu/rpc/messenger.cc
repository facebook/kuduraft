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

#include "kudu/rpc/messenger.h"

#include <cstdlib>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include "kudu/gutil/port.h"
#include "kudu/rpc/connection_direction.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/flags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_pool_builder.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace boost {
template <typename Signature>
class function;
}

namespace kudu {
namespace rpc {

MessengerBuilder::MessengerBuilder(std::string name)
    : name_(std::move(name)),
      connectionKeepaliveTime_(MonoDelta::FromMilliseconds(65000)),
      numReactors_(4),
      minNegotiationThreads_(0),
      maxNegotiationThreads_(4),
      coarseTimerGranularity_(MonoDelta::FromMilliseconds(100)),
      rpcNegotiationTimeoutMs_(3000),
      rpcAuthentication_("optional"),
      rpcEncryption_("optional"),
      rpcTlsCiphers_(kudu::security::SecurityDefaults::kDefaultTlsCiphers),
      rpcTlsMinProtocol_(
          kudu::security::SecurityDefaults::kDefaultTlsMinVersion),
      enableInboundTls_(false) {}

MessengerBuilder& MessengerBuilder::set_connection_keepalive_time(
    const MonoDelta& keepalive) {
  connectionKeepaliveTime_ = keepalive;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_num_reactors(int num_reactors) {
  numReactors_ = num_reactors;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_min_negotiation_threads(
    int min_negotiation_threads) {
  minNegotiationThreads_ = min_negotiation_threads;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_max_negotiation_threads(
    int max_negotiation_threads) {
  maxNegotiationThreads_ = max_negotiation_threads;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_coarse_timer_granularity(
    const MonoDelta& granularity) {
  coarseTimerGranularity_ = granularity;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_metric_entity(
    const std::shared_ptr<MetricEntity>& metric_entity) {
  metricEntity_ = metric_entity;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_connection_keep_alive_time(
    int32_t time_in_ms) {
  connectionKeepaliveTime_ = MonoDelta::FromMilliseconds(time_in_ms);
  return *this;
}

MessengerBuilder& MessengerBuilder::set_rpc_negotiation_timeout_ms(
    int64_t time_in_ms) {
  rpcNegotiationTimeoutMs_ = time_in_ms;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_rpc_authentication(
    const std::string& rpc_authentication) {
  rpcAuthentication_ = rpc_authentication;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_rpc_encryption(
    const std::string& rpc_encryption) {
  rpcEncryption_ = rpc_encryption;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_rpc_tls_ciphers(
    const std::string& rpc_tls_ciphers) {
  rpcTlsCiphers_ = rpc_tls_ciphers;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_rpc_tls_min_protocol(
    const std::string& rpc_tls_min_protocol) {
  rpcTlsMinProtocol_ = rpc_tls_min_protocol;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_cert_key_files(
    const std::string& cert,
    const std::string& private_key) {
  rpcCertificateFile_ = cert;
  rpcPrivateKeyFile_ = private_key;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_certificate_authority_file(
    const std::string& ca) {
  rpcCaCertificateFile_ = ca;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_epki_private_password_key_cmd(
    const std::string& cmd) {
  rpcPrivateKeyPasswordCmd_ = cmd;
  return *this;
}

MessengerBuilder& MessengerBuilder::enable_inbound_tls() {
  enableInboundTls_ = true;
  return *this;
}

Status MessengerBuilder::Build(shared_ptr<Messenger>* msgr) {
  Messenger* new_msgr(new Messenger(*this));

  auto cleanup =
      folly::makeGuard([&]() { new_msgr->AllExternalReferencesDropped(); });

  RETURN_NOT_OK(parseTriState(
      "--rpc_authentication", rpcAuthentication_, &new_msgr->authentication_));

  RETURN_NOT_OK(parseTriState(
      "--rpc_encryption", rpcEncryption_, &new_msgr->encryption_));

  RETURN_NOT_OK(new_msgr->Init());
  if (new_msgr->encryption_ != RpcEncryption::DISABLED && enableInboundTls_) {
    auto* tls_context = new_msgr->mutable_tls_context();

    if (!rpcCertificateFile_.empty()) {
      CHECK(!rpcPrivateKeyFile_.empty());
      CHECK(!rpcCaCertificateFile_.empty());

      // TODO(KUDU-1920): should we try and enforce that the server
      // is in the subject or alt names of the cert?
      RETURN_NOT_OK(
          tls_context->LoadCertificateAuthority(rpcCaCertificateFile_));
      if (rpcPrivateKeyPasswordCmd_.empty()) {
        RETURN_NOT_OK(tls_context->LoadCertificateAndKey(
            rpcCertificateFile_, rpcPrivateKeyFile_));
      } else {
        RETURN_NOT_OK(tls_context->LoadCertificateAndPasswordProtectedKey(
            rpcCertificateFile_, rpcPrivateKeyFile_, [&]() {
              string ret;
              WARN_NOT_OK(
                  security::GetPasswordFromShellCommand(
                      rpcPrivateKeyPasswordCmd_, &ret),
                  "could not get RPC password from configured command");
              return ret;
            }));
      }
    } else {
      RETURN_NOT_OK(tls_context->GenerateSelfSignedCertAndKey());
    }
  }

  // See docs on Messenger::retain_self_ for info about this odd hack.
  cleanup.dismiss();
  *msgr = shared_ptr<Messenger>(
      new_msgr, std::mem_fun(&Messenger::AllExternalReferencesDropped));
  return Status::OK();
}

// See comment on Messenger::retain_self_ member.
void Messenger::AllExternalReferencesDropped() {
  // The last external ref may have been dropped in the context of a task
  // running on a reactor thread. If that's the case, a SYNC shutdown here
  // would deadlock.
  //
  // If a SYNC shutdown is desired, Shutdown() should be called explicitly.
  ShutdownInternal(ShutdownMode::ASYNC);

  CHECK(retain_self_.get());
  // If we have no more external references, then we no longer
  // need to retain ourself. We'll destruct as soon as all our
  // internal-facing references are dropped (ie those from reactor
  // threads).
  retain_self_.reset();
}

void Messenger::Shutdown() {
  ShutdownInternal(ShutdownMode::SYNC);
}

void Messenger::ShutdownInternal(ShutdownMode mode) {
  if (mode == ShutdownMode::SYNC) {
    ThreadRestrictions::assertWaitAllowed();
  }

  // Since we're shutting down, it's OK to block.
  //
  // TODO(adar): this ought to be removed (i.e. if ASYNC, waiting should be
  // forbidden, and if SYNC, we already asserted above), but that's not
  // possible while shutting down thread and acceptor pools still involves
  // joining threads.
  ThreadRestrictions::ScopedAllowWait allow_wait;

  std::shared_ptr<RpcService> serviceToRelease;

  bool closed = closing_.exchange(true);
  if (closed) {
    return;
  }
  VLOG(1) << "shutting down messenger " << name_;

  rpcService_.store(nullptr);

  // Need to shut down negotiation pool before the reactors, since the
  // reactors close the Connection sockets, and may race against the negotiation
  // threads' blocking reads & writes.
  client_negotiation_pool_->Shutdown();
  server_negotiation_pool_->Shutdown();

  for (Reactor* reactor : reactors_) {
    reactor->Shutdown(mode);
  }
}

// Register a new RpcService to handle inbound requests.
Status Messenger::RegisterService(
    const string& service_name,
    const std::shared_ptr<RpcService>& service) {
  DCHECK(service);
  std::shared_ptr<RpcService> _nullptr = nullptr;
  if (rpcService_.compare_exchange_strong(_nullptr, service)) {
    return Status::OK();
  } else {
    return Status::AlreadyPresent("This service is already present");
  }
}

void Messenger::UnregisterAllServices() {
  std::shared_ptr<RpcService> toRelease;
  rpcService_.store(nullptr);
}

void Messenger::queueOutboundCall(const shared_ptr<OutboundCall>& call) {
  Reactor* reactor = RemoteToReactor(call->conn_id().remote());
  reactor->queueOutboundCall(call);
}

void Messenger::QueueInboundCall(unique_ptr<InboundCall> call) {
  auto rpcService = rpcService_.load();
  if (PREDICT_FALSE(rpcService == nullptr)) {
    Status s = Status::ServiceUnavailable(
        fmt::format(
            "service {} not registered on {}",
            call->remote_method().serviceName(),
            name_));
    LOG(INFO) << s.ToString();
    call.release()->respondFailure(ErrorStatusPB::ERROR_NO_SUCH_SERVICE, s);
    return;
  }

  call->set_method_info(rpcService->lookupMethod(call->remote_method()));

  // The RpcService will respond to the client on success or failure.
  WARN_NOT_OK(
      rpcService->QueueInboundCall(std::move(call)),
      "Unable to handle RPC call");
}

void Messenger::queueCancellation(const shared_ptr<OutboundCall>& call) {
  Reactor* reactor = RemoteToReactor(call->conn_id().remote());
  reactor->queueCancellation(call);
}

void Messenger::registerInboundSocket(
    Socket* new_socket,
    const Sockaddr& remote) {
  Reactor* reactor = RemoteToReactor(remote);
  reactor->registerInboundSocket(new_socket, remote);
}

std::function<void()> Messenger::SignalLongInboundCall(
    std::string service,
    std::string method) {
  auto rpcService = rpcService_.load();
  if (PREDICT_FALSE(rpcService == nullptr)) {
    VLOG(2) << "No such service: " << service << "for SignalLongInboundCall";
    return {};
  }
  RemoteMethod remoteMethod = {std::move(service), std::move(method)};

  rpcService->NotifyLongCallLoading(remoteMethod);
  return [rpcService = std::move(rpcService),
          remoteMethod = std::move(remoteMethod)]() {
    rpcService->NotifyLongCallLoaded(remoteMethod);
  };
}

Messenger::Messenger(const MessengerBuilder& bld)
    : name_(bld.name_),
      closing_(false),
      authentication_(RpcAuthentication::REQUIRED),
      encryption_(RpcEncryption::REQUIRED),
      tls_context_(
          new security::TlsContext(bld.rpcTlsCiphers_, bld.rpcTlsMinProtocol_)),
      token_verifier_(new security::TokenVerifier()),
      rpcz_store_(new RpczStore()),
      metric_entity_(bld.metricEntity_),
      rpc_negotiation_timeout_ms_(bld.rpcNegotiationTimeoutMs_),
      retain_self_(this) {
  for (int i = 0; i < bld.numReactors_; i++) {
    reactors_.push_back(new Reactor(retain_self_, i, bld));
  }
  CHECK_OK(ThreadPoolBuilder("client-negotiator")
               .set_min_threads(bld.minNegotiationThreads_)
               .set_max_threads(bld.maxNegotiationThreads_)
               .Build(&client_negotiation_pool_));
  CHECK_OK(ThreadPoolBuilder("server-negotiator")
               .set_min_threads(bld.minNegotiationThreads_)
               .set_max_threads(bld.maxNegotiationThreads_)
               .Build(&server_negotiation_pool_));
}

Messenger::~Messenger() {
  CHECK(closing_) << "Should have already shut down";
  // Delete all reactors.
  for (auto* reactor : reactors_) {
    delete reactor;
  }
  reactors_.clear();
}

Reactor* Messenger::RemoteToReactor(const Sockaddr& remote) {
  uint32_t hashCode = remote.HashCode();
  int reactor_idx = hashCode % reactors_.size();
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[reactor_idx];
}

Status Messenger::Init() {
  RETURN_NOT_OK(tls_context_->Init());
  for (Reactor* r : reactors_) {
    RETURN_NOT_OK(r->Init());
  }

  return Status::OK();
}

Status Messenger::dumpRunningRpcs(
    const DumpRunningRpcsRequestPB& req,
    DumpRunningRpcsResponsePB* resp) {
  for (Reactor* reactor : reactors_) {
    RETURN_NOT_OK(reactor->dumpRunningRpcs(req, resp));
  }
  return Status::OK();
}

void Messenger::queueResetConnections() {
  for (Reactor* reactor : reactors_) {
    reactor->queueResetConnections();
  }
}

void Messenger::ScheduleOnReactor(
    const boost::function<void(const Status&)>& func,
    MonoDelta when) {
  DCHECK(!reactors_.empty());

  // If we're already running on a reactor thread, reuse it.
  Reactor* chosen = nullptr;
  for (Reactor* r : reactors_) {
    if (r->isCurrentThread()) {
      chosen = r;
    }
  }
  if (chosen == nullptr) {
    // Not running on a reactor thread, pick one at random.
    chosen = reactors_[rand() % reactors_.size()];
  }

  DelayedTask* task = new DelayedTask(func, when);
  chosen->scheduleReactorTask(task);
}

const std::shared_ptr<RpcService> Messenger::rpc_service(
    const string& service_name) const {
  return rpcService_.load();
}

ThreadPool* Messenger::negotiation_pool(ConnectionDirection dir) {
  switch (dir) {
    case ConnectionDirection::kClient:
      return client_negotiation_pool_.get();
    case ConnectionDirection::kServer:
      return server_negotiation_pool_.get();
  }
  DCHECK(false) << "Unknown ConnectionDirection value: " << dir;
  return nullptr;
}

} // namespace rpc
} // namespace kudu
