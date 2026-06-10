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

MessengerBuilder& MessengerBuilder::setConnectionKeepaliveTime(
    const MonoDelta& keepalive) {
  connectionKeepaliveTime_ = keepalive;
  return *this;
}

MessengerBuilder& MessengerBuilder::setNumReactors(int numReactors) {
  numReactors_ = numReactors;
  return *this;
}

MessengerBuilder& MessengerBuilder::setMinNegotiationThreads(
    int minNegotiationThreads) {
  minNegotiationThreads_ = minNegotiationThreads;
  return *this;
}

MessengerBuilder& MessengerBuilder::setMaxNegotiationThreads(
    int maxNegotiationThreads) {
  maxNegotiationThreads_ = maxNegotiationThreads;
  return *this;
}

MessengerBuilder& MessengerBuilder::setCoarseTimerGranularity(
    const MonoDelta& granularity) {
  coarseTimerGranularity_ = granularity;
  return *this;
}

MessengerBuilder& MessengerBuilder::setMetricEntity(
    const std::shared_ptr<MetricEntity>& metricEntity) {
  metricEntity_ = metricEntity;
  return *this;
}

MessengerBuilder& MessengerBuilder::setConnectionKeepAliveTime(
    int32_t timeInMs) {
  connectionKeepaliveTime_ = MonoDelta::FromMilliseconds(timeInMs);
  return *this;
}

MessengerBuilder& MessengerBuilder::setRpcNegotiationTimeoutMs(
    int64_t timeInMs) {
  rpcNegotiationTimeoutMs_ = timeInMs;
  return *this;
}

MessengerBuilder& MessengerBuilder::setRpcAuthentication(
    const std::string& rpcAuthentication) {
  rpcAuthentication_ = rpcAuthentication;
  return *this;
}

MessengerBuilder& MessengerBuilder::setRpcEncryption(
    const std::string& rpcEncryption) {
  rpcEncryption_ = rpcEncryption;
  return *this;
}

MessengerBuilder& MessengerBuilder::setRpcTlsCiphers(
    const std::string& rpcTlsCiphers) {
  rpcTlsCiphers_ = rpcTlsCiphers;
  return *this;
}

MessengerBuilder& MessengerBuilder::setRpcTlsMinProtocol(
    const std::string& rpcTlsMinProtocol) {
  rpcTlsMinProtocol_ = rpcTlsMinProtocol;
  return *this;
}

MessengerBuilder& MessengerBuilder::setEpkiCertKeyFiles(
    const std::string& cert,
    const std::string& privateKey) {
  rpcCertificateFile_ = cert;
  rpcPrivateKeyFile_ = privateKey;
  return *this;
}

MessengerBuilder& MessengerBuilder::setEpkiCertificateAuthorityFile(
    const std::string& ca) {
  rpcCaCertificateFile_ = ca;
  return *this;
}

MessengerBuilder& MessengerBuilder::setEpkiPrivatePasswordKeyCmd(
    const std::string& cmd) {
  rpcPrivateKeyPasswordCmd_ = cmd;
  return *this;
}

MessengerBuilder& MessengerBuilder::enableInboundTls() {
  enableInboundTls_ = true;
  return *this;
}

Status MessengerBuilder::build(shared_ptr<Messenger>* msgr) {
  Messenger* new_msgr(new Messenger(*this));

  auto cleanup =
      folly::makeGuard([&]() { new_msgr->allExternalReferencesDropped(); });

  RETURN_NOT_OK(parseTriState(
      "--rpc_authentication", rpcAuthentication_, &new_msgr->authentication_));

  RETURN_NOT_OK(parseTriState(
      "--rpc_encryption", rpcEncryption_, &new_msgr->encryption_));

  RETURN_NOT_OK(new_msgr->Init());
  if (new_msgr->encryption_ != RpcEncryption::Disabled && enableInboundTls_) {
    auto* tls_context = new_msgr->mutableTlsContext();

    if (!rpcCertificateFile_.empty()) {
      CHECK(!rpcPrivateKeyFile_.empty());
      CHECK(!rpcCaCertificateFile_.empty());

      // TODO(KUDU-1920): should we try and enforce that the server
      // is in the subject or alt names of the cert?
      RETURN_NOT_OK(
          tls_context->loadCertificateAuthority(rpcCaCertificateFile_));
      if (rpcPrivateKeyPasswordCmd_.empty()) {
        RETURN_NOT_OK(tls_context->loadCertificateAndKey(
            rpcCertificateFile_, rpcPrivateKeyFile_));
      } else {
        RETURN_NOT_OK(tls_context->loadCertificateAndPasswordProtectedKey(
            rpcCertificateFile_, rpcPrivateKeyFile_, [&]() {
              string ret;
              WARN_NOT_OK(
                  security::getPasswordFromShellCommand(
                      rpcPrivateKeyPasswordCmd_, &ret),
                  "could not get RPC password from configured command");
              return ret;
            }));
      }
    } else {
      RETURN_NOT_OK(tls_context->generateSelfSignedCertAndKey());
    }
  }

  // See docs on Messenger::retainSelf_ for info about this odd hack.
  cleanup.dismiss();
  *msgr = shared_ptr<Messenger>(
      new_msgr, std::mem_fun(&Messenger::allExternalReferencesDropped));
  return Status::OK();
}

// See comment on Messenger::retainSelf_ member.
void Messenger::allExternalReferencesDropped() {
  // The last external ref may have been dropped in the context of a task
  // running on a reactor thread. If that's the case, a Sync shutdown here
  // would deadlock.
  //
  // If a Sync shutdown is desired, Shutdown() should be called explicitly.
  shutdownInternal(ShutdownMode::Async);

  CHECK(retainSelf_.get());
  // If we have no more external references, then we no longer
  // need to retain ourself. We'll destruct as soon as all our
  // internal-facing references are dropped (ie those from reactor
  // threads).
  retainSelf_.reset();
}

void Messenger::Shutdown() {
  shutdownInternal(ShutdownMode::Sync);
}

void Messenger::shutdownInternal(ShutdownMode mode) {
  if (mode == ShutdownMode::Sync) {
    ThreadRestrictions::assertWaitAllowed();
  }

  // Since we're shutting down, it's OK to block.
  //
  // TODO(adar): this ought to be removed (i.e. if Async, waiting should be
  // forbidden, and if Sync, we already asserted above), but that's not
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
  clientNegotiationPool_->Shutdown();
  serverNegotiationPool_->Shutdown();

  for (Reactor* reactor : reactors_) {
    reactor->shutdown(mode);
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
  Reactor* reactor = remoteToReactor(call->connId().remote());
  reactor->queueOutboundCall(call);
}

void Messenger::queueInboundCall(unique_ptr<InboundCall> call) {
  auto rpcService = rpcService_.load();
  if (PREDICT_FALSE(rpcService == nullptr)) {
    Status s = Status::ServiceUnavailable(
        fmt::format(
            "service {} not registered on {}",
            call->remoteMethod().serviceName(),
            name_));
    LOG(INFO) << s.ToString();
    call.release()->respondFailure(ErrorStatusPB::ERROR_NO_SUCH_SERVICE, s);
    return;
  }

  call->setMethodInfo(rpcService->lookupMethod(call->remoteMethod()));

  // The RpcService will respond to the client on success or failure.
  WARN_NOT_OK(
      rpcService->queueInboundCall(std::move(call)),
      "Unable to handle RPC call");
}

void Messenger::queueCancellation(const shared_ptr<OutboundCall>& call) {
  Reactor* reactor = remoteToReactor(call->connId().remote());
  reactor->queueCancellation(call);
}

void Messenger::registerInboundSocket(
    Socket* new_socket,
    const Sockaddr& remote) {
  Reactor* reactor = remoteToReactor(remote);
  reactor->registerInboundSocket(new_socket, remote);
}

std::function<void()> Messenger::signalLongInboundCall(
    std::string service,
    std::string method) {
  auto rpcService = rpcService_.load();
  if (PREDICT_FALSE(rpcService == nullptr)) {
    VLOG(2) << "No such service: " << service << "for signalLongInboundCall";
    return {};
  }
  RemoteMethod remoteMethod = {std::move(service), std::move(method)};

  rpcService->notifyLongCallLoading(remoteMethod);
  return [rpcService = std::move(rpcService),
          remoteMethod = std::move(remoteMethod)]() {
    rpcService->notifyLongCallLoaded(remoteMethod);
  };
}

Messenger::Messenger(const MessengerBuilder& bld)
    : name_(bld.name_),
      closing_(false),
      authentication_(RpcAuthentication::Required),
      encryption_(RpcEncryption::Required),
      tlsContext_(
          new security::TlsContext(bld.rpcTlsCiphers_, bld.rpcTlsMinProtocol_)),
      tokenVerifier_(new security::TokenVerifier()),
      rpczStore_(new RpczStore()),
      metricEntity_(bld.metricEntity_),
      rpcNegotiationTimeoutMs_(bld.rpcNegotiationTimeoutMs_),
      retainSelf_(this) {
  for (int i = 0; i < bld.numReactors_; i++) {
    reactors_.push_back(new Reactor(retainSelf_, i, bld));
  }
  CHECK_OK(ThreadPoolBuilder("client-negotiator")
               .setMinThreads(bld.minNegotiationThreads_)
               .setMaxThreads(bld.maxNegotiationThreads_)
               .build(&clientNegotiationPool_));
  CHECK_OK(ThreadPoolBuilder("server-negotiator")
               .setMinThreads(bld.minNegotiationThreads_)
               .setMaxThreads(bld.maxNegotiationThreads_)
               .build(&serverNegotiationPool_));
}

Messenger::~Messenger() {
  CHECK(closing_) << "Should have already shut down";
  // Delete all reactors.
  for (auto* reactor : reactors_) {
    delete reactor;
  }
  reactors_.clear();
}

Reactor* Messenger::remoteToReactor(const Sockaddr& remote) {
  uint32_t hashCode = remote.HashCode();
  int reactor_idx = hashCode % reactors_.size();
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[reactor_idx];
}

Status Messenger::Init() {
  RETURN_NOT_OK(tlsContext_->init());
  for (Reactor* r : reactors_) {
    RETURN_NOT_OK(r->init());
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
      return clientNegotiationPool_.get();
    case ConnectionDirection::kServer:
      return serverNegotiationPool_.get();
  }
  DCHECK(false) << "Unknown ConnectionDirection value: " << dir;
  return nullptr;
}

} // namespace rpc
} // namespace kudu
