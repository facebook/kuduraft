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
#ifndef KUDU_RPC_RPC_TEST_BASE_H
#define KUDU_RPC_RPC_TEST_BASE_H

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>

#include "kudu/gutil/walltime.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/rtest.service.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/security-test-util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

DECLARE_bool(rpc_encrypt_loopback_connections);

namespace kudu {
namespace rpc {

using kudu::rpc_test::AddRequestPB;
using kudu::rpc_test::AddResponsePB;
using kudu::rpc_test::CalculatorError;
using kudu::rpc_test::CalculatorServiceIf;
using kudu::rpc_test::CalculatorServiceProxy;
using kudu::rpc_test::EchoRequestPB;
using kudu::rpc_test::EchoResponsePB;
using kudu::rpc_test::ExactlyOnceRequestPB;
using kudu::rpc_test::ExactlyOnceResponsePB;
using kudu::rpc_test::FeatureFlags;
using kudu::rpc_test::PanicRequestPB;
using kudu::rpc_test::PanicResponsePB;
using kudu::rpc_test::PushTwoStringsRequestPB;
using kudu::rpc_test::PushTwoStringsResponsePB;
using kudu::rpc_test::SendTwoStringsRequestPB;
using kudu::rpc_test::SendTwoStringsResponsePB;
using kudu::rpc_test::SleepRequestPB;
using kudu::rpc_test::SleepResponsePB;
using kudu::rpc_test::SleepWithSidecarRequestPB;
using kudu::rpc_test::SleepWithSidecarResponsePB;
using kudu::rpc_test::TestInvalidResponseRequestPB;
using kudu::rpc_test::TestInvalidResponseResponsePB;
using kudu::rpc_test::WhoAmIRequestPB;
using kudu::rpc_test::WhoAmIResponsePB;
using kudu::rpc_test_diff_package::ReqDiffPackagePB;
using kudu::rpc_test_diff_package::RespDiffPackagePB;

// Implementation of CalculatorService which just implements the generic
// RPC handler (no generated code).
class GenericCalculatorService : public ServiceIf {
 public:
  static const char* kFullServiceName;
  static const char* kAddMethodName;
  static const char* kSleepMethodName;
  static const char* kSleepWithSidecarMethodName;
  static const char* kPushTwoStringsMethodName;
  static const char* kSendTwoStringsMethodName;
  static const char* kAddExactlyOnce;

  static const char* kFirstString;
  static const char* kSecondString;

  GenericCalculatorService() {}

  // To match the argument list of the generated CalculatorService.
  explicit GenericCalculatorService(
      const std::shared_ptr<MetricEntity>& entity,
      const std::shared_ptr<ResultTracker>& resultTracker) {
    // this test doesn't generate metrics, so we ignore the argument.
  }

  void handle(InboundCall* incoming) override {
    if (incoming->remoteMethod().methodName() == kAddMethodName) {
      doAdd(incoming);
    } else if (incoming->remoteMethod().methodName() == kSleepMethodName) {
      doSleep(incoming);
    } else if (
        incoming->remoteMethod().methodName() == kSleepWithSidecarMethodName) {
      doSleepWithSidecar(incoming);
    } else if (
        incoming->remoteMethod().methodName() == kSendTwoStringsMethodName) {
      doSendTwoStrings(incoming);
    } else if (
        incoming->remoteMethod().methodName() == kPushTwoStringsMethodName) {
      doPushTwoStrings(incoming);
    } else {
      incoming->respondFailure(
          ErrorStatusPB::ERROR_NO_SUCH_METHOD,
          Status::InvalidArgument("bad method"));
    }
  }

  void notifyLongCallLoading(const RemoteMethod& method) {}
  void notifyLongCallLoaded(const RemoteMethod& method) {}

  std::string serviceName() const override {
    return kFullServiceName;
  }
  static std::string staticServiceName() {
    return kFullServiceName;
  }

 private:
  void doAdd(InboundCall* incoming) {
    Slice param(incoming->serializedRequest());
    AddRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.toDebugString();
    }

    AddResponsePB resp;
    resp.set_result(req.x() + req.y());
    incoming->respondSuccess(resp);
  }

  void doSendTwoStrings(InboundCall* incoming) {
    Slice param(incoming->serializedRequest());
    SendTwoStringsRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.toDebugString();
    }

    std::unique_ptr<faststring> first(new faststring);
    std::unique_ptr<faststring> second(new faststring);

    Random r(req.random_seed());
    first->resize(req.size1());
    randomString(first->data(), req.size1(), &r);

    second->resize(req.size2());
    randomString(second->data(), req.size2(), &r);

    SendTwoStringsResponsePB resp;
    int idx1, idx2;
    CHECK_OK(incoming->addOutboundSidecar(
        RpcSidecar::fromFaststring(std::move(first)), &idx1));
    CHECK_OK(incoming->addOutboundSidecar(
        RpcSidecar::fromFaststring(std::move(second)), &idx2));
    resp.set_sidecar1(idx1);
    resp.set_sidecar2(idx2);

    incoming->respondSuccess(resp);
  }

  void doPushTwoStrings(InboundCall* incoming) {
    Slice param(incoming->serializedRequest());
    PushTwoStringsRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      LOG(FATAL) << "couldn't parse: " << param.toDebugString();
    }

    Slice sidecar1;
    CHECK_OK(incoming->getInboundSidecar(req.sidecar1_idx(), &sidecar1));

    Slice sidecar2;
    CHECK_OK(incoming->getInboundSidecar(req.sidecar2_idx(), &sidecar2));

    // Check that reading non-existant sidecars doesn't work.
    Slice tmp;
    CHECK(!incoming->getInboundSidecar(req.sidecar2_idx() + 2, &tmp).ok());

    PushTwoStringsResponsePB resp;
    resp.set_size1(sidecar1.size());
    resp.set_data1(
        reinterpret_cast<const char*>(sidecar1.data()), sidecar1.size());
    resp.set_size2(sidecar2.size());
    resp.set_data2(
        reinterpret_cast<const char*>(sidecar2.data()), sidecar2.size());

    // Drop the sidecars etc, just to confirm that it's safe to do so.
    CHECK_GT(incoming->getTransferSize(), 0);
    incoming->discardTransfer();
    CHECK_EQ(0, incoming->getTransferSize());
    incoming->respondSuccess(resp);
  }

  void doSleep(InboundCall* incoming) {
    Slice param(incoming->serializedRequest());
    SleepRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      incoming->respondFailure(
          ErrorStatusPB::ERROR_INVALID_REQUEST,
          Status::InvalidArgument(
              "Couldn't parse pb", req.InitializationErrorString()));
      return;
    }

    LOG(INFO) << "got call: " << pb_util::SecureShortDebugString(req);
    SleepFor(MonoDelta::FromMicroseconds(req.sleep_micros()));
    MonoDelta duration(
        MonoTime::Now().GetDeltaSince(incoming->getTimeReceived()));
    CHECK_GE(duration.ToMicroseconds(), req.sleep_micros());
    SleepResponsePB resp;
    incoming->respondSuccess(resp);
  }

  void doSleepWithSidecar(InboundCall* incoming) {
    Slice param(incoming->serializedRequest());
    SleepWithSidecarRequestPB req;
    if (!req.ParseFromArray(param.data(), param.size())) {
      incoming->respondFailure(
          ErrorStatusPB::ERROR_INVALID_REQUEST,
          Status::InvalidArgument(
              "Couldn't parse pb", req.InitializationErrorString()));
      return;
    }

    LOG(INFO) << "got call: " << pb_util::SecureShortDebugString(req);
    SleepFor(MonoDelta::FromMicroseconds(req.sleep_micros()));

    uint32_t pattern = req.pattern();
    uint32_t numRepetitions = req.num_repetitions();
    Slice sidecar;
    CHECK_OK(incoming->getInboundSidecar(req.sidecar_idx(), &sidecar));
    CHECK_EQ(sidecar.size(), sizeof(uint32_t) * numRepetitions);
    const uint32_t* data = reinterpret_cast<const uint32_t*>(sidecar.data());
    for (int i = 0; i < numRepetitions; ++i) {
      CHECK_EQ(data[i], pattern);
    }

    SleepResponsePB resp;
    incoming->respondSuccess(resp);
  }
};

class CalculatorService : public CalculatorServiceIf {
 public:
  explicit CalculatorService(
      const std::shared_ptr<MetricEntity>& entity,
      const std::shared_ptr<ResultTracker> resultTracker)
      : CalculatorServiceIf(entity, resultTracker), exactlyOnceTestVal_(0) {}

  void Add(const AddRequestPB* req, AddResponsePB* resp, RpcContext* context)
      override {
    CHECK_GT(context->getTransferSize(), 0);
    resp->set_result(req->x() + req->y());
    context->respondSuccess();
  }

  void Sleep(
      const SleepRequestPB* req,
      SleepResponsePB* resp,
      RpcContext* context) override {
    if (req->return_app_error()) {
      CalculatorError myError;
      myError.set_extra_error_data("some application-specific error data");
      context->respondApplicationError(
          CalculatorError::app_error_ext.number(), "Got some error", myError);
      return;
    }

    // Respond w/ error if the RPC specifies that the client deadline is set,
    // but it isn't.
    if (req->client_timeout_defined()) {
      MonoTime deadline = context->getClientDeadline();
      if (deadline == MonoTime::Max()) {
        CalculatorError myError;
        myError.set_extra_error_data("Timeout not set");
        context->respondApplicationError(
            CalculatorError::app_error_ext.number(),
            "Missing required timeout",
            myError);
        return;
      }
    }

    if (req->deferred()) {
      // Spawn a new thread which does the sleep and responds later.
      std::shared_ptr<Thread> thread;
      CHECK_OK(
          Thread::create(
              "rpc-test",
              "deferred",
              &CalculatorService::doSleep,
              this,
              req,
              context,
              &thread));
      return;
    }
    doSleep(req, context);
  }

  void Echo(const EchoRequestPB* req, EchoResponsePB* resp, RpcContext* context)
      override {
    resp->set_data(req->data());
    context->respondSuccess();
  }

  void WhoAmI(
      const WhoAmIRequestPB* /*req*/,
      WhoAmIResponsePB* resp,
      RpcContext* context) override {
    const RemoteUser& user = context->remoteUser();
    resp->mutable_credentials()->set_real_user(user.username());
    resp->set_address(context->remoteAddress().ToString());
    context->respondSuccess();
  }

  void TestArgumentsInDiffPackage(
      const ReqDiffPackagePB* req,
      RespDiffPackagePB* resp,
      ::kudu::rpc::RpcContext* context) override {
    context->respondSuccess();
  }

  void Panic(
      const PanicRequestPB* req,
      PanicResponsePB* resp,
      RpcContext* context) override {
    TRACE("Got panic request");
    PANIC_RPC(context, "Test method panicking!");
  }

  void TestInvalidResponse(
      const TestInvalidResponseRequestPB* req,
      TestInvalidResponseResponsePB* resp,
      RpcContext* context) override {
    switch (req->error_type()) {
      case rpc_test::
          TestInvalidResponseRequestPB_ErrorType_MISSING_REQUIRED_FIELD:
        // Respond without setting the 'resp->response' protobuf field, which is
        // marked as required. This exercises the error path of invalid
        // responses.
        context->respondSuccess();
        break;
      case rpc_test::TestInvalidResponseRequestPB_ErrorType_RESPONSE_TOO_LARGE:
        resp->mutable_response()->resize(FLAGS_rpc_max_message_size + 1000);
        context->respondSuccess();
        break;
      default:
        LOG(FATAL);
    }
  }

  bool supportsFeature(uint32_t feature) const override {
    return feature == FeatureFlags::FOO;
  }

  void AddExactlyOnce(
      const ExactlyOnceRequestPB* req,
      ExactlyOnceResponsePB* resp,
      ::kudu::rpc::RpcContext* context) override {
    if (req->sleep_for_ms() > 0) {
      usleep(req->sleep_for_ms() * 1000);
    }
    // If failures are enabled, cause them some percentage of the time.
    if (req->randomly_fail()) {
      if (rand() % 10 < 3) {
        context->respondFailure(
            Status::ServiceUnavailable("Random injected failure."));
        return;
      }
    }
    int result = exactlyOnceTestVal_ += req->value_to_add();
    resp->set_current_val(result);
    resp->set_current_time_micros(getCurrentTimeMicros());
    context->respondSuccess();
  }

  bool AuthorizeDisallowAlice(
      const google::protobuf::Message* /*req*/,
      google::protobuf::Message* /*resp*/,
      RpcContext* context) override {
    if (context->remoteUser().username() == "alice") {
      context->respondFailure(
          Status::NotAuthorized("alice is not allowed to call this method"));
      return false;
    }
    return true;
  }

  bool AuthorizeDisallowBob(
      const google::protobuf::Message* /*req*/,
      google::protobuf::Message* /*resp*/,
      RpcContext* context) override {
    if (context->remoteUser().username() == "bob") {
      context->respondFailure(
          Status::NotAuthorized("bob is not allowed to call this method"));
      return false;
    }
    return true;
  }

 private:
  void doSleep(const SleepRequestPB* req, RpcContext* context) {
    TRACE_COUNTER_INCREMENT("test_sleep_us", req->sleep_micros());
    if (Trace::currentTrace()) {
      std::shared_ptr<Trace> childTrace = std::make_shared<Trace>();
      Trace::currentTrace()->addChildTrace("test_child", childTrace);
      ADOPT_TRACE(childTrace);
      TRACE_COUNTER_INCREMENT("related_trace_metric", 1);
    }

    SleepFor(MonoDelta::FromMicroseconds(req->sleep_micros()));
    context->respondSuccess();
  }

  std::atomic_int exactlyOnceTestVal_;
};

const char* GenericCalculatorService::kFullServiceName =
    "kudu.rpc.GenericCalculatorService";
const char* GenericCalculatorService::kAddMethodName = "Add";
const char* GenericCalculatorService::kSleepMethodName = "Sleep";
const char* GenericCalculatorService::kSleepWithSidecarMethodName =
    "SleepWithSidecar";
const char* GenericCalculatorService::kPushTwoStringsMethodName =
    "PushTwoStrings";
const char* GenericCalculatorService::kSendTwoStringsMethodName =
    "SendTwoStrings";
const char* GenericCalculatorService::kAddExactlyOnce = "AddExactlyOnce";

const char* GenericCalculatorService::kFirstString =
    "1111111111111111111111111111111111111111111111111111111111";
const char* GenericCalculatorService::kSecondString =
    "2222222222222222222222222222222222222222222222222222222222222222222222";

class RpcTestBase : public KuduTest {
 public:
  RpcTestBase()
      : nWorkerThreads_(3),
        serviceQueueLength_(100),
        nServerReactorThreads_(3),
        keepaliveTimeMs_(1000),
        metricEntity_(METRIC_ENTITY_server.instantiate(
            &metricRegistry_,
            "test.rpc_test")) {
    FLAGS_skip_verify_tls_cert = true;
  }

  void TearDown() override {
    if (acceptorPool_) {
      acceptorPool_->shutdown();
      acceptorPool_.reset();
    }
    if (servicePool_) {
      serverMessenger_->UnregisterAllServices();
      servicePool_->shutdown();
    }
    if (serverMessenger_) {
      serverMessenger_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  Status createMessenger(
      const std::string& name,
      std::shared_ptr<Messenger>* messenger,
      int nReactors = 1,
      bool enableSsl = true,
      const std::string& rpcCertificateFile = "",
      const std::string& rpcPrivateKeyFile = "",
      const std::string& rpcCaCertificateFile = "",
      const std::string& rpcPrivateKeyPasswordCmd = "") {
    MessengerBuilder bld(name);

    if (enableSsl) {
      FLAGS_rpc_encrypt_loopback_connections = true;
      bld.set_epki_cert_key_files(rpcCertificateFile, rpcPrivateKeyFile);
      bld.set_epki_certificate_authority_file(rpcCaCertificateFile);
      bld.set_epki_private_password_key_cmd(rpcPrivateKeyPasswordCmd);
      bld.set_rpc_encryption("required");
      bld.enable_inbound_tls();
    }

    bld.set_num_reactors(nReactors);
    bld.set_connection_keepalive_time(
        MonoDelta::FromMilliseconds(keepaliveTimeMs_));
    if (keepaliveTimeMs_ >= 0) {
      // In order for the keepalive timing to be accurate, we need to scan
      // connections significantly more frequently than the keepalive time. This
      // "coarse timer" granularity determines this.
      bld.set_coarse_timer_granularity(
          MonoDelta::FromMilliseconds(std::min(keepaliveTimeMs_ / 5, 100)));
    }
    bld.set_metric_entity(metricEntity_);
    return bld.Build(messenger);
  }

  Status doTestSyncCall(
      const Proxy& p,
      const char* method,
      CredentialsPolicy policy = CredentialsPolicy::AnyCredentials) {
    AddRequestPB req;
    req.set_x(rand());
    req.set_y(rand());
    AddResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    controller.set_credentials_policy(policy);
    RETURN_NOT_OK(p.syncRequest(method, req, &resp, &controller));

    CHECK_EQ(req.x() + req.y(), resp.result());
    return Status::OK();
  }

  void doTestAsyncCall(
      const Proxy& p,
      const char* method,
      AddResponsePB& resp,
      RpcController& controller,
      const ResponseCallback& callback = []() {},
      CredentialsPolicy policy = CredentialsPolicy::AnyCredentials) {
    AddRequestPB req;
    req.set_x(rand());
    req.set_y(rand());
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    controller.set_credentials_policy(policy);

    p.asyncRequest(method, req, &resp, &controller, callback);
  }

  void doTestSidecar(const Proxy& p, int size1, int size2) {
    const uint32_t kSeed = 12345;

    SendTwoStringsRequestPB req;
    req.set_size1(size1);
    req.set_size2(size2);
    req.set_random_seed(kSeed);

    SendTwoStringsResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));
    CHECK_OK(p.syncRequest(
        GenericCalculatorService::kSendTwoStringsMethodName,
        req,
        &resp,
        &controller));

    Slice first = getSidecarPointer(controller, resp.sidecar1(), size1);
    Slice second = getSidecarPointer(controller, resp.sidecar2(), size2);
    Random rng(kSeed);
    faststring expected;

    expected.resize(size1);
    randomString(expected.data(), size1, &rng);
    CHECK_EQ(0, first.compare(Slice(expected)));

    expected.resize(size2);
    randomString(expected.data(), size2, &rng);
    CHECK_EQ(0, second.compare(Slice(expected)));
  }

  Status doTestOutgoingSidecar(const Proxy& p, int size1, int size2) {
    PushTwoStringsRequestPB request;
    RpcController controller;

    int idx1;
    std::string s1(size1, 'a');
    CHECK_OK(
        controller.addOutboundSidecar(RpcSidecar::fromSlice(Slice(s1)), &idx1));

    int idx2;
    std::string s2(size2, 'b');
    CHECK_OK(
        controller.addOutboundSidecar(RpcSidecar::fromSlice(Slice(s2)), &idx2));

    request.set_sidecar1_idx(idx1);
    request.set_sidecar2_idx(idx2);

    PushTwoStringsResponsePB resp;
    KUDU_RETURN_NOT_OK(p.syncRequest(
        GenericCalculatorService::kPushTwoStringsMethodName,
        request,
        &resp,
        &controller));
    CHECK_EQ(size1, resp.size1());
    CHECK_EQ(resp.data1(), s1);
    CHECK_EQ(size2, resp.size2());
    CHECK_EQ(resp.data2(), s2);
    return Status::OK();
  }

  void doTestOutgoingSidecarExpectOk(const Proxy& p, int size1, int size2) {
    CHECK_OK(doTestOutgoingSidecar(p, size1, size2));
  }

  void doTestExpectTimeout(
      const Proxy& p,
      const MonoDelta& timeout,
      bool* isNegotiationError = nullptr) {
    SleepRequestPB req;
    SleepResponsePB resp;
    // Sleep for 500ms longer than the call timeout.
    int sleepMicros = timeout.ToMicroseconds() + 500 * 1000;
    req.set_sleep_micros(sleepMicros);

    RpcController c;
    c.set_timeout(timeout);
    Stopwatch sw;
    sw.start();
    Status s = p.syncRequest(
        GenericCalculatorService::kSleepMethodName, req, &resp, &c);
    sw.stop();
    ASSERT_FALSE(s.ok());
    if (isNegotiationError != nullptr) {
      *isNegotiationError = c.negotiationFailed();
    }

    int expectedMillis = timeout.ToMilliseconds();
    int elapsedMillis = sw.elapsed().wallMillis();

    // We shouldn't timeout significantly faster than our configured timeout.
    EXPECT_GE(elapsedMillis, expectedMillis - 10);
    // And we also shouldn't take the full time that we asked for
    EXPECT_LT(elapsedMillis * 1000, sleepMicros);
    EXPECT_TRUE(s.IsTimedOut());
    LOG(INFO) << "status: " << s.ToString()
              << ", seconds elapsed: " << sw.elapsed().wallSeconds();
  }

  Status startTestServer(
      Sockaddr* serverAddr,
      bool enableSsl = false,
      const std::string& rpcCertificateFile = "",
      const std::string& rpcPrivateKeyFile = "",
      const std::string& rpcCaCertificateFile = "",
      const std::string& rpcPrivateKeyPasswordCmd = "",
      const std::shared_ptr<Messenger>& messenger = nullptr) {
    return doStartTestServer<GenericCalculatorService>(
        serverAddr,
        enableSsl,
        rpcCertificateFile,
        rpcPrivateKeyFile,
        rpcCaCertificateFile,
        rpcPrivateKeyPasswordCmd,
        messenger);
  }

  Status startTestServerWithGeneratedCode(
      Sockaddr* serverAddr,
      bool enableSsl = false) {
    return doStartTestServer<CalculatorService>(serverAddr, enableSsl);
  }

  Status startTestServerWithCustomMessenger(
      Sockaddr* serverAddr,
      const std::shared_ptr<Messenger>& messenger,
      bool enableSsl = false) {
    return doStartTestServer<GenericCalculatorService>(
        serverAddr, enableSsl, "", "", "", "", messenger);
  }

  // Start a simple socket listening on a local port, returning the address.
  // This isn't an RPC server -- just a plain socket which can be helpful for
  // testing.
  Status startFakeServer(Socket* listenSock, Sockaddr* listenAddr) {
    Sockaddr bindAddr;
    bindAddr.set_port(0);
    RETURN_NOT_OK(listenSock->init(0));
    RETURN_NOT_OK(listenSock->bindAndListen(bindAddr, 1));
    RETURN_NOT_OK(listenSock->getSocketAddress(listenAddr));
    LOG(INFO) << "Bound to: " << listenAddr->ToString();
    return Status::OK();
  }

 private:
  static Slice getSidecarPointer(
      const RpcController& controller,
      int idx,
      int expectedSize) {
    Slice sidecar;
    CHECK_OK(controller.getInboundSidecar(idx, &sidecar));
    CHECK_EQ(expectedSize, sidecar.size());
    return Slice(sidecar.data(), expectedSize);
  }

  template <class ServiceClass>
  Status doStartTestServer(
      Sockaddr* serverAddr,
      bool enableSsl = false,
      const std::string& rpcCertificateFile = "",
      const std::string& rpcPrivateKeyFile = "",
      const std::string& rpcCaCertificateFile = "",
      const std::string& rpcPrivateKeyPasswordCmd = "",
      const std::shared_ptr<Messenger>& messenger = nullptr) {
    if (!messenger) {
      RETURN_NOT_OK(createMessenger(
          "TestServer",
          &serverMessenger_,
          nServerReactorThreads_,
          enableSsl,
          rpcCertificateFile,
          rpcPrivateKeyFile,
          rpcCaCertificateFile,
          rpcPrivateKeyPasswordCmd));
    } else {
      serverMessenger_ = messenger;
    }

    Socket sock;
    RETURN_NOT_OK(sock.init(0));
    RETURN_NOT_OK(sock.setReuseAddr(true));
    RETURN_NOT_OK(sock.bind(Sockaddr()));
    Sockaddr remote;
    RETURN_NOT_OK(sock.getSocketAddress(&remote));
    acceptorPool_ =
        std::make_shared<AcceptorPool>(serverMessenger_.get(), &sock, remote);

    RETURN_NOT_OK(acceptorPool_->start(2));
    *serverAddr = acceptorPool_->bindAddress();
    memTracker_ = MemTracker::createTracker(-1, "result_tracker");
    resultTracker_.reset(new ResultTracker(memTracker_));

    std::unique_ptr<ServiceIf> service(
        new ServiceClass(metricEntity_, resultTracker_));
    serviceName_ = service->serviceName();
    std::shared_ptr<MetricEntity> metricEntity =
        serverMessenger_->metric_entity();
    servicePool_ = std::make_shared<ServicePool>(
        std::move(service), metricEntity, serviceQueueLength_);
    serverMessenger_->RegisterService(serviceName_, servicePool_);
    RETURN_NOT_OK(servicePool_->init(nWorkerThreads_));

    return Status::OK();
  }

 protected:
  std::string serviceName_;
  std::shared_ptr<Messenger> serverMessenger_;
  std::shared_ptr<ServicePool> servicePool_;
  std::shared_ptr<AcceptorPool> acceptorPool_;
  std::shared_ptr<kudu::MemTracker> memTracker_;
  std::shared_ptr<ResultTracker> resultTracker_;
  int nWorkerThreads_;
  int serviceQueueLength_;
  int nServerReactorThreads_;
  int keepaliveTimeMs_;

  MetricRegistry metricRegistry_;
  std::shared_ptr<MetricEntity> metricEntity_;
};

} // namespace rpc
} // namespace kudu
#endif
