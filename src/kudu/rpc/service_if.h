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
#ifndef KUDU_RPC_SERVICE_IF_H
#define KUDU_RPC_SERVICE_IF_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <google/protobuf/message.h>

#include "kudu/util/metrics.h"

namespace kudu {
namespace rpc {

class InboundCall;
class RemoteMethod;
class ResultTracker;
class RpcContext;

// Generated services define an instance of this class for each
// method that they implement. The generic server code implemented
// by GeneratedServiceIf look up the RpcMethodInfo in order to handle
// each RPC.
//
// Inherits from enable_shared_from_this to document that this object
// is managed by shared_ptr (stored in GeneratedServiceIf::methodsByName_)
// and to allow conversion from raw pointer to shared_ptr if needed in future.
struct RpcMethodInfo : public std::enable_shared_from_this<RpcMethodInfo> {
  // Prototype protobufs for requests and responses.
  // These are empty protobufs which are cloned in order to provide an
  // instance for each request.
  std::unique_ptr<google::protobuf::Message> reqPrototype;
  std::unique_ptr<google::protobuf::Message> respPrototype;

  std::shared_ptr<Histogram> handlerLatencyHistogram;

  // Whether we should track this method's result, using ResultTracker.
  bool trackResult;

  // The authorization function for this RPC. If this function
  // returns false, the RPC has already been handled (i.e. rejected)
  // by the authorization function.
  std::function<bool(
      const google::protobuf::Message* req,
      google::protobuf::Message* resp,
      RpcContext* ctx)>
      authzMethod;

  // The actual function to be called.
  std::function<void(
      const google::protobuf::Message* req,
      google::protobuf::Message* resp,
      RpcContext* ctx)>
      func;

  std::function<void()> longCallLoadingHook = []() {};

  std::function<void()> longCallLoadedHook = []() {};
};

// Handles incoming messages that initiate an RPC.
class ServiceIf {
 public:
  virtual ~ServiceIf();
  virtual void Handle(InboundCall* incoming) = 0;
  virtual void NotifyLongCallLoading(const RemoteMethod& method) = 0;
  virtual void NotifyLongCallLoaded(const RemoteMethod& method) = 0;
  virtual void Shutdown();
  virtual std::string service_name() const = 0;

  // The service should return true if it supports the provided application
  // specific feature flag.
  virtual bool supportsFeature(uint32_t feature) const;

  // Look up the method being requested by the remote call.
  //
  // Returns a raw pointer to the RpcMethodInfo. The lifetime is guaranteed
  // by the Service, which owns the method info and outlives all InboundCalls.
  // Returns nullptr if the method is not found.
  virtual RpcMethodInfo* lookupMethod(const RemoteMethod& method);

  // Default authorization method, which just allows all RPCs.
  //
  // See docs/design-docs/rpc.md for details on how to add custom
  // authorization checks to a service.
  bool authorizeAllowAll(
      const google::protobuf::Message* /*req*/,
      google::protobuf::Message* /*resp*/,
      RpcContext* /*ctx*/) {
    return true;
  }

  virtual void longCallLoading() {}
  virtual void longCallLoaded() {}

 protected:
  bool parseParam(InboundCall* call, google::protobuf::Message* message);
  void respondBadMethod(InboundCall* call);
};

// Base class for code-generated service classes.
class GeneratedServiceIf : public ServiceIf {
 public:
  virtual ~GeneratedServiceIf();

  // Looks up the appropriate method in 'methodsByName_' and executes
  // it on the current thread.
  //
  // If no such method is found, responds with an error.
  void Handle(InboundCall* incoming) override;

  void NotifyLongCallLoading(const RemoteMethod& method) override;

  void NotifyLongCallLoaded(const RemoteMethod& method) override;

  RpcMethodInfo* lookupMethod(const RemoteMethod& method) override;

  // Returns the mapping from method names to method infos.
  using MethodInfoMap =
      std::unordered_map<std::string, std::shared_ptr<RpcMethodInfo>>;
  const MethodInfoMap& methodsByName() const {
    return methodsByName_;
  }

 protected:
  // For each method, stores the relevant information about how to handle the
  // call. Methods are inserted by the constructor of the generated subclass.
  // After construction, this map is accessed by multiple threads and therefore
  // must not be modified.
  MethodInfoMap methodsByName_;

  // The result tracker for this service's methods.
  std::shared_ptr<ResultTracker> resultTracker_;
};

} // namespace rpc
} // namespace kudu
#endif
