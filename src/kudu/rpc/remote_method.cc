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

#include <ostream>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"

namespace kudu {
namespace rpc {

RemoteMethod::RemoteMethod(std::string serviceName, std::string methodName)
    : serviceName_(std::move(serviceName)),
      methodName_(std::move(methodName)) {}

void RemoteMethod::fromPb(const RemoteMethodPB& pb) {
  DCHECK(pb.IsInitialized())
      << "PB is uninitialized: " << pb.InitializationErrorString();
  serviceName_ = pb.service_name();
  methodName_ = pb.method_name();
}

void RemoteMethod::toPb(RemoteMethodPB* pb) const {
  pb->set_service_name(serviceName_);
  pb->set_method_name(methodName_);
}

std::string RemoteMethod::toString() const {
  return fmt::format("{}.{}", serviceName_, methodName_);
}

} // namespace rpc
} // namespace kudu
