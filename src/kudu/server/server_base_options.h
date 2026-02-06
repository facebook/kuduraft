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
#ifndef KUDU_SERVER_SERVER_BASE_OPTIONS_H
#define KUDU_SERVER_SERVER_BASE_OPTIONS_H

#include <cstdint>
#include <string>

#include "kudu/fs/fs_manager.h"
#include "kudu/server/rpc_server.h"

namespace kudu {

class Env;

namespace server {

// Options common to both types of servers.
// The subclass constructor should fill these in with defaults from
// server-specific flags.
struct ServerBaseOptions {
  Env* env;

  FsManagerOpts fsOpts;
  RpcServerOptions rpcOpts;

  std::string dumpInfoPath;
  std::string dumpInfoFormat;
  std::string appProvidedInstanceUuid;

  std::string metricsLogDir;
  int32_t metricsLogIntervalMs;

 protected:
  ServerBaseOptions();
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_OPTIONS_H */
