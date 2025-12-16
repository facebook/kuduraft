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
#ifndef KUDU_TSERVER_TABLET_SERVER_OPTIONS_H
#define KUDU_TSERVER_TABLET_SERVER_OPTIONS_H

#include <memory>
#include <vector>

#include "kudu/consensus/StateMachineMetricsInterface.h"
#include "kudu/consensus/leader_election.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/proxy_policy.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/net/net_util.h"

namespace kudu {
namespace log {
class LogFactory;
}
namespace consensus {
class ConsensusRoundHandler;
class OpId;
struct ElectionResult;
struct ElectionContext;
} // namespace consensus

namespace KC = kudu::consensus;

namespace tserver {

// Options for constructing a tablet server.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
//
// This allows tests to easily start miniclusters with different
// tablet servers having different options.
struct TabletServerOptions : public kudu::server::ServerBaseOptions {
  TabletServerOptions();

  std::vector<HostPort> tserverAddresses;
  std::vector<std::string> tserverRegions;
  std::vector<bool> tserverBbd;

  // bootstrap tservers can be directly passed in
  // by application
  std::vector<KC::RaftPeerPB> bootstrapTservers;

  std::shared_ptr<kudu::log::LogFactory> logFactory;

  std::shared_ptr<kudu::consensus::VoteLoggerInterface> voteLogger;

  kudu::consensus::ConsensusRoundHandler* roundHandler = nullptr;

  std::shared_ptr<kudu::consensus::StateMachineMetricsInterface>
      stateMachineMetrics = nullptr;

  kudu::consensus::ProxyPolicy proxyPolicy =
      kudu::consensus::ProxyPolicy::DURABLE_ROUTING_POLICY;

  std::vector<std::unordered_set<std::string>> proxyRegionGroups = {};

  // Election Decision Callback
  std::function<
      void(const consensus::ElectionResult&, const consensus::ElectionContext&)>
      edcb;

  // Term Advancement Callback
  std::function<void(int64_t)> tacb;

  // No-OP received Callback
  std::function<
      void(const consensus::OpId id, const kudu::consensus::RaftPeerPB&)>
      norcb;

  // Leader Detected Callback. This should eventually be reconciled
  // with NORCB.
  std::function<void(int64_t, const kudu::consensus::RaftPeerPB&)> ldcb;
  bool disableNoop = false;

  // This is to enable a fresh instance join the ring with logs from
  // a certain opid and term.
  bool logBootstrapOnFirstRun = false;

  consensus::TopologyConfigPB topologyConfig;

  // Enables functionality provided by kudu::consensus::TimeManager
  bool enableTimeManager = true;

  bool isDistributed() const;
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TABLET_SERVER_OPTIONS_H */
