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

#include "kudu/consensus/peer_manager.h"

#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/port.h"
#include "kudu/util/pb_util.h"

using kudu::pb_util::SecureShortDebugString;
using std::shared_ptr;

namespace kudu::consensus {

PeerManager::PeerManager(
    std::string tabletId,
    std::string localUuid,
    PeerProxyFactory* peerProxyFactory,
    PeerMessageQueue* queue,
    ThreadPoolToken* raftPoolToken)
    : tabletId_(std::move(tabletId)),
      localUuid_(std::move(localUuid)),
      peerProxyFactory_(peerProxyFactory),
      queue_(queue),
      raftPoolToken_(raftPoolToken) {}

PeerManager::~PeerManager() {
  close();
}

Status PeerManager::updateRaftConfig(const RaftConfigPB& config) {
  VLOG(1) << "Updating peers from new config: "
          << SecureShortDebugString(config);

  std::lock_guard<simple_spinlock> lock(lock_);

  std::vector<const RaftPeerPB*> configPeers;
  // Identify peers in the config
  for (const RaftPeerPB& peerPb : config.peers()) {
    configPeers.push_back(&peerPb);
  }
  // Identify peers for transitional config (i.e., C_old => C_old_new in Raft)
  for (const RaftPeerPB& peerPb : config.next_config_peers()) {
    configPeers.push_back(&peerPb);
  }

  // Instantiate the new peers, including proxies
  for (const RaftPeerPB* peerPbPtr : configPeers) {
    const RaftPeerPB& peerPb = *peerPbPtr;
    if (peers_.contains(peerPb.permanent_uuid())) {
      continue;
    }
    if (peerPb.permanent_uuid() == localUuid_) {
      continue;
    }

    VLOG(1) << getLogPrefix()
            << "Adding remote peer. Peer: " << SecureShortDebugString(peerPb);
    shared_ptr<PeerProxy> peerProxy;
    RETURN_NOT_OK_PREPEND(
        peerProxyFactory_->NewProxy(peerPb, &peerProxy),
        "Could not obtain a remote proxy to the peer.");
    peerProxyPool_.Put(peerPb.permanent_uuid(), peerProxy);
    std::shared_ptr<Peer> remotePeer;
    RETURN_NOT_OK(
        Peer::NewRemotePeer(
            peerPb,
            tabletId_,
            localUuid_,
            queue_,
            &peerProxyPool_,
            raftPoolToken_,
            std::move(peerProxy),
            peerProxyFactory_->messenger(),
            &remotePeer));
    peers_.emplace(peerPb.permanent_uuid(), std::move(remotePeer));
  }

  return Status::OK();
}

void PeerManager::signalRequest(
    bool forceIfQueueEmpty,
    bool isLeaderLeaseRevoke) {
  std::lock_guard<simple_spinlock> lock(lock_);
  for (auto iter = peers_.begin(); iter != peers_.end();) {
    Status s =
        (*iter).second->SignalRequest(forceIfQueueEmpty, isLeaderLeaseRevoke);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << getLogPrefix()
                   << "Peer was closed, removing from peers. Peer: "
                   << SecureShortDebugString((*iter).second->peerPb());
      peers_.erase(iter++);
    } else {
      ++iter;
    }
  }
}

Status PeerManager::startElection(
    const std::string& uuid,
    RunLeaderElectionResponsePB* resp,
    RunLeaderElectionRequestPB req) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    auto it = peers_.find(uuid);
    if (it != peers_.end()) {
      peer = it->second;
    }
  }
  if (!peer) {
    return Status::NotFound("unknown peer");
  }

  return peer->StartElection(resp, std::move(req));
}

void PeerManager::close() {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    for (const auto& entry : peers_) {
      entry.second->Close();
    }
    peers_.clear();
    peerProxyPool_.Clear();
  }
}

std::string PeerManager::getLogPrefix() const {
  return fmt::format("T {} P {}: ", tabletId_, localUuid_);
}

} // namespace kudu::consensus
