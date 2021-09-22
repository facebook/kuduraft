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

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"

using kudu::log::Log;
using kudu::pb_util::SecureShortDebugString;
using std::shared_ptr;
using strings::Substitute;

DECLARE_bool(enable_flexi_raft);

namespace kudu {
namespace consensus {

PeerManager::PeerManager(std::string tablet_id,
                         std::string local_uuid,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         ThreadPoolToken* raft_pool_token,
                         scoped_refptr<log::Log> log)
    : tablet_id_(std::move(tablet_id)),
      local_uuid_(std::move(local_uuid)),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      raft_pool_token_(raft_pool_token),
      log_(std::move(log)) {
}

PeerManager::PeerManager(std::string tablet_id,
                         std::string local_uuid,
                         std::string local_region,
                         PeerProxyFactory* peer_proxy_factory,
                         PeerMessageQueue* queue,
                         ThreadPoolToken* raft_pool_token,
                         scoped_refptr<log::Log> log)
    : tablet_id_(std::move(tablet_id)),
      local_uuid_(std::move(local_uuid)),
      local_region_(std::move(local_region)),
      peer_proxy_factory_(peer_proxy_factory),
      queue_(queue),
      raft_pool_token_(raft_pool_token),
      log_(std::move(log)) {
}

PeerManager::~PeerManager() {
  Close();
}

Status PeerManager::UpdateRaftConfig(
    const RaftConfigPB& config, boost::optional<QuorumMode> quorum_mode) {
  VLOG(1) << "Updating peers from new config: " << SecureShortDebugString(config);

  std::lock_guard<simple_spinlock> lock(lock_);
  // Create new peers
  for (const RaftPeerPB& peer_pb : config.peers()) {
    if (ContainsKey(peers_, peer_pb.permanent_uuid()) ||
        ContainsKey(data_commit_quorum_peers_, peer_pb.permanent_uuid())) {
      continue;
    }
    if (peer_pb.permanent_uuid() == local_uuid_) {
      continue;
    }

    VLOG(1) << GetLogPrefix() << "Adding remote peer. Peer: " << SecureShortDebugString(peer_pb);
    shared_ptr<PeerProxy> peer_proxy;
    RETURN_NOT_OK_PREPEND(peer_proxy_factory_->NewProxy(peer_pb, &peer_proxy),
                          "Could not obtain a remote proxy to the peer.");
    peer_proxy_pool_.Put(peer_pb.permanent_uuid(), peer_proxy);
    std::shared_ptr<Peer> remote_peer;
    RETURN_NOT_OK(Peer::NewRemotePeer(peer_pb,
                                      tablet_id_,
                                      local_uuid_,
                                      queue_,
                                      &peer_proxy_pool_,
                                      raft_pool_token_,
                                      std::move(peer_proxy),
                                      peer_proxy_factory_->messenger(),
                                      &remote_peer));

    // This peer is part of data commit quorum if:
    // 1. Flexi-raft is enable in SINGLE_REGION_DYNAMIC mode
    // 2. This peer is in the same region as the local region (leader region)
    //
    // In SINGLE_REGION_DYNAMIC mode the 'data-commit' quorum is determined as
    // majority of leader-region. Hence this optimization makes sense.
    //
    // In STATIC quorum mode, the najority depends on the specified rules adn
    // involve multiple regions. Hence we cannot efficiently determine if a
    // given peer is part of the data commit quorum and it is best to treat
    // every peer the same
    bool is_data_commit_quorum_peer =
      FLAGS_enable_flexi_raft &&
      quorum_mode.has_value() &&
      quorum_mode.value() == QuorumMode::SINGLE_REGION_DYNAMIC &&
      peer_pb.has_attrs() &&
      peer_pb.attrs().has_region() &&
      (peer_pb.attrs().region() == local_region_);

    if (is_data_commit_quorum_peer) {
      data_commit_quorum_peers_.emplace(
          peer_pb.permanent_uuid(), std::move(remote_peer));
    } else {
      peers_.emplace(peer_pb.permanent_uuid(), std::move(remote_peer));
    }
  }

  return Status::OK();
}

void PeerManager::SignalRequest(bool force_if_queue_empty) {
  std::lock_guard<simple_spinlock> lock(lock_);

  // First signal the peers who are part of data commit quorum. This enables
  // faster commits when FR is enabled
  SignalRequest(data_commit_quorum_peers_, force_if_queue_empty);

  // Now signal all other peers
  SignalRequest(peers_, force_if_queue_empty);
}

void PeerManager::SignalRequest(
    std::unordered_map<std::string, std::shared_ptr<Peer>>& peers,
    bool force_if_queue_empty) {
  DCHECK(lock_.is_locked());

  for (auto iter = peers.begin(); iter != peers.end();) {
    Status s = (*iter).second->SignalRequest(force_if_queue_empty);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << GetLogPrefix()
                   << "Peer was closed, removing from peers. Peer: "
                   << SecureShortDebugString((*iter).second->peer_pb());
      peers.erase(iter++);
    } else {
      ++iter;
    }
  }
}

Status PeerManager::StartElection(
    const std::string& uuid, RunLeaderElectionRequestPB req) {
  std::shared_ptr<Peer> peer;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    peer = FindPtrOrNull(peers_, uuid);
    if (!peer) {
      peer = FindPtrOrNull(data_commit_quorum_peers_, uuid);
    }
  }

  if (!peer) {
    return Status::NotFound("unknown peer");
  }
  return peer->StartElection(std::move(req));
}

void PeerManager::Close() {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    for (const auto& entry : peers_) {
      entry.second->Close();
    }

    for (const auto& entry : data_commit_quorum_peers_) {
      entry.second->Close();
    }
    peers_.clear();
    data_commit_quorum_peers_.clear();
    peer_proxy_pool_.Clear();
  }
}

std::string PeerManager::GetLogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, local_uuid_);
}

} // namespace consensus
} // namespace kudu
