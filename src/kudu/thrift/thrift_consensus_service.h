// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "common/services/cpp/ServiceFrameworkLight.h"
#include "kudu/thrift/thrift_consensus_handler.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

namespace facebook {
namespace raft {
class ConsensusService {
 public:
  ConsensusService() = default;
  ~ConsensusService();

  ConsensusService(const ConsensusService&) = delete;
  ConsensusService& operator=(const ConsensusService&) = delete;
  ConsensusService(ConsensusService&&) = delete;
  ConsensusService& operator=(ConsensusService&&) = delete;

  kudu::Status shutdown();

  std::shared_ptr<ConsensusServiceHandler> getHandler() {
    return handler_;
  }

  kudu::Status startService(
      int32_t serverport,
      const std::shared_ptr<services::ServiceFrameworkLight>& serviceframework);

 private:
  std::shared_ptr<ConsensusServiceHandler> handler_;
  std::shared_ptr<apache::thrift::ThriftServer> server_;
};
} // namespace raft
} // namespace facebook
