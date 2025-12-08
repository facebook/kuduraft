// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/thrift/thrift_consensus_service.h"
#include <fmt/core.h>
#include "kudu/util/logging.h"

namespace facebook {
namespace raft {

kudu::Status ConsensusService::startService(
    const int32_t serverport,
    const std::shared_ptr<services::ServiceFrameworkLight>& serviceframework) {
  if (serverport <= 0) {
    std::string msg = fmt::format(
        "Invalid port provided for thrift Consensus Service. Port val: {}",
        serverport);
    LOG(ERROR) << msg;
    return kudu::Status::ConfigurationError(msg);
  }
  handler_ = std::make_shared<ConsensusServiceHandler>(serverport);
  server_ = std::make_shared<apache::thrift::ThriftServer>();
  server_->setInterface(handler_);
  server_->setPort(serverport);
  serviceframework->addThriftService(server_, handler_.get(), serverport);
  serviceframework->go(false /* waitForStop */);
  LOG(INFO) << "Started Consensus Service on port: " << serverport;
  return kudu::Status::OK();
}

kudu::Status ConsensusService::shutdown() {
  if (server_) {
    LOG(INFO) << "Stopping Consensus Server";
    server_->stopListening();
    server_.reset();
  }

  if (handler_) {
    handler_->shutdown();
    handler_.reset();
  }

  LOG(INFO) << "Consensus Service shutdown complete";
  return kudu::Status::OK();
}

ConsensusService::~ConsensusService() {
  shutdown();
}
} // namespace raft
} // namespace facebook
