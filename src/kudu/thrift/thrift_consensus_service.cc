// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/thrift/thrift_consensus_service.h"
#include <fmt/core.h>
#include "kudu/util/logging.h"

namespace facebook {
namespace raft {

kudu::Status ConsensusService::startService(
    const int32_t serverPort,
    const std::shared_ptr<services::ServiceFrameworkLight>& serviceFramework) {
  if (serverPort <= 0) {
    std::string msg = fmt::format(
        "Invalid port provided for thrift Consensus Service. Port val: {}",
        serverPort);
    LOG(ERROR) << msg;
    return kudu::Status::ConfigurationError(msg);
  }
  handler_ = std::make_shared<ConsensusServiceHandler>(serverPort);
  server_ = std::make_shared<apache::thrift::ThriftServer>();
  server_->setInterface(handler_);
  server_->setPort(serverPort);
  serviceFramework->addThriftService(server_, handler_.get(), serverPort);
  serviceFramework->go(false /* waitForStop */);
  LOG(INFO) << "Started Consensus Service on port: " << serverPort;
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
