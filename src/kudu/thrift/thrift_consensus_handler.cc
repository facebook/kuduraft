// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/thrift/thrift_consensus_handler.h"

namespace facebook {
namespace raft {

ConsensusServiceHandler::ConsensusServiceHandler(
    const int32_t /* serverPort */) {}

ConsensusServiceHandler::~ConsensusServiceHandler() {
  shutdown();
}

kudu::Status ConsensusServiceHandler::shutdown() {
  return kudu::Status::OK();
}

} // namespace raft
} // namespace facebook
