// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "common/fb303/cpp/FacebookBase2.h"
#include "kudu/consensus/gen-cpp2/ConsensusService.h"
#include "kudu/util/status.h"

namespace facebook {
namespace raft {
class ConsensusServiceHandler
    : virtual public apache::thrift::ServiceHandler<ConsensusService> {
 public:
  explicit ConsensusServiceHandler(int32_t serverport);

  ~ConsensusServiceHandler();

  static kudu::Status shutdown();

  static fb303::cpp2::fb_status getStatus() {
    return fb303::cpp2::fb_status::ALIVE;
  }

 private:
  const int32_t serverPort_;
};

} // namespace raft
} // namespace facebook
