// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "kudu/consensus/metadata.pb.h"

namespace kudu {
namespace consensus {

/**
 * Interface for metrics on the state machine.
 *
 * The state machine should implement this and return a StateMachineMetricsPB on
 * request to reflect the current state of replication in the state machine.
 */
class StateMachineMetricsInterface {
 public:
  /**
   * Virtual destructor.
   */
  virtual ~StateMachineMetricsInterface() = default;

  /**
   * Returns the metrics for the state machine such as how far it has applied
   * the replicated logs.
   *
   * @return The metrics
   */
  virtual StateMachineMetricsPB getStateMachineMetrics() = 0;
};

} // namespace consensus
} // namespace kudu
