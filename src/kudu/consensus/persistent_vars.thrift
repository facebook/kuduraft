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

package "facebook.com/raft"

namespace cpp2 facebook.raft
namespace py3 facebook.py3

// This struct is used to serialize all of the persistent variables to disk
// whenever they are updated. These will be read during Init()
struct PersistentVars {
  // Flag to allow starting elections on the peer. If disabled, the peer will
  // not start elections - even if there are heartbeat failures from the leader
  1: bool allow_start_election = true;

  // This serves like a ring UUID. RPCs present this token as a proof that
  // they're part of a ring
  2: optional string raft_rpc_token;

  // Dictionary from compression codec
  3: optional binary compression_dictionary;
}
