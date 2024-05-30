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
//
// Helpers for dealing with the protobufs defined in wire_protocol.proto.

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#ifndef KUDU_COMMON_WIRE_PROTOCOL_H
#define KUDU_COMMON_WIRE_PROTOCOL_H

#include <cstdint>
#include <vector>

#include "kudu/util/status.h"

namespace boost {
template <class T>
class optional;
}

namespace google::protobuf {
template <typename Element>
class RepeatedPtrField;
} // namespace google::protobuf

namespace kudu {

class faststring;
class HostPort;
class Sockaddr;

class AppStatusPB;
class HostPortPB;

// Convert the given C++ Status object into the equivalent Protobuf.
void StatusToPB(const Status& status, AppStatusPB* pb);

// Convert the given protobuf into the equivalent C++ Status object.
Status StatusFromPB(const AppStatusPB& pb);

// Convert the specified HostPort to protobuf.
Status HostPortToPB(const HostPort& host_port, HostPortPB* host_port_pb);

// Returns the HostPort created from the specified protobuf.
Status HostPortFromPB(const HostPortPB& host_port_pb, HostPort* host_port);

// Adds addresses in 'addrs' to 'pbs'. If an address is a wildcard
// (e.g., "0.0.0.0"), then the local machine's hostname is used in
// its place.
Status AddHostPortPBs(
    const std::vector<Sockaddr>& addrs,
    google::protobuf::RepeatedPtrField<HostPortPB>* pbs);
} // namespace kudu
#endif
