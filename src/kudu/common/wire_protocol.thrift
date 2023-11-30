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
// Protobufs used by both client-server and server-server traffic
// for user data transfer. This file should only contain protobufs
// which are exclusively used on the wire. If a protobuf is persisted on
// disk and not used as part of the wire protocol, it belongs in another
// place such as common/common.proto or within cfile/, server/, etc.

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

namespace cpp2 facebook.raft
namespace py3 facebook.py3

include "kudu/common/common.thrift"

typedef common.HostPort HostPort

enum AppStatusErrorCode {
  UNKNOWN_ERROR = 999,
  OK = 0,
  NOT_FOUND = 1,
  CORRUPTION = 2,
  NOT_SUPPORTED = 3,
  INVALID_ARGUMENT = 4,
  IO_ERROR = 5,
  ALREADY_PRESENT = 6,
  RUNTIME_ERROR = 7,
  NETWORK_ERROR = 8,
  ILLEGAL_STATE = 9,
  NOT_AUTHORIZED = 10,
  ABORTED = 11,
  REMOTE_ERROR = 12,
  SERVICE_UNAVAILABLE = 13,
  TIMED_OUT = 14,
  UNINITIALIZED = 15,
  CONFIGURATION_ERROR = 16,
  INCOMPLETE = 17,
  END_OF_FILE = 18,
  CANCELLED = 19,
}

// Error status returned by any RPC method.
// Every RPC method which could generate an application-level error
// should have this (or a more complex error result) as an optional field
// in its response.
//
// This maps to kudu::Status in C++ and org.apache.kudu.Status in Java.
struct AppStatus {
  1: AppStatusErrorCode code;
  2: optional string message;
  4: optional i32 posix_code;
}

// Uniquely identify a particular instance of a particular server in the
// cluster.
struct NodeInstance {
  // Unique ID which is created when the server is first started
  // up. This is stored persistently on disk.
  1: binary permanent_uuid;

  // Sequence number incremented on every start-up of the server.
  // This makes it easy to detect when an instance has restarted (and
  // thus can be assumed to have forgotten any soft state it had in
  // memory).
  //
  // On a freshly initialized server, the first sequence number
  // should be 0.
  2: i64 instance_seqno;
}

// Some basic properties common to both masters and tservers.
// They are guaranteed not to change unless the server is restarted.
struct ServerRegistration {
  1: list<HostPort> rpc_addresses;
  2: list<HostPort> http_addresses;
  3: optional string software_version;

  // True if HTTPS has been enabled for the web interface.
  // In this case, https:// URLs should be generated for the above
  // 'http_addresses' field.
  4: optional bool https_enabled;
}
