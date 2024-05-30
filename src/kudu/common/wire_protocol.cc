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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/common/wire_protocol.h"

#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <optional>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/fixedarray.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

using google::protobuf::RepeatedPtrField;
using kudu::pb_util::SecureShortDebugString;
using std::vector;

namespace kudu {

void StatusToPB(const Status& status, AppStatusPB* pb) {
  pb->Clear();
  bool is_unknown = false;
  if (status.ok()) {
    pb->set_code(AppStatusPB::OK);
    // OK statuses don't have any message or posix code.
    return;
  } else if (status.IsNotFound()) {
    pb->set_code(AppStatusPB::NOT_FOUND);
  } else if (status.IsCorruption()) {
    pb->set_code(AppStatusPB::CORRUPTION);
  } else if (status.IsNotSupported()) {
    pb->set_code(AppStatusPB::NOT_SUPPORTED);
  } else if (status.IsInvalidArgument()) {
    pb->set_code(AppStatusPB::INVALID_ARGUMENT);
  } else if (status.IsIOError()) {
    pb->set_code(AppStatusPB::IO_ERROR);
  } else if (status.IsAlreadyPresent()) {
    pb->set_code(AppStatusPB::ALREADY_PRESENT);
  } else if (status.IsRuntimeError()) {
    pb->set_code(AppStatusPB::RUNTIME_ERROR);
  } else if (status.IsNetworkError()) {
    pb->set_code(AppStatusPB::NETWORK_ERROR);
  } else if (status.IsIllegalState()) {
    pb->set_code(AppStatusPB::ILLEGAL_STATE);
  } else if (status.IsNotAuthorized()) {
    pb->set_code(AppStatusPB::NOT_AUTHORIZED);
  } else if (status.IsAborted()) {
    pb->set_code(AppStatusPB::ABORTED);
  } else if (status.IsRemoteError()) {
    pb->set_code(AppStatusPB::REMOTE_ERROR);
  } else if (status.IsServiceUnavailable()) {
    pb->set_code(AppStatusPB::SERVICE_UNAVAILABLE);
  } else if (status.IsTimedOut()) {
    pb->set_code(AppStatusPB::TIMED_OUT);
  } else if (status.IsUninitialized()) {
    pb->set_code(AppStatusPB::UNINITIALIZED);
  } else if (status.IsConfigurationError()) {
    pb->set_code(AppStatusPB::CONFIGURATION_ERROR);
  } else if (status.IsIncomplete()) {
    pb->set_code(AppStatusPB::INCOMPLETE);
  } else if (status.IsEndOfFile()) {
    pb->set_code(AppStatusPB::END_OF_FILE);
  } else if (status.IsCompressionDictMismatch()) {
    pb->set_code(AppStatusPB::COMPRESSION_DICT_MISMATCH);
  } else {
    LOG(WARNING) << "Unknown error code translation from internal error "
                 << status.ToString() << ": sending UNKNOWN_ERROR";
    pb->set_code(AppStatusPB::UNKNOWN_ERROR);
    is_unknown = true;
  }
  if (is_unknown) {
    // For unknown status codes, include the original stringified error
    // code.
    pb->set_message(status.CodeAsString() + ": " + status.message().ToString());
  } else {
    // Otherwise, just encode the message itself, since the other end
    // will reconstruct the other parts of the ToString() response.
    pb->set_message(status.message().ToString());
  }
  if (status.posix_code() != -1) {
    pb->set_posix_code(status.posix_code());
  }
}

Status StatusFromPB(const AppStatusPB& pb) {
  int16_t posix_code =
      static_cast<int16_t>(pb.has_posix_code() ? pb.posix_code() : -1);

  switch (pb.code()) {
    case AppStatusPB::OK:
      return Status::OK();
    case AppStatusPB::NOT_FOUND:
      return Status::NotFound(pb.message(), "", posix_code);
    case AppStatusPB::CORRUPTION:
      return Status::Corruption(pb.message(), "", posix_code);
    case AppStatusPB::NOT_SUPPORTED:
      return Status::NotSupported(pb.message(), "", posix_code);
    case AppStatusPB::INVALID_ARGUMENT:
      return Status::InvalidArgument(pb.message(), "", posix_code);
    case AppStatusPB::IO_ERROR:
      return Status::IOError(pb.message(), "", posix_code);
    case AppStatusPB::ALREADY_PRESENT:
      return Status::AlreadyPresent(pb.message(), "", posix_code);
    case AppStatusPB::RUNTIME_ERROR:
      return Status::RuntimeError(pb.message(), "", posix_code);
    case AppStatusPB::NETWORK_ERROR:
      return Status::NetworkError(pb.message(), "", posix_code);
    case AppStatusPB::ILLEGAL_STATE:
      return Status::IllegalState(pb.message(), "", posix_code);
    case AppStatusPB::NOT_AUTHORIZED:
      return Status::NotAuthorized(pb.message(), "", posix_code);
    case AppStatusPB::ABORTED:
      return Status::Aborted(pb.message(), "", posix_code);
    case AppStatusPB::REMOTE_ERROR:
      return Status::RemoteError(pb.message(), "", posix_code);
    case AppStatusPB::SERVICE_UNAVAILABLE:
      return Status::ServiceUnavailable(pb.message(), "", posix_code);
    case AppStatusPB::TIMED_OUT:
      return Status::TimedOut(pb.message(), "", posix_code);
    case AppStatusPB::UNINITIALIZED:
      return Status::Uninitialized(pb.message(), "", posix_code);
    case AppStatusPB::CONFIGURATION_ERROR:
      return Status::ConfigurationError(pb.message(), "", posix_code);
    case AppStatusPB::INCOMPLETE:
      return Status::Incomplete(pb.message(), "", posix_code);
    case AppStatusPB::END_OF_FILE:
      return Status::EndOfFile(pb.message(), "", posix_code);
    case AppStatusPB::COMPRESSION_DICT_MISMATCH:
      return Status::CompressionDictMismatch(pb.message(), "", posix_code);
    case AppStatusPB::CONTINUE:
      return Status::Continue(pb.message(), "", posix_code);
    case AppStatusPB::UNKNOWN_ERROR:
    default:
      LOG(WARNING) << "Unknown error code in status: "
                   << SecureShortDebugString(pb);
      return Status::RuntimeError(
          "(unknown error code)", pb.message(), posix_code);
  }
}

Status HostPortToPB(const HostPort& host_port, HostPortPB* host_port_pb) {
  host_port_pb->set_host(host_port.host());
  host_port_pb->set_port(host_port.port());
  return Status::OK();
}

Status HostPortFromPB(const HostPortPB& host_port_pb, HostPort* host_port) {
  host_port->set_host(host_port_pb.host());
  host_port->set_port(host_port_pb.port());
  return Status::OK();
}

Status AddHostPortPBs(
    const vector<Sockaddr>& addrs,
    RepeatedPtrField<HostPortPB>* pbs) {
  for (const Sockaddr& addr : addrs) {
    HostPortPB* pb = pbs->Add();
    if (addr.IsWildcard()) {
      RETURN_NOT_OK(GetFQDN(pb->mutable_host()));
    } else {
      pb->set_host(addr.host());
    }
    pb->set_port(addr.port());
  }
  return Status::OK();
}
} // namespace kudu
