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

#include "kudu/util/oid_generator.h"

#include <cstdint>
#include <exception>
#include <mutex>
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fmt/core.h>
#include "kudu/util/status.h"

using std::string;

DEFINE_bool(
    cononicalize_uuid,
    true,
    "If set this strips -/dashes from all uuids and makes "
    "it a 16 character string at generation time.");

DEFINE_bool(
    skip_uuid_validation,
    false,
    "If an app wants to use a custom unique ID as uuid.");

namespace kudu {

namespace {

string convertUuidToString(const boost::uuids::uuid& toConvert) {
  if (FLAGS_cononicalize_uuid) {
    const uint8_t* uuid = toConvert.data;
    return fmt::format(
        "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid[0],
        uuid[1],
        uuid[2],
        uuid[3],
        uuid[4],
        uuid[5],
        uuid[6],
        uuid[7],
        uuid[8],
        uuid[9],
        uuid[10],
        uuid[11],
        uuid[12],
        uuid[13],
        uuid[14],
        uuid[15]);
  } else {
    return boost::uuids::to_string(toConvert);
  }
}

} // anonymous namespace

string ObjectIdGenerator::next() {
  std::lock_guard<LockType> l(oidLock_);
  boost::uuids::uuid uuid = oidGenerator_();
  return convertUuidToString(uuid);
}

Status ObjectIdGenerator::canonicalize(const string& input, string* output)
    const {
  if (FLAGS_skip_uuid_validation) {
    *output = input;
    return Status::OK();
  }
  try {
    boost::uuids::uuid uuid = oidValidator_(input);
    *output = convertUuidToString(uuid);
    return Status::OK();
  } catch (std::exception& e) {
    return Status::InvalidArgument(
        fmt::format("invalid uuid {}: {}", input, e.what()));
  }
}

} // namespace kudu
