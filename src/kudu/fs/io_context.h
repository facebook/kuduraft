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

#pragma once

#include <string>

namespace kudu {
namespace fs {

// An IoContext provides a single interface to pass state around during IO. A
// single IoContext should correspond to a single high-level operation that
// does IO, e.g. a scan, a tablet bootstrap, etc.
//
// For each operation, there should exist one IoContext owned by the top-level
// module. A pointer to the context should then be passed around by lower-level
// modules. These lower-level modules should enforce that their access via
// pointer to the IoContext is bounded by the lifetime of the IoContext itself.
//
// Examples:
// - A Tablet::Iterator will do IO and own an IoContext. All sub-iterators may
//   pass around and store pointers to this IoContext, under the assumption
//   that they will not outlive the parent Tablet::Iterator.
// - Tablet bootstrap will do IO and an IoContext will be created during
//   bootstrap and passed around to lower-level modules (e.g. to the CFiles).
//   The expectation is that, because the lower-level modules may outlive the
//   bootstrap and its IoContext, they will not store the pointers to the
//   context, but may use them as method arguments as needed.
struct IoContext {
  // The tablet id associated with this IO.
  std::string tabletId;
};

} // namespace fs
} // namespace kudu
