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

#include "kudu/util/trace_metrics.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/util/debug/leakcheck_disabler.h"

using std::string;

namespace kudu {

// Make glog's STL-compatible operators visible inside this namespace.
using ::operator<<;

namespace {

static simple_spinlock gInternMapLock;
using InternMap = std::map<string, const char*>;
static InternMap* gInternMap;

} // anonymous namespace

const char* TraceMetrics::internName(const string& name) {
  DCHECK(
      std::all_of(name.begin(), name.end(), [](char c) { return isprint(c); }))
      << "not printable: " << name;

  debug::ScopedLeakCheckDisabler noLeakcheck;
  std::lock_guard<simple_spinlock> l(gInternMapLock);
  if (gInternMap == nullptr) {
    gInternMap = new InternMap();
  }

  InternMap::iterator it = gInternMap->find(name);
  if (it != gInternMap->end()) {
    return it->second;
  }

  const char* dup = strdup(name.c_str());
  (*gInternMap)[name] = dup;

  // We don't expect this map to grow large.
  DCHECK_LT(gInternMap->size(), 100)
      << "Too many interned strings: " << *gInternMap;

  return dup;
}

} // namespace kudu
