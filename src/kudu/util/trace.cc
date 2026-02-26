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

#include "kudu/util/trace.h"

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/walltime.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "secure_lib/secure_string.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {

__thread Trace* Trace::threadlocalTrace_;

Trace::Trace()
    : arena_(new ThreadSafeArena(1024)),
      entriesHead_(nullptr),
      entriesTail_(nullptr) {
  // We expect small allocations from our Arena so no need to have
  // a large arena component. Small allocations are more likely to
  // come out of thread cache and be fast.
  arena_->setMaxBufferSize(4096);
}

Trace::~Trace() {}

// Struct which precedes each entry in the trace.
struct TraceEntry {
  kudu::MicrosecondsInt64 timestampMicros;

  // The source file and line number which generated the trace message.
  const char* filePath;
  int lineNumber;

  uint32_t messageLen;
  TraceEntry* next;

  // The actual trace message follows the entry header.
  char* message() {
    return reinterpret_cast<char*>(this) + sizeof(*this);
  }
};

// Get the part of filepath after the last path separator.
// (Doesn't modify filepath, contrary to basename() in libgen.h.)
// Borrowed from glog.
static const char* constBasename(const char* filepath) {
  const char* base = strrchr(filepath, '/');
#ifdef OS_WINDOWS // Look for either path separator in Windows
  if (!base)
    base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

TraceEntry* Trace::NewEntry(int msgLen, const char* filePath, int lineNumber) {
  int size = sizeof(TraceEntry) + msgLen;
  uint8_t* dst = reinterpret_cast<uint8_t*>(arena_->allocateBytes(size));
  TraceEntry* entry = reinterpret_cast<TraceEntry*>(dst);
  entry->timestampMicros = GetCurrentTimeMicros();
  entry->messageLen = msgLen;
  entry->filePath = filePath;
  entry->lineNumber = lineNumber;
  return entry;
}

void Trace::TraceString(
    const char* filepath,
    int lineNumber,
    const std::string& msg) {
  size_t msgLen = msg.length();
  TraceEntry* entry = NewEntry(msgLen, filepath, lineNumber);
  checked_memcpy(entry->message(), msgLen, msg.data(), msgLen);
  AddEntry(entry);
}

void Trace::AddEntry(TraceEntry* entry) {
  std::lock_guard<simple_spinlock> l(lock_);
  entry->next = nullptr;

  if (entriesTail_ != nullptr) {
    entriesTail_->next = entry;
  } else {
    DCHECK(entriesHead_ == nullptr);
    entriesHead_ = entry;
  }
  entriesTail_ = entry;
}

void Trace::Dump(std::ostream* out, int flags) const {
  // Gather a copy of the list of entries under the lock. This is fast
  // enough that we aren't worried about stalling concurrent tracers
  // (whereas doing the logging itself while holding the lock might be
  // too slow, if the output stream is a file, for example).
  vector<TraceEntry*> entries;
  vector<pair<StringPiece, std::shared_ptr<Trace>>> childTraces;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    for (TraceEntry* cur = entriesHead_; cur != nullptr; cur = cur->next) {
      entries.push_back(cur);
    }

    childTraces = childTraces_;
  }

  // Save original flags.
  std::ios::fmtflags saveFlags(out->flags());

  int64_t prevUsecs = 0;
  for (TraceEntry* e : entries) {
    // Log format borrowed from glog/logging.cc
    int64_t usecsSincePrev = 0;
    if (prevUsecs != 0) {
      usecsSincePrev = e->timestampMicros - prevUsecs;
    }
    prevUsecs = e->timestampMicros;

    using std::setw;
    *out << FormatTimestampForLog(e->timestampMicros);
    *out << ' ';
    if (flags & INCLUDE_TIME_DELTAS) {
      out->fill(' ');
      *out << "(+" << setw(6) << usecsSincePrev << "us) ";
    }
    *out << constBasename(e->filePath) << ':' << e->lineNumber << "] ";
    out->write(reinterpret_cast<char*>(e) + sizeof(TraceEntry), e->messageLen);
    *out << std::endl;
  }

  for (const auto& entry : childTraces) {
    const auto& t = entry.second;
    *out << "Related trace '" << entry.first << "':" << std::endl;
    *out << t->DumpToString(flags & (~INCLUDE_METRICS));
  }

  if (flags & INCLUDE_METRICS) {
    *out << "Metrics: " << MetricsAsJSON();
  }

  // Restore stream flags.
  out->flags(saveFlags);
}

string Trace::DumpToString(int flags) const {
  std::ostringstream s;
  Dump(&s, flags);
  return s.str();
}

string Trace::MetricsAsJSON() const {
  std::ostringstream s;
  JsonWriter jw(&s, JsonWriter::kCompact);
  MetricsToJSON(&jw);
  return s.str();
}

void Trace::MetricsToJSON(JsonWriter* jw) const {
  // Convert into a map with 'std::string' keys instead of 'const char*'
  // keys, so that the results are in a consistent (sorted) order.
  std::map<string, int64_t> counters;
  for (const auto& entry : metrics_.get()) {
    counters[entry.first] = entry.second;
  }

  jw->StartObject();
  for (const auto& e : counters) {
    jw->String(e.first);
    jw->Int64(e.second);
  }
  vector<pair<StringPiece, std::shared_ptr<Trace>>> childTraces;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    childTraces = childTraces_;
  }

  if (!childTraces.empty()) {
    jw->String("child_traces");
    jw->StartArray();

    for (const auto& e : childTraces) {
      jw->StartArray();
      jw->String(e.first.data(), e.first.size());
      e.second->MetricsToJSON(jw);
      jw->EndArray();
    }
    jw->EndArray();
  }
  jw->EndObject();
}

void Trace::DumpCurrentTrace() {
  Trace* t = CurrentTrace();
  if (t == nullptr) {
    LOG(INFO) << "No trace is currently active.";
    return;
  }
  t->Dump(&std::cerr, true);
}

void Trace::AddChildTrace(
    StringPiece label,
    const std::shared_ptr<Trace>& childTrace) {
  CHECK(arena_->relocateStringPiece(label, &label));

  std::lock_guard<simple_spinlock> l(lock_);
  childTraces_.emplace_back(label, childTrace);
}

std::vector<std::pair<StringPiece, std::shared_ptr<Trace>>> Trace::ChildTraces()
    const {
  std::lock_guard<simple_spinlock> l(lock_);
  return childTraces_;
}

} // namespace kudu
