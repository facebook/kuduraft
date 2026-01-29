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

__thread Trace* Trace::threadlocal_trace_;

Trace::Trace()
    : arena_(new ThreadSafeArena(1024)),
      entries_head_(nullptr),
      entries_tail_(nullptr) {
  // We expect small allocations from our Arena so no need to have
  // a large arena component. Small allocations are more likely to
  // come out of thread cache and be fast.
  arena_->SetMaxBufferSize(4096);
}

Trace::~Trace() {}

// Struct which precedes each entry in the trace.
struct TraceEntry {
  kudu::MicrosecondsInt64 timestamp_micros;

  // The source file and line number which generated the trace message.
  const char* file_path;
  int line_number;

  uint32_t message_len;
  TraceEntry* next;

  // The actual trace message follows the entry header.
  char* message() {
    return reinterpret_cast<char*>(this) + sizeof(*this);
  }
};

// Get the part of filepath after the last path separator.
// (Doesn't modify filepath, contrary to basename() in libgen.h.)
// Borrowed from glog.
static const char* const_basename(const char* filepath) {
  const char* base = strrchr(filepath, '/');
#ifdef OS_WINDOWS // Look for either path separator in Windows
  if (!base)
    base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

TraceEntry*
Trace::NewEntry(int msg_len, const char* file_path, int line_number) {
  int size = sizeof(TraceEntry) + msg_len;
  uint8_t* dst = reinterpret_cast<uint8_t*>(arena_->AllocateBytes(size));
  TraceEntry* entry = reinterpret_cast<TraceEntry*>(dst);
  entry->timestamp_micros = GetCurrentTimeMicros();
  entry->message_len = msg_len;
  entry->file_path = file_path;
  entry->line_number = line_number;
  return entry;
}

void Trace::TraceString(
    const char* filepath,
    int line_number,
    const std::string& msg) {
  size_t msg_len = msg.length();
  TraceEntry* entry = NewEntry(msg_len, filepath, line_number);
  checked_memcpy(entry->message(), msg_len, msg.data(), msg_len);
  AddEntry(entry);
}

void Trace::AddEntry(TraceEntry* entry) {
  std::lock_guard<simple_spinlock> l(lock_);
  entry->next = nullptr;

  if (entries_tail_ != nullptr) {
    entries_tail_->next = entry;
  } else {
    DCHECK(entries_head_ == nullptr);
    entries_head_ = entry;
  }
  entries_tail_ = entry;
}

void Trace::Dump(std::ostream* out, int flags) const {
  // Gather a copy of the list of entries under the lock. This is fast
  // enough that we aren't worried about stalling concurrent tracers
  // (whereas doing the logging itself while holding the lock might be
  // too slow, if the output stream is a file, for example).
  vector<TraceEntry*> entries;
  vector<pair<StringPiece, std::shared_ptr<Trace>>> child_traces;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    for (TraceEntry* cur = entries_head_; cur != nullptr; cur = cur->next) {
      entries.push_back(cur);
    }

    child_traces = child_traces_;
  }

  // Save original flags.
  std::ios::fmtflags save_flags(out->flags());

  int64_t prev_usecs = 0;
  for (TraceEntry* e : entries) {
    // Log format borrowed from glog/logging.cc
    int64_t usecs_since_prev = 0;
    if (prev_usecs != 0) {
      usecs_since_prev = e->timestamp_micros - prev_usecs;
    }
    prev_usecs = e->timestamp_micros;

    using std::setw;
    *out << FormatTimestampForLog(e->timestamp_micros);
    *out << ' ';
    if (flags & INCLUDE_TIME_DELTAS) {
      out->fill(' ');
      *out << "(+" << setw(6) << usecs_since_prev << "us) ";
    }
    *out << const_basename(e->file_path) << ':' << e->line_number << "] ";
    out->write(reinterpret_cast<char*>(e) + sizeof(TraceEntry), e->message_len);
    *out << std::endl;
  }

  for (const auto& entry : child_traces) {
    const auto& t = entry.second;
    *out << "Related trace '" << entry.first << "':" << std::endl;
    *out << t->DumpToString(flags & (~INCLUDE_METRICS));
  }

  if (flags & INCLUDE_METRICS) {
    *out << "Metrics: " << MetricsAsJSON();
  }

  // Restore stream flags.
  out->flags(save_flags);
}

string Trace::DumpToString(int flags) const {
  std::ostringstream s;
  Dump(&s, flags);
  return s.str();
}

string Trace::MetricsAsJSON() const {
  std::ostringstream s;
  JsonWriter jw(&s, JsonWriter::COMPACT);
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
  vector<pair<StringPiece, std::shared_ptr<Trace>>> child_traces;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    child_traces = child_traces_;
  }

  if (!child_traces.empty()) {
    jw->String("child_traces");
    jw->StartArray();

    for (const auto& e : child_traces) {
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
    const std::shared_ptr<Trace>& child_trace) {
  CHECK(arena_->RelocateStringPiece(label, &label));

  std::lock_guard<simple_spinlock> l(lock_);
  child_traces_.emplace_back(label, child_trace);
}

std::vector<std::pair<StringPiece, std::shared_ptr<Trace>>> Trace::ChildTraces()
    const {
  std::lock_guard<simple_spinlock> l(lock_);
  return child_traces_;
}

} // namespace kudu
