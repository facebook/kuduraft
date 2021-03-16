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

#include "kudu/server/diagnostics_log.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sparsehash/dense_hash_set>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/array_view.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/rolling_log.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::pair;
using std::priority_queue;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// GLog already implements symbolization. Just import their hidden symbol.
namespace google {
// Symbolizes a program counter.  On success, returns true and write the
// symbol name to "out".  The symbol name is demangled if possible
// (supports symbols generated by GCC 3.x or newer).  Otherwise,
// returns false.
bool Symbolize(void *pc, char *out, int out_size);
}

DEFINE_int32(diagnostics_log_stack_traces_interval_ms, 60000,
             "The interval at which the server will a snapshot of its thread stacks to the "
             "diagnostics log. In fact, the server will log at a random interval betweeen "
             "zero and twice the configured value to avoid biasing samples towards periodic "
             "processes which happen exactly on some particular schedule. If this is set to "
             "0, stack traces will be not be periodically logged, but will still be logged "
             "on events such as queue overflows. Setting this to a negative value disables "
             "stack trace logging entirely.");
TAG_FLAG(diagnostics_log_stack_traces_interval_ms, runtime);
TAG_FLAG(diagnostics_log_stack_traces_interval_ms, experimental);

namespace kudu {
namespace server {

// Track which symbols have been emitted to the log already.
class DiagnosticsLog::SymbolSet {
 public:
  SymbolSet() {
    set_.set_empty_key(nullptr);
  }

  // Return true if the addr was added, false if it already existed.
  bool Add(void* addr) {
    // We can't add nullptr since that's the 'empty' key. However this
    // also will never have a real symbol, so we'll just pretend it's already
    // present.
    return addr && InsertIfNotPresent(&set_, addr);
  }

  void ResetIfLogRolled(int roll_count) {
    if (roll_count_ != roll_count) {
      roll_count_ = roll_count;
      set_.clear();
    }
  }

 private:
  int roll_count_ = 0;
  google::dense_hash_set<void*> set_;
};

DiagnosticsLog::DiagnosticsLog(string log_dir,
                               MetricRegistry* metric_registry) :
    log_dir_(std::move(log_dir)),
    metric_registry_(metric_registry),
    wake_(&lock_),
    metrics_log_interval_(MonoDelta::FromSeconds(60)),
    symbols_(new SymbolSet()) {
}
DiagnosticsLog::~DiagnosticsLog() {
  Stop();
}

void DiagnosticsLog::SetMetricsLogInterval(MonoDelta interval) {
  MutexLock l(lock_);
  metrics_log_interval_ = interval;
}

#ifdef FB_DO_NOT_REMOVE
void DiagnosticsLog::DumpStacksNow(std::string reason) {
  MutexLock l(lock_);
  dump_stacks_now_reason_ = std::move(reason);
  wake_.Signal();
}
#endif


Status DiagnosticsLog::Start() {
  unique_ptr<RollingLog> l(new RollingLog(Env::Default(), log_dir_, "diagnostics"));
  RETURN_NOT_OK_PREPEND(l->Open(), "unable to open diagnostics log");
  log_ = std::move(l);
  Status s = Thread::Create("server", "diag-logger",
                            &DiagnosticsLog::RunThread,
                            this, &thread_);
  if (!s.ok()) {
    // Don't leave the log open if we failed to start our thread.
    log_.reset();
  }
  return s;
}

void DiagnosticsLog::Stop() {
  if (!thread_) return;

  {
    MutexLock l(lock_);
    stop_ = true;
    wake_.Signal();
  }
  thread_->Join();
  thread_.reset();
  stop_ = false;
  WARN_NOT_OK(log_->Close(), "Unable to close diagnostics log");
}

MonoTime DiagnosticsLog::ComputeNextWakeup(DiagnosticsLog::WakeupType type) const {
  switch (type) {
    case WakeupType::STACKS:
      if (FLAGS_diagnostics_log_stack_traces_interval_ms > 0) {
        // Instead of directly using the configured interval, we use a uniform random
        // interval whose mean is the configured value. This prevents biasing our stack
        // samples. For example, if there is some background process which happens once a
        // minute, and the user also configured the stacks to once a minute, an operator
        // might incorrectly surmise that the background task was _always_ running.
        // Randomizing the samples avoids such correlations.
        Random rng(GetRandomSeed32());
        int64_t ms = rng.Uniform(FLAGS_diagnostics_log_stack_traces_interval_ms * 2);
        return MonoTime::Now() + MonoDelta::FromMilliseconds(ms);
      } else {
        // Stack tracing is disabled. However we still wake up periodically because the
        // flag is runtime-modifiable, and we need to wake up to notice that it might have
        // changed.
        return MonoTime::Now() + MonoDelta::FromSeconds(5);
      }
      break;
    case WakeupType::METRICS:
      return MonoTime::Now() + metrics_log_interval_;
  }
  __builtin_unreachable();
}

void DiagnosticsLog::RunThread() {
  MutexLock l(lock_);

  // Set up a priority queue which tracks our future scheduled wake-ups.
  typedef pair<MonoTime, WakeupType> QueueElem;
  priority_queue<QueueElem, vector<QueueElem>, std::greater<QueueElem>> wakeups;
  wakeups.emplace(ComputeNextWakeup(WakeupType::METRICS), WakeupType::METRICS);
#ifdef FB_DO_NOT_REMOVE
  wakeups.emplace(ComputeNextWakeup(WakeupType::STACKS), WakeupType::STACKS);
#endif

  while (!stop_) {
    MonoTime next_log = wakeups.top().first;
    wake_.WaitUntil(next_log);

    string reason;
    WakeupType what;

#ifdef FB_DO_NOT_REMOVE
    if (dump_stacks_now_reason_) {
      what = WakeupType::STACKS;
      reason = std::move(*dump_stacks_now_reason_);
      dump_stacks_now_reason_ = boost::none;
    }
#endif

    if (MonoTime::Now() >= next_log) {
      what = wakeups.top().second;
      reason = "periodic";
      wakeups.pop();
      wakeups.emplace(ComputeNextWakeup(what), what);
    } else {
      // Spurious wakeup, or a stop trigger.
      continue;
    }

    // Unlock the mutex while actually logging metrics or stacks since it's somewhat
    // slow and we don't want to block threads trying to signal us.
    l.Unlock();
    SCOPED_CLEANUP({ l.Lock(); });
    Status s;
    if (what == WakeupType::METRICS) {
      WARN_NOT_OK(LogMetrics(), "Unable to collect metrics to diagnostics log");
    }

#ifdef FB_DO_NOT_REMOVE
    if (what == WakeupType::STACKS && FLAGS_diagnostics_log_stack_traces_interval_ms >= 0) {
      WARN_NOT_OK(LogStacks(reason), "Unable to collect stacks to diagnostics log");
    }
#endif

  }
}

#ifdef FB_DO_NOT_REMOVE
Status DiagnosticsLog::LogStacks(const string& reason) {
  StackTraceSnapshot snap;
  snap.set_capture_thread_names(false);
  RETURN_NOT_OK(snap.SnapshotAllStacks());

  std::ostringstream buf;
  MicrosecondsInt64 now = GetCurrentTimeMicros();

  // Because symbols are potentially long strings, and likely to be
  // very repetitive, we do a sort of dictionary encoding here. When
  // we roll a file, we clear our symbol table. Then, within that
  // file, the first time we see any address, we add it to the table
  // and make sure it is output in a 'symbols' record. Subsequent
  // repetitions of the same address do not need to re-output the
  // symbol.
  symbols_->ResetIfLogRolled(log_->roll_count());
  vector<std::pair<void*, string>> new_symbols;
  snap.VisitGroups([&](ArrayView<StackTraceSnapshot::ThreadInfo> group) {
      const StackTrace& stack = group[0].stack;
      for (int i = 0; i < stack.num_frames(); i++) {
        void* addr = stack.frame(i);
        if (symbols_->Add(addr)) {
          char buf[1024];
          // Subtract 1 from the address before symbolizing, because the
          // address on the stack is actually the return address of the function
          // call rather than the address of the call instruction itself.
          if (google::Symbolize(static_cast<char*>(addr) - 1, buf, sizeof(buf))) {
            new_symbols.emplace_back(addr, buf);
          }
          // If symbolization fails, don't bother adding it. Readers of the log
          // will just see that it's missing from the symbol map and should handle that
          // as an unknown symbol.
        }
      }
    });
  if (!new_symbols.empty()) {
    buf << "I" << FormatTimestampForLog(now)
        << " symbols " << now << " ";
    JsonWriter jw(&buf, JsonWriter::COMPACT);
    jw.StartObject();
    for (auto& p : new_symbols) {
      jw.String(StringPrintf("%p", p.first));
      jw.String(p.second);
    }
    jw.EndObject();
    buf << "\n";
  }

  buf << "I" << FormatTimestampForLog(now) << " stacks " << now << " ";
  JsonWriter jw(&buf, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("reason");
  jw.String(reason);
  jw.String("groups");
  jw.StartArray();
  snap.VisitGroups([&](ArrayView<StackTraceSnapshot::ThreadInfo> group) {
      jw.StartObject();

      jw.String("tids");
      jw.StartArray();
      for (auto& t : group) {
        // TODO(todd): should we also output the thread names, perhaps in
        // a sort of dictionary fashion? It's more instructive but in
        // practice the stack traces should usually indicate the work
        // that's being done, anyway, which is enough to tie back to the
        // thread. The TID is smaller and useful for correlating against
        // messages in the normal glog as well.
        jw.Int64(t.tid);
      }
      jw.EndArray();

      jw.String("stack");
      jw.StartArray();
      const StackTrace& stack = group[0].stack;
      for (int i = 0; i < stack.num_frames(); i++) {
        jw.String(StringPrintf("%p", stack.frame(i)));
      }
      jw.EndArray();
      jw.EndObject();
    });
  jw.EndArray(); // array of thread groups
  jw.EndObject(); // end of top-level object
  buf << "\n";

  RETURN_NOT_OK(log_->Append(buf.str()));

  return Status::OK();
}
#endif

Status DiagnosticsLog::LogMetrics() {
  MetricJsonOptions opts;
  opts.include_raw_histograms = false;

  opts.only_modified_in_or_after_epoch = metrics_epoch_;

  // We don't output any metrics which have never been incremented. Though
  // this seems redundant with the "only include changed metrics" above, it
  // also ensures that we don't dump a bunch of zero data on startup.
  opts.include_untouched_metrics = false;

  // Entity attributes aren't that useful in the context of this log. We can
  // always grab the entity attributes separately if necessary.
  opts.include_entity_attributes = false;
  opts.refresh_histogram_metrics = true;

  std::ostringstream buf;
  MicrosecondsInt64 now = GetCurrentTimeMicros();
  buf << "I" << FormatTimestampForLog(now)
      << " metrics " << now << " ";

  // Collect the metrics JSON string.
  int64_t this_log_epoch = Metric::current_epoch();
  Metric::IncrementEpoch();
  JsonWriter writer(&buf, JsonWriter::COMPACT);
  RETURN_NOT_OK(metric_registry_->WriteAsJson(&writer, {"*"}, opts));
  buf << "\n";

  RETURN_NOT_OK(log_->Append(buf.str()));

  // Next time we fetch, only show those that changed after the epoch
  // we just logged.
  //
  // NOTE: we only bump this in the successful log case so that if we failed to
  // write above, we wouldn't skip any changes.
  metrics_epoch_ = this_log_epoch + 1;
  return Status::OK();
}


} // namespace server
} // namespace kudu
