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

#include <cctype>
#include <cstdint>
#include <cstring>
#include <map>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <folly/ScopeGuard.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_event_impl.h"
#include "kudu/util/debug/trace_event_synthetic_delay.h"
#include "kudu/util/debug/trace_logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

using kudu::debug::CategoryFilter;
using kudu::debug::TraceLog;
using kudu::debug::TraceResultBuffer;
using rapidjson::Document;
using rapidjson::Value;
using std::string;
using std::thread;
using std::vector;

namespace kudu {

class TraceTest : public KuduTest {};

// Replace all digits in 's' with the character 'X'.
static string xOutDigits(const string& s) {
  string ret;
  ret.reserve(s.size());
  for (char c : s) {
    if (isdigit(c)) {
      ret.push_back('X');
    } else {
      ret.push_back(c);
    }
  }
  return ret;
}

TEST_F(TraceTest, TestBasic) {
  std::shared_ptr<Trace> t = std::make_shared<Trace>();
  TRACE_TO(t, "hello $0, $1", "world", 12345);
  TRACE_TO(t, "goodbye $0, $1", "cruel world", 54321);

  string result = xOutDigits(t->dumpToString(Trace::kNoFlags));
  ASSERT_EQ(
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello world, XXXXX\n"
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] goodbye cruel world, XXXXX\n",
      result);
}

TEST_F(TraceTest, TestAttach) {
  std::shared_ptr<Trace> traceA = std::make_shared<Trace>();
  std::shared_ptr<Trace> traceB = std::make_shared<Trace>();
  {
    ADOPT_TRACE(traceA);
    EXPECT_EQ(traceA.get(), Trace::currentTrace());
    {
      ADOPT_TRACE(traceB);
      EXPECT_EQ(traceB.get(), Trace::currentTrace());
      TRACE("hello from traceB");
    }
    EXPECT_EQ(traceA.get(), Trace::currentTrace());
    TRACE("hello from traceA");
  }
  EXPECT_TRUE(Trace::currentTrace() == nullptr);
  TRACE("this goes nowhere");

  EXPECT_EQ(
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceA\n",
      xOutDigits(traceA->dumpToString(Trace::kNoFlags)));
  EXPECT_EQ(
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceB\n",
      xOutDigits(traceB->dumpToString(Trace::kNoFlags)));
}

TEST_F(TraceTest, TestChildTrace) {
  std::shared_ptr<Trace> traceA = std::make_shared<Trace>();
  std::shared_ptr<Trace> traceB = std::make_shared<Trace>();
  ADOPT_TRACE(traceA);
  traceA->addChildTrace("child", traceB);
  TRACE("hello from traceA");
  {
    ADOPT_TRACE(traceB);
    TRACE("hello from traceB");
  }
  EXPECT_EQ(
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceA\n"
      "Related trace 'child':\n"
      "XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceB\n",
      xOutDigits(traceA->dumpToString(Trace::kNoFlags)));
}

static void generateTraceEvents(int threadId, int numEvents) {
  for (int i = 0; i < numEvents; i++) {
    TRACE_EVENT1("test", "foo", "thread_id", threadId);
  }
}

// Parse the dumped trace data and return the number of events
// found within, including only those with the "test" category.
int parseAndReturnEventCount(const string& traceJson) {
  Document d;
  d.Parse<0>(traceJson.c_str());
  CHECK(d.IsObject()) << "bad json: " << traceJson;
  const Value& eventsJson = d["traceEvents"];
  CHECK(eventsJson.IsArray()) << "bad json: " << traceJson;

  // Count how many of our events were seen. We have to filter out
  // the metadata events.
  int seenRealEvents = 0;
  for (int i = 0; i < eventsJson.Size(); i++) {
    if (eventsJson[i]["cat"].GetString() == string("test")) {
      seenRealEvents++;
    }
  }

  return seenRealEvents;
}

TEST_F(TraceTest, TestChromeTracing) {
  const int kNumThreads = 4;
  const int kEventsPerThread = AllowSlowTests() ? 1000000 : 10000;

  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);

  vector<std::shared_ptr<Thread>> threads(kNumThreads);

  Stopwatch s;
  s.start();
  for (int i = 0; i < kNumThreads; i++) {
    CHECK_OK(
        Thread::Create(
            "test",
            "gen-traces",
            &generateTraceEvents,
            i,
            kEventsPerThread,
            &threads[i]));
  }

  for (int i = 0; i < kNumThreads; i++) {
    threads[i]->Join();
  }
  tl->SetDisabled();

  int totalEvents = kNumThreads * kEventsPerThread;
  double elapsed = s.elapsed().wall_seconds();

  LOG(INFO) << "Trace performance: " << static_cast<int>(totalEvents / elapsed)
            << " traces/sec";

  string traceJson = TraceResultBuffer::FlushTraceLogToString();

  // Verify that the JSON contains events. It won't have exactly
  // kEventsPerThread * kNumThreads because the trace buffer isn't large enough
  // for that.
  ASSERT_GE(parseAndReturnEventCount(traceJson), 100);
}

// Test that, if a thread exits before filling a full trace buffer, we still
// see its results. This is a regression test for a bug in the earlier
// integration of Chromium tracing into Kudu.
TEST_F(TraceTest, TestTraceFromExitedThread) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);

  // Generate 10 trace events in a separate thread.
  int kNumEvents = 10;
  std::shared_ptr<Thread> t;
  CHECK_OK(
      Thread::Create(
          "test", "gen-traces", &generateTraceEvents, 1, kNumEvents, &t));
  t->Join();
  tl->SetDisabled();
  string traceJson = TraceResultBuffer::FlushTraceLogToString();
  LOG(INFO) << traceJson;

  // Verify that the buffer contains 10 trace events
  ASSERT_EQ(10, parseAndReturnEventCount(traceJson));
}

static void generateWideSpan() {
  TRACE_EVENT0("test", "GenerateWideSpan");
  for (int i = 0; i < 1000; i++) {
    TRACE_EVENT0("test", "InnerLoop");
  }
}

// Test creating a trace event which contains many other trace events.
// This ensures that we can go back and update a TraceEvent which fell in
// a different trace chunk.
TEST_F(TraceTest, TestWideSpan) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);

  std::shared_ptr<Thread> t;
  CHECK_OK(Thread::Create("test", "gen-traces", &generateWideSpan, &t));
  t->Join();
  tl->SetDisabled();

  string traceJson = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_EQ(1001, parseAndReturnEventCount(traceJson));
}

// Regression test for KUDU-753: faulty JSON escaping when dealing with
// single quote characters.
TEST_F(TraceTest, TestJsonEncodingString) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);
  {
    TRACE_EVENT1(
        "test",
        "test",
        "arg",
        "this is a test with \"'\"' and characters\nand new lines");
  }
  tl->SetDisabled();
  string traceJson = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_EQ(1, parseAndReturnEventCount(traceJson));
}

// Generate trace events continuously until 'latch' fires.
// Increment *numEventsGenerated for each event generated.
void generateTracesUntilLatch(
    AtomicInt<int64_t>* numEventsGenerated,
    CountDownLatch* latch) {
  while (latch->count()) {
    {
      // This goes in its own scope so that the event is fully generated (with
      // both its START and END times) before we do the counter increment below.
      TRACE_EVENT0("test", "GenerateTracesUntilLatch");
    }
    numEventsGenerated->Increment();
  }
}

// Test starting and stopping tracing while a thread is running.
// This is a regression test for bugs in earlier versions of the imported
// trace code.
TEST_F(TraceTest, TestStartAndStopCollection) {
  TraceLog* tl = TraceLog::GetInstance();

  CountDownLatch latch(1);
  AtomicInt<int64_t> numEventsGenerated(0);
  std::shared_ptr<Thread> t;
  CHECK_OK(
      Thread::Create(
          "test",
          "gen-traces",
          &generateTracesUntilLatch,
          &numEventsGenerated,
          &latch,
          &t));

  const int numFlushes = AllowSlowTests() ? 50 : 3;
  for (int i = 0; i < numFlushes; i++) {
    tl->SetEnabled(
        CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
        TraceLog::RECORDING_MODE,
        TraceLog::RECORD_CONTINUOUSLY);

    const int64_t numEventsBefore = numEventsGenerated.Load();
    SleepFor(MonoDelta::FromMilliseconds(10));
    const int64_t numEventsAfter = numEventsGenerated.Load();
    tl->SetDisabled();

    string traceJson = TraceResultBuffer::FlushTraceLogToString();
    // We might under-count the number of events, since we only measure the
    // sleep, and tracing is enabled before and disabled after we start
    // counting. We might also over-count by at most 1, because we could enable
    // tracing right in between creating a trace event and incrementing the
    // counter. But, we should never over-count by more than 1.
    int expectedEventsLowerbound = numEventsAfter - numEventsBefore - 1;
    int capturedEvents = parseAndReturnEventCount(traceJson);
    ASSERT_GE(capturedEvents, expectedEventsLowerbound);
  }

  latch.countDown();
  t->Join();
}

TEST_F(TraceTest, TestChromeSampling) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      static_cast<TraceLog::Options>(
          TraceLog::RECORD_CONTINUOUSLY | TraceLog::ENABLE_SAMPLING));

  for (int i = 0; i < 100; i++) {
    switch (i % 3) {
      case 0:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-0");
        break;
      case 1:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-1");
        break;
      case 2:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-2");
        break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  tl->SetDisabled();
  string traceJson = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_GT(parseAndReturnEventCount(traceJson), 0);
}

class TraceEventCallbackTest : public KuduTest {
 public:
  virtual void SetUp() override {
    KuduTest::SetUp();
    ASSERT_EQ(nullptr, sInstance_);
    sInstance_ = this;
  }
  virtual void TearDown() override {
    TraceLog::GetInstance()->SetDisabled();

    // Flush the buffer so that one test doesn't end up leaving any
    // extra results for the next test.
    TraceResultBuffer::FlushTraceLogToString();

    ASSERT_TRUE(!!sInstance_);
    sInstance_ = nullptr;
    KuduTest::TearDown();
  }

 protected:
  void endTraceAndFlush() {
    TraceLog::GetInstance()->SetDisabled();
    string traceJson = TraceResultBuffer::FlushTraceLogToString();
    traceDoc_.Parse<0>(traceJson.c_str());
    LOG(INFO) << traceJson;
    ASSERT_TRUE(traceDoc_.IsObject());
    traceParsed_ = traceDoc_["traceEvents"];
    ASSERT_TRUE(traceParsed_.IsArray());
  }

  void dropTracedMetadataRecords() {
    // NB: rapidjson has move-semantics, like auto_ptr.
    Value oldTraceParsed;
    oldTraceParsed = traceParsed_;
    traceParsed_.SetArray();
    size_t oldTraceParsedSize = oldTraceParsed.Size();

    for (size_t i = 0; i < oldTraceParsedSize; i++) {
      Value value;
      value = oldTraceParsed[i];
      if (value.GetType() != rapidjson::kObjectType) {
        traceParsed_.PushBack(value, traceDoc_.GetAllocator());
        continue;
      }
      string tmp;
      if (value.HasMember("ph") && strcmp(value["ph"].GetString(), "M") == 0) {
        continue;
      }

      traceParsed_.PushBack(value, traceDoc_.GetAllocator());
    }
  }

  // Search through the given array for any dictionary which has a key
  // or value which has 'stringToMatch' as a substring.
  // Returns the matching dictionary, or NULL.
  static const Value* findTraceEntry(
      const Value& traceParsed,
      const char* stringToMatch) {
    // Scan all items
    size_t traceParsedCount = traceParsed.Size();
    for (size_t i = 0; i < traceParsedCount; i++) {
      const Value& value = traceParsed[i];
      if (value.GetType() != rapidjson::kObjectType) {
        continue;
      }

      for (Value::ConstMemberIterator it = value.MemberBegin();
           it != value.MemberEnd();
           ++it) {
        if (it->name.IsString() &&
            strstr(it->name.GetString(), stringToMatch) != nullptr) {
          return &value;
        }
        if (it->value.IsString() &&
            strstr(it->value.GetString(), stringToMatch) != nullptr) {
          return &value;
        }
      }
    }
    return nullptr;
  }

  // For TraceEventCallbackAndRecordingX tests.
  void verifyCallbackAndRecordedEvents(
      size_t expectedCallbackCount,
      size_t expectedRecordedCount) {
    // Callback events.
    EXPECT_EQ(expectedCallbackCount, collectedEventsNames_.size());
    for (size_t i = 0; i < collectedEventsNames_.size(); ++i) {
      EXPECT_EQ("callback", collectedEventsCategories_[i]);
      EXPECT_EQ("yes", collectedEventsNames_[i]);
    }

    // Recorded events.
    EXPECT_EQ(expectedRecordedCount, traceParsed_.Size());
    EXPECT_TRUE(findTraceEntry(traceParsed_, "recording"));
    EXPECT_FALSE(findTraceEntry(traceParsed_, "callback"));
    EXPECT_TRUE(findTraceEntry(traceParsed_, "yes"));
    EXPECT_FALSE(findTraceEntry(traceParsed_, "no"));
  }

  void verifyCollectedEvent(
      size_t i,
      unsigned phase,
      const string& category,
      const string& name) {
    EXPECT_EQ(phase, collectedEventsPhases_[i]);
    EXPECT_EQ(category, collectedEventsCategories_[i]);
    EXPECT_EQ(name, collectedEventsNames_[i]);
  }

  Document traceDoc_;
  Value traceParsed_;

  vector<string> collectedEventsCategories_;
  vector<string> collectedEventsNames_;
  vector<unsigned char> collectedEventsPhases_;
  vector<kudu::MicrosecondsInt64> collectedEventsTimestamps_;

  static TraceEventCallbackTest* sInstance_;
  static void callback(
      kudu::MicrosecondsInt64 timestamp,
      char phase,
      const unsigned char* categoryGroupEnabled,
      const char* name,
      uint64_t id,
      int numArgs,
      const char* const argNames[],
      const unsigned char argTypes[],
      const uint64_t argValues[],
      unsigned char flags) {
    sInstance_->collectedEventsPhases_.push_back(phase);
    sInstance_->collectedEventsCategories_.emplace_back(
        TraceLog::GetCategoryGroupName(categoryGroupEnabled));
    sInstance_->collectedEventsNames_.emplace_back(name);
    sInstance_->collectedEventsTimestamps_.push_back(timestamp);
  }
};

TraceEventCallbackTest* TraceEventCallbackTest::sInstance_;

TEST_F(TraceEventCallbackTest, TraceEventCallback) {
  TRACE_EVENT_INSTANT0("all", "before enable", TRACE_EVENT_SCOPE_THREAD);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("*"), callback);
  TRACE_EVENT_INSTANT0("all", "event1", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("all", "event2", TRACE_EVENT_SCOPE_GLOBAL);
  {
    TRACE_EVENT0("all", "duration");
    TRACE_EVENT_INSTANT0("all", "event3", TRACE_EVENT_SCOPE_GLOBAL);
  }
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0(
      "all", "after callback removed", TRACE_EVENT_SCOPE_GLOBAL);
  ASSERT_EQ(5u, collectedEventsNames_.size());
  EXPECT_EQ("event1", collectedEventsNames_[0]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collectedEventsPhases_[0]);
  EXPECT_EQ("event2", collectedEventsNames_[1]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collectedEventsPhases_[1]);
  EXPECT_EQ("duration", collectedEventsNames_[2]);
  EXPECT_EQ(TRACE_EVENT_PHASE_BEGIN, collectedEventsPhases_[2]);
  EXPECT_EQ("event3", collectedEventsNames_[3]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collectedEventsPhases_[3]);
  EXPECT_EQ("duration", collectedEventsNames_[4]);
  EXPECT_EQ(TRACE_EVENT_PHASE_END, collectedEventsPhases_[4]);
  for (size_t i = 1; i < collectedEventsTimestamps_.size(); i++) {
    EXPECT_LE(collectedEventsTimestamps_[i - 1], collectedEventsTimestamps_[i]);
  }
}

TEST_F(TraceEventCallbackTest, TraceEventCallbackWhileFull) {
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("*"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  do {
    TRACE_EVENT_INSTANT0("all", "badger badger", TRACE_EVENT_SCOPE_GLOBAL);
  } while (!TraceLog::GetInstance()->BufferIsFull());
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("*"), callback);
  TRACE_EVENT_INSTANT0("all", "a snake", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  ASSERT_EQ(1u, collectedEventsNames_.size());
  EXPECT_EQ("a snake", collectedEventsNames_[0]);
}

// 1: Enable callback, enable recording, disable callback, disable recording.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording1) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("callback"), callback);
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  endTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  dropTracedMetadataRecords();
  ASSERT_NO_FATAL_FAILURE();
  verifyCallbackAndRecordedEvents(2, 2);
}

// 2: Enable callback, enable recording, disable recording, disable callback.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording2) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("callback"), callback);
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  endTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  dropTracedMetadataRecords();
  verifyCallbackAndRecordedEvents(3, 1);
}

// 3: Enable recording, enable callback, disable callback, disable recording.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording3) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("callback"), callback);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  endTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  dropTracedMetadataRecords();
  verifyCallbackAndRecordedEvents(1, 3);
}

// 4: Enable recording, enable callback, disable recording, disable callback.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording4) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("callback"), callback);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  endTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  dropTracedMetadataRecords();
  verifyCallbackAndRecordedEvents(2, 2);
}

TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecordingDuration) {
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("*"), callback);
  {
    TRACE_EVENT0("callback", "duration1");
    TraceLog::GetInstance()->SetEnabled(
        CategoryFilter("*"),
        TraceLog::RECORDING_MODE,
        TraceLog::RECORD_UNTIL_FULL);
    TRACE_EVENT0("callback", "duration2");
    endTraceAndFlush();
    TRACE_EVENT0("callback", "duration3");
  }
  TraceLog::GetInstance()->SetEventCallbackDisabled();

  ASSERT_EQ(6u, collectedEventsNames_.size());
  verifyCollectedEvent(0, TRACE_EVENT_PHASE_BEGIN, "callback", "duration1");
  verifyCollectedEvent(1, TRACE_EVENT_PHASE_BEGIN, "callback", "duration2");
  verifyCollectedEvent(2, TRACE_EVENT_PHASE_BEGIN, "callback", "duration3");
  verifyCollectedEvent(3, TRACE_EVENT_PHASE_END, "callback", "duration3");
  verifyCollectedEvent(4, TRACE_EVENT_PHASE_END, "callback", "duration2");
  verifyCollectedEvent(5, TRACE_EVENT_PHASE_END, "callback", "duration1");
}

////////////////////////////////////////////////////////////
// Tests for synthetic delay
// (from chromium-base/debug/trace_event_synthetic_delay_unittest.cc)
////////////////////////////////////////////////////////////

namespace {

const int kTargetDurationMs = 100;
// Allow some leeway in timings to make it possible to run these tests with a
// wall clock time source too.
const int kShortDurationMs = 10;

} // namespace

namespace debug {

class TraceEventSyntheticDelayTest : public KuduTest,
                                     public TraceEventSyntheticDelayClock {
 public:
  TraceEventSyntheticDelayTest() {
    now_ = MonoTime::Min();
  }

  virtual ~TraceEventSyntheticDelayTest() {
    ResetTraceEventSyntheticDelays();
  }

  // TraceEventSyntheticDelayClock implementation.
  virtual MonoTime now() override {
    advanceTime(MonoDelta::FromMilliseconds(kShortDurationMs / 10));
    return now_;
  }

  TraceEventSyntheticDelay* configureDelay(const char* name) {
    TraceEventSyntheticDelay* delay = TraceEventSyntheticDelay::Lookup(name);
    delay->SetClock(this);
    delay->SetTargetDuration(MonoDelta::FromMilliseconds(kTargetDurationMs));
    return delay;
  }

  void advanceTime(MonoDelta delta) {
    now_ += delta;
  }

  int testFunction() {
    MonoTime start = now();
    {
      TRACE_EVENT_SYNTHETIC_DELAY("test.Delay");
    }
    MonoTime end = now();
    return (end - start).ToMilliseconds();
  }

  int asyncTestFunctionBegin() {
    MonoTime start = now();
    {
      TRACE_EVENT_SYNTHETIC_DELAY_BEGIN("test.AsyncDelay");
    }
    MonoTime end = now();
    return (end - start).ToMilliseconds();
  }

  int asyncTestFunctionEnd() {
    MonoTime start = now();
    {
      TRACE_EVENT_SYNTHETIC_DELAY_END("test.AsyncDelay");
    }
    MonoTime end = now();
    return (end - start).ToMilliseconds();
  }

 private:
  MonoTime now_;

  DISALLOW_COPY_AND_ASSIGN(TraceEventSyntheticDelayTest);
};

TEST_F(TraceEventSyntheticDelayTest, StaticDelay) {
  TraceEventSyntheticDelay* delay = configureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::STATIC);
  EXPECT_GE(testFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, OneShotDelay) {
  TraceEventSyntheticDelay* delay = configureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ONE_SHOT);
  EXPECT_GE(testFunction(), kTargetDurationMs);
  EXPECT_LT(testFunction(), kShortDurationMs);

  delay->SetTargetDuration(MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_GE(testFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AlternatingDelay) {
  TraceEventSyntheticDelay* delay = configureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ALTERNATING);
  EXPECT_GE(testFunction(), kTargetDurationMs);
  EXPECT_LT(testFunction(), kShortDurationMs);
  EXPECT_GE(testFunction(), kTargetDurationMs);
  EXPECT_LT(testFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelay) {
  configureDelay("test.AsyncDelay");
  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(asyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayExceeded) {
  configureDelay("test.AsyncDelay");
  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  advanceTime(MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_LT(asyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNoActivation) {
  configureDelay("test.AsyncDelay");
  EXPECT_LT(asyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNested) {
  configureDelay("test.AsyncDelay");
  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(asyncTestFunctionEnd(), kShortDurationMs);
  EXPECT_GE(asyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayUnbalanced) {
  configureDelay("test.AsyncDelay");
  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(asyncTestFunctionEnd(), kTargetDurationMs / 2);
  EXPECT_LT(asyncTestFunctionEnd(), kShortDurationMs);

  EXPECT_LT(asyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(asyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, ResetDelays) {
  configureDelay("test.Delay");
  ResetTraceEventSyntheticDelays();
  EXPECT_LT(testFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, BeginParallel) {
  TraceEventSyntheticDelay* delay = configureDelay("test.AsyncDelay");
  MonoTime endTimes[2];
  MonoTime startTime = now();

  delay->BeginParallel(&endTimes[0]);
  EXPECT_FALSE(!endTimes[0].Initialized());

  delay->BeginParallel(&endTimes[1]);
  EXPECT_FALSE(!endTimes[1].Initialized());

  delay->EndParallel(endTimes[0]);
  EXPECT_GE((now() - startTime).ToMilliseconds(), kTargetDurationMs);

  startTime = now();
  delay->EndParallel(endTimes[1]);
  EXPECT_LT((now() - startTime).ToMilliseconds(), kShortDurationMs);
}

TEST_F(TraceTest, TestVLogTrace) {
  for (FLAGS_v = 0; FLAGS_v <= 1; FLAGS_v++) {
    TraceLog* tl = TraceLog::GetInstance();
    tl->SetEnabled(
        CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
        TraceLog::RECORDING_MODE,
        TraceLog::RECORD_CONTINUOUSLY);
    VLOG_AND_TRACE("test", 1) << "hello world";
    tl->SetDisabled();
    string traceJson = TraceResultBuffer::FlushTraceLogToString();
    ASSERT_STR_CONTAINS(traceJson, "hello world");
    ASSERT_STR_CONTAINS(traceJson, "trace-test.cc");
  }
}

namespace {
string functionWithSideEffect(bool* b) {
  *b = true;
  return "function-result";
}
} // anonymous namespace

// Test that, if tracing is not enabled, a VLOG_AND_TRACE doesn't evaluate its
// arguments.
TEST_F(TraceTest, TestVLogTraceLazyEvaluation) {
  FLAGS_v = 0;
  bool functionRun = false;
  VLOG_AND_TRACE("test", 1) << functionWithSideEffect(&functionRun);
  ASSERT_FALSE(functionRun);

  // If we enable verbose logging, we should run the side effect even though
  // trace logging is disabled.
  FLAGS_v = 1;
  VLOG_AND_TRACE("test", 1) << functionWithSideEffect(&functionRun);
  ASSERT_TRUE(functionRun);
}

TEST_F(TraceTest, TestVLogAndEchoToConsole) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::ECHO_TO_CONSOLE);
  FLAGS_v = 1;
  VLOG_AND_TRACE("test", 1) << "hello world";
  tl->SetDisabled();
}

TEST_F(TraceTest, TestTraceMetrics) {
  std::shared_ptr<Trace> trace = std::make_shared<Trace>();
  trace->metrics()->increment("foo", 10);
  trace->metrics()->increment("bar", 10);
  for (int i = 0; i < 1000; i++) {
    trace->metrics()->increment("baz", i);
  }
  EXPECT_EQ("{\"bar\":10,\"baz\":499500,\"foo\":10}", trace->metricsAsJson());

  {
    ADOPT_TRACE(trace);
    TRACE_COUNTER_SCOPE_LATENCY_US("test_scope_us");
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  auto m = trace->metrics()->Get();
  EXPECT_GE(m["test_scope_us"], 80 * 1000);
}

// Regression test for KUDU-2075: using tracing from vanilla threads
// should work fine, even if some pthread_self identifiers have been
// reused.
TEST_F(TraceTest, TestTraceFromVanillaThreads) {
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);
  SCOPE_EXIT {
    TraceLog::GetInstance()->SetDisabled();
  };

  // Do several passes to make it more likely that the thread identifiers
  // will get reused.
  for (int pass = 0; pass < 10; pass++) {
    vector<thread> threads;
    for (int i = 0; i < 100; i++) {
      threads.emplace_back([i] { generateTraceEvents(i, 1); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }
}
} // namespace debug
} // namespace kudu
