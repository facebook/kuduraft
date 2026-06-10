// Copyright (c) 2012 The Chromium Authors. All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/util/debug/trace_event_impl.h"

#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <list>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include <fmt/core.h>
#include "kudu/gutil/bind.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted_memory.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"

#include "kudu/util/atomic.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_event_synthetic_delay.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadlocal.h"

DEFINE_string(
    trace_to_console,
    "",
    "Trace pattern specifying which trace events should be dumped "
    "directly to the console");
TAG_FLAG(trace_to_console, experimental);

// The thread buckets for the sampling profiler.
BASE_EXPORT TRACE_EVENT_API_ATOMIC_WORD gTraceState[3];

namespace kudu {
namespace debug {

using base::SpinLockHolder;

using std::string;
using std::unique_ptr;
using std::vector;

__thread TraceLog::PerThreadInfo* TraceLog::threadLocalInfo_ = nullptr;

namespace {

// Controls the number of trace events we will buffer in-memory
// before throwing them away.
const size_t kTraceBufferChunkSize = TraceBufferChunk::kTraceBufferChunkSize;
const size_t kTraceEventVectorBufferChunks = 256000 / kTraceBufferChunkSize;
const size_t kTraceEventRingBufferChunks = kTraceEventVectorBufferChunks / 4;
const size_t kTraceEventBatchChunks = 1000 / kTraceBufferChunkSize;
// Can store results for 30 seconds with 1 ms sampling interval.
const size_t kMonitorTraceEventBufferChunks = 30000 / kTraceBufferChunkSize;
// kEchoToConsole needs a small buffer to hold the unfinished COMPLETE events.
const size_t kEchoToConsoleTraceEventBufferChunks = 256;

const char kSyntheticDelayCategoryFilterPrefix[] = "DELAY(";

#define MAX_CATEGORY_GROUPS 100

// Parallel arrays gCategoryGroups and gCategoryGroupEnabled are separate
// so that a pointer to a member of gCategoryGroupEnabled can be easily
// converted to an index into gCategoryGroups. This allows macros to deal
// only with char enabled pointers from gCategoryGroupEnabled, and we can
// convert internally to determine the category name from the char enabled
// pointer.
const char* gCategoryGroups[MAX_CATEGORY_GROUPS] = {
    "toplevel",
    "tracing already shutdown",
    "tracing categories exhausted; must increase MAX_CATEGORY_GROUPS",
    "__metadata"};

// The enabled flag is char instead of bool so that the API can be used from C.
unsigned char gCategoryGroupEnabled[MAX_CATEGORY_GROUPS] = {0};
// Indexes here have to match the gCategoryGroups array indexes above.
const int kCategoryAlreadyShutdown = 1;
const int kCategoryCategoriesExhausted = 2;
const int kCategoryMetadata = 3;
const int kNumBuiltinCategories = 4;
// Skip default categories.
AtomicWord gCategoryIndex = kNumBuiltinCategories;

// The name of the current thread. This is used to decide if the current
// thread name has changed. We combine all the seen thread names into the
// output name for the thread.
__thread const char* gCurrentThreadName = "";

[[noreturn]] static void NOTIMPLEMENTED() {
  LOG(FATAL);
}

class TraceBufferRingBuffer : public TraceBuffer {
 public:
  explicit TraceBufferRingBuffer(size_t max_chunks)
      : maxChunks_(max_chunks),
        recyclableChunksQueue_(new size_t[queueCapacity()]),
        queueHead_(0),
        queueTail_(max_chunks),
        currentIterationIndex_(0),
        currentChunkSeq_(1) {
    chunks_.reserve(max_chunks);
    for (size_t i = 0; i < max_chunks; ++i) {
      recyclableChunksQueue_[i] = i;
    }
  }

  ~TraceBufferRingBuffer() {
    for (auto* chunk : chunks_) {
      delete chunk;
    }
    chunks_.clear();
  }

  virtual unique_ptr<TraceBufferChunk> getChunk(size_t* index) override {
    // Because the number of threads is much less than the number of chunks,
    // the queue should never be empty.
    DCHECK(!queueIsEmpty());

    *index = recyclableChunksQueue_[queueHead_];
    queueHead_ = nextQueueIndex(queueHead_);
    currentIterationIndex_ = queueHead_;

    if (*index >= chunks_.size()) {
      chunks_.resize(*index + 1);
    }

    TraceBufferChunk* chunk = chunks_[*index];
    chunks_[*index] = nullptr; // Put NULL in the slot of a in-flight chunk.
    if (chunk) {
      chunk->reset(currentChunkSeq_++);
    } else {
      chunk = new TraceBufferChunk(currentChunkSeq_++);
    }

    return unique_ptr<TraceBufferChunk>(chunk);
  }

  virtual void returnChunk(size_t index, unique_ptr<TraceBufferChunk> chunk)
      override {
    // When this method is called, the queue should not be full because it
    // can contain all chunks including the one to be returned.
    DCHECK(!queueIsFull());
    DCHECK(chunk);
    DCHECK_LT(index, chunks_.size());
    DCHECK(!chunks_[index]);
    chunks_[index] = chunk.release();
    recyclableChunksQueue_[queueTail_] = index;
    queueTail_ = nextQueueIndex(queueTail_);
  }

  virtual bool isFull() const override {
    return false;
  }

  virtual size_t size() const override {
    // This is approximate because not all of the chunks are full.
    return chunks_.size() * kTraceBufferChunkSize;
  }

  virtual size_t capacity() const override {
    return maxChunks_ * kTraceBufferChunkSize;
  }

  virtual TraceEvent* getEventByHandle(TraceEventHandle handle) override {
    if (handle.chunkIndex >= chunks_.size()) {
      return nullptr;
    }
    TraceBufferChunk* chunk = chunks_[handle.chunkIndex];
    if (!chunk || chunk->seq() != handle.chunkSeq) {
      return nullptr;
    }
    return chunk->getEventAt(handle.eventIndex);
  }

  virtual const TraceBufferChunk* nextChunk() override {
    if (chunks_.empty()) {
      return nullptr;
    }

    while (currentIterationIndex_ != queueTail_) {
      size_t chunkIndex = recyclableChunksQueue_[currentIterationIndex_];
      currentIterationIndex_ = nextQueueIndex(currentIterationIndex_);
      if (chunkIndex >= chunks_.size()) { // Skip uninitialized chunks.
        continue;
      }
      DCHECK(chunks_[chunkIndex]);
      return chunks_[chunkIndex];
    }
    return nullptr;
  }

  virtual unique_ptr<TraceBuffer> cloneForIteration() const override {
    unique_ptr<ClonedTraceBuffer> clonedBuffer(new ClonedTraceBuffer());
    for (size_t queueIndex = queueHead_; queueIndex != queueTail_;
         queueIndex = nextQueueIndex(queueIndex)) {
      size_t chunkIndex = recyclableChunksQueue_[queueIndex];
      if (chunkIndex >= chunks_.size()) { // Skip uninitialized chunks.
        continue;
      }
      TraceBufferChunk* chunk = chunks_[chunkIndex];
      clonedBuffer->chunks_.push_back(
          chunk ? chunk->clone().release() : nullptr);
    }
    return clonedBuffer;
  }

 private:
  class ClonedTraceBuffer : public TraceBuffer {
   public:
    ClonedTraceBuffer() : currentIterationIndex_(0) {}
    ~ClonedTraceBuffer() {
      for (auto* chunk : chunks_) {
        delete chunk;
      }
      chunks_.clear();
    }

    // The only implemented method.
    virtual const TraceBufferChunk* nextChunk() override {
      return currentIterationIndex_ < chunks_.size()
          ? chunks_[currentIterationIndex_++]
          : nullptr;
    }

    virtual unique_ptr<TraceBufferChunk> getChunk(size_t* /*index*/) override {
      NOTIMPLEMENTED();
      return unique_ptr<TraceBufferChunk>();
    }
    virtual void returnChunk(
        size_t /* index */,
        unique_ptr<TraceBufferChunk> /* trace_buffer_chunk */) override {
      NOTIMPLEMENTED();
    }
    virtual bool isFull() const override {
      return false;
    }
    virtual size_t size() const override {
      return 0;
    }
    virtual size_t capacity() const override {
      return 0;
    }
    virtual TraceEvent* getEventByHandle(TraceEventHandle handle) override {
      return nullptr;
    }
    virtual unique_ptr<TraceBuffer> cloneForIteration() const override {
      NOTIMPLEMENTED();
      return unique_ptr<TraceBuffer>();
    }

    size_t currentIterationIndex_;
    vector<TraceBufferChunk*> chunks_;
  };

  bool queueIsEmpty() const {
    return queueHead_ == queueTail_;
  }

  size_t queueSize() const {
    return queueTail_ > queueHead_ ? queueTail_ - queueHead_
                                   : queueTail_ + queueCapacity() - queueHead_;
  }

  bool queueIsFull() const {
    return queueSize() == queueCapacity() - 1;
  }

  size_t queueCapacity() const {
    // One extra space to help distinguish full state and empty state.
    return maxChunks_ + 1;
  }

  size_t nextQueueIndex(size_t index) const {
    index++;
    if (index >= queueCapacity()) {
      index = 0;
    }
    return index;
  }

  size_t maxChunks_;
  vector<TraceBufferChunk*> chunks_;

  unique_ptr<size_t[]> recyclableChunksQueue_;
  size_t queueHead_;
  size_t queueTail_;

  size_t currentIterationIndex_;
  uint32_t currentChunkSeq_;

  DISALLOW_COPY_AND_ASSIGN(TraceBufferRingBuffer);
};

class TraceBufferVector : public TraceBuffer {
 public:
  TraceBufferVector() : inFlightChunkCount_(0), currentIterationIndex_(0) {
    chunks_.reserve(kTraceEventVectorBufferChunks);
  }
  ~TraceBufferVector() {
    for (auto* chunk : chunks_) {
      delete chunk;
    }
    chunks_.clear();
  }

  virtual unique_ptr<TraceBufferChunk> getChunk(size_t* index) override {
    // This function may be called when adding normal events or indirectly from
    // AddMetadataEventsWhileLocked(). We can not DECHECK(!isFull()) because we
    // have to add the metadata events and flush thread-local buffers even if
    // the buffer is full.
    *index = chunks_.size();
    chunks_.push_back(nullptr); // Put NULL in the slot of a in-flight chunk.
    ++inFlightChunkCount_;
    // + 1 because zero chunk_seq is not allowed.
    return unique_ptr<TraceBufferChunk>(
        new TraceBufferChunk(static_cast<uint32_t>(*index) + 1));
  }

  virtual void returnChunk(size_t index, unique_ptr<TraceBufferChunk> chunk)
      override {
    DCHECK_GT(inFlightChunkCount_, 0u);
    DCHECK_LT(index, chunks_.size());
    DCHECK(!chunks_[index]);
    --inFlightChunkCount_;
    chunks_[index] = chunk.release();
  }

  virtual bool isFull() const override {
    return chunks_.size() >= kTraceEventVectorBufferChunks;
  }

  virtual size_t size() const override {
    // This is approximate because not all of the chunks are full.
    return chunks_.size() * kTraceBufferChunkSize;
  }

  virtual size_t capacity() const override {
    return kTraceEventVectorBufferChunks * kTraceBufferChunkSize;
  }

  virtual TraceEvent* getEventByHandle(TraceEventHandle handle) override {
    if (handle.chunkIndex >= chunks_.size()) {
      return nullptr;
    }
    TraceBufferChunk* chunk = chunks_[handle.chunkIndex];
    if (!chunk || chunk->seq() != handle.chunkSeq) {
      return nullptr;
    }
    return chunk->getEventAt(handle.eventIndex);
  }

  virtual const TraceBufferChunk* nextChunk() override {
    while (currentIterationIndex_ < chunks_.size()) {
      // Skip in-flight chunks.
      const TraceBufferChunk* chunk = chunks_[currentIterationIndex_++];
      if (chunk) {
        return chunk;
      }
    }
    return nullptr;
  }

  virtual unique_ptr<TraceBuffer> cloneForIteration() const override {
    NOTIMPLEMENTED();
    return unique_ptr<TraceBuffer>();
  }

 private:
  size_t inFlightChunkCount_;
  size_t currentIterationIndex_;
  vector<TraceBufferChunk*> chunks_;

  DISALLOW_COPY_AND_ASSIGN(TraceBufferVector);
};

template <typename T>
void initializeMetadataEvent(
    TraceEvent* traceEvent,
    int thread_id,
    const char* metadata_name,
    const char* arg_name,
    const T& value) {
  if (!traceEvent) {
    return;
  }

  int num_args = 1;
  unsigned char arg_type;
  uint64_t arg_value;
  ::trace_event_internal::setTraceValue(value, &arg_type, &arg_value);
  traceEvent->initialize(
      thread_id,
      kudu::MicrosecondsInt64(0),
      kudu::MicrosecondsInt64(0),
      TRACE_EVENT_PHASE_METADATA,
      &gCategoryGroupEnabled[kCategoryMetadata],
      metadata_name,
      ::trace_event_internal::kNoEventId,
      num_args,
      &arg_name,
      &arg_type,
      &arg_value,
      nullptr,
      TRACE_EVENT_FLAG_NONE);
}

// RAII object which marks '*dst' with a non-zero value while in scope.
// This assumes that no other threads write to '*dst'.
class MarkFlagInScope {
 public:
  explicit MarkFlagInScope(Atomic32* dst) : dst_(dst) {
    // We currently use acquireAtomicExchange here because it appears
    // to be the cheapest way of getting an "Acquire_Store" barrier. Actually
    // using Acquire_Store generates more assembly instructions and benchmarks
    // slightly slower.
    //
    // TODO: it would be even faster to avoid the memory barrier here entirely,
    // and do an asymmetric barrier, for example by having the flusher thread
    // send a signal to every registered thread, or wait until every other
    // thread has experienced at least one context switch. A number of options
    // for this are outlined in:
    // http://home.comcast.net/~pjbishop/Dave/Asymmetric-Dekker-Synchronization.txt
    Atomic32 oldVal = base::subtle::acquireAtomicExchange(dst_, 1);
    DCHECK_EQ(oldVal, 0);
  }
  ~MarkFlagInScope() {
    base::subtle::Release_Store(dst_, 0);
  }

 private:
  Atomic32* dst_;
  DISALLOW_COPY_AND_ASSIGN(MarkFlagInScope);
};
} // anonymous namespace

TraceLog::ThreadLocalEventBuffer* TraceLog::PerThreadInfo::atomicTakeBuffer() {
  return reinterpret_cast<TraceLog::ThreadLocalEventBuffer*>(
      base::subtle::acquireAtomicExchange(
          reinterpret_cast<AtomicWord*>(&eventBuffer_), 0));
}

void TraceBufferChunk::reset(uint32_t newSeq) {
  for (size_t i = 0; i < nextFree_; ++i) {
    chunk_[i].reset();
  }
  nextFree_ = 0;
  seq_ = newSeq;
}

TraceEvent* TraceBufferChunk::addTraceEvent(size_t* eventIndex) {
  DCHECK(!isFull());
  *eventIndex = nextFree_++;
  return &chunk_[*eventIndex];
}

unique_ptr<TraceBufferChunk> TraceBufferChunk::clone() const {
  unique_ptr<TraceBufferChunk> clonedChunk(new TraceBufferChunk(seq_));
  clonedChunk->nextFree_ = nextFree_;
  for (size_t i = 0; i < nextFree_; ++i) {
    clonedChunk->chunk_[i].copyFrom(chunk_[i]);
  }
  return clonedChunk;
}

// A helper class that allows the lock to be acquired in the middle of the scope
// and unlocks at the end of scope if locked.
class TraceLog::OptionalAutoLock {
 public:
  explicit OptionalAutoLock(
      base::SpinLock& lock) // NOLINT(google-runtime-references)
      : lock_(lock), locked_(false) {}

  ~OptionalAutoLock() {
    if (locked_) {
      lock_.unlock();
    }
  }

  void ensureAcquired() {
    if (!locked_) {
      lock_.lock();
      locked_ = true;
    }
  }

 private:
  base::SpinLock& lock_;
  bool locked_;
  DISALLOW_COPY_AND_ASSIGN(OptionalAutoLock);
};

// Use this function instead of TraceEventHandle constructor to keep the
// overhead of ScopedTracer (trace_event.h) constructor minimum.
void makeHandle(
    uint32_t chunkSeq,
    size_t chunkIndex,
    size_t eventIndex,
    TraceEventHandle* handle) {
  DCHECK(chunkSeq);
  DCHECK(chunkIndex < (1u << 16));
  DCHECK(eventIndex < (1u << 16));
  handle->chunkSeq = chunkSeq;
  handle->chunkIndex = static_cast<uint16_t>(chunkIndex);
  handle->eventIndex = static_cast<uint16_t>(eventIndex);
}

////////////////////////////////////////////////////////////////////////////////
//
// TraceEvent
//
////////////////////////////////////////////////////////////////////////////////

namespace {

size_t getAllocLength(const char* str) {
  return str ? strlen(str) + 1 : 0;
}

// Copies |*member| into |*buffer|, sets |*member| to point to this new
// location, and then advances |*buffer| by the amount written.
void copyTraceEventParameter(
    char** buffer,
    const char** member,
    const char* end) {
  if (*member) {
    size_t written = strings::strlcpy(*buffer, *member, end - *buffer) + 1;
    DCHECK_LE(static_cast<int>(written), end - *buffer);
    *member = *buffer;
    *buffer += written;
  }
}

} // namespace

TraceEvent::TraceEvent()
    : duration_(-1),
      threadDuration_(-1),
      id_(0u),
      categoryGroupEnabled_(nullptr),
      name_(nullptr),
      threadId_(0),
      phase_(TRACE_EVENT_PHASE_BEGIN),
      flags_(0) {
  for (auto& arg_name : argNames_) {
    arg_name = nullptr;
  }
  memset(argValues_, 0, sizeof(argValues_));
}

void TraceEvent::copyFrom(const TraceEvent& other) {
  timestamp_ = other.timestamp_;
  threadTimestamp_ = other.threadTimestamp_;
  duration_ = other.duration_;
  id_ = other.id_;
  categoryGroupEnabled_ = other.categoryGroupEnabled_;
  name_ = other.name_;
  threadId_ = other.threadId_;
  phase_ = other.phase_;
  flags_ = other.flags_;
  parameterCopyStorage_ = other.parameterCopyStorage_;

  for (int i = 0; i < kTraceMaxNumArgs; ++i) {
    argNames_[i] = other.argNames_[i];
    argTypes_[i] = other.argTypes_[i];
    argValues_[i] = other.argValues_[i];
    convertableValues_[i] = other.convertableValues_[i];
  }
}

void TraceEvent::initialize(
    int thread_id,
    kudu::MicrosecondsInt64 timestamp,
    kudu::MicrosecondsInt64 thread_timestamp,
    char phase,
    const unsigned char* category_group_enabled,
    const char* name,
    uint64_t id,
    int num_args,
    const char** arg_names,
    const unsigned char* arg_types,
    const uint64_t* arg_values,
    const std::shared_ptr<ConvertableToTraceFormat>* convertable_values,
    unsigned char flags) {
  timestamp_ = timestamp;
  threadTimestamp_ = thread_timestamp;
  duration_ = -1;
  ;
  id_ = id;
  categoryGroupEnabled_ = category_group_enabled;
  name_ = name;
  threadId_ = thread_id;
  phase_ = phase;
  flags_ = flags;

  // Clamp num_args since it may have been set by a third_party library.
  num_args = (num_args > kTraceMaxNumArgs) ? kTraceMaxNumArgs : num_args;
  int i = 0;
  for (; i < num_args; ++i) {
    argNames_[i] = arg_names[i];
    argTypes_[i] = arg_types[i];

    if (arg_types[i] == TRACE_VALUE_TYPE_CONVERTABLE) {
      convertableValues_[i] = convertable_values[i];
    } else {
      argValues_[i].asUint = arg_values[i];
    }
  }
  for (; i < kTraceMaxNumArgs; ++i) {
    argNames_[i] = nullptr;
    argValues_[i].asUint = 0u;
    convertableValues_[i] = nullptr;
    argTypes_[i] = TRACE_VALUE_TYPE_UINT;
  }

  bool copy = !!(flags & TRACE_EVENT_FLAG_COPY);
  size_t allocSize = 0;
  if (copy) {
    allocSize += getAllocLength(name);
    for (i = 0; i < num_args; ++i) {
      allocSize += getAllocLength(argNames_[i]);
      if (argTypes_[i] == TRACE_VALUE_TYPE_STRING) {
        argTypes_[i] = TRACE_VALUE_TYPE_COPY_STRING;
      }
    }
  }

  bool argIsCopy[kTraceMaxNumArgs];
  for (i = 0; i < num_args; ++i) {
    // No copying of convertable types, we retain ownership.
    if (argTypes_[i] == TRACE_VALUE_TYPE_CONVERTABLE) {
      continue;
    }

    // We only take a copy of arg_vals if they are of type COPY_STRING.
    argIsCopy[i] = (argTypes_[i] == TRACE_VALUE_TYPE_COPY_STRING);
    if (argIsCopy[i]) {
      allocSize += getAllocLength(argValues_[i].asString);
    }
  }

  if (allocSize) {
    parameterCopyStorage_ = std::make_shared<RefCountedString>();
    parameterCopyStorage_->data().resize(allocSize);
    char* ptr = parameterCopyStorage_->data().data();
    const char* end = ptr + allocSize;
    if (copy) {
      copyTraceEventParameter(&ptr, &name_, end);
      for (i = 0; i < num_args; ++i) {
        copyTraceEventParameter(&ptr, &argNames_[i], end);
      }
    }
    for (i = 0; i < num_args; ++i) {
      if (argTypes_[i] == TRACE_VALUE_TYPE_CONVERTABLE) {
        continue;
      }
      if (argIsCopy[i]) {
        copyTraceEventParameter(&ptr, &argValues_[i].asString, end);
      }
    }
    DCHECK_EQ(end, ptr) << "Overrun by " << ptr - end;
  }
}

void TraceEvent::reset() {
  // Only reset fields that won't be initialized in Initialize(), or that may
  // hold references to other objects.
  duration_ = -1;
  ;
  parameterCopyStorage_ = nullptr;
  for (int i = 0; i < kTraceMaxNumArgs && argNames_[i]; ++i) {
    convertableValues_[i] = nullptr;
  }
}

void TraceEvent::updateDuration(
    const kudu::MicrosecondsInt64& now,
    const kudu::MicrosecondsInt64& thread_now) {
  DCHECK(duration_ == -1);
  duration_ = now - timestamp_;
  threadDuration_ = thread_now - threadTimestamp_;
}

namespace {
// Escape the given string using JSON rules.
void jsonEscape(StringPiece s, string* out) {
  out->reserve(out->size() + s.size() * 2);
  const char* pEnd = s.data() + s.size();
  for (const char* p = s.data(); p != pEnd; p++) {
    // Only the following characters need to be escaped, according to json.org.
    // In particular, it's illegal to escape the single-quote character, and
    // JSON does not support the "\x" escape sequence like C/Java.
    switch (*p) {
      case '"':
      case '\\':
        out->push_back('\\');
        out->push_back(*p);
        break;
      case '\b':
        out->append("\\b");
        break;
      case '\f':
        out->append("\\f");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\r':
        out->append("\\r");
        break;
      case '\t':
        out->append("\\t");
        break;
      default:
        out->push_back(*p);
    }
  }
}
} // anonymous namespace

// static
void TraceEvent::appendValueAsJson(
    unsigned char type,
    TraceEvent::TraceValue value,
    std::string* out) {
  switch (type) {
    case TRACE_VALUE_TYPE_BOOL:
      *out += value.asBool ? "true" : "false";
      break;
    case TRACE_VALUE_TYPE_UINT:
      *out += fmt::format("{}", static_cast<uint64_t>(value.asUint));
      break;
    case TRACE_VALUE_TYPE_INT:
      *out += fmt::format("{}", static_cast<int64_t>(value.asInt));
      break;
    case TRACE_VALUE_TYPE_DOUBLE: {
      // FIXME: base/json/json_writer.cc is using the same code,
      //        should be made into a common method.
      std::string real;
      double val = value.asDouble;
      if (MathLimits<double>::isFinite(val)) {
        real = fmt::format("{}", val);
        // Ensure that the number has a .0 if there's no decimal or 'e'.  This
        // makes sure that when we read the JSON back, it's interpreted as a
        // real rather than an int.
        if (real.find('.') == std::string::npos &&
            real.find('e') == std::string::npos &&
            real.find('E') == std::string::npos) {
          real.append(".0");
        }
        // The JSON spec requires that non-integer values in the range (-1,1)
        // have a zero before the decimal point - ".52" is not valid, "0.52" is.
        if (real[0] == '.') {
          real.insert(0, "0");
        } else if (real.length() > 1 && real[0] == '-' && real[1] == '.') {
          // "-.1" bad "-0.1" good
          real.insert(1, "0");
        }
      } else if (MathLimits<double>::isNaN(val)) {
        // The JSON spec doesn't allow NaN and Infinity (since these are
        // objects in EcmaScript).  Use strings instead.
        real = "\"NaN\"";
      } else if (val < 0) {
        real = "\"-Infinity\"";
      } else {
        real = "\"Infinity\"";
      }
      *out += fmt::format("{}", real);
      break;
    }
    case TRACE_VALUE_TYPE_POINTER:
      // JSON only supports double and int numbers.
      // So as not to lose bits from a 64-bit pointer, output as a hex string.
      *out += fmt::format(
          "\"0x{:x}\"",
          static_cast<uint64_t>(reinterpret_cast<intptr_t>(value.asPointer)));
      break;
    case TRACE_VALUE_TYPE_STRING:
    case TRACE_VALUE_TYPE_COPY_STRING:
      *out += "\"";
      jsonEscape(value.asString ? value.asString : "NULL", out);
      *out += "\"";
      break;
    default:
      LOG(FATAL) << "Don't know how to print this value";
  }
}

void TraceEvent::appendAsJson(std::string* out) const {
  int64_t timeInt64 = timestamp_;
  int process_id = TraceLog::getInstance()->processId();
  // Category group checked at category creation time.
  DCHECK(!strchr(name_, '"'));
  *out += fmt::format(
      "{{\"cat\":\"{}\",\"pid\":{},\"tid\":{},\"ts\":{},"
      "\"ph\":\"{}\",\"name\":\"{}\",\"args\":{{",
      TraceLog::getCategoryGroupName(categoryGroupEnabled_),
      process_id,
      threadId_,
      timeInt64,
      static_cast<char>(phase_),
      name_);

  // Output argument names and values, stop at first NULL argument name.
  for (int i = 0; i < kTraceMaxNumArgs && argNames_[i]; ++i) {
    if (i > 0) {
      *out += ",";
    }
    *out += "\"";
    *out += argNames_[i];
    *out += "\":";

    if (argTypes_[i] == TRACE_VALUE_TYPE_CONVERTABLE) {
      convertableValues_[i]->appendAsTraceFormat(out);
    } else {
      appendValueAsJson(argTypes_[i], argValues_[i], out);
    }
  }
  *out += "}";

  if (phase_ == TRACE_EVENT_PHASE_COMPLETE) {
    int64_t duration = duration_;
    if (duration != -1) {
      *out += fmt::format(",\"dur\":{}", duration);
    }
    if (threadTimestamp_ >= 0) {
      int64_t thread_duration = threadDuration_;
      if (thread_duration != -1) {
        *out += fmt::format(",\"tdur\":{}", thread_duration);
      }
    }
  }

  // Output tts if thread_timestamp is valid.
  if (threadTimestamp_ >= 0) {
    int64_t threadTimeInt64 = threadTimestamp_;
    *out += fmt::format(",\"tts\":{}", threadTimeInt64);
  }

  // If id_ is set, print it out as a hex string so we don't loose any
  // bits (it might be a 64-bit pointer).
  if (flags_ & TRACE_EVENT_FLAG_HAS_ID) {
    *out += fmt::format(",\"id\":\"0x{:x}\"", static_cast<uint64_t>(id_));
  }

  // Instant events also output their scope.
  if (phase_ == TRACE_EVENT_PHASE_INSTANT) {
    char scope = '?';
    switch (flags_ & TRACE_EVENT_FLAG_SCOPE_MASK) {
      case TRACE_EVENT_SCOPE_GLOBAL:
        scope = TRACE_EVENT_SCOPE_NAME_GLOBAL;
        break;

      case TRACE_EVENT_SCOPE_PROCESS:
        scope = TRACE_EVENT_SCOPE_NAME_PROCESS;
        break;

      case TRACE_EVENT_SCOPE_THREAD:
        scope = TRACE_EVENT_SCOPE_NAME_THREAD;
        break;
    }
    *out += fmt::format(",\"s\":\"{}\"", scope);
  }

  *out += "}";
}

void TraceEvent::appendPrettyPrinted(std::ostringstream* out) const {
  *out << name_ << "[";
  *out << TraceLog::getCategoryGroupName(categoryGroupEnabled_);
  *out << "]";
  if (argNames_[0]) {
    *out << ", {";
    for (int i = 0; i < kTraceMaxNumArgs && argNames_[i]; ++i) {
      if (i > 0) {
        *out << ", ";
      }
      *out << argNames_[i] << ":";
      std::string valueAsText;

      if (argTypes_[i] == TRACE_VALUE_TYPE_CONVERTABLE) {
        convertableValues_[i]->appendAsTraceFormat(&valueAsText);
      } else {
        appendValueAsJson(argTypes_[i], argValues_[i], &valueAsText);
      }

      *out << valueAsText;
    }
    *out << "}";
  }
}

////////////////////////////////////////////////////////////////////////////////
//
// TraceResultBuffer
//
////////////////////////////////////////////////////////////////////////////////

string TraceResultBuffer::flushTraceLogToString() {
  return doFlush(false);
}

string TraceResultBuffer::flushTraceLogToStringButLeaveBufferIntact() {
  return doFlush(true);
}

string TraceResultBuffer::doFlush(bool leaveIntact) {
  TraceResultBuffer buf;
  TraceLog* tl = TraceLog::getInstance();
  if (leaveIntact) {
    tl->flushButLeaveBufferIntact(
        Bind(&TraceResultBuffer::collect, unretained(&buf)));
  } else {
    tl->flush(Bind(&TraceResultBuffer::collect, unretained(&buf)));
  }
  buf.json_.append("]}\n");
  return buf.json_;
}

TraceResultBuffer::TraceResultBuffer() : first_(true) {}
TraceResultBuffer::~TraceResultBuffer() {}

void TraceResultBuffer::collect(
    const std::shared_ptr<RefCountedString>& s,
    bool /* hasMoreEvents */) {
  if (first_) {
    json_.append("{\"traceEvents\": [\n");
    first_ = false;
  } else if (!s->data().empty()) {
    // Sometimes we get sent an empty chunk at the end,
    // and we don't want to end up with an extra trailing ','
    json_.append(",\n");
  }
  json_.append(s->data());
}

////////////////////////////////////////////////////////////////////////////////
//
// TraceSamplingThread
//
////////////////////////////////////////////////////////////////////////////////
class TraceBucketData;
using TraceSampleCallback = Callback<void(TraceBucketData*)>;

class TraceBucketData {
 public:
  TraceBucketData(
      AtomicWord* bucket,
      const char* name,
      TraceSampleCallback callback);
  ~TraceBucketData();

  TRACE_EVENT_API_ATOMIC_WORD* bucket;
  const char* bucketName;
  TraceSampleCallback callback;
};

// This object must be created on the IO thread.
class TraceSamplingThread {
 public:
  TraceSamplingThread();
  virtual ~TraceSamplingThread();

  void threadMain();

  static void defaultSamplingCallback(TraceBucketData* bucketData);

  void stop();

 private:
  friend class TraceLog;

  void getSamples();
  // Not thread-safe. Once the threadMain has been called, this can no longer
  // be called.
  void registerSampleBucket(
      TRACE_EVENT_API_ATOMIC_WORD* bucket,
      const char* const name,
      TraceSampleCallback callback);
  // Splits a combined "category\0name" into the two component parts.
  static void extractCategoryAndName(
      const char* combined,
      const char** category,
      const char** name);
  std::vector<TraceBucketData> sampleBuckets_;
  bool threadRunning_;
  AtomicBool cancellationFlag_;
};

TraceSamplingThread::TraceSamplingThread()
    : threadRunning_(false), cancellationFlag_(false) {}

TraceSamplingThread::~TraceSamplingThread() {}

void TraceSamplingThread::threadMain() {
  threadRunning_ = true;
  const MonoDelta sleepDelta = MonoDelta::FromMicroseconds(1000);
  while (!cancellationFlag_.load()) {
    SleepFor(sleepDelta);
    getSamples();
  }
}

// static
void TraceSamplingThread::defaultSamplingCallback(TraceBucketData* bucketData) {
  TRACE_EVENT_API_ATOMIC_WORD categoryAndName =
      TRACE_EVENT_API_ATOMIC_LOAD(*bucketData->bucket);
  if (!categoryAndName) {
    return;
  }
  const char* const combined =
      reinterpret_cast<const char* const>(categoryAndName);
  const char* category_group;
  const char* name;
  extractCategoryAndName(combined, &category_group, &name);
  TRACE_EVENT_API_ADD_TRACE_EVENT(
      TRACE_EVENT_PHASE_SAMPLE,
      TraceLog::getCategoryGroupEnabled(category_group),
      name,
      0,
      0,
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      0);
}

void TraceSamplingThread::getSamples() {
  for (auto& sampleBucket : sampleBuckets_) {
    TraceBucketData* bucketData = &sampleBucket;
    bucketData->callback.Run(bucketData);
  }
}

void TraceSamplingThread::registerSampleBucket(
    TRACE_EVENT_API_ATOMIC_WORD* bucket,
    const char* const name,
    TraceSampleCallback callback) {
  // Access to sampleBuckets_ doesn't cause races with the sampling thread
  // that uses the sampleBuckets_, because it is guaranteed that
  // registerSampleBucket is called before the sampling thread is created.
  DCHECK(!threadRunning_);
  sampleBuckets_.emplace_back(bucket, name, std::move(callback));
}

// static
void TraceSamplingThread::extractCategoryAndName(
    const char* combined,
    const char** category,
    const char** name) {
  *category = combined;
  *name = &combined[strlen(combined) + 1];
}

void TraceSamplingThread::stop() {
  cancellationFlag_.store(true);
}

TraceBucketData::TraceBucketData(
    AtomicWord* bucket,
    const char* name,
    TraceSampleCallback callback)
    : bucket(bucket), bucketName(name), callback(std::move(callback)) {}

TraceBucketData::~TraceBucketData() {}

////////////////////////////////////////////////////////////////////////////////
//
// TraceLog
//
////////////////////////////////////////////////////////////////////////////////

class TraceLog::ThreadLocalEventBuffer {
 public:
  explicit ThreadLocalEventBuffer(TraceLog* trace_log);
  virtual ~ThreadLocalEventBuffer();

  TraceEvent* addTraceEvent(TraceEventHandle* handle);

  TraceEvent* getEventByHandle(TraceEventHandle handle) {
    if (!chunk_ || handle.chunkSeq != chunk_->seq() ||
        handle.chunkIndex != chunkIndex_) {
      return nullptr;
    }

    return chunk_->getEventAt(handle.eventIndex);
  }

  int generation() const {
    return generation_;
  }

  void flush(int64_t tid);

 private:
  // Check that the current thread is the one that constructed this trace
  // buffer.
  void checkIsOwnerThread() const {
    DCHECK_EQ(kudu::Thread::uniqueThreadId(), ownerTid_);
  }

  // Since TraceLog is a leaky singleton, traceLog_ will always be valid
  // as long as the thread exists.
  TraceLog* traceLog_;
  unique_ptr<TraceBufferChunk> chunk_;
  size_t chunkIndex_;
  int generation_;

  // The TID of the thread that constructed this event buffer. Only this thread
  // may add trace events.
  int64_t ownerTid_;

  DISALLOW_COPY_AND_ASSIGN(ThreadLocalEventBuffer);
};

TraceLog::ThreadLocalEventBuffer::ThreadLocalEventBuffer(TraceLog* trace_log)
    : traceLog_(trace_log),
      chunkIndex_(0),
      generation_(trace_log->generation()),
      ownerTid_(kudu::Thread::uniqueThreadId()) {}

TraceLog::ThreadLocalEventBuffer::~ThreadLocalEventBuffer() {}

TraceEvent* TraceLog::ThreadLocalEventBuffer::addTraceEvent(
    TraceEventHandle* handle) {
  checkIsOwnerThread();

  if (chunk_ && chunk_->isFull()) {
    SpinLockHolder lock(traceLog_->lock_);
    flush(Thread::uniqueThreadId());
    chunk_.reset();
  }
  if (!chunk_) {
    SpinLockHolder lock(traceLog_->lock_);
    chunk_ = traceLog_->loggedEvents_->getChunk(&chunkIndex_);
    traceLog_->checkIfBufferIsFullWhileLocked();
  }
  if (!chunk_) {
    return nullptr;
  }

  size_t eventIndex;
  TraceEvent* traceEvent = chunk_->addTraceEvent(&eventIndex);
  if (traceEvent && handle) {
    makeHandle(chunk_->seq(), chunkIndex_, eventIndex, handle);
  }

  return traceEvent;
}

void TraceLog::ThreadLocalEventBuffer::flush(int64_t tid) {
  DCHECK(traceLog_->lock_.isHeld());

  if (!chunk_) {
    return;
  }

  if (traceLog_->checkGeneration(generation_)) {
    // Return the chunk to the buffer only if the generation matches.
    traceLog_->loggedEvents_->returnChunk(chunkIndex_, std::move(chunk_));
  }
}

// static
TraceLog* TraceLog::getInstance() {
  return Singleton<TraceLog>::get();
}

TraceLog::TraceLog()
    : mode_(kDisabled),
      numTracesRecorded_(0),
      eventCallback_(0),
      dispatchingToObserverList_(false),
      processSortIndex_(0),
      processIdHash_(0),
      processId_(0),
      timeOffset_(0),
      watchCategory_(0),
      traceOptions_(kRecordUntilFull),
      samplingThreadHandle_(nullptr),
      categoryFilter_(CategoryFilter::kDefaultCategoryFilterString),
      eventCallbackCategoryFilter_(
          CategoryFilter::kDefaultCategoryFilterString),
      threadSharedChunkIndex_(0),
      generation_(0) {
  // Trace is enabled or disabled on one thread while other threads are
  // accessing the enabled flag. We don't care whether edge-case events are
  // traced or not, so we allow races on the enabled flag to keep the trace
  // macros fast.
  KUDU_ANNONTATE_BENIGN_RACE_SIZED(
      gCategoryGroupEnabled,
      sizeof(gCategoryGroupEnabled),
      "trace_event category enabled");
  for (int i = 0; i < MAX_CATEGORY_GROUPS; ++i) {
    KUDU_ANNONTATE_BENIGN_RACE(
        &gCategoryGroupEnabled[i], "trace_event category enabled");
  }
  setProcessId(static_cast<int>(getpid()));

  string filter = FLAGS_trace_to_console;
  if (!filter.empty()) {
    setEnabled(CategoryFilter(filter), kRecordingMode, kEchoToConsole);
    LOG(ERROR) << "Tracing to console with CategoryFilter '" << filter << "'.";
  }

  loggedEvents_.reset(createTraceBuffer());
}

const unsigned char* TraceLog::getCategoryGroupEnabled(
    const char* category_group) {
  TraceLog* tracelog = getInstance();
  if (!tracelog) {
    DCHECK(!gCategoryGroupEnabled[kCategoryAlreadyShutdown]);
    return &gCategoryGroupEnabled[kCategoryAlreadyShutdown];
  }
  return tracelog->getCategoryGroupEnabledInternal(category_group);
}

const char* TraceLog::getCategoryGroupName(
    const unsigned char* category_group_enabled) {
  // Calculate the index of the category group by finding
  // category_group_enabled in gCategoryGroupEnabled array.
  uintptr_t category_begin = reinterpret_cast<uintptr_t>(gCategoryGroupEnabled);
  uintptr_t category_ptr = reinterpret_cast<uintptr_t>(category_group_enabled);
  DCHECK(
      category_ptr >= category_begin &&
      category_ptr < reinterpret_cast<uintptr_t>(
                         gCategoryGroupEnabled + MAX_CATEGORY_GROUPS))
      << "out of bounds category pointer";
  uintptr_t category_index =
      (category_ptr - category_begin) / sizeof(gCategoryGroupEnabled[0]);
  return gCategoryGroups[category_index];
}

void TraceLog::updateCategoryGroupEnabledFlag(int categoryIndex) {
  unsigned char enabled_flag = 0;
  const char* category_group = gCategoryGroups[categoryIndex];
  if (mode_ == kRecordingMode &&
      categoryFilter_.isCategoryGroupEnabled(category_group)) {
    enabled_flag |= kEnabledForRecording;
  } else if (
      mode_ == kMonitoringMode &&
      categoryFilter_.isCategoryGroupEnabled(category_group)) {
    enabled_flag |= kEnabledForMonitoring;
  }
  if (eventCallback_ &&
      eventCallbackCategoryFilter_.isCategoryGroupEnabled(category_group)) {
    enabled_flag |= kEnabledForEventCallback;
  }
  gCategoryGroupEnabled[categoryIndex] = enabled_flag;
}

void TraceLog::updateCategoryGroupEnabledFlags() {
  int categoryIndex = base::subtle::NoBarrier_Load(&gCategoryIndex);
  for (int i = 0; i < categoryIndex; i++) {
    updateCategoryGroupEnabledFlag(i);
  }
}

void TraceLog::updateSyntheticDelaysFromCategoryFilter() {
  resetTraceEventSyntheticDelays();
  const CategoryFilter::StringList& delays =
      categoryFilter_.getSyntheticDelayValues();
  CategoryFilter::StringList::const_iterator ci;
  for (ci = delays.begin(); ci != delays.end(); ++ci) {
    std::list<string> tokens = strings::split(*ci, ";");
    if (tokens.empty()) {
      continue;
    }

    TraceEventSyntheticDelay* delay =
        TraceEventSyntheticDelay::lookup(tokens.front());
    tokens.pop_front();
    while (!tokens.empty()) {
      std::string token = tokens.front();
      tokens.pop_front();
      char* durationEnd;
      double targetDuration = strtod(token.c_str(), &durationEnd);
      if (durationEnd != token.c_str()) {
        delay->setTargetDuration(MonoDelta::FromSeconds(targetDuration));
      } else if (token == "static") {
        delay->setMode(TraceEventSyntheticDelay::kStatic);
      } else if (token == "oneshot") {
        delay->setMode(TraceEventSyntheticDelay::kOneShot);
      } else if (token == "alternating") {
        delay->setMode(TraceEventSyntheticDelay::kAlternating);
      }
    }
  }
}

const unsigned char* TraceLog::getCategoryGroupEnabledInternal(
    const char* category_group) {
  DCHECK(!strchr(category_group, '"'))
      << "Category groups may not contain double quote";
  // The gCategoryGroups is append only, avoid using a lock for the fast path.
  int current_category_index = base::subtle::Acquire_Load(&gCategoryIndex);

  // Search for pre-existing category group.
  for (int i = 0; i < current_category_index; ++i) {
    if (strcmp(gCategoryGroups[i], category_group) == 0) {
      return &gCategoryGroupEnabled[i];
    }
  }

  unsigned char* category_group_enabled = nullptr;
  // This is the slow path: the lock is not held in the case above, so more
  // than one thread could have reached here trying to add the same category.
  // Only hold to lock when actually appending a new category, and
  // check the categories groups again.
  SpinLockHolder lock(lock_);
  int category_index = base::subtle::Acquire_Load(&gCategoryIndex);
  for (int i = 0; i < category_index; ++i) {
    if (strcmp(gCategoryGroups[i], category_group) == 0) {
      return &gCategoryGroupEnabled[i];
    }
  }

  // Create a new category group.
  DCHECK(category_index < MAX_CATEGORY_GROUPS)
      << "must increase MAX_CATEGORY_GROUPS";
  if (category_index < MAX_CATEGORY_GROUPS) {
    // Don't hold on to the category_group pointer, so that we can create
    // category groups with strings not known at compile time (this is
    // required by SetWatchEvent).
    const char* new_group = strdup(category_group);
    // NOTE: new_group is leaked, but this is a small finite amount of data
    gCategoryGroups[category_index] = new_group;
    DCHECK(!gCategoryGroupEnabled[category_index]);
    // Note that if both included and excluded patterns in the
    // CategoryFilter are empty, we exclude nothing,
    // thereby enabling this category group.
    updateCategoryGroupEnabledFlag(category_index);
    category_group_enabled = &gCategoryGroupEnabled[category_index];
    // Update the max index now.
    base::subtle::Release_Store(&gCategoryIndex, category_index + 1);
  } else {
    category_group_enabled =
        &gCategoryGroupEnabled[kCategoryCategoriesExhausted];
  }
  return category_group_enabled;
}

void TraceLog::getKnownCategoryGroups(
    std::vector<std::string>* category_groups) {
  SpinLockHolder lock(lock_);
  int category_index = base::subtle::NoBarrier_Load(&gCategoryIndex);
  for (int i = kNumBuiltinCategories; i < category_index; i++) {
    category_groups->emplace_back(gCategoryGroups[i]);
  }
}

void TraceLog::setEnabled(
    const CategoryFilter& category_filter,
    Mode mode,
    Options options) {
  std::vector<EnabledStateObserver*> observerList;
  {
    SpinLockHolder lock(lock_);

    // Can't enable tracing when flush() is in progress.
    Options oldOptions = traceOptions();

    if (isEnabled()) {
      if (options != oldOptions) {
        DLOG(ERROR) << "Attempting to re-enable tracing with a different "
                    << "set of options.";
      }

      if (mode != mode_) {
        DLOG(ERROR) << "Attempting to re-enable tracing with a different mode.";
      }

      categoryFilter_.merge(category_filter);
      updateCategoryGroupEnabledFlags();
      return;
    }

    if (dispatchingToObserverList_) {
      DLOG(ERROR)
          << "Cannot manipulate TraceLog::Enabled state from an observer.";
      return;
    }

    mode_ = mode;

    if (options != oldOptions) {
      base::subtle::NoBarrier_Store(&traceOptions_, options);
      useNextTraceBuffer();
    }

    numTracesRecorded_++;

    categoryFilter_ = CategoryFilter(category_filter);
    updateCategoryGroupEnabledFlags();
    updateSyntheticDelaysFromCategoryFilter();

    if (options & kEnableSampling) {
      samplingThread_.reset(new TraceSamplingThread);
      samplingThread_->registerSampleBucket(
          &gTraceState[0],
          "bucket0",
          Bind(&TraceSamplingThread::defaultSamplingCallback));
      samplingThread_->registerSampleBucket(
          &gTraceState[1],
          "bucket1",
          Bind(&TraceSamplingThread::defaultSamplingCallback));
      samplingThread_->registerSampleBucket(
          &gTraceState[2],
          "bucket2",
          Bind(&TraceSamplingThread::defaultSamplingCallback));

      Status s = Thread::create(
          "tracing",
          "sampler",
          &TraceSamplingThread::threadMain,
          samplingThread_.get(),
          &samplingThreadHandle_);
      if (!s.ok()) {
        LOG(DFATAL) << "failed to create trace sampling thread: "
                    << s.ToString();
      }
    }

    dispatchingToObserverList_ = true;
    observerList = enabledStateObserverList_;
  }
  // Notify observers outside the lock in case they trigger trace events.
  for (const auto& observer : observerList) {
    observer->onTraceLogEnabled();
  }

  {
    SpinLockHolder lock(lock_);
    dispatchingToObserverList_ = false;
  }
}

CategoryFilter TraceLog::getCurrentCategoryFilter() {
  SpinLockHolder lock(lock_);
  return categoryFilter_;
}

void TraceLog::setDisabled() {
  SpinLockHolder lock(lock_);
  setDisabledWhileLocked();
}

void TraceLog::setDisabledWhileLocked() {
  DCHECK(lock_.isHeld());

  if (!isEnabled()) {
    return;
  }

  if (dispatchingToObserverList_) {
    DLOG(ERROR)
        << "Cannot manipulate TraceLog::Enabled state from an observer.";
    return;
  }

  mode_ = kDisabled;

  if (samplingThread_.get()) {
    // Stop the sampling thread.
    samplingThread_->stop();
    lock_.unlock();
    samplingThreadHandle_->join();
    lock_.lock();
    samplingThreadHandle_.reset();
    samplingThread_.reset();
  }

  categoryFilter_.clear();
  base::subtle::NoBarrier_Store(&watchCategory_, 0);
  watchEventName_ = "";
  updateCategoryGroupEnabledFlags();
  addMetadataEventsWhileLocked();

  dispatchingToObserverList_ = true;
  std::vector<EnabledStateObserver*> observerList = enabledStateObserverList_;

  {
    // Dispatch to observers outside the lock in case the observer triggers a
    // trace event.
    lock_.unlock();
    for (const auto& observer : observerList) {
      observer->onTraceLogDisabled();
    }
    lock_.lock();
  }
  dispatchingToObserverList_ = false;
}

int TraceLog::getNumTracesRecorded() {
  SpinLockHolder lock(lock_);
  if (!isEnabled()) {
    return -1;
  }
  return numTracesRecorded_;
}

void TraceLog::addEnabledStateObserver(EnabledStateObserver* listener) {
  enabledStateObserverList_.push_back(listener);
}

void TraceLog::removeEnabledStateObserver(EnabledStateObserver* listener) {
  auto it = std::find(
      enabledStateObserverList_.begin(),
      enabledStateObserverList_.end(),
      listener);
  if (it != enabledStateObserverList_.end()) {
    enabledStateObserverList_.erase(it);
  }
}

bool TraceLog::hasEnabledStateObserver(EnabledStateObserver* listener) const {
  auto it = std::find(
      enabledStateObserverList_.begin(),
      enabledStateObserverList_.end(),
      listener);
  return it != enabledStateObserverList_.end();
}

float TraceLog::getBufferPercentFull() const {
  SpinLockHolder lock(lock_);
  return static_cast<float>(
      static_cast<double>(loggedEvents_->size()) / loggedEvents_->capacity());
}

bool TraceLog::bufferIsFull() const {
  SpinLockHolder lock(lock_);
  return loggedEvents_->isFull();
}

TraceBuffer* TraceLog::createTraceBuffer() {
  Options options = traceOptions();
  if (options & kRecordContinuously) {
    return new TraceBufferRingBuffer(kTraceEventRingBufferChunks);
  } else if ((options & kEnableSampling) && mode_ == kMonitoringMode) {
    return new TraceBufferRingBuffer(kMonitorTraceEventBufferChunks);
  } else if (options & kEchoToConsole) {
    return new TraceBufferRingBuffer(kEchoToConsoleTraceEventBufferChunks);
  }
  return new TraceBufferVector();
}

TraceEvent* TraceLog::addEventToThreadSharedChunkWhileLocked(
    TraceEventHandle* handle,
    bool checkBufferIsFull) {
  DCHECK(lock_.isHeld());

  if (threadSharedChunk_ && threadSharedChunk_->isFull()) {
    loggedEvents_->returnChunk(
        threadSharedChunkIndex_, std::move(threadSharedChunk_));
  }

  if (!threadSharedChunk_) {
    threadSharedChunk_ = loggedEvents_->getChunk(&threadSharedChunkIndex_);
    if (checkBufferIsFull) {
      checkIfBufferIsFullWhileLocked();
    }
  }
  if (!threadSharedChunk_) {
    return nullptr;
  }

  size_t eventIndex;
  TraceEvent* traceEvent = threadSharedChunk_->addTraceEvent(&eventIndex);
  if (traceEvent && handle) {
    makeHandle(
        threadSharedChunk_->seq(), threadSharedChunkIndex_, eventIndex, handle);
  }
  return traceEvent;
}

void TraceLog::checkIfBufferIsFullWhileLocked() {
  DCHECK(lock_.isHeld());
  if (loggedEvents_->isFull()) {
    setDisabledWhileLocked();
  }
}

void TraceLog::setEventCallbackEnabled(
    const CategoryFilter& category_filter,
    EventCallback cb) {
  SpinLockHolder lock(lock_);
  base::subtle::NoBarrier_Store(
      &eventCallback_, reinterpret_cast<AtomicWord>(cb));
  eventCallbackCategoryFilter_ = category_filter;
  updateCategoryGroupEnabledFlags();
};

void TraceLog::setEventCallbackDisabled() {
  SpinLockHolder lock(lock_);
  base::subtle::NoBarrier_Store(&eventCallback_, 0);
  updateCategoryGroupEnabledFlags();
}

// flush() works as the following:
//
// We ensure by taking the global lock that we have exactly one Flusher thread
// (the caller of this function) and some number of "target" threads. We do
// not want to block the target threads, since they are running application
// code, so this implementation takes an approach based on asymmetric
// synchronization.
//
// For each active thread, we grab its PerThreadInfo object, which may contain
// a pointer to its active trace chunk. We use an AtomicExchange to swap this
// out for a null pointer. This ensures that, on the *next* TRACE call made by
// that thread, it will see a NULL buffer and create a _new_ trace buffer. That
// new buffer would be assigned the generation of the next collection and we
// don't have to worry about it in the current flush().
//
// However, the swap doesn't ensure that the thread doesn't already have a local
// copy of the 'eventBuffer_' that we are trying to flush. So, if the thread is
// in the middle of a Trace call, we have to wait until it exits. We do that by
// spinning on the 'isInTraceEvent_' member of that thread's thread-local
// structure.
//
// After we've swapped the buffer pointer and waited on the thread to exit any
// concurrent Trace() call, we know that no other thread can hold a pointer to
// the trace buffer, and we can safely flush it and delete it.
void TraceLog::flush(const TraceLog::OutputCallback& cb) {
  if (isEnabled()) {
    // Can't flush when tracing is enabled because otherwise PostTask would
    // - generate more trace events;
    // - deschedule the calling thread on some platforms causing inaccurate
    //   timing of the trace events.
    std::shared_ptr<RefCountedString> emptyResult =
        std::make_shared<RefCountedString>();
    if (!cb.is_null()) {
      cb.Run(emptyResult, false);
    }
    LOG(WARNING) << "Ignored TraceLog::flush called when tracing is enabled";
    return;
  }

  int generation = this->generation();
  {
    // Holding the active threads lock ensures that no thread will exit and
    // delete its own PerThreadInfo object.
    MutexLock l(activeThreadsLock_);
    for (const ActiveThreadMap::value_type& entry : activeThreads_) {
      int64_t tid = entry.first;
      PerThreadInfo* thrInfo = entry.second;

      // Swap out their buffer from their thread-local data.
      // After this, any _future_ trace calls on that thread will create a new
      // buffer and not use the one we obtain here.
      ThreadLocalEventBuffer* buf = thrInfo->atomicTakeBuffer();

      // If this thread hasn't traced anything since our last
      // flush, we can skip it.
      if (!buf) {
        continue;
      }

      // The buffer may still be in use by that thread if they're in a call.
      // Sleep until they aren't, so we can flush/delete their old buffer.
      //
      // It's important that we do not hold 'lock_' here, because otherwise we
      // can get a deadlock: a thread may be in the middle of a trace event
      // (isInTraceEvent_ == true) and waiting to take lock_, while we are
      // holding the lock and waiting for it to not be in the trace event.
      while (base::subtle::Acquire_Load(&thrInfo->isInTraceEvent_)) {
        sched_yield();
      }

      {
        SpinLockHolder lock(lock_);
        buf->flush(tid);
      }
      delete buf;
    }
  }

  {
    SpinLockHolder lock(lock_);

    if (threadSharedChunk_) {
      loggedEvents_->returnChunk(
          threadSharedChunkIndex_, std::move(threadSharedChunk_));
    }
  }

  finishFlush(generation, cb);
}

void TraceLog::convertTraceEventsToTraceFormat(
    unique_ptr<TraceBuffer> logged_events,
    const TraceLog::OutputCallback& flush_output_callback) {
  if (flush_output_callback.is_null()) {
    return;
  }

  // The callback need to be called at least once even if there is no events
  // to let the caller know the completion of flush.
  bool hasMoreEvents = true;
  do {
    std::shared_ptr<RefCountedString> jsonEventsStrPtr =
        std::make_shared<RefCountedString>();

    for (size_t i = 0; i < kTraceEventBatchChunks; ++i) {
      const TraceBufferChunk* chunk = logged_events->nextChunk();
      if (!chunk) {
        hasMoreEvents = false;
        break;
      }
      for (size_t j = 0; j < chunk->size(); ++j) {
        if (i > 0 || j > 0) {
          jsonEventsStrPtr->data().append(",");
        }
        chunk->getEventAt(j)->appendAsJson(&(jsonEventsStrPtr->data()));
      }
    }

    flush_output_callback.Run(jsonEventsStrPtr, hasMoreEvents);
  } while (hasMoreEvents);
  logged_events.reset();
}

void TraceLog::finishFlush(
    int generation,
    const TraceLog::OutputCallback& flush_output_callback) {
  unique_ptr<TraceBuffer> previousLoggedEvents;

  if (!checkGeneration(generation)) {
    return;
  }

  {
    SpinLockHolder lock(lock_);

    previousLoggedEvents.swap(loggedEvents_);
    useNextTraceBuffer();
  }

  convertTraceEventsToTraceFormat(
      std::move(previousLoggedEvents), flush_output_callback);
}

void TraceLog::flushButLeaveBufferIntact(
    const TraceLog::OutputCallback& flush_output_callback) {
  unique_ptr<TraceBuffer> previousLoggedEvents;
  {
    SpinLockHolder lock(lock_);
    if (mode_ == kDisabled || (traceOptions_ & kRecordContinuously) == 0) {
      std::shared_ptr<RefCountedString> emptyResult =
          std::make_shared<RefCountedString>();
      flush_output_callback.Run(emptyResult, false);
      LOG(WARNING)
          << "Ignored TraceLog::flushButLeaveBufferIntact when monitoring is not enabled";
      return;
    }

    addMetadataEventsWhileLocked();
    if (threadSharedChunk_) {
      // Return the chunk to the main buffer to flush the sampling data.
      loggedEvents_->returnChunk(
          threadSharedChunkIndex_, std::move(threadSharedChunk_));
    }
    previousLoggedEvents = loggedEvents_->cloneForIteration();
  }

  convertTraceEventsToTraceFormat(
      std::move(previousLoggedEvents), flush_output_callback);
}

void TraceLog::useNextTraceBuffer() {
  loggedEvents_.reset(createTraceBuffer());
  base::subtle::NoBarrier_AtomicIncrement(&generation_, 1);
  threadSharedChunk_.reset();
  threadSharedChunkIndex_ = 0;
}

TraceEventHandle TraceLog::addTraceEvent(
    char phase,
    const unsigned char* category_group_enabled,
    const char* name,
    uint64_t id,
    int num_args,
    const char** arg_names,
    const unsigned char* arg_types,
    const uint64_t* arg_values,
    const std::shared_ptr<ConvertableToTraceFormat>* convertable_values,
    unsigned char flags) {
  int thread_id = static_cast<int>(kudu::Thread::uniqueThreadId());
  kudu::MicrosecondsInt64 now = getMonoTimeMicros();
  return addTraceEventWithThreadIdAndTimestamp(
      phase,
      category_group_enabled,
      name,
      id,
      thread_id,
      now,
      num_args,
      arg_names,
      arg_types,
      arg_values,
      convertable_values,
      flags);
}

TraceLog::PerThreadInfo* TraceLog::setupThreadLocalBuffer() {
  int64_t curTid = Thread::uniqueThreadId();

  auto thrInfo = new PerThreadInfo();
  thrInfo->eventBuffer_ = nullptr;
  thrInfo->isInTraceEvent_ = 0;
  threadLocalInfo_ = thrInfo;

  threadlocal::internal::addDestructor(&TraceLog::threadExitingCb, this);

  {
    MutexLock lock(activeThreadsLock_);
    auto [it, inserted] = activeThreads_.insert({curTid, thrInfo});
    CHECK(inserted);
  }
  return thrInfo;
}

void TraceLog::threadExitingCb(void* arg) {
  static_cast<TraceLog*>(arg)->threadExiting();
}

void TraceLog::threadExiting() {
  PerThreadInfo* thrInfo = threadLocalInfo_;
  if (!thrInfo) {
    return;
  }

  int64_t curTid = Thread::uniqueThreadId();

  // Flush our own buffer back to the central event buffer.
  // We do the atomic exchange because a flusher thread may
  // also be trying to flush us at the same time, and we need to avoid
  // conflict.
  ThreadLocalEventBuffer* buf = thrInfo->atomicTakeBuffer();
  if (buf) {
    SpinLockHolder lock(lock_);
    buf->flush(Thread::uniqueThreadId());
  }
  delete buf;

  {
    MutexLock lock(activeThreadsLock_);
    activeThreads_.erase(curTid);
  }
  delete thrInfo;
}

TraceEventHandle TraceLog::addTraceEventWithThreadIdAndTimestamp(
    char phase,
    const unsigned char* category_group_enabled,
    const char* name,
    uint64_t id,
    int thread_id,
    const kudu::MicrosecondsInt64& timestamp,
    int num_args,
    const char** arg_names,
    const unsigned char* arg_types,
    const uint64_t* arg_values,
    const std::shared_ptr<ConvertableToTraceFormat>* convertable_values,
    unsigned char flags) {
  TraceEventHandle handle = {0, 0, 0};
  if (!*category_group_enabled) {
    return handle;
  }

  DCHECK(name);

  if (flags & TRACE_EVENT_FLAG_MANGLE_ID) {
    id ^= processIdHash_;
  }

  kudu::MicrosecondsInt64 now = offsetTimestamp(timestamp);
  kudu::MicrosecondsInt64 thread_now = getThreadCpuTimeMicros();

  PerThreadInfo* thrInfo = threadLocalInfo_;
  if (PREDICT_FALSE(!thrInfo)) {
    thrInfo = setupThreadLocalBuffer();
  }

  // Avoid re-entrance of addTraceEvent. This may happen in GPU process when
  // kEchoToConsole is enabled: addTraceEvent -> LOG(ERROR) ->
  // GpuProcessLogMessageHandler -> PostPendingTask -> TRACE_EVENT ...
  if (base::subtle::NoBarrier_Load(&thrInfo->isInTraceEvent_)) {
    return handle;
  }

  MarkFlagInScope threadIsInTraceEvent(&thrInfo->isInTraceEvent_);

  ThreadLocalEventBuffer* threadLocalEventBuffer =
      reinterpret_cast<ThreadLocalEventBuffer*>(base::subtle::NoBarrier_Load(
          reinterpret_cast<AtomicWord*>(&thrInfo->eventBuffer_)));

  // If we have an event buffer, but it's a left-over from a previous trace,
  // delete it.
  if (PREDICT_FALSE(
          threadLocalEventBuffer &&
          !checkGeneration(threadLocalEventBuffer->generation()))) {
    // We might also race against a flusher thread, so we have to atomically
    // take the buffer.
    threadLocalEventBuffer = thrInfo->atomicTakeBuffer();
    delete threadLocalEventBuffer;
    threadLocalEventBuffer = nullptr;
  }

  // If there is no current buffer, create one for this event.
  if (PREDICT_FALSE(!threadLocalEventBuffer)) {
    threadLocalEventBuffer = new ThreadLocalEventBuffer(this);

    base::subtle::NoBarrier_Store(
        reinterpret_cast<AtomicWord*>(&thrInfo->eventBuffer_),
        reinterpret_cast<AtomicWord>(threadLocalEventBuffer));
  }

  // Check and update the current thread name only if the event is for the
  // current thread to avoid locks in most cases.
  if (thread_id == static_cast<int>(Thread::uniqueThreadId())) {
    Thread* kudu_thr = Thread::currentThread();
    if (kudu_thr) {
      const char* newName = kudu_thr->name().c_str();
      // Check if the thread name has been set or changed since the previous
      // call (if any), but don't bother if the new name is empty. Note this
      // will not detect a thread name change within the same char* buffer
      // address: we favor common case performance over corner case correctness.
      if (PREDICT_FALSE(newName != gCurrentThreadName && newName && *newName)) {
        gCurrentThreadName = newName;

        SpinLockHolder threadInfoLock(threadInfoLock_);

        auto existingName = threadNames_.find(thread_id);
        if (existingName == threadNames_.end()) {
          // This is a new thread id, and a new name.
          threadNames_[thread_id] = newName;
        } else {
          // This is a thread id that we've seen before, but potentially with a
          // new name.
          std::vector<StringPiece> existingNames =
              strings::split(existingName->second, ",");
          bool found =
              std::find(existingNames.begin(), existingNames.end(), newName) !=
              existingNames.end();
          if (!found) {
            if (existingNames.size()) {
              existingName->second.push_back(',');
            }
            existingName->second.append(newName);
          }
        }
      }
    }
  }

  std::string consoleMessage;
  if (*category_group_enabled &
      (kEnabledForRecording | kEnabledForMonitoring)) {
    TraceEvent* traceEvent = threadLocalEventBuffer->addTraceEvent(&handle);

    if (traceEvent) {
      traceEvent->initialize(
          thread_id,
          now,
          thread_now,
          phase,
          category_group_enabled,
          name,
          id,
          num_args,
          arg_names,
          arg_types,
          arg_values,
          convertable_values,
          flags);

#if defined(OS_ANDROID)
      traceEvent->SendToATrace();
#endif
    }

    if (traceOptions() & kEchoToConsole) {
      consoleMessage = eventToConsoleMessage(
          phase == TRACE_EVENT_PHASE_COMPLETE ? TRACE_EVENT_PHASE_BEGIN : phase,
          timestamp,
          traceEvent);
    }
  }

  if (PREDICT_FALSE(consoleMessage.size())) {
    LOG(ERROR) << consoleMessage;
  }

  if (PREDICT_FALSE(
          reinterpret_cast<const unsigned char*>(base::subtle::NoBarrier_Load(
              &watchCategory_)) == category_group_enabled)) {
    bool eventNameMatches;
    WatchEventCallback watchEventCallbackCopy;
    {
      SpinLockHolder lock(lock_);
      eventNameMatches = watchEventName_ == name;
      watchEventCallbackCopy = watchEventCallback_;
    }
    if (eventNameMatches) {
      if (!watchEventCallbackCopy.is_null()) {
        watchEventCallbackCopy.Run();
      }
    }
  }

  if (PREDICT_FALSE(*category_group_enabled & kEnabledForEventCallback)) {
    EventCallback eventCallback = reinterpret_cast<EventCallback>(
        base::subtle::NoBarrier_Load(&eventCallback_));
    if (eventCallback) {
      eventCallback(
          now,
          phase == TRACE_EVENT_PHASE_COMPLETE ? TRACE_EVENT_PHASE_BEGIN : phase,
          category_group_enabled,
          name,
          id,
          num_args,
          arg_names,
          arg_types,
          arg_values,
          flags);
    }
  }

  return handle;
}

// May be called when a COMPELETE event ends and the unfinished event has been
// recycled (phase == TRACE_EVENT_PHASE_END and traceEvent == NULL).
std::string TraceLog::eventToConsoleMessage(
    unsigned char phase,
    const kudu::MicrosecondsInt64& timestamp,
    TraceEvent* traceEvent) {
  SpinLockHolder threadInfoLock(threadInfoLock_);

  // The caller should translate TRACE_EVENT_PHASE_COMPLETE to
  // TRACE_EVENT_PHASE_BEGIN or TRACE_EVENT_END.
  DCHECK(phase != TRACE_EVENT_PHASE_COMPLETE);

  kudu::MicrosecondsInt64 duration;
  int thread_id =
      traceEvent ? traceEvent->threadId() : Thread::uniqueThreadId();
  if (phase == TRACE_EVENT_PHASE_END) {
    duration = timestamp - threadEventStartTimes_[thread_id].top();
    threadEventStartTimes_[thread_id].pop();
  }

  std::string threadName = threadNames_[thread_id];
  if (!threadColors_.contains(threadName)) {
    threadColors_[threadName] = (threadColors_.size() % 6) + 1;
  }

  std::ostringstream log;
  log << fmt::format("{}: \x1b[0;3{}m", threadName, threadColors_[threadName]);

  size_t depth = 0;
  if (threadEventStartTimes_.find(thread_id) != threadEventStartTimes_.end()) {
    depth = threadEventStartTimes_[thread_id].size();
  }

  for (size_t i = 0; i < depth; ++i) {
    log << "| ";
  }

  if (traceEvent) {
    traceEvent->appendPrettyPrinted(&log);
  }
  if (phase == TRACE_EVENT_PHASE_END) {
    log << fmt::format(" ({:.3f} ms)", duration / 1000.0f);
  }

  log << "\x1b[0;m";

  if (phase == TRACE_EVENT_PHASE_BEGIN) {
    threadEventStartTimes_[thread_id].push(timestamp);
  }

  return log.str();
}

void TraceLog::addTraceEventEtw(
    char phase,
    const char* name,
    const void* id,
    const char* extra) {
#if defined(OS_WIN)
  TraceEventETWProvider::Trace(name, phase, id, extra);
#endif
  INTERNAL_TRACE_EVENT_ADD(
      phase,
      "ETW Trace Event",
      name,
      TRACE_EVENT_FLAG_COPY,
      "id",
      id,
      "extra",
      extra);
}

void TraceLog::addTraceEventEtw(
    char phase,
    const char* name,
    const void* id,
    const std::string& extra) {
#if defined(OS_WIN)
  TraceEventETWProvider::Trace(name, phase, id, extra);
#endif
  INTERNAL_TRACE_EVENT_ADD(
      phase,
      "ETW Trace Event",
      name,
      TRACE_EVENT_FLAG_COPY,
      "id",
      id,
      "extra",
      extra);
}

void TraceLog::updateTraceEventDuration(
    const unsigned char* category_group_enabled,
    const char* name,
    TraceEventHandle handle) {
  PerThreadInfo* thrInfo = threadLocalInfo_;
  if (!thrInfo) {
    thrInfo = setupThreadLocalBuffer();
  }

  // Avoid re-entrance of addTraceEvent. This may happen in GPU process when
  // kEchoToConsole is enabled: addTraceEvent -> LOG(ERROR) ->
  // GpuProcessLogMessageHandler -> PostPendingTask -> TRACE_EVENT ...
  if (base::subtle::NoBarrier_Load(&thrInfo->isInTraceEvent_)) {
    return;
  }
  MarkFlagInScope threadIsInTraceEvent(&thrInfo->isInTraceEvent_);

  kudu::MicrosecondsInt64 thread_now = getThreadCpuTimeMicros();
  kudu::MicrosecondsInt64 now = offsetNow();

  std::string consoleMessage;
  if (*category_group_enabled & kEnabledForRecording) {
    OptionalAutoLock lock(lock_);

    TraceEvent* traceEvent = getEventByHandleInternal(handle, &lock);
    if (traceEvent) {
      DCHECK(traceEvent->phase() == TRACE_EVENT_PHASE_COMPLETE);
      traceEvent->updateDuration(now, thread_now);
#if defined(OS_ANDROID)
      traceEvent->SendToATrace();
#endif
    }

    if (traceOptions() & kEchoToConsole) {
      consoleMessage =
          eventToConsoleMessage(TRACE_EVENT_PHASE_END, now, traceEvent);
    }
  }

  if (consoleMessage.size()) {
    LOG(ERROR) << consoleMessage;
  }

  if (*category_group_enabled & kEnabledForEventCallback) {
    EventCallback eventCallback = reinterpret_cast<EventCallback>(
        base::subtle::NoBarrier_Load(&eventCallback_));
    if (eventCallback) {
      eventCallback(
          now,
          TRACE_EVENT_PHASE_END,
          category_group_enabled,
          name,
          trace_event_internal::kNoEventId,
          0,
          nullptr,
          nullptr,
          nullptr,
          TRACE_EVENT_FLAG_NONE);
    }
  }
}

void TraceLog::setWatchEvent(
    const std::string& category_name,
    const std::string& event_name,
    const WatchEventCallback& callback) {
  const unsigned char* category =
      getCategoryGroupEnabled(category_name.c_str());
  SpinLockHolder lock(lock_);
  base::subtle::NoBarrier_Store(
      &watchCategory_, reinterpret_cast<AtomicWord>(category));
  watchEventName_ = event_name;
  watchEventCallback_ = callback;
}

void TraceLog::cancelWatchEvent() {
  SpinLockHolder lock(lock_);
  base::subtle::NoBarrier_Store(&watchCategory_, 0);
  watchEventName_ = "";
  watchEventCallback_.Reset();
}

void TraceLog::addMetadataEventsWhileLocked() {
  DCHECK(lock_.isHeld());

#if !defined(OS_NACL) // NaCl shouldn't expose the process id.
  initializeMetadataEvent(
      addEventToThreadSharedChunkWhileLocked(nullptr, false),
      0,
      "num_cpus",
      "number",
      base::numCpus());
#endif

  int currentThreadId = static_cast<int>(kudu::Thread::uniqueThreadId());
  if (processSortIndex_ != 0) {
    initializeMetadataEvent(
        addEventToThreadSharedChunkWhileLocked(nullptr, false),
        currentThreadId,
        "process_sort_index",
        "sort_index",
        processSortIndex_);
  }

  if (processName_.size()) {
    initializeMetadataEvent(
        addEventToThreadSharedChunkWhileLocked(nullptr, false),
        currentThreadId,
        "process_name",
        "name",
        processName_);
  }

  if (processLabels_.size() > 0) {
    std::vector<std::string> labels;
    for (auto& label : processLabels_) {
      labels.push_back(label.second);
    }
    initializeMetadataEvent(
        addEventToThreadSharedChunkWhileLocked(nullptr, false),
        currentThreadId,
        "process_labels",
        "labels",
        JoinStrings(labels, ","));
  }

  // Thread sort indices.
  for (auto& sort_index : threadSortIndices_) {
    if (sort_index.second == 0) {
      continue;
    }
    initializeMetadataEvent(
        addEventToThreadSharedChunkWhileLocked(nullptr, false),
        sort_index.first,
        "thread_sort_index",
        "sort_index",
        sort_index.second);
  }

  // Thread names.
  SpinLockHolder threadInfoLock(threadInfoLock_);
  for (auto& name : threadNames_) {
    if (name.second.empty()) {
      continue;
    }
    initializeMetadataEvent(
        addEventToThreadSharedChunkWhileLocked(nullptr, false),
        name.first,
        "thread_name",
        "name",
        name.second);
  }
}

TraceEvent* TraceLog::getEventByHandle(TraceEventHandle handle) {
  return getEventByHandleInternal(handle, nullptr);
}

TraceEvent* TraceLog::getEventByHandleInternal(
    TraceEventHandle handle,
    OptionalAutoLock* lock) {
  TraceLog::PerThreadInfo* thrInfo = TraceLog::threadLocalInfo_;

  if (!handle.chunkSeq) {
    return nullptr;
  }

  if (thrInfo) {
    ThreadLocalEventBuffer* buf =
        reinterpret_cast<ThreadLocalEventBuffer*>(base::subtle::NoBarrier_Load(
            reinterpret_cast<AtomicWord*>(&thrInfo->eventBuffer_)));

    if (buf) {
      DCHECK_EQ(1, KUDU_ANNONTATE_UNPROTECTED_READ(thrInfo->isInTraceEvent_));

      TraceEvent* traceEvent = buf->getEventByHandle(handle);
      if (traceEvent) {
        return traceEvent;
      }
    }
  }

  // The event has been out-of-control of the thread local buffer.
  // Try to get the event from the main buffer with a lock.
  if (lock) {
    lock->ensureAcquired();
  }

  if (threadSharedChunk_ && handle.chunkIndex == threadSharedChunkIndex_) {
    return handle.chunkSeq == threadSharedChunk_->seq()
        ? threadSharedChunk_->getEventAt(handle.eventIndex)
        : nullptr;
  }

  return loggedEvents_->getEventByHandle(handle);
}

ATTRIBUTE_NO_SANITIZE_INTEGER
void TraceLog::setProcessId(int processId) {
  processId_ = processId;
  // Create a FNV hash from the process ID for XORing.
  // See http://isthe.com/chongo/tech/comp/fnv/ for algorithm details.
  uint64_t offsetBasis = 14695981039346656037ull;
  uint64_t fnvPrime = 1099511628211ull;
  uint64_t pid = static_cast<uint64_t>(processId_);
  processIdHash_ = (offsetBasis ^ pid) * fnvPrime;
}

void TraceLog::setProcessSortIndex(int sortIndex) {
  SpinLockHolder lock(lock_);
  processSortIndex_ = sortIndex;
}

void TraceLog::setProcessName(const std::string& processName) {
  SpinLockHolder lock(lock_);
  processName_ = processName;
}

void TraceLog::updateProcessLabel(
    int labelId,
    const std::string& currentLabel) {
  if (!currentLabel.length()) {
    return removeProcessLabel(labelId);
  }

  SpinLockHolder lock(lock_);
  processLabels_[labelId] = currentLabel;
}

void TraceLog::removeProcessLabel(int labelId) {
  SpinLockHolder lock(lock_);
  auto it = processLabels_.find(labelId);
  if (it == processLabels_.end()) {
    return;
  }

  processLabels_.erase(it);
}

void TraceLog::setThreadSortIndex(int64_t thread_id, int sort_index) {
  SpinLockHolder lock(lock_);
  threadSortIndices_[static_cast<int>(thread_id)] = sort_index;
}

void TraceLog::setTimeOffset(kudu::MicrosecondsInt64 offset) {
  timeOffset_ = offset;
}

size_t TraceLog::getObserverCountForTest() const {
  return enabledStateObserverList_.size();
}

bool CategoryFilter::isEmptyOrContainsLeadingOrTrailingWhitespace(
    const std::string& str) {
  return str.empty() || str.at(0) == ' ' || str.at(str.length() - 1) == ' ';
}

bool CategoryFilter::doesCategoryGroupContainCategory(
    const char* category_group,
    const char* category) const {
  DCHECK(category);
  vector<string> pieces = strings::split(category_group, ",");
  for (const string& categoryGroupToken : pieces) {
    // Don't allow empty tokens, nor tokens with leading or trailing space.
    DCHECK(!CategoryFilter::isEmptyOrContainsLeadingOrTrailingWhitespace(
        categoryGroupToken))
        << "Disallowed category string";

    if (matchPattern(categoryGroupToken.c_str(), category)) {
      return true;
    }
  }
  return false;
}

CategoryFilter::CategoryFilter(const std::string& filterString) {
  if (!filterString.empty()) {
    initializeFilter(filterString);
  } else {
    initializeFilter(CategoryFilter::kDefaultCategoryFilterString);
  }
}

CategoryFilter::CategoryFilter(const CategoryFilter& cf)
    : included_(cf.included_),
      disabled_(cf.disabled_),
      excluded_(cf.excluded_),
      delays_(cf.delays_) {}

CategoryFilter& CategoryFilter::operator=(const CategoryFilter& rhs) {
  if (this == &rhs) {
    return *this;
  }

  included_ = rhs.included_;
  disabled_ = rhs.disabled_;
  excluded_ = rhs.excluded_;
  delays_ = rhs.delays_;
  return *this;
}

void CategoryFilter::initializeFilter(const std::string& filterString) {
  // Tokenize list of categories, delimited by ','.
  vector<string> tokens = strings::split(filterString, ",");
  // Add each token to the appropriate list (included_,excluded_).
  for (string category : tokens) {
    // Ignore empty categories.
    if (category.empty()) {
      continue;
    }
    // Synthetic delays are of the form 'DELAY(delay;option;option;...)'.
    if (category.find(kSyntheticDelayCategoryFilterPrefix) == 0 &&
        category.at(category.size() - 1) == ')') {
      category = category.substr(
          strlen(kSyntheticDelayCategoryFilterPrefix),
          category.size() - strlen(kSyntheticDelayCategoryFilterPrefix) - 1);
      size_t nameLength = category.find(';');
      if (nameLength != std::string::npos && nameLength > 0 &&
          nameLength != category.size() - 1) {
        delays_.push_back(category);
      }
    } else if (category.at(0) == '-') {
      // Excluded categories start with '-'.
      // Remove '-' from category string.
      category = category.substr(1);
      excluded_.push_back(category);
    } else if (
        category.compare(
            0,
            strlen(TRACE_DISABLED_BY_DEFAULT("")),
            TRACE_DISABLED_BY_DEFAULT("")) == 0) {
      disabled_.push_back(category);
    } else {
      included_.push_back(category);
    }
  }
}

void CategoryFilter::writeString(
    const StringList& values,
    std::string* out,
    bool included) const {
  bool prependComma = !out->empty();
  int tokenCnt = 0;
  for (const auto& value : values) {
    if (tokenCnt > 0 || prependComma) {
      *out += ",";
    }
    *out += fmt::format("{}{}", (included ? "" : "-"), value);
    ++tokenCnt;
  }
}

void CategoryFilter::writeString(const StringList& delays, std::string* out)
    const {
  bool prependComma = !out->empty();
  int tokenCnt = 0;
  for (const auto& delay : delays) {
    if (tokenCnt > 0 || prependComma) {
      *out += ",";
    }
    *out += fmt::format("{}{})", kSyntheticDelayCategoryFilterPrefix, delay);
    ++tokenCnt;
  }
}

std::string CategoryFilter::toString() const {
  std::string filter_string;
  writeString(included_, &filter_string, true);
  writeString(disabled_, &filter_string, true);
  writeString(excluded_, &filter_string, false);
  writeString(delays_, &filter_string);
  return filter_string;
}

bool CategoryFilter::isCategoryGroupEnabled(
    const char* category_group_name) const {
  // TraceLog should call this method only as  part of enabling/disabling
  // categories.
  StringList::const_iterator ci;

  // Check the disabled- filters and the disabled-* wildcard first so that a
  // "*" filter does not include the disabled.
  for (ci = disabled_.begin(); ci != disabled_.end(); ++ci) {
    if (doesCategoryGroupContainCategory(category_group_name, ci->c_str())) {
      return true;
    }
  }
  if (doesCategoryGroupContainCategory(
          category_group_name, TRACE_DISABLED_BY_DEFAULT("*"))) {
    return false;
  }

  for (ci = included_.begin(); ci != included_.end(); ++ci) {
    if (doesCategoryGroupContainCategory(category_group_name, ci->c_str())) {
      return true;
    }
  }

  for (ci = excluded_.begin(); ci != excluded_.end(); ++ci) {
    if (doesCategoryGroupContainCategory(category_group_name, ci->c_str())) {
      return false;
    }
  }
  // If the category group is not excluded, and there are no included patterns
  // we consider this pattern enabled.
  return included_.empty();
}

bool CategoryFilter::hasIncludedPatterns() const {
  return !included_.empty();
}

void CategoryFilter::merge(const CategoryFilter& nestedFilter) {
  // Keep included patterns only if both filters have an included entry.
  // Otherwise, one of the filter was specifying "*" and we want to honour the
  // broadest filter.
  if (hasIncludedPatterns() && nestedFilter.hasIncludedPatterns()) {
    included_.insert(
        included_.end(),
        nestedFilter.included_.begin(),
        nestedFilter.included_.end());
  } else {
    included_.clear();
  }

  disabled_.insert(
      disabled_.end(),
      nestedFilter.disabled_.begin(),
      nestedFilter.disabled_.end());
  excluded_.insert(
      excluded_.end(),
      nestedFilter.excluded_.begin(),
      nestedFilter.excluded_.end());
  delays_.insert(
      delays_.end(), nestedFilter.delays_.begin(), nestedFilter.delays_.end());
}

void CategoryFilter::clear() {
  included_.clear();
  disabled_.clear();
  excluded_.clear();
}

const CategoryFilter::StringList& CategoryFilter::getSyntheticDelayValues()
    const {
  return delays_;
}

} // namespace debug
} // namespace kudu

namespace trace_event_internal {

ScopedTraceBinaryEfficient::ScopedTraceBinaryEfficient(
    const char* category_group,
    const char* name) {
  // The single atom works because for now the category_group can only be "gpu".
  DCHECK(strcmp(category_group, "gpu") == 0);
  static TRACE_EVENT_API_ATOMIC_WORD atomic = 0;
  INTERNAL_TRACE_EVENT_GET_CATEGORY_INFO_CUSTOM_VARIABLES(
      category_group, atomic, categoryGroupEnabled_);
  name_ = name;
  if (*categoryGroupEnabled_) {
    eventHandle_ = TRACE_EVENT_API_ADD_TRACE_EVENT_WITH_THREAD_ID_AND_TIMESTAMP(
        TRACE_EVENT_PHASE_COMPLETE,
        categoryGroupEnabled_,
        name,
        trace_event_internal::kNoEventId,
        static_cast<int>(kudu::Thread::uniqueThreadId()),
        kudu::getMonoTimeMicros(),
        0,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        TRACE_EVENT_FLAG_NONE);
  }
}

ScopedTraceBinaryEfficient::~ScopedTraceBinaryEfficient() {
  if (*categoryGroupEnabled_) {
    TRACE_EVENT_API_UPDATE_TRACE_EVENT_DURATION(
        categoryGroupEnabled_, name_, eventHandle_);
  }
}

} // namespace trace_event_internal
