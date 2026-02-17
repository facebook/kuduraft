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

#include "kudu/util/async_logger.h"

#include <string>
#include <thread>

#include "kudu/util/monotime.h"

using std::string;

namespace kudu {

AsyncLogger::AsyncLogger(google::base::Logger* wrapped, int maxBufferBytes)
    : maxBufferBytes_(maxBufferBytes),
      wrapped_(DCHECK_NOTNULL(wrapped)),
      wakeFlusherCond_(&lock_),
      freeBufferCond_(&lock_),
      flushCompleteCond_(&lock_),
      activeBuf_(new Buffer()),
      flushingBuf_(new Buffer()) {
  DCHECK_GT(maxBufferBytes_, 0);
}

AsyncLogger::~AsyncLogger() {}

void AsyncLogger::Start() {
  CHECK_EQ(state_, kInitted);
  state_ = kRunning;
  thread_ = std::thread(&AsyncLogger::runThread, this);
}

void AsyncLogger::Stop() {
  {
    MutexLock l(lock_);
    CHECK_EQ(state_, kRunning);
    state_ = kStopped;
    wakeFlusherCond_.signal();
  }
  thread_.join();
  CHECK(activeBuf_->messages.empty());
  CHECK(flushingBuf_->messages.empty());
}

void AsyncLogger::Write(
    bool forceFlush,
    time_t timestamp,
    const char* message,
    int messageLen) {
  {
    MutexLock l(lock_);
    DCHECK_EQ(state_, kRunning);
    while (bufferFull(*activeBuf_)) {
      appThreadsBlockedCountForTests_++;
      freeBufferCond_.wait();
    }
    activeBuf_->add(Msg(timestamp, string(message, messageLen)), forceFlush);
    wakeFlusherCond_.signal();
  }

  // In most cases, we take the 'forceFlush' argument to mean that we'll let
  // the logger thread do the flushing for us, but not block the application.
  // However, for the special case of a FATAL log message, we really want to
  // make sure that our message hits the log before we continue, or else it's
  // likely that the application will exit while it's still in our buffer.
  //
  // NOTE: even if the application doesn't wrap the FATAL-level logger, log
  // messages at FATAL are also written to all other log files with lower
  // levels. So, a FATAL message will force a synchronous flush of all
  // lower-level logs before exiting.
  //
  // Unfortunately, the underlying log level isn't passed through to this
  // interface, so we have to use this hack: messages from FATAL errors start
  // with the character 'F'.
  if (messageLen > 0 && message[0] == 'F') {
    Flush();
  }
}

void AsyncLogger::Flush() {
  MutexLock l(lock_);
  DCHECK_EQ(state_, kRunning);

  // Wake up the writer thread at least twice.
  // This ensures that it has completely flushed both buffers.
  uint64_t origFlushCount = flushCount_;
  while (flushCount_ < origFlushCount + 2 && state_ == kRunning) {
    activeBuf_->flush = true;
    wakeFlusherCond_.signal();
    flushCompleteCond_.wait();
  }
}

uint32_t AsyncLogger::LogSize() {
  return wrapped_->LogSize();
}

void AsyncLogger::runThread() {
  MutexLock l(lock_);
  while (state_ == kRunning || activeBuf_->needsFlushOrWrite()) {
    while (!activeBuf_->needsFlushOrWrite() && state_ == kRunning) {
      if (!wakeFlusherCond_.waitFor(MonoDelta::FromSeconds(FLAGS_logbufsecs))) {
        // In case of wait timeout, force it to flush regardless whether there
        // is anything enqueued.
        activeBuf_->flush = true;
      }
    }

    activeBuf_.swap(flushingBuf_);
    // If the buffer that we are about to flush was full, then
    // we may have other threads which were blocked that we now
    // need to wake up.
    if (bufferFull(*flushingBuf_)) {
      freeBufferCond_.broadcast();
    }
    l.unlock();

    for (const auto& msg : flushingBuf_->messages) {
      wrapped_->Write(false, msg.ts, msg.message.data(), msg.message.size());
    }
    if (flushingBuf_->flush) {
      wrapped_->Flush();
    }
    flushingBuf_->clear();

    l.lock();
    flushCount_++;
    flushCompleteCond_.broadcast();
  }
}

bool AsyncLogger::bufferFull(const Buffer& buf) const {
  // We evenly divide our total buffer space between the two buffers.
  return buf.size > (maxBufferBytes_ / 2);
}

} // namespace kudu
