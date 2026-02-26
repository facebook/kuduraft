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

#include "kudu/rpc/service_queue.h"

#include <mutex>
#include <ostream>

#include <optional>

#include "kudu/gutil/port.h"

namespace kudu {
namespace rpc {

__thread LifoServiceQueue::ConsumerState* LifoServiceQueue::tlConsumer_ =
    nullptr;

LifoServiceQueue::LifoServiceQueue(int maxQueueSize)
    : shutdown_(false), maxQueueSize_(maxQueueSize) {
  CHECK_GT(maxQueueSize_, 0);
}

LifoServiceQueue::~LifoServiceQueue() {
  DCHECK(queue_.empty())
      << "ServiceQueue holds bare pointers at destruction time";
}

bool LifoServiceQueue::blockingGet(std::unique_ptr<InboundCall>* out) {
  auto consumer = tlConsumer_;
  if (PREDICT_FALSE(!consumer)) {
    consumer = tlConsumer_ = new ConsumerState(this);
    std::lock_guard<simple_spinlock> l(lock_);
    consumers_.emplace_back(consumer);
  }

  while (true) {
    {
      std::lock_guard<simple_spinlock> l(lock_);
      if (!queue_.empty()) {
        auto it = queue_.begin();
        out->reset(*it);
        queue_.erase(it);
        return true;
      }
      if (PREDICT_FALSE(shutdown_)) {
        return false;
      }
      consumer->dCheckBoundInstance(this);
      waitingConsumers_.push_back(consumer);
    }
    InboundCall* call = consumer->wait();
    if (call != nullptr) {
      out->reset(call);
      return true;
    }
    // if call == nullptr, this means we are shutting down the queue.
    // Loop back around and re-check 'shutdown_'.
  }
}

QueueStatus LifoServiceQueue::put(
    InboundCall* call,
    std::optional<InboundCall*>* evicted) {
  std::unique_lock<simple_spinlock> l(lock_);
  if (PREDICT_FALSE(shutdown_)) {
    return kQueueShutdown;
  }

  DCHECK(!(waitingConsumers_.size() > 0 && queue_.size() > 0));

  // fast path
  if (queue_.empty() && waitingConsumers_.size() > 0) {
    auto consumer = waitingConsumers_[waitingConsumers_.size() - 1];
    waitingConsumers_.pop_back();
    // Notify condition var(and wake up consumer thread) takes time,
    // so put it out of spinlock scope.
    l.unlock();
    consumer->post(call);
    return kQueueSuccess;
  }

  if (PREDICT_FALSE(queue_.size() >= maxQueueSize_)) {
    // eviction
    DCHECK_EQ(queue_.size(), maxQueueSize_);
    auto it = queue_.end();
    --it;
    if (deadlineLess(*it, call)) {
      return kQueueFull;
    }

    *evicted = *it;
    queue_.erase(it);
  }

  queue_.insert(call);
  return kQueueSuccess;
}

void LifoServiceQueue::shutdown() {
  std::lock_guard<simple_spinlock> l(lock_);
  shutdown_ = true;

  // Post a nullptr to wake up any consumers which are waiting.
  for (auto* cs : waitingConsumers_) {
    cs->post(nullptr);
  }
  waitingConsumers_.clear();
}

bool LifoServiceQueue::empty() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return queue_.empty();
}

int LifoServiceQueue::maxSize() const {
  return maxQueueSize_;
}

std::string LifoServiceQueue::toString() const {
  std::string ret;

  std::lock_guard<simple_spinlock> l(lock_);
  for (const auto* t : queue_) {
    ret.append(t->toString());
    ret.append("\n");
  }
  return ret;
}

} // namespace rpc
} // namespace kudu
