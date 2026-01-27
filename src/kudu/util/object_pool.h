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
//
// Simple pool/freelist for objects of the same type, typically used
// in local context.
#ifndef KUDU_UTIL_OBJECT_POOL_H
#define KUDU_UTIL_OBJECT_POOL_H

#include <cstdint>
#include <memory>
#include <new>

#include <glog/logging.h>

namespace kudu {

template <class T>
class ReturnToPool;

// An object pool allocates and destroys a single class of objects
// off of a free-list.
//
// Upon destruction of the pool, any objects allocated from this pool are
// destroyed, regardless of whether they have been explicitly returned to the
// pool.
//
// This class is similar to the boost::pool::object_pool, except that the boost
// implementation seems to have O(n) deallocation performance and benchmarked
// really poorly.
//
// This class is not thread-safe.
template <typename T>
class ObjectPool {
 public:
  using DeleterType = ReturnToPool<T>;
  using ScopedPtr = std::unique_ptr<T, DeleterType>;

  ObjectPool()
      : freeListHead_(nullptr), allocListHead_(nullptr), deleter_(this) {}

  ~ObjectPool() {
    // Delete all objects ever allocated from this pool
    ListNode* node = allocListHead_;
    while (node != nullptr) {
      ListNode* tmp = node;
      node = node->nextOnAllocList;
      if (!tmp->isOnFreelist) {
        // Have to run the actual destructor if the user forgot to free it.
        tmp->destroy();
      }
      delete tmp;
    }
  }

  // Construct a new object instance from the pool.
  T* construct() {
    ListNode* node = getObject();
    // Use placement new to construct T in the pre-allocated storage
    return new (node->storage()) T();
  }

  template <class Arg1>
  T* construct(Arg1 arg1) {
    ListNode* node = getObject();
    // Use placement new to construct T with argument
    return new (node->storage()) T(arg1);
  }

  // Destroy an object, running its destructor and returning it to the
  // free-list.
  void destroy(T* t) {
    CHECK_NOTNULL(t);
    // Cast back to ListNode - the storage is at the beginning of ListNode
    ListNode* node = reinterpret_cast<ListNode*>(
        reinterpret_cast<char*>(t) - offsetof(ListNode, storage_));

    node->destroy();

    DCHECK(!node->isOnFreelist);
    node->isOnFreelist = true;
    node->nextOnFreeList = freeListHead_;
    freeListHead_ = node;
  }

  // Create a ScopedPtr wrapper around the given pointer which came from this
  // pool.
  // When the ScopedPtr goes out of scope, the object will get released back
  // to the pool.
  ScopedPtr makeScopedPtr(T* ptr) {
    return ScopedPtr(ptr, deleter_);
  }

 private:
  struct ListNode {
    friend class ObjectPool<T>;

    // Aligned storage for T
    alignas(T) char storage_[sizeof(T)];

    ListNode* nextOnFreeList;
    ListNode* nextOnAllocList;
    bool isOnFreelist;

    // Get pointer to the storage as T*
    T* storage() {
      return std::launder(reinterpret_cast<T*>(storage_));
    }

    // Destroy the T object in storage (if constructed)
    void destroy() {
      storage()->~T();
    }
  };

  ListNode* getObject() {
    if (freeListHead_ != nullptr) {
      ListNode* tmp = freeListHead_;
      freeListHead_ = tmp->nextOnFreeList;
      tmp->nextOnFreeList = nullptr;
      DCHECK(tmp->isOnFreelist);
      tmp->isOnFreelist = false;
      return tmp;
    }
    auto newNode = new ListNode();
    newNode->nextOnFreeList = nullptr;
    newNode->nextOnAllocList = allocListHead_;
    newNode->isOnFreelist = false;
    allocListHead_ = newNode;
    return newNode;
  }

  // Keeps track of free objects in this pool.
  ListNode* freeListHead_;

  // Keeps track of all objects ever allocated by this pool.
  ListNode* allocListHead_;

  DeleterType deleter_;
};

// Functor which returns the passed objects to a specific object pool.
// This can be used in conjunction with ScopedPtr to automatically release
// an object back to a pool when it goes out of scope.
template <class T>
class ReturnToPool {
 public:
  explicit ReturnToPool(ObjectPool<T>* pool) : pool_(pool) {}

  inline void operator()(T* ptr) const {
    pool_->destroy(ptr);
  }

 private:
  ObjectPool<T>* pool_;
};

} // namespace kudu
#endif
