// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/threading/thread_collision_warner.h"

#ifndef BASE_EXPORT
#define BASE_EXPORT
#endif

namespace kudu {

// A generic interface to memory. This object is reference counted because one
// of its two subclasses own the data they carry, and we need to have
// heterogeneous containers of these two types of memory.
class BASE_EXPORT RefCountedMemory
    : public std::enable_shared_from_this<RefCountedMemory> {
 public:
  // Retrieves a pointer to the beginning of the data we point to. If the data
  // is empty, this will return NULL.
  virtual const unsigned char* front() const = 0;

  // Size of the memory pointed to.
  virtual size_t size() const = 0;

  // Handy method to simplify calling front() with a reinterpret_cast.
  template <typename T>
  const T* frontAs() const {
    return reinterpret_cast<const T*>(front());
  }

 protected:
  RefCountedMemory();
  virtual ~RefCountedMemory();
};

// An implementation of RefCountedMemory, where the bytes are stored in an STL
// string. Use this if your data naturally arrives in that format.
class BASE_EXPORT RefCountedString : public RefCountedMemory {
 public:
  RefCountedString();

  // Overridden from RefCountedMemory:
  virtual const unsigned char* front() const override;
  virtual size_t size() const override;

  const std::string& data() const {
    return data_;
  }
  std::string& data() {
    return data_;
  }

  virtual ~RefCountedString();

 private:
  std::string data_;

  DISALLOW_COPY_AND_ASSIGN(RefCountedString);
};

} // namespace kudu
