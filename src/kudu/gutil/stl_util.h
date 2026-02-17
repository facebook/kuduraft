// Copyright 2002 Google Inc.
//
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
// ---
//
//
// STL utility functions.  Usually, these replace built-in, but slow(!),
// STL functions with more efficient versions or provide a more convenient
// and Google friendly API.
//

#pragma once

#include <stddef.h>
#include <string.h> // for memcpy
#include <cassert>
#include <string>

#include "kudu/gutil/macros.h"

// STLDeleteContainerPointers()
//  For a range within a container of pointers, calls delete
//  (non-array version) on these pointers.
// NOTE: for these three functions, we could just implement a DeleteObject
// functor and then call for_each() on the range and functor, but this
// requires us to pull in all of <algorithm>, which seems expensive.
// For hash_[multi]set, it is important that this deletes behind the iterator
// because the hash_set may call the hash function on the iterator when it is
// advanced, which could result in the hash function trying to deference a
// stale pointer.
// NOTE: If you're calling this on an entire container, you probably want
// to call STLDeleteElements(&container) instead, or use an ElementDeleter.
template <class ForwardIterator>
void STLDeleteContainerPointers(ForwardIterator begin, ForwardIterator end) {
  while (begin != end) {
    ForwardIterator temp = begin;
    ++begin;
    delete *temp;
  }
}

// A struct that mirrors the GCC4 implementation of a string. See:
// /usr/crosstool/v8/gcc-4.1.0-glibc-2.2.2/i686-unknown-linux-gnu/include/c++/4.1.0/ext/sso_string_base.h
struct InternalStringRepGCC4 {
  char* _M_data;
  size_t _M_string_length;

  enum { _S_local_capacity = 15 };

  union {
    char _M_local_data[_S_local_capacity + 1];
    size_t _M_allocated_capacity;
  };
};

// Like str->resize(new_size), except any new characters added to
// "*str" as a result of resizing may be left uninitialized, rather
// than being filled with '0' bytes.  Typically used when code is then
// going to overwrite the backing store of the string with known data.
inline void STLStringResizeUninitialized(std::string* s, size_t new_size) {
  if (sizeof(*s) == sizeof(InternalStringRepGCC4)) {
    if (new_size > s->capacity()) {
      s->reserve(new_size);
    }
    // The line below depends on the layout of 'string'.  THIS IS
    // NON-PORTABLE CODE.  If our STL implementation changes, we will
    // need to change this as well.
    InternalStringRepGCC4* rep = reinterpret_cast<InternalStringRepGCC4*>(s);
    assert(rep->_M_data == s->data());
    assert(rep->_M_string_length == s->size());

    // We have to null-terminate the string for c_str() to work properly.
    // So we leave the actual contents of the string uninitialized, but
    // we set the byte one past the new end of the string to '\0'
    const_cast<char*>(s->data())[new_size] = '\0';
    rep->_M_string_length = new_size;
  } else {
    // Slow path: have to reallocate stuff, or an unknown string rep
    s->resize(new_size);
  }
}

inline void STLAssignToString(std::string* str, const char* ptr, size_t n) {
  STLStringResizeUninitialized(str, n);
  if (n == 0)
    return;
  memcpy(&*str->begin(), ptr, n);
}

inline void STLAppendToString(std::string* str, const char* ptr, size_t n) {
  if (n == 0)
    return;
  size_t old_size = str->size();
  STLStringResizeUninitialized(str, old_size + n);
  memcpy(&*str->begin() + old_size, ptr, n);
}

// The following functions are useful for cleaning up STL containers
// whose elements point to allocated memory.

// STLDeleteElements() deletes all the elements in an STL container and clears
// the container.  This function is suitable for use with a vector, set,
// hash_set, or any other STL container which defines sensible begin(), end(),
// and clear() methods.
//
// If container is NULL, this function is a no-op.
//
// As an alternative to calling STLDeleteElements() directly, consider
// ElementDeleter (defined below), which ensures that your container's elements
// are deleted when the ElementDeleter goes out of scope.
template <class T>
void STLDeleteElements(T* container) {
  if (!container)
    return;
  STLDeleteContainerPointers(container->begin(), container->end());
  container->clear();
}

// ElementDeleter provides a convenient way to delete all elements from STL
// containers when they go out of scope. This greatly simplifies code that
// creates temporary objects and has multiple return statements. Example:
//
// vector<MyProto *> tmp_proto;
// ElementDeleter d(&tmp_proto);
// if (...) return false;
// ...
// return success;

// A very simple interface that simply provides a virtual destructor.  It is
// used as a non-templated base class for the TemplatedElementDeleter class.
// Clients should not typically use this class directly.
class BaseDeleter {
 public:
  virtual ~BaseDeleter() {}

 protected:
  BaseDeleter() {}

 private:
  DISALLOW_EVIL_CONSTRUCTORS(BaseDeleter);
};

// Given a pointer to an STL container, this class will delete all the element
// pointers when it goes out of scope.  Clients should typically use
// ElementDeleter rather than invoking this class directly.
template <class STLContainer>
class TemplatedElementDeleter : public BaseDeleter {
 public:
  explicit TemplatedElementDeleter<STLContainer>(STLContainer* ptr)
      : container_ptr_(ptr) {}

  virtual ~TemplatedElementDeleter<STLContainer>() {
    STLDeleteElements(container_ptr_);
  }

 private:
  STLContainer* container_ptr_;

  DISALLOW_EVIL_CONSTRUCTORS(TemplatedElementDeleter);
};

// Like TemplatedElementDeleter, this class will delete element pointers from a
// container when it goes out of scope.  However, it is much nicer to use,
// since the class itself is not templated.
class ElementDeleter {
 public:
  template <class STLContainer>
  explicit ElementDeleter(STLContainer* ptr)
      : deleter_(new TemplatedElementDeleter<STLContainer>(ptr)) {}

  ~ElementDeleter() {
    delete deleter_;
  }

 private:
  BaseDeleter* deleter_;

  DISALLOW_EVIL_CONSTRUCTORS(ElementDeleter);
};
