// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This defines a set of argument wrappers and related factory methods that
// can be used specify the refcounting and reference semantics of arguments
// that are bound by the Bind() function in kudu/gutil/bind.h.
//
// It also defines a set of simple functions and utilities that people want
// when using Callback<> and Bind().
//
//
// ARGUMENT BINDING WRAPPERS
//
// The wrapper function is kudu::Unretained().
//
// Unretained() allows Bind() to bind a non-refcounted class, and to disable
// refcounting on arguments that are refcounted objects.
//
// EXAMPLE OF Unretained():
//
//   class Foo {
//    public:
//     void func() { cout << "Foo:f" << endl; }
//   };
//
//   // In some function somewhere.
//   Foo foo;
//   Closure foo_callback =
//       Bind(&Foo::func, Unretained(&foo));
//   foo_callback.Run();  // Prints "Foo:f".
//
// Without the Unretained() wrapper on |&foo|, the above call would fail
// to compile because Foo does not support the AddRef() and Release() methods.
//
//
// SIMPLE FUNCTIONS AND UTILITIES.
//
//   DoNothing() - Useful for creating a Closure that does nothing when called.
#pragma once

#include <memory>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/template_util.h"

// Unneeded define from Chromium
#define BASE_EXPORT

namespace kudu {
namespace internal {

template <typename T>
class HasIsMethodTag {
  using Yes = char[1];
  using No = char[2];

  template <typename U>
  static Yes& Check(typename U::IsMethod*);

  template <typename U>
  static No& Check(...);

 public:
  static const bool value = sizeof(Check<T>(0)) == sizeof(Yes);
};

template <typename T>
class UnretainedWrapper {
 public:
  explicit UnretainedWrapper(T* o) : ptr_(o) {}
  T* get() const {
    return ptr_;
  }

 private:
  T* ptr_;
};

template <typename T>
struct IgnoreResultHelper {
  explicit IgnoreResultHelper(T functor) : functor_(functor) {}

  T functor_;
};

template <typename T>
struct IgnoreResultHelper<Callback<T>> {
  explicit IgnoreResultHelper(const Callback<T>& functor) : functor_(functor) {}

  const Callback<T>& functor_;
};

// Unwrap the stored parameters for the wrappers above.
template <typename T>
struct UnwrapTraits {
  using ForwardType = const T&;
  static ForwardType Unwrap(const T& o) {
    return o;
  }
};

template <typename T>
struct UnwrapTraits<UnretainedWrapper<T>> {
  using ForwardType = T*;
  static ForwardType Unwrap(UnretainedWrapper<T> unretained) {
    return unretained.get();
  }
};

// Utility for handling different refcounting semantics in the Bind()
// function.
template <bool is_method, typename T>
struct MaybeRefcount;

template <typename T>
struct MaybeRefcount<false, T> {
  static void AddRef(const T&) {}
  static void Release(const T&) {}
};

template <typename T, size_t n>
struct MaybeRefcount<false, T[n]> {
  static void AddRef(const T*) {}
  static void Release(const T*) {}
};

template <typename T>
struct MaybeRefcount<true, T> {
  static void AddRef(const T&) {}
  static void Release(const T&) {}
};

template <typename T>
struct MaybeRefcount<true, T*> {
  static void AddRef(T* o) {
    o->AddRef();
  }
  static void Release(T* o) {
    o->Release();
  }
};

// No need to additionally AddRef() and Release() since we are storing a
// std::shared_ptr<> inside the storage object already.
template <typename T>
struct MaybeRefcount<true, std::shared_ptr<T>> {
  static void AddRef(const std::shared_ptr<T>&) {}
  static void Release(const std::shared_ptr<T>&) {}
};

template <typename T>
struct MaybeRefcount<true, const T*> {
  static void AddRef(const T* o) {
    o->AddRef();
  }
  static void Release(const T* o) {
    o->Release();
  }
};

} // namespace internal

template <typename T>
static inline internal::UnretainedWrapper<T> Unretained(T* o) {
  return internal::UnretainedWrapper<T>(o);
}

} // namespace kudu
