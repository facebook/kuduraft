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
// The wrapper functions are kudu::Unretained() and kudu::Passed().
//
// Unretained() allows Bind() to bind a non-refcounted class, and to disable
// refcounting on arguments that are refcounted objects.
//
// Passed() is for transferring movable-but-not-copyable types (eg. scoped_ptr)
// through a Callback. Logically, this signifies a destructive transfer of
// the state of the argument into the target function.  Invoking
// Callback::Run() twice on a Callback that was created with a Passed()
// argument will CHECK() because the first invocation would have already
// transferred ownership to the target function.
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
// EXAMPLE OF Passed():
//
//   void TakesOwnership(scoped_ptr<Foo> arg) { }
//   scoped_ptr<Foo> CreateFoo() { return scoped_ptr<Foo>(new Foo()); }
//
//   scoped_ptr<Foo> f(new Foo());
//
//   // |cb| is given ownership of Foo(). |f| is now NULL.
//   // You can use f.Pass() in place of &f, but it's more verbose.
//   Closure cb = Bind(&TakesOwnership, Passed(&f));
//
//   // Run was never called so |cb| still owns Foo() and deletes
//   // it on Reset().
//   cb.Reset();
//
//   // |cb| is given a new Foo created by CreateFoo().
//   cb = Bind(&TakesOwnership, Passed(CreateFoo()));
//
//   // |arg| in TakesOwnership() is given ownership of Foo(). |cb|
//   // no longer owns Foo() and, if reset, would not delete Foo().
//   cb.Run();  // Foo() is now transferred to |arg| and deleted.
//   cb.Run();  // This CHECK()s since Foo() already been used once.
//
// Passed() is particularly useful with PostTask() when you are transferring
// ownership of an argument into a task, but don't necessarily know if the
// task will always be executed. This can happen if the task is cancellable
// or if it is posted to a MessageLoopProxy.
//
//
// SIMPLE FUNCTIONS AND UTILITIES.
//
//   DoNothing() - Useful for creating a Closure that does nothing when called.
#pragma once

#include <assert.h>
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
  typedef char Yes[1];
  typedef char No[2];

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

// PassedWrapper is a copyable adapter for a scoper that ignores const.
//
// It is needed to get around the fact that Bind() takes a const reference to
// all its arguments.  Because Bind() takes a const reference to avoid
// unnecessary copies, it is incompatible with movable-but-not-copyable
// types; doing a destructive "move" of the type into Bind() would violate
// the const correctness.
//
// This conundrum cannot be solved without either C++11 rvalue references or
// a O(2^n) blowup of Bind() templates to handle each combination of regular
// types and movable-but-not-copyable types.  Thus we introduce a wrapper type
// that is copyable to transmit the correct type information down into
// BindState<>. Ignoring const in this type makes sense because it is only
// created when we are explicitly trying to do a destructive move.
//
// Two notes:
//  1) PassedWrapper supports any type that has a "Pass()" function.
//     This is intentional. The whitelisting of which specific types we
//     support is maintained by CallbackParamTraits<>.
//  2) is_valid_ is distinct from NULL because it is valid to bind a "NULL"
//     scoper to a Callback and allow the Callback to execute once.
template <typename T>
class PassedWrapper {
 public:
  explicit PassedWrapper(T scoper) : is_valid_(true), scoper_(scoper.Pass()) {}
  PassedWrapper(const PassedWrapper& other)
      : is_valid_(other.is_valid_), scoper_(other.scoper_.Pass()) {}
  T Pass() const {
    assert(is_valid_);
    is_valid_ = false;
    return scoper_.Pass();
  }

 private:
  mutable bool is_valid_;
  mutable T scoper_;
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

template <typename T>
struct UnwrapTraits<PassedWrapper<T>> {
  using ForwardType = T;
  static T Unwrap(PassedWrapper<T>& o) {
    return o.Pass();
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

// We offer 2 syntaxes for calling Passed(). The first takes a temporary and
// is best suited for use with the return value of a function. The second
// takes a pointer to the scoper and is just syntactic sugar to avoid having
// to write Passed(scoper.Pass()).
template <typename T>
static inline internal::PassedWrapper<T> Passed(T scoper) {
  return internal::PassedWrapper<T>(scoper.Pass());
}
template <typename T>
static inline internal::PassedWrapper<T> Passed(T* scoper) {
  return internal::PassedWrapper<T>(scoper->Pass());
}

} // namespace kudu
