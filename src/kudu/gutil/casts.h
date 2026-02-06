// Copyright 2009 Google Inc. All Rights Reserved.
//
// Various Google-specific casting templates.
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//

#pragma once

#include <assert.h> // for use with down_cast<>
#include <string.h> // for memcpy

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/template_util.h"
#include "kudu/gutil/type_traits.h"

// Note: implicit_cast has been removed. Use static_cast instead.
// For implicit conversions, static_cast is clearer and more explicit.

// namespace down_cast() as it conflicts with
// //mysql/server/include/template_utils.h
namespace kudu {
// When you upcast (that is, cast a pointer from type Foo to type
// SuperclassOfFoo), it's fine to use implicit_cast<>, since upcasts
// always succeed.  When you downcast (that is, cast a pointer from
// type Foo to type SubclassOfFoo), static_cast<> isn't safe, because
// how do you know the pointer is really of type SubclassOfFoo?  It
// could be a bare Foo, or of type DifferentSubclassOfFoo.  Thus,
// when you downcast, you should use this macro.  In debug mode, we
// use dynamic_cast<> to double-check the downcast is legal (we die
// if it's not).  In normal mode, we do the efficient static_cast<>
// instead.  Thus, it's important to test in debug mode to make sure
// the cast is legal!
//    This is the only place in the code we should use dynamic_cast<>.
// In particular, you SHOULDN'T be using dynamic_cast<> in order to
// do RTTI (eg code like this:
//    if (dynamic_cast<Subclass1>(foo)) HandleASubclass1Object(foo);
//    if (dynamic_cast<Subclass2>(foo)) HandleASubclass2Object(foo);
// You should design the code some other way not to need this.

template <typename To, typename From> // use like this: down_cast<T*>(foo);
inline To down_cast(From* f) { // so we only accept pointers
  // Ensures that To is a sub-type of From *.  This test is here only
  // for compile-time type checking, and has no overhead in an
  // optimized build at run-time, as it will be optimized away
  // completely.

  // TODO(user): This should use KUDU_COMPILE_ASSERT.
  if (false) {
    static_cast<From*>(static_cast<To>(nullptr));
  }

  // uses RTTI in dbg and fastbuild. asserts are disabled in opt builds.
  assert(f == NULL || dynamic_cast<To>(f) != NULL);
  return static_cast<To>(f);
}

// Overload of down_cast for references. Use like this: down_cast<T&>(foo).
// The code is slightly convoluted because we're still using the pointer
// form of dynamic cast. (The reference form throws an exception if it
// fails.)
//
// There's no need for a special const overload either for the pointer
// or the reference form. If you call down_cast with a const T&, the
// compiler will just bind From to const T.
template <typename To, typename From>
inline To down_cast(From& f) {
  KUDU_COMPILE_ASSERT(
      base::is_reference<To>::value, target_type_not_a_reference);
  using ToAsPointer = typename base::remove_reference<To>::type*;
  if (false) {
    // Compile-time check that To inherits from From. See above for details.
    static_cast<From*>(static_cast<ToAsPointer>(NULL));
  }

  assert(dynamic_cast<ToAsPointer>(&f) != NULL); // RTTI: debug mode only
  return static_cast<To>(f);
}
} // namespace kudu
