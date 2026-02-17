// Copyright 2005 Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ----
//
// Template metaprogramming utility functions.
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//
//
// The names choosen here reflect those used in tr1 and the boost::mpl
// library, there are similar operations used in the Loki library as
// well.  I prefer the boost names for 2 reasons:
// 1.  I think that portions of the Boost libraries are more likely to
// be included in the c++ standard.
// 2.  It is not impossible that some of the boost libraries will be
// included in our own build in the future.
// Both of these outcomes means that we may be able to directly replace
// some of these with boost equivalents.
//

// These #defines collide with sparsehash
#pragma once

namespace base {

// Types small_ and big_ are guaranteed such that sizeof(small_) <
// sizeof(big_)
using small_ = char;

struct big_ {
  char dummy[2];
};

// Types YesType and NoType are guaranteed such that sizeof(YesType) <
// sizeof(NoType).
using YesType = small_;
using NoType = big_;

// integral_constant, defined in tr1, is a wrapper for an integer
// value. We don't really need this generality; we could get away
// with hardcoding the integer type to bool. We use the fully
// general integer_constant for compatibility with tr1.

template <class T, T v>
struct integral_constant {
  static const T value = v;
  using value_type = T;
  using type = integral_constant<T, v>;
};

template <class T, T v>
const T integral_constant<T, v>::value;

// Abbreviations: true_type and false_type are structs that represent boolean
// true and false values.
using true_type = integral_constant<bool, true>;
using false_type = integral_constant<bool, false>;

template <class T>
struct is_non_const_reference : false_type {};
template <class T>
struct is_non_const_reference<T&> : true_type {};
template <class T>
struct is_non_const_reference<const T&> : false_type {};

template <class T>
struct is_const : false_type {};
template <class T>
struct is_const<const T> : true_type {};

// Used to determine if a type is a struct/union/class. Inspired by Boost's
// is_class type_trait implementation.
struct IsClassHelper {
  template <typename C>
  static YesType Test(void (C::*)(void));

  template <typename C>
  static NoType Test(...);
};

template <typename T>
struct is_class : integral_constant<
                      bool,
                      sizeof(IsClassHelper::Test<T>(0)) == sizeof(YesType)> {};

} // namespace base
