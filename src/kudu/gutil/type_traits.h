// Copyright (c) 2006, Google Inc.
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
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//
// Define a small subset of tr1 type traits. The traits we define are:
//   enable_if
//   is_array
//   is_reference
//   remove_reference
// We can add more type traits as required.

// THESE #defines collide with spareshash
#pragma once

#include "kudu/gutil/template_util.h" // For true_type and false_type

namespace base {

template <bool cond, class T>
struct enable_if;
template <class T>
struct is_array;
template <class T>
struct is_reference;
template <class T>
struct remove_reference;

// enable_if, equivalent semantics to c++11 std::enable_if, specifically:
//   "If B is true, the member typedef type shall equal T; otherwise, there
//    shall be no member typedef type."
// Specified by 20.9.7.6 [Other transformations]
template <bool cond, class T = void>
struct enable_if {
  typedef T type;
};
template <class T>
struct enable_if<false, T> {};

template <class>
struct is_array : public false_type {};
template <class T, size_t n>
struct is_array<T[n]> : public true_type {};
template <class T>
struct is_array<T[]> : public true_type {};

// is_reference is false except for reference types.
template <typename T>
struct is_reference : false_type {};
template <typename T>
struct is_reference<T&> : true_type {};

// Specified by TR1 [4.7.2] Reference modifications.
template <typename T>
struct remove_reference {
  typedef T type;
};
template <typename T>
struct remove_reference<T&> {
  typedef T type;
};

} // namespace base

// Right now these macros are no-ops, and mostly just document the fact
// these types are PODs, for human use.  They may be made more contentful
// later.  The typedef is just to make it legal to put a semicolon after
// these macros.
#define KDECLARE_POD(TypeName) \
  typedef int Dummy_Type_For_DECLARE_POD ATTRIBUTE_UNUSED
#define KENFORCE_POD(TypeName) \
  typedef int Dummy_Type_For_ENFORCE_POD ATTRIBUTE_UNUSED
