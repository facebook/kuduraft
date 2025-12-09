// Copyright 2003 Google Inc.
//
// The Singleton<Type> class manages a single instance of Type which will be
// created on first use and (usually) never destroyed.
//
//   MyClass* ptr = Singleton<MyClass>::get()
//   ptr->DoSomething();
//
// Singleton<> has no non-static members and is never actually instantiated.
//
// WARNING: Read go/singletons before using.
//
// This class is thread safe; the constructor will be run at most once, and
// no user will gain access to the object until the constructor is completed.
// The underlying Type must of course be thread-safe if you want to use it
// concurrently.
//
// If you want to ensure that your class can only exist as a singleton, make
// its constructors private, and make Singleton<> a friend:
//
//   class MySingletonOnlyClass {
//    public:
//     void DoSomething() { ... }
//    private:
//     DISALLOW_COPY_AND_ASSIGN(MySingletonOnlyClass);
//     MySingletonOnlyClass() { ... }
//     friend class Singleton<MySingletonOnlyClass>;
//   }
//
// If you want to initialize something eagerly at startup, rather than lazily
// upon use, consider using REGISTER_MODULE_INITIALIZER (in base/googleinit.h).
//
// Caveats:
// (a) The instance is normally never destroyed.  Destroying a Singleton is
//     complex and error-prone; C++ books go on about this at great length,
//     and I have seen no perfect general solution to the problem.
//
// (b) Your class must have a default (no-argument) constructor.
//
// (c) Your class's constructor must never throw an exception.
//
// Singleton::get() is very fast - about 1ns on a 2.4GHz Core 2.
//
// MODERNIZATION NOTE: This implementation now uses folly::LeakySingleton
// internally for improved lifecycle management while maintaining API
// compatibility. LeakySingleton is specifically designed for singletons
// that should never be destroyed, which matches the original semantics.

#pragma once

#include <folly/Singleton.h>

template <typename Type>
class Singleton {
 public:
  // Return a pointer to the one true instance of the class.
  //
  // Implementation note: This uses folly::LeakySingleton internally but
  // maintains the raw pointer API for backward compatibility.
  // LeakySingleton intentionally leaks the instance (never destroys it),
  // which matches the original Singleton<> semantics and works with
  // classes that have private destructors.
  static Type* get() {
    // folly::LeakySingleton returns a reference, not a pointer
    // It constructs the instance lazily on first access and never destroys it
    // Use ::new to work around a gcc bug when operator new is overloaded
    static folly::LeakySingleton<Type> impl([]() { return ::new Type; });
    return &impl.get();
  }
};
