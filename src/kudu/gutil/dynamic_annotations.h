/* Copyright (c) 2008-2009, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ---
 * Author: Kostya Serebryany
 */

/* This file defines dynamic annotations for use with dynamic analysis
   tool such as valgrind, PIN, etc.

   Dynamic annotation is a source code annotation that affects
   the generated code (that is, the annotation is not a comment).
   Each such annotation is attached to a particular
   instruction and/or to a particular object (address) in the program.

   The annotations that should be used by users are macros in all upper-case
   (e.g., KUDU_ANNONTATE_NEW_MEMORY).

   Actual implementation of these macros may differ depending on the
   dynamic analysis tool being used.

   See http://code.google.com/p/data-race-test/  for more information.

   This file supports the following dynamic analysis tools:
   - None (DYNAMIC_ANNOTATIONS_ENABLED is not defined or zero).
      Macros are defined empty.
   - ThreadSanitizer, Helgrind, DRD (DYNAMIC_ANNOTATIONS_ENABLED is 1).
      Macros are defined as calls to non-inlinable empty functions
      that are intercepted by Valgrind. */

#pragma once

#include <stddef.h>

// Detect ThreadSanitizer using standard compiler macros.
// Note: We duplicate this logic here instead of including port.h because
// dynamic_annotations.c is a C file and port.h requires C++.
#ifndef KUDU_SANITIZE_THREAD
#if defined(THREAD_SANITIZER) || defined(__SANITIZE_THREAD__) || \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
#define KUDU_SANITIZE_THREAD 1
#endif
#endif

#ifndef DYNAMIC_ANNOTATIONS_ENABLED
// Enable dynamic annotations for TSAN builds so that happens-before
// relationships are properly communicated to the sanitizer.
// Upstream Kudu enables this via CMake:
// add_definitions("-DDYNAMIC_ANNOTATIONS_ENABLED")
#if defined(KUDU_SANITIZE_THREAD)
#define DYNAMIC_ANNOTATIONS_ENABLED 1
// TSAN runtime provides its own implementations of the Annotate* functions,
// so tell dynamic_annotations.c to not define stub implementations.
#define DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL 1
#else
#define DYNAMIC_ANNOTATIONS_ENABLED 0
#endif
#endif

#if DYNAMIC_ANNOTATIONS_ENABLED != 0

/* -------------------------------------------------------------
   Annotations useful when implementing condition variables such as CondVar,
   using conditional critical sections (Await/LockWhen) and when constructing
   user-defined synchronization mechanisms.

   The annotations KUDU_ANNONTATE_HAPPENS_BEFORE() and
   KUDU_ANNONTATE_HAPPENS_AFTER() can be used to define happens-before arcs in
   user-defined synchronization mechanisms:  the race detector will infer an arc
   from the former to the latter when they share the same argument pointer.

   Example 1 (reference counting):

   void Unref() {
     KUDU_ANNONTATE_HAPPENS_BEFORE(&refcount_);
     if (AtomicDecrementByOne(&refcount_) == 0) {
       KUDU_ANNONTATE_HAPPENS_AFTER(&refcount_);
       delete this;
     }
   }

   Example 2 (message queue):

   void MyQueue::Put(Type *e) {
     MutexLock lock(&mu_);
     KUDU_ANNONTATE_HAPPENS_BEFORE(e);
     PutElementIntoMyQueue(e);
   }

   Type *MyQueue::Get() {
     MutexLock lock(&mu_);
     Type *e = GetElementFromMyQueue();
     KUDU_ANNONTATE_HAPPENS_AFTER(e);
     return e;
   }

   Note: when possible, please use the existing reference counting and message
   queue implementations instead of inventing new ones. */

/* Annotations for user-defined synchronization mechanisms. */
#define KUDU_ANNONTATE_HAPPENS_BEFORE(obj) \
  AnnotateHappensBefore(__FILE__, __LINE__, obj)
#define KUDU_ANNONTATE_HAPPENS_AFTER(obj) \
  AnnotateHappensAfter(__FILE__, __LINE__, obj)

/* -------------------------------------------------------------
   Annotations that suppress errors.  It is usually better to express the
   program's synchronization using the other annotations, but these can
   be used when all else fails. */

/* Report that we may have a benign race at "pointer", with size
   "sizeof(*(pointer))". "pointer" must be a non-void* pointer.  Insert at the
   point where "pointer" has been allocated, preferably close to the point
   where the race happens.  See also KUDU_ANNONTATE_BENIGN_RACE_STATIC. */
#define KUDU_ANNONTATE_BENIGN_RACE(pointer, description) \
  AnnotateBenignRaceSized(                               \
      __FILE__, __LINE__, pointer, sizeof(*(pointer)), description)

/* Same as KUDU_ANNONTATE_BENIGN_RACE(address, description), but applies to
   the memory range [address, address+size). */
#define KUDU_ANNONTATE_BENIGN_RACE_SIZED(address, size, description) \
  AnnotateBenignRaceSized(__FILE__, __LINE__, address, size, description)

/* Request the analysis tool to ignore all reads in the current thread
   until KUDU_ANNONTATE_IGNORE_READS_END is called.
   Useful to ignore intentional racey reads, while still checking
   other reads and all writes.
   See also KUDU_ANNONTATE_UNPROTECTED_READ. */
#define KUDU_ANNONTATE_IGNORE_READS_BEGIN() \
  AnnotateIgnoreReadsBegin(__FILE__, __LINE__)

/* Stop ignoring reads. */
#define KUDU_ANNONTATE_IGNORE_READS_END() \
  AnnotateIgnoreReadsEnd(__FILE__, __LINE__)

/* Similar to KUDU_ANNONTATE_IGNORE_READS_BEGIN, but ignore writes. */
#define KUDU_ANNONTATE_IGNORE_WRITES_BEGIN() \
  AnnotateIgnoreWritesBegin(__FILE__, __LINE__)

/* Stop ignoring writes. */
#define KUDU_ANNONTATE_IGNORE_WRITES_END() \
  AnnotateIgnoreWritesEnd(__FILE__, __LINE__)

/* Start ignoring all memory accesses (reads and writes). */
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN() \
  do {                                                 \
    KUDU_ANNONTATE_IGNORE_READS_BEGIN();               \
    KUDU_ANNONTATE_IGNORE_WRITES_BEGIN();              \
  } while (0)

/* Stop ignoring all memory accesses. */
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END() \
  do {                                               \
    KUDU_ANNONTATE_IGNORE_WRITES_END();              \
    KUDU_ANNONTATE_IGNORE_READS_END();               \
  } while (0)

/* Start ignoring all synchronization until KUDU_ANNONTATE_IGNORE_SYNC_END
   is called. */
#define KUDU_ANNONTATE_IGNORE_SYNC_BEGIN() \
  AnnotateIgnoreSyncBegin(__FILE__, __LINE__)

/* Stop ignoring all synchronization. */
#define KUDU_ANNONTATE_IGNORE_SYNC_END() \
  AnnotateIgnoreSyncEnd(__FILE__, __LINE__)

/* -------------------------------------------------------------
   Annotations useful when implementing locks.  They are not
   normally needed by modules that merely use locks.
   The "lock" argument is a pointer to the lock object. */

/* Report that a lock has been created at address "lock". */
#define KUDU_ANNONTATE_RWLOCK_CREATE(lock) \
  AnnotateRWLockCreate(__FILE__, __LINE__, lock)

/* Report that the lock at address "lock" is about to be destroyed. */
#define KUDU_ANNONTATE_RWLOCK_DESTROY(lock) \
  AnnotateRWLockDestroy(__FILE__, __LINE__, lock)

/* Report that the lock at address "lock" has been acquired.
   isW=1 for writer lock, isW=0 for reader lock. */
#define KUDU_ANNONTATE_RWLOCK_ACQUIRED(lock, isW) \
  AnnotateRWLockAcquired(__FILE__, __LINE__, lock, isW)

/* Report that the lock at address "lock" is about to be released. */
#define KUDU_ANNONTATE_RWLOCK_RELEASED(lock, isW) \
  AnnotateRWLockReleased(__FILE__, __LINE__, lock, isW)

#else /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */

#define KUDU_ANNONTATE_RWLOCK_CREATE(lock) /* empty */
#define KUDU_ANNONTATE_RWLOCK_DESTROY(lock) /* empty */
#define KUDU_ANNONTATE_RWLOCK_ACQUIRED(lock, isW) /* empty */
#define KUDU_ANNONTATE_RWLOCK_RELEASED(lock, isW) /* empty */
#define KUDU_ANNONTATE_HAPPENS_BEFORE(obj) /* empty */
#define KUDU_ANNONTATE_HAPPENS_AFTER(obj) /* empty */
#define KUDU_ANNONTATE_BENIGN_RACE(address, description) /* empty */
#define KUDU_ANNONTATE_BENIGN_RACE_SIZED(address, size, description) /* empty \
                                                                      */
#define KUDU_ANNONTATE_IGNORE_READS_BEGIN() /* empty */
#define KUDU_ANNONTATE_IGNORE_READS_END() /* empty */
#define KUDU_ANNONTATE_IGNORE_WRITES_BEGIN() /* empty */
#define KUDU_ANNONTATE_IGNORE_WRITES_END() /* empty */
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN() /* empty */
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END() /* empty */
#define KUDU_ANNONTATE_IGNORE_SYNC_BEGIN() /* empty */
#define KUDU_ANNONTATE_IGNORE_SYNC_END() /* empty */

#endif /* DYNAMIC_ANNOTATIONS_ENABLED */

/* Macro definitions for GCC attributes that allow static thread safety
   analysis to recognize and use some of the dynamic annotations as
   escape hatches.
   TODO(user): remove the check for __SUPPORT_DYN_ANNOTATION__ once the
   default crosstool/GCC supports these GCC attributes.  */

#define ANNOTALYSIS_STATIC_INLINE
#define ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY ;
#define ANNOTALYSIS_IGNORE_READS_BEGIN
#define ANNOTALYSIS_IGNORE_READS_END
#define ANNOTALYSIS_IGNORE_WRITES_BEGIN
#define ANNOTALYSIS_IGNORE_WRITES_END
#define ANNOTALYSIS_UNPROTECTED_READ

#if defined(__GNUC__) && (!defined(SWIG)) && (!defined(__clang__))

#if DYNAMIC_ANNOTATIONS_ENABLED == 0
#define ANNOTALYSIS_ONLY 1
#undef ANNOTALYSIS_STATIC_INLINE
#define ANNOTALYSIS_STATIC_INLINE static inline
#undef ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
#define ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY \
  {                                         \
    (void)file;                             \
    (void)line;                             \
  }
#endif

/* Only emit attributes when annotalysis is enabled. */
#if defined(__SUPPORT_TS_ANNOTATION__) && defined(__SUPPORT_DYN_ANNOTATION__)
#undef ANNOTALYSIS_IGNORE_READS_BEGIN
#define ANNOTALYSIS_IGNORE_READS_BEGIN __attribute__((ignore_reads_begin))
#undef ANNOTALYSIS_IGNORE_READS_END
#define ANNOTALYSIS_IGNORE_READS_END __attribute__((ignore_reads_end))
#undef ANNOTALYSIS_IGNORE_WRITES_BEGIN
#define ANNOTALYSIS_IGNORE_WRITES_BEGIN __attribute__((ignore_writes_begin))
#undef ANNOTALYSIS_IGNORE_WRITES_END
#define ANNOTALYSIS_IGNORE_WRITES_END __attribute__((ignore_writes_end))
#undef ANNOTALYSIS_UNPROTECTED_READ
#define ANNOTALYSIS_UNPROTECTED_READ __attribute__((unprotected_read))
#endif

#endif // defined(__GNUC__) && (!defined(SWIG)) && (!defined(__clang__))

/* TODO(user) -- Replace __CLANG_SUPPORT_DYN_ANNOTATION__ with the
   appropriate feature ID. */
#if defined(__clang__) && (!defined(SWIG)) && \
    defined(__CLANG_SUPPORT_DYN_ANNOTATION__)

/* TODO(user) -- The exclusive lock here ignores writes as well, but
   allows INGORE_READS_AND_WRITES to work properly. */
#undef ANNOTALYSIS_IGNORE_READS_BEGIN
#define ANNOTALYSIS_IGNORE_READS_BEGIN \
  __attribute__((exclusive_lock_function("*")))
#undef ANNOTALYSIS_IGNORE_READS_END
#define ANNOTALYSIS_IGNORE_READS_END __attribute__((unlock_function("*")))

#if DYNAMIC_ANNOTATIONS_ENABLED == 0
/* Turn on certain macros for static analysis, even if dynamic annotations are
   not enabled. */
#define CLANG_ANNOTALYSIS_ONLY 1

#undef ANNOTALYSIS_STATIC_INLINE
#define ANNOTALYSIS_STATIC_INLINE static inline
#undef ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
#define ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY \
  {                                         \
    (void)file;                             \
    (void)line;                             \
  }

#endif /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */
#endif /* defined(__clang__) && (!defined(SWIG)) */

/* Use the macros above rather than using these functions directly. */
#ifdef __cplusplus
extern "C" {
#endif
void AnnotateRWLockCreate(
    const char* file,
    int line,
    const volatile void* lock);
void AnnotateRWLockDestroy(
    const char* file,
    int line,
    const volatile void* lock);
void AnnotateRWLockAcquired(
    const char* file,
    int line,
    const volatile void* lock,
    long isW);
void AnnotateRWLockReleased(
    const char* file,
    int line,
    const volatile void* lock,
    long isW);
void AnnotateHappensBefore(
    const char* file,
    int line,
    const volatile void* obj);
void AnnotateHappensAfter(const char* file, int line, const volatile void* obj);
void AnnotateBenignRaceSized(
    const char* file,
    int line,
    const volatile void* address,
    size_t size,
    const char* description);
ANNOTALYSIS_STATIC_INLINE
void AnnotateIgnoreReadsBegin(const char* file, int line)
    ANNOTALYSIS_IGNORE_READS_BEGIN ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
    ANNOTALYSIS_STATIC_INLINE
    void AnnotateIgnoreReadsEnd(const char* file, int line)
        ANNOTALYSIS_IGNORE_READS_END ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
    ANNOTALYSIS_STATIC_INLINE
    void AnnotateIgnoreWritesBegin(const char* file, int line)
        ANNOTALYSIS_IGNORE_WRITES_BEGIN ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
    ANNOTALYSIS_STATIC_INLINE
    void AnnotateIgnoreWritesEnd(const char* file, int line)
        ANNOTALYSIS_IGNORE_WRITES_END ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
    void AnnotateIgnoreSyncBegin(const char* file, int line);
void AnnotateIgnoreSyncEnd(const char* file, int line);

/* Return non-zero value if running under valgrind.

  If "valgrind.h" is included into dynamic_annotations.c,
  the regular valgrind mechanism will be used.
  See http://valgrind.org/docs/manual/manual-core-adv.html about
  RUNNING_ON_VALGRIND and other valgrind "client requests".
  The file "valgrind.h" may be obtained by doing
     svn co svn://svn.valgrind.org/valgrind/trunk/include

  If for some reason you can't use "valgrind.h" or want to fake valgrind,
  there are two ways to make this function return non-zero:
    - Use environment variable: export RUNNING_ON_VALGRIND=1
    - Make your tool intercept the function runningOnValgrind() and
      change its return value.
 */
int runningOnValgrind(void);

/* AddressSanitizer annotations from LLVM asan_interface.h */

#if defined(__SANITIZE_ADDRESS__) || defined(ADDRESS_SANITIZER)
// Marks memory region [addr, addr+size) as unaddressable.
// This memory must be previously allocated by the user program. Accessing
// addresses in this region from instrumented code is forbidden until
// this region is unpoisoned. This function is not guaranteed to poison
// the whole region - it may poison only subregion of [addr, addr+size) due
// to ASan alignment restrictions.
// Method is NOT thread-safe in the sense that no two threads can
// (un)poison memory in the same memory region simultaneously.
void __asan_poison_memory_region(void const volatile* addr, size_t size);
// Marks memory region [addr, addr+size) as addressable.
// This memory must be previously allocated by the user program. Accessing
// addresses in this region is allowed until this region is poisoned again.
// This function may unpoison a superregion of [addr, addr+size) due to
// ASan alignment restrictions.
// Method is NOT thread-safe in the sense that no two threads can
// (un)poison memory in the same memory region simultaneously.
void __asan_unpoison_memory_region(void const volatile* addr, size_t size);

// User code should use macros instead of functions.
#define KUDU_ASAN_POISON_MEMORY_REGION(addr, size) \
  __asan_poison_memory_region((addr), (size))
#define KUDU_ASAN_UNPOISON_MEMORY_REGION(addr, size) \
  __asan_unpoison_memory_region((addr), (size))
#else
#define KUDU_ASAN_POISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#define KUDU_ASAN_UNPOISON_MEMORY_REGION(addr, size) \
  ((void)(addr), (void)(size))
#endif

#ifdef __cplusplus
}
#endif

#if DYNAMIC_ANNOTATIONS_ENABLED != 0 && defined(__cplusplus)

/* KUDU_ANNONTATE_UNPROTECTED_READ is the preferred way to annotate racey reads.

   Instead of doing
      KUDU_ANNONTATE_IGNORE_READS_BEGIN();
      ... = x;
      KUDU_ANNONTATE_IGNORE_READS_END();
   one can use
      ... = KUDU_ANNONTATE_UNPROTECTED_READ(x); */
template <class T>
inline T KUDU_ANNONTATE_UNPROTECTED_READ(const volatile T& x)
    ANNOTALYSIS_UNPROTECTED_READ {
  KUDU_ANNONTATE_IGNORE_READS_BEGIN();
  T res = x;
  KUDU_ANNONTATE_IGNORE_READS_END();
  return res;
}
#else /* DYNAMIC_ANNOTATIONS_ENABLED == 0 */

#define KUDU_ANNONTATE_UNPROTECTED_READ(x) (x)

#endif /* DYNAMIC_ANNOTATIONS_ENABLED */

/* Annotalysis, a GCC based static analyzer, is able to understand and use
   some of the dynamic annotations defined in this file. However, dynamic
   annotations are usually disabled in the opt mode (to avoid additional
   runtime overheads) while Annotalysis only works in the opt mode.
   In order for Annotalysis to use these dynamic annotations when they
   are disabled, we re-define these annotations here. Note that unlike the
   original macro definitions above, these macros are expanded to calls to
   static inline functions so that the compiler will be able to remove the
   calls after the analysis. */

#ifdef ANNOTALYSIS_ONLY

#undef ANNOTALYSIS_ONLY

/* Undefine and re-define the macros that the static analyzer understands. */
#undef KUDU_ANNONTATE_IGNORE_READS_BEGIN
#define KUDU_ANNONTATE_IGNORE_READS_BEGIN() \
  AnnotateIgnoreReadsBegin(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_READS_END
#define KUDU_ANNONTATE_IGNORE_READS_END() \
  AnnotateIgnoreReadsEnd(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_WRITES_BEGIN
#define KUDU_ANNONTATE_IGNORE_WRITES_BEGIN() \
  AnnotateIgnoreWritesBegin(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_WRITES_END
#define KUDU_ANNONTATE_IGNORE_WRITES_END() \
  AnnotateIgnoreWritesEnd(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN() \
  do {                                                 \
    KUDU_ANNONTATE_IGNORE_READS_BEGIN();               \
    KUDU_ANNONTATE_IGNORE_WRITES_BEGIN();              \
  } while (0)

#undef KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END() \
  do {                                               \
    KUDU_ANNONTATE_IGNORE_WRITES_END();              \
    KUDU_ANNONTATE_IGNORE_READS_END();               \
  } while (0)

#if defined(__cplusplus)
#undef KUDU_ANNONTATE_UNPROTECTED_READ
template <class T>
inline T KUDU_ANNONTATE_UNPROTECTED_READ(const volatile T& x)
    ANNOTALYSIS_UNPROTECTED_READ {
  KUDU_ANNONTATE_IGNORE_READS_BEGIN();
  T res = x;
  KUDU_ANNONTATE_IGNORE_READS_END();
  return res;
}
#endif /* __cplusplus */

#endif /* ANNOTALYSIS_ONLY */

#ifdef CLANG_ANNOTALYSIS_ONLY

#undef CLANG_ANNOTALYSIS_ONLY

/* Turn on macros that the static analyzer understands.  These should be on
 * even if dynamic annotations are off. */

#undef KUDU_ANNONTATE_IGNORE_READS_BEGIN
#define KUDU_ANNONTATE_IGNORE_READS_BEGIN() \
  AnnotateIgnoreReadsBegin(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_READS_END
#define KUDU_ANNONTATE_IGNORE_READS_END() \
  AnnotateIgnoreReadsEnd(__FILE__, __LINE__)

#undef KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_BEGIN() \
  do {                                                 \
    KUDU_ANNONTATE_IGNORE_READS_BEGIN();               \
    KUDU_ANNONTATE_IGNORE_WRITES_BEGIN();              \
  } while (0)

#undef KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END
#define KUDU_ANNONTATE_IGNORE_READS_AND_WRITES_END() \
  do {                                               \
    KUDU_ANNONTATE_IGNORE_WRITES_END();              \
    KUDU_ANNONTATE_IGNORE_READS_END();               \
  } while (0)

#if defined(__cplusplus)
#undef KUDU_ANNONTATE_UNPROTECTED_READ
template <class T>
inline T KUDU_ANNONTATE_UNPROTECTED_READ(const volatile T& x) {
  KUDU_ANNONTATE_IGNORE_READS_BEGIN();
  T res = x;
  KUDU_ANNONTATE_IGNORE_READS_END();
  return res;
}
#endif

#endif /* CLANG_ANNOTALYSIS_ONLY */

/* Undefine the macros intended only in this file. */
#undef ANNOTALYSIS_STATIC_INLINE
#undef ANNOTALYSIS_SEMICOLON_OR_EMPTY_BODY
