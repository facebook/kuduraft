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

#ifdef __cplusplus
#error "This file should be built as pure C to avoid name mangling"
#endif

#include <stdlib.h>
#include <string.h>

#include "kudu/gutil/dynamic_annotations.h"

#ifdef __GNUC__
/* valgrind.h uses gcc extensions so it won't build with other compilers */
#include "kudu/gutil/valgrind.h"
#endif

/* Compiler-based ThreadSanitizer defines
   DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL = 1
   and provides its own definitions of the functions. */

#ifndef DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL
#define DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL 0
#endif

/* Each function is empty and called (via a macro) only in debug mode.
   The arguments are captured by dynamic tools at runtime. */

#if DYNAMIC_ANNOTATIONS_ENABLED == 1 && DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL == 0

void AnnotateRWLockCreate(
    const char* file,
    int line,
    const volatile void* lock) {}
void AnnotateRWLockDestroy(
    const char* file,
    int line,
    const volatile void* lock) {}
void AnnotateRWLockAcquired(
    const char* file,
    int line,
    const volatile void* lock,
    long isW) {}
void AnnotateRWLockReleased(
    const char* file,
    int line,
    const volatile void* lock,
    long isW) {}
void AnnotateHappensBefore(
    const char* file,
    int line,
    const volatile void* obj) {}
void AnnotateHappensAfter(
    const char* file,
    int line,
    const volatile void* obj) {}
void AnnotateBenignRaceSized(
    const char* file,
    int line,
    const volatile void* mem,
    size_t size,
    const char* description) {}
void AnnotateIgnoreReadsBegin(const char* file, int line) {}
void AnnotateIgnoreReadsEnd(const char* file, int line) {}
void AnnotateIgnoreWritesBegin(const char* file, int line) {}
void AnnotateIgnoreWritesEnd(const char* file, int line) {}

#endif /* DYNAMIC_ANNOTATIONS_ENABLED == 1 \
   && DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL == 0 */

// Note: runningOnValgrind and valgrindSlowdown are NOT provided by the TSAN
// runtime, so we always need to define them ourselves even when
// DYNAMIC_ANNOTATIONS_EXTERNAL_IMPL=1. The TSAN runtime only provides the
// Annotate* functions.

static int getRunningOnValgrind(void) {
#ifdef RUNNING_ON_VALGRIND
  if (RUNNING_ON_VALGRIND) {
    return 1;
  }
#endif
  char* runningOnValgrindStr = getenv("RUNNING_ON_VALGRIND");
  if (runningOnValgrindStr) {
    return strcmp(runningOnValgrindStr, "0") != 0;
  }
  return 0;
}

/* See the comments in dynamic_annotations.h */
int runningOnValgrind(void) {
  static volatile int cachedRunningOnValgrind = -1;
  int localRunningOnValgrind = cachedRunningOnValgrind;
  /* C doesn't have thread-safe initialization of statics, and we
     don't want to depend on pthread_once here, so hack it. */
  KUDU_ANNONTATE_BENIGN_RACE(&cachedRunningOnValgrind, "safe hack");
  if (localRunningOnValgrind == -1) {
    cachedRunningOnValgrind = localRunningOnValgrind = getRunningOnValgrind();
  }
  return localRunningOnValgrind;
}
