// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include "kudu/util/Stats.h"
#include "kudu/util/logging.h"

namespace kudu {

// DCHECK variants for kudu code. Besides triggering crash in debug build mode
// like DCHECK, it also logs the check violation information for both
// debug build mode and prod build mode.
// Valid usages:
//    K_DCHECK(false, example_check);
//    K_DCHECK(false, example_check, "this is kudu dcheck");
#define K_DCHECK(expression, tag, ...)                               \
  do {                                                               \
    if (!(expression)) {                                             \
      auto crashMsg = fmt::format(" " __VA_ARGS__).substr(1);        \
      KLOG_EVERY_N_SECS(ERROR, 5)                                    \
          << #tag << ": " << #expression << " failed: " << crashMsg; \
      STATS_kudu_check_violations.add(1, #tag);                      \
      DCHECK(false);                                                 \
    }                                                                \
  } while (0)

// CHECK version of K_DCHECK
#define K_CHECK(expression, tag, ...)                                \
  do {                                                               \
    if (!(expression)) {                                             \
      auto crashMsg = fmt::format(" " __VA_ARGS__).substr(1);        \
      KLOG_EVERY_N_SECS(ERROR, 5)                                    \
          << #tag << ": " << #expression << " failed: " << crashMsg; \
      STATS_kudu_check_violations.add(1, #tag);                      \
      CHECK(false);                                                  \
    }                                                                \
  } while (0)

} // namespace kudu
