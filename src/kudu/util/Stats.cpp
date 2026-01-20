// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "kudu/util/Stats.h"

namespace kudu {

DEFINE_dynamic_timeseries(
    kuduCheckViolations,
    "kudu_check_violations.{}.count",
    facebook::fb303::ExportType::COUNT);

} // namespace kudu
