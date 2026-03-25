// Copyright 2004 Google Inc.
// All Rights Reserved.
//
//

#include "kudu/gutil/int128.h"
#include <cstdint>
#include <iostream>

namespace kudu {

const uint128_pod kUint128PodMax = {
    static_cast<uint64_t>(0xFFFFFFFFFFFFFFFFULL),
    static_cast<uint64_t>(0xFFFFFFFFFFFFFFFFULL)};

std::ostream& operator<<(std::ostream& o, const kudu::uint128& b) {
  return (o << b.hi_ << "::" << b.lo_);
}

} // namespace kudu
