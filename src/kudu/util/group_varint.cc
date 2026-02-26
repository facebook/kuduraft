// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>

#include <boost/utility/binary.hpp>
#include <glog/logging.h>

#include "kudu/util/group_varint-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace coding {

bool sseTableInitted = false;
uint8_t sseTable[256 * 16] __attribute__((aligned(16)));
uint8_t varintSelectorLengths[256];

__attribute__((constructor)) static void initializeSseTables() {
  memset(sseTable, 0xff, sizeof(sseTable));

  for (int i = 0; i < 256; i++) {
    uint32_t* entry = reinterpret_cast<uint32_t*>(&sseTable[i * 16]);

    uint8_t selectors[] = {
        static_cast<uint8_t>((i & BOOST_BINARY(11 00 00 00)) >> 6),
        static_cast<uint8_t>((i & BOOST_BINARY(00 11 00 00)) >> 4),
        static_cast<uint8_t>((i & BOOST_BINARY(00 00 11 00)) >> 2),
        static_cast<uint8_t>((i & BOOST_BINARY(00 00 00 11)))};

    // 00000000 ->
    // 00 ff ff ff  01 ff ff ff  02 ff ff ff  03 ff ff ff

    // 01000100 ->
    // 00 01 ff ff  02 ff ff ff  03 04 ff ff  05 ff ff ff

    uint8_t offset = 0;

    for (int j = 0; j < 4; j++) {
      uint8_t numBytes = selectors[j] + 1;
      uint8_t* entryBytes = reinterpret_cast<uint8_t*>(&entry[j]);

      for (int k = 0; k < numBytes; k++) {
        *entryBytes++ = offset++;
      }
    }

    varintSelectorLengths[i] = offset;
  }

  sseTableInitted = true;
}

void dumpSseTable() {
  LOG(INFO) << "SSE table:\n"
            << kudu::hexDump(Slice(sseTable, sizeof(sseTable)));
}

} // namespace coding
} // namespace kudu
