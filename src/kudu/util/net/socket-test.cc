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

#include "kudu/util/net/socket.h"

#include <thread>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stddef.h>
#include <cstdint>
#include <memory>
#include <string>

#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

constexpr size_t kEchoChunkSize = 32 * 1024 * 1024;

class SocketTest : public KuduTest {
 protected:
  void doTest(bool accept, const std::string& message) {
    Sockaddr address;
    address.ParseString("0.0.0.0", 0);
    Socket listener;

    CHECK_OK(listener.init(0));
    CHECK_OK(listener.bindAndListen(address, 0));
    Sockaddr listenAddress;
    CHECK_OK(listener.getSocketAddress(&listenAddress));

    std::thread t([&] {
      if (accept) {
        Sockaddr newAddr;
        Socket sock;
        CHECK_OK(listener.accept(&sock, &newAddr, 0));
        CHECK_OK(sock.close());
      } else {
        SleepFor(MonoDelta::FromMilliseconds(200));
        CHECK_OK(listener.close());
      }
    });

    Socket client;
    ASSERT_OK(client.init(0));
    ASSERT_OK(client.connect(listenAddress));
    CHECK_OK(client.setRecvTimeout(MonoDelta::FromMilliseconds(100)));

    int n;
    std::unique_ptr<uint8_t[]> buf(new uint8_t[kEchoChunkSize]);
    Status s = client.recv(buf.get(), kEchoChunkSize, &n);

    ASSERT_TRUE(!s.ok());
    ASSERT_TRUE(s.IsNetworkError());
    ASSERT_STR_MATCHES(s.message().toString(), message);

    t.join();
  }
};

TEST_F(SocketTest, TestRecvReset) {
  doTest(
      false,
      "recv error from (\\[::1\\]|127.0.0.1):[0-9]+: Resource temporarily unavailable");
}

TEST_F(SocketTest, TestRecvEOF) {
  doTest(true, "recv got EOF from (\\[::1\\]|127.0.0.1):[0-9]+");
}
} // namespace kudu
