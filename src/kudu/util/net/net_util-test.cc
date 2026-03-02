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

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class NetUtilTest : public KuduTest {
 protected:
  Status doParseBindAddresses(const string& input, string* result) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(ParseAddressList(input, kDefaultPort, &addrs));
    std::sort(addrs.begin(), addrs.end());

    vector<string> addrStrs;
    for (const Sockaddr& addr : addrs) {
      addrStrs.push_back(addr.ToString());
    }
    *result = JoinStrings(addrStrs, ",");
    return Status::OK();
  }

  static const uint16_t kDefaultPort = 7150;
};

TEST(SockaddrTest, Test) {
  Sockaddr addr;
  ASSERT_OK(addr.ParseString("[::1]:12345", 12345));
  ASSERT_EQ(12345, addr.port());
  ASSERT_EQ("::1", addr.host());
}

TEST(SockaddrTest, Test2) {
  Sockaddr addr;
  ASSERT_OK(addr.ParseString("[::1]", 12345));
  ASSERT_EQ("::1", addr.host());
}

TEST_F(NetUtilTest, TestParseAddresses) {
  string ret;
  ASSERT_OK(doParseBindAddresses("[::]:12345", &ret));
  // TODO(mpercy): If this requires square brackets to parse it should generate
  // them as well. For now, it does not.
  ASSERT_EQ("[::]:12345", ret);

  ASSERT_OK(doParseBindAddresses("[::]", &ret));
  ASSERT_EQ("[::]:7150", ret);

  ASSERT_OK(doParseBindAddresses("[::]:12345, [::]:12346", &ret));
  ASSERT_EQ("[::]:12345,[::]:12346", ret);

  // Test some invalid addresses.
  Status s = doParseBindAddresses("[::]:xyz", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");

  s = doParseBindAddresses("[::]:100000", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");

  s = doParseBindAddresses("[::]:", &ret);
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid port");
}

TEST_F(NetUtilTest, TestResolveAddresses) {
  HostPort hp("localhost", 12345);
  vector<Sockaddr> addrs;
  ASSERT_OK(hp.resolveAddresses(&addrs));
  ASSERT_TRUE(!addrs.empty());
  for (const Sockaddr& addr : addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    ASSERT_STR_MATCHES(addr.ToString(), "^(127\\.|\\[::1])");
    ASSERT_STR_MATCHES(addr.ToString(), ":12345$");
    EXPECT_TRUE(addr.IsAnyLocalAddress());
  }

  ASSERT_OK(hp.resolveAddresses(nullptr));
}

TEST_F(NetUtilTest, TestWithinNetwork) {
  Sockaddr addr;
  Network network;

  ASSERT_OK(addr.ParseString("[2020::1]:12345", 0));
  ASSERT_OK(network.parseCidrString("2020::/16"));
  EXPECT_TRUE(network.withinNetwork(addr));

  ASSERT_OK(addr.ParseString("[2020:1010::]:0", 0));
  ASSERT_OK(network.parseCidrString("2020:1010::/32"));
  EXPECT_TRUE(network.withinNetwork(addr));

  ASSERT_OK(addr.ParseString("[ffff::1]", 0));
  ASSERT_OK(network.parseCidrString("[::1]/0"));
  EXPECT_TRUE(network.withinNetwork(addr));

  ASSERT_OK(addr.ParseString("[::1]:0", 0));
  ASSERT_OK(network.parseCidrString("2020:1010::/64"));
  EXPECT_FALSE(network.withinNetwork(addr));
}

// Ensure that we are able to do a reverse DNS lookup on various IP addresses.
// The reverse lookups should never fail, but may return numeric strings.
TEST_F(NetUtilTest, TestReverseLookup) {
  string host;
  Sockaddr addr;
  HostPort hp;
  ASSERT_OK(addr.ParseString("[::]:12345", 0));
  EXPECT_EQ(12345, addr.port());
  // addr.addr().sin6_addr.s6_addr; // 16-element byte array
  // LOG(INFO) << addr.addr().sin6_addr.s;
  ASSERT_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  EXPECT_NE("::", hp.host());
  EXPECT_NE("", hp.host());
  EXPECT_EQ(12345, hp.port());

  ASSERT_OK(addr.ParseString("[::1]:12345", 0));
  ASSERT_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  EXPECT_EQ("::1", hp.host());
  EXPECT_EQ(12345, hp.port());
}

TEST_F(NetUtilTest, TestLsof) {
  Socket s;
  ASSERT_OK(s.Init(0));

  Sockaddr addr; // wildcard
  ASSERT_OK(s.BindAndListen(addr, 1));

  ASSERT_OK(s.GetSocketAddress(&addr));
  ASSERT_NE(addr.port(), 0);
  vector<string> lsofLines;
  TryRunLsof(addr, &lsofLines);
  SCOPED_TRACE(JoinStrings(lsofLines, "\n"));

  ASSERT_GE(lsofLines.size(), 3);
  ASSERT_STR_CONTAINS(lsofLines[2], "net_util-test");
}

TEST_F(NetUtilTest, TestGetFQDN) {
  string fqdn;
  ASSERT_OK(GetFQDN(&fqdn));
  LOG(INFO) << "fqdn is " << fqdn;
}

} // namespace kudu
