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

#include <string>

#include <gtest/gtest.h>
#include <optional>

#include "kudu/security/init.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

class MiniKdcTest : public KuduTest {};

TEST_F(MiniKdcTest, TestBasicOperation) {
  MiniKdcOptions options;
  MiniKdc kdc(options);
  ASSERT_OK(kdc.Start());
  ASSERT_GT(kdc.port(), 0);
  ASSERT_OK(kdc.CreateUserPrincipal("alice"));
  ASSERT_OK(kdc.Kinit("alice"));

  ASSERT_OK(kdc.Stop());
  ASSERT_OK(kdc.Start());

  // Check that alice is kinit'd.
  string klist;
  ASSERT_OK(kdc.Klist(&klist));
  ASSERT_STR_CONTAINS(klist, "alice@KRBTEST.COM");

  ASSERT_OK(kdc.CreateUserPrincipal("bob"));
  ASSERT_OK(kdc.Kinit("bob"));

  // Check that bob has replaced alice as the kinit'd principal.
  ASSERT_OK(kdc.Klist(&klist));
  ASSERT_STR_NOT_CONTAINS(klist, "alice@KRBTEST.COM");
  ASSERT_STR_CONTAINS(klist, "bob@KRBTEST.COM");
  ASSERT_STR_CONTAINS(klist, "krbtgt/KRBTEST.COM@KRBTEST.COM");

  // Drop 'bob' credentials. We'll get a RuntimeError because klist
  // exits with a non-zero exit code if there are no cached credentials.
  ASSERT_OK(kdc.Kdestroy());
  ASSERT_TRUE(kdc.Klist(&klist).IsRuntimeError());

  // Test keytab creation.
  const string kSpn = "kudu/foo.example.com";
  string ktPath;
  ASSERT_OK(kdc.CreateServiceKeytab(kSpn, &ktPath));
  SCOPED_TRACE(ktPath);
  ASSERT_OK(kdc.KlistKeytab(ktPath, &klist));
  ASSERT_STR_CONTAINS(klist, "kudu/foo.example.com@KRBTEST.COM");

  // Test programmatic keytab login.
  kdc.SetKrb5Environment();
  ASSERT_OK(security::InitKerberosForServer(kSpn, ktPath));
  ASSERT_EQ(
      "kudu/foo.example.com@KRBTEST.COM",
      *security::getLoggedInPrincipalFromKeytab());

  // Test principal canonicalization.
  string princ = "foo";
  ASSERT_OK(security::canonicalizeKrb5Principal(&princ));
  ASSERT_EQ("foo@KRBTEST.COM", princ);

  // Test auth-to-local mapping for a user from the local realm as well as a
  // remote realm.
  {
    string localUser;
    ASSERT_OK(security::mapPrincipalToLocalName("foo@KRBTEST.COM", &localUser));
    ASSERT_EQ("foo", localUser);

    ASSERT_OK(
        security::mapPrincipalToLocalName("foo/host@KRBTEST.COM", &localUser));
    ASSERT_EQ("foo", localUser);

    // The Heimdal implementation in macOS does not correctly implement auth to
    // local mapping (see init.cc).
    ASSERT_OK(
        security::mapPrincipalToLocalName("foo@OTHERREALM.COM", &localUser));
    ASSERT_EQ("other-foo", localUser);
  }
}

// Regression test to ensure that dropping a stopped MiniKdc doesn't panic.
TEST_F(MiniKdcTest, TestStopDrop) {
  MiniKdcOptions options;
  MiniKdc kdc(options);
}

TEST_F(MiniKdcTest, TestOperationsWhenKdcNotRunning) {
  MiniKdcOptions options;
  MiniKdc kdc(options);
  ASSERT_OK(kdc.Start());
  ASSERT_OK(kdc.Stop());

  // MiniKdc::CreateUserPrincipal() works directly with the local files,
  // so it should work fine even if KDC is shut down.
  ASSERT_OK(kdc.CreateUserPrincipal("alice"));

  {
    // Without running KDC it should not be possible to obtain and cache an
    // initial ticket-granting ticket for principal.
    const Status s = kdc.Kinit("alice");
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
  }
  {
    // Without running KDC klist should fail.
    string klist;
    const Status s = kdc.Klist(&klist);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
  }

  ASSERT_OK(kdc.Start());

  // Once KDC has started, 'kinit' and 'klist' should work with no issues.
  ASSERT_OK(kdc.Kinit("alice"));
  {
    // Check that alice is kinit'd.
    string klist;
    ASSERT_OK(kdc.Klist(&klist));
    ASSERT_STR_CONTAINS(klist, "alice@KRBTEST.COM");
  }
}

} // namespace kudu
