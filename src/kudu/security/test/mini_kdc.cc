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

#include "kudu/security/test/mini_kdc.h"

#include <csignal>
#include <cstdlib>

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/strings/strip.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

string MiniKdcOptions::ToString() const {
  return fmt::format(
      "{{ realm: {}, dataRoot: {}, port: {}, "
      "ticketLifetime: {}, renewLifetime: {} }}",
      realm,
      dataRoot,
      port,
      ticketLifetime,
      renewLifetime);
}

MiniKdc::MiniKdc() : MiniKdc(MiniKdcOptions()) {}

MiniKdc::MiniKdc(MiniKdcOptions options) : options_(std::move(options)) {
  if (options_.realm.empty()) {
    options_.realm = "KRBTEST.COM";
  }
  if (options_.dataRoot.empty()) {
    options_.dataRoot = JoinPathSegments(GetTestDataDirectory(), "krb5kdc");
  }
  if (options_.ticketLifetime.empty()) {
    options_.ticketLifetime = "24h";
  }
  if (options_.renewLifetime.empty()) {
    options_.renewLifetime = "7d";
  }
}

MiniKdc::~MiniKdc() {
  if (kdcProcess_) {
    WARN_NOT_OK(Stop(), "Unable to stop MiniKdc");
  }
}

map<string, string> MiniKdc::GetEnvVars() const {
  return {
      {"KRB5_CONFIG", JoinPathSegments(options_.dataRoot, "krb5.conf")},
      {"KRB5_KDC_PROFILE", JoinPathSegments(options_.dataRoot, "kdc.conf")},
      {"KRB5CCNAME", JoinPathSegments(options_.dataRoot, "krb5cc")},
      // Enable the workaround for MIT krb5 1.10 bugs from
      // krb5_realm_override.cc.
      {"KUDU_ENABLE_KRB5_REALM_FIX", "yes"}};
}

vector<string> MiniKdc::MakeArgv(const vector<string>& inArgv) {
  vector<string> realArgv = {"env"};
  for (const auto& p : GetEnvVars()) {
    realArgv.push_back(fmt::format("{}={}", p.first, p.second));
  }
  for (const string& a : inArgv) {
    realArgv.push_back(a);
  }
  return realArgv;
}

namespace {
// Attempts to find the path to the specified Kerberos binary, storing it in
// 'path'.
Status getBinaryPath(const string& binary, string* path) {
  static const vector<string> kCommonLocations = {
      "/usr/local/opt/krb5/sbin", // Homebrew
      "/usr/local/opt/krb5/bin", // Homebrew
      "/opt/local/sbin", // Macports
      "/opt/local/bin", // Macports
      "/usr/lib/mit/sbin", // SLES
      "/usr/sbin", // Linux
  };
  return FindExecutable(binary, kCommonLocations, path);
}
} // namespace

Status MiniKdc::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, "starting KDC");
  CHECK(!kdcProcess_);
  VLOG(1) << "Starting Kerberos KDC: " << options_.ToString();

  if (!Env::Default()->FileExists(options_.dataRoot)) {
    VLOG(1) << "Creating KDC database and configuration files";
    RETURN_NOT_OK(Env::Default()->CreateDir(options_.dataRoot));

    RETURN_NOT_OK(CreateKdcConf());
    RETURN_NOT_OK(CreateKrb5Conf());

    // Create the KDC database using the kdb5_util tool.
    string kdb5UtilBin;
    RETURN_NOT_OK(getBinaryPath("kdb5_util", &kdb5UtilBin));

    RETURN_NOT_OK(
        Subprocess::Call(MakeArgv({
            kdb5UtilBin,
            "create",
            "-s", // Stash the master password.
            "-P",
            "masterpw", // Set a password.
            "-W", // Use weak entropy (since we don't need real security).
        })));
  }

  // Start the Kerberos KDC.
  string krb5kdcBin;
  RETURN_NOT_OK(getBinaryPath("krb5kdc", &krb5kdcBin));

  kdcProcess_.reset(new Subprocess(MakeArgv({
      krb5kdcBin,
      "-n", // Do not daemonize.
  })));

  RETURN_NOT_OK(kdcProcess_->Start());

  const bool needConfigUpdate = (options_.port == 0);
  // Wait for KDC to start listening on its ports and commencing operation.
  RETURN_NOT_OK(WaitForUdpBind(
      kdcProcess_->pid(), &options_.port, MonoDelta::FromSeconds(1)));

  if (needConfigUpdate) {
    // If we asked for an ephemeral port, grab the actual ports and
    // rewrite the configuration so that clients can connect.
    RETURN_NOT_OK(CreateKrb5Conf());
    RETURN_NOT_OK(CreateKdcConf());
  }

  return Status::OK();
}

Status MiniKdc::Stop() {
  if (!kdcProcess_) {
    return Status::OK();
  }
  VLOG(1) << "Stopping KDC";
  unique_ptr<Subprocess> proc(kdcProcess_.release());
  RETURN_NOT_OK(proc->Kill(SIGKILL));
  RETURN_NOT_OK(proc->Wait());

  return Status::OK();
}

// Creates a kdc.conf file according to the provided options.
Status MiniKdc::CreateKdcConf() const {
  static constexpr std::string_view kFileTemplate = R"(
[kdcdefaults]
kdc_ports = {2}
kdc_tcp_ports = ""

[realms]
{1} = {{
        acl_file = {0}/kadm5.acl
        admin_keytab = {0}/kadm5.keytab
        database_name = {0}/principal
        key_stash_file = {0}/.k5.{1}
        max_renewable_life = 7d 0h 0m 0s
}}
  )";
  string fileContents = fmt::format(
      kFileTemplate, options_.dataRoot, options_.realm, options_.port);
  return WriteStringToFile(
      Env::Default(),
      fileContents,
      JoinPathSegments(options_.dataRoot, "kdc.conf"));
}

// Creates a krb5.conf file according to the provided options.
Status MiniKdc::CreateKrb5Conf() const {
  static constexpr std::string_view kFileTemplate = R"(
[logging]
    kdc = FILE:/dev/stderr

[libdefaults]
    default_realm = {1}
    dns_lookup_kdc = false
    dns_lookup_realm = false
    forwardable = true
    renew_lifetime = {2}
    ticket_lifetime = {3}

    # Disable aes256 since Java does not support it without JCE. Java is only
    # one of several minicluster consumers, but disabling aes256 doesn't
    # appreciably hurt Kudu code coverage, so we disable it universally.
    #
    # For more details, see:
    # https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html
    default_tkt_enctypes = aes128-cts des3-cbc-sha1
    default_tgs_enctypes = aes128-cts des3-cbc-sha1
    permitted_enctypes = aes128-cts des3-cbc-sha1

    # In miniclusters, we start daemons on local loopback IPs that
    # have no reverse DNS entries. So, disable reverse DNS.
    rdns = false

    # The server side will start its GSSAPI server using the local FQDN.
    # However, in tests, we connect to it via a non-matching loopback IP.
    # This enables us to connect despite that mismatch.
    ignore_acceptor_hostname = true

[realms]
    {1} = {{
        kdc = 127.0.0.1:{0}
        # This super-arcane syntax can be found documented in various Hadoop
        # vendors' security guides and very briefly in the MIT krb5 docs.
        # Basically, this one says to map anyone coming in as foo@OTHERREALM.COM
        # and map them to a local user 'other-foo'
        auth_to_local = RULE:[1:other-${{1}}@${{0}}](.*@OTHERREALM.COM$)s/@.*//
    }}
  )";
  string fileContents = fmt::format(
      kFileTemplate,
      options_.port,
      options_.realm,
      options_.renewLifetime,
      options_.ticketLifetime);
  return WriteStringToFile(
      Env::Default(),
      fileContents,
      JoinPathSegments(options_.dataRoot, "krb5.conf"));
}

Status MiniKdc::CreateUserPrincipal(const string& username) {
  SCOPED_LOG_SLOW_EXECUTION(
      WARNING, 100, fmt::format("creating user principal {}", username));
  string kadmin;
  RETURN_NOT_OK(getBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(
      Subprocess::Call(MakeArgv(
          {kadmin,
           "-q",
           fmt::format("add_principal -pw {} {}", username, username)})));
  return Status::OK();
}

Status MiniKdc::CreateServiceKeytab(const string& spn, string* path) {
  SCOPED_LOG_SLOW_EXECUTION(
      WARNING, 100, fmt::format("creating service keytab for {}", spn));
  string ktPath = spn;
  StripString(&ktPath, "/", '_');
  ktPath = JoinPathSegments(options_.dataRoot, ktPath) + ".keytab";

  string kadmin;
  RETURN_NOT_OK(getBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(
      Subprocess::Call(MakeArgv(
          {kadmin, "-q", fmt::format("add_principal -randkey {}", spn)})));
  RETURN_NOT_OK(
      Subprocess::Call(MakeArgv(
          {kadmin, "-q", fmt::format("ktadd -k {} {}", ktPath, spn)})));
  *path = ktPath;
  return Status::OK();
}

Status MiniKdc::CreateKeytabForExistingPrincipal(const string& spn) {
  SCOPED_LOG_SLOW_EXECUTION(
      WARNING, 100, fmt::format("creating keytab for {}", spn));
  string ktPath = spn;
  StripString(&ktPath, "/", '_');
  ktPath = JoinPathSegments(options_.dataRoot, ktPath) + ".keytab";

  string kadmin;
  RETURN_NOT_OK(getBinaryPath("kadmin.local", &kadmin));
  RETURN_NOT_OK(
      Subprocess::Call(MakeArgv(
          {kadmin,
           "-q",
           fmt::format("xst -norandkey -k {} {}", ktPath, spn)})));
  return Status::OK();
}

Status MiniKdc::Kinit(const string& username) {
  SCOPED_LOG_SLOW_EXECUTION(
      WARNING, 100, fmt::format("kinit for {}", username));
  string kinit;
  RETURN_NOT_OK(getBinaryPath("kinit", &kinit));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({kinit, username}), username));
  return Status::OK();
}

Status MiniKdc::Kdestroy() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 100, "kdestroy");
  string kdestroy;
  RETURN_NOT_OK(getBinaryPath("kdestroy", &kdestroy));
  return Subprocess::Call(MakeArgv({kdestroy, "-A"}));
}

Status MiniKdc::Klist(string* output) {
  string klist;
  RETURN_NOT_OK(getBinaryPath("klist", &klist));
  RETURN_NOT_OK(Subprocess::Call(MakeArgv({klist, "-A"}), "", output));
  return Status::OK();
}

Status MiniKdc::KlistKeytab(const string& keytab_path, string* output) {
  string klist;
  RETURN_NOT_OK(getBinaryPath("klist", &klist));
  RETURN_NOT_OK(
      Subprocess::Call(MakeArgv({klist, "-k", keytab_path}), "", output));
  return Status::OK();
}

Status MiniKdc::SetKrb5Environment() const {
  if (!kdcProcess_) {
    return Status::IllegalState("KDC not started");
  }
  for (const auto& p : GetEnvVars()) {
    CHECK_ERR(setenv(p.first.c_str(), p.second.c_str(), 1 /*overwrite*/));
  }

  return Status::OK();
}

} // namespace kudu
