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
#include <cstdlib>
#include <deque>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/gutil/strings/join.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

DECLARE_bool(help);
DECLARE_bool(helppackage);
DECLARE_bool(helpshort);
DECLARE_bool(helpxml);
DECLARE_string(helpmatch);
DECLARE_string(helpon);

using std::cerr;
using std::cout;
using std::deque;
using std::endl;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace tools {

unique_ptr<Mode> rootMode(const string& name) {
  return ModeBuilder(name)
      .description(
          "Kudu Command Line Tools") // root mode description isn't printed
      //.addMode(BuildClusterMode())
      //.addMode(BuildDiagnoseMode())
      //.addMode(BuildFsMode())
      //.addMode(BuildHmsMode())
      //.addMode(BuildLocalReplicaMode())
      //.addMode(BuildMasterMode())
      .addMode(buildPbcMode())
      //.addMode(BuildPerfMode())
      //.addMode(BuildRemoteReplicaMode())
      //.addMode(BuildTableMode())
      //.addMode(BuildTabletMode())
      //.addMode(BuildTestMode())
      //.addMode(BuildTServerMode())
      //.addMode(BuildWalMode())
      .build();
}

Status marshalArgs(
    const vector<Mode*>& chain,
    Action* action,
    deque<string> input,
    unordered_map<string, string>* required,
    vector<string>* variadic) {
  const ActionArgsDescriptor& args = action->args();

  // Marshal the required arguments from the command line.
  for (const auto& a : args.required) {
    if (input.empty()) {
      return Status::InvalidArgument(
          fmt::format("must provide positional argument {}", a.name));
    }
    auto [it, inserted] = required->emplace(a.name, input.front());
    DCHECK(inserted) << "Duplicate argument name: " << a.name;
    input.pop_front();
  }

  // Marshal the variable length arguments, if they exist.
  if (args.variadic) {
    const ActionArgsDescriptor::Arg& a = *args.variadic;
    if (input.empty()) {
      return Status::InvalidArgument(
          fmt::format("must provide variadic positional argument {}", a.name));
    }

    variadic->assign(input.begin(), input.end());
    input.clear();
  }

  // There should be no unparsed arguments left.
  if (!input.empty()) {
    DCHECK(!chain.empty());
    return Status::InvalidArgument(
        fmt::format(
            "too many arguments: '{}'\n{}",
            JoinStrings(input, " "),
            action->buildHelp(chain)));
  }
  return Status::OK();
}

int dispatchCommand(
    const vector<Mode*>& chain,
    Action* action,
    const deque<string>& remainingArgs) {
  unordered_map<string, string> requiredArgs;
  vector<string> variadicArgs;
  Status s =
      marshalArgs(chain, action, remainingArgs, &requiredArgs, &variadicArgs);
  if (!s.ok()) {
    cerr << s.ToString() << endl;
    cerr << endl;
    cerr << action->buildHelp(chain, Action::kUsageOnly) << endl;
    return 1;
  }
  s = action->run(chain, requiredArgs, variadicArgs);
  if (s.ok()) {
    return 0;
  }
  cerr << s.ToString() << endl;
  return 1;
}

// Replace hyphens with underscores in a string and return a copy.
static string hyphensToUnderscores(string str) {
  std::replace(str.begin(), str.end(), '-', '_');
  return str;
}

void dumpToolXml(const string& path) {
  unique_ptr<Mode> root = rootMode(BaseName(path));
  cout << "<?xml version=\"1.0\"?>";
  cout << "<AllModes>";
  for (const auto& mode : root->modes()) {
    vector<Mode*> chain = {root.get(), mode.get()};
    cout << mode->buildHelpXml(chain);
  }
  cout << "</AllModes>" << endl;
}

int runTool(int argc, char** argv, bool showHelp) {
  unique_ptr<Mode> root = rootMode(argv[0]);
  // Initialize arg parsing state.
  vector<Mode*> chain = {root.get()};

  // Parse the arguments, matching each to a mode or action.
  for (int i = 1; i < argc; i++) {
    Mode* cur = chain.back();
    Mode* nextMode = nullptr;
    Action* nextAction = nullptr;

    // Match argument with a mode.
    for (const auto& m : cur->modes()) {
      if (m->name() == argv[i] ||
          // Allow hyphens in addition to underscores in mode names.
          m->name() == hyphensToUnderscores(argv[i])) {
        nextMode = m.get();
        break;
      }
    }

    // Match argument with an action.
    for (const auto& a : cur->actions()) {
      if (a->name() == argv[i] ||
          // Allow hyphens in addition to underscores in action names.
          a->name() == hyphensToUnderscores(argv[i])) {
        nextAction = a.get();
        break;
      }
    }

    // If both matched, there's an error with the tree.
    DCHECK(!nextMode || !nextAction);

    if (nextMode) {
      // Add the mode and keep parsing.
      chain.push_back(nextMode);
    } else if (nextAction) {
      if (showHelp) {
        cerr << nextAction->buildHelp(chain);
        return 1;
      } else {
        // Invoke the action with whatever arguments remain, skipping this one.
        deque<string> remainingArgs;
        for (int j = i + 1; j < argc; j++) {
          remainingArgs.emplace_back(argv[j]);
        }
        return dispatchCommand(chain, nextAction, remainingArgs);
      }
    } else {
      // Couldn't match the argument at all. Print the help.
      Status s = Status::InvalidArgument(
          fmt::format("unknown command '{}'\n", argv[i]));
      cerr << s.ToString() << cur->buildHelp(chain);
      return 1;
    }
  }

  // Ran out of arguments before reaching an action. Print the last mode's help.
  DCHECK(!chain.empty());
  const Mode* last = chain.back();
  cerr << last->buildHelp(chain);
  return 1;
}

bool parseCommandLineFlags(const char* progName) {
  // Leverage existing helpxml flag to print mode/action xml.
  if (FLAGS_helpxml) {
    kudu::tools::dumpToolXml(progName);
    exit(1);
  }

  bool showHelp = false;
  if (FLAGS_help || FLAGS_helpshort || !FLAGS_helpon.empty() ||
      !FLAGS_helpmatch.empty() || FLAGS_helppackage) {
    FLAGS_help = false;
    FLAGS_helpshort = false;
    FLAGS_helpon = "";
    FLAGS_helpmatch = "";
    FLAGS_helppackage = false;
    showHelp = true;
  }
  kudu::HandleCommonFlags();
  return showHelp;
}

int toolMain(int argc, char** argv) {
  // Disable redaction by default so that user data printed to the console will
  // be shown in full.
  CHECK_NE(
      "",
      gflags::SetCommandLineOptionWithMode(
          "redact", "", gflags::SET_FLAGS_DEFAULT));

  // Hide the regular gflags help unless --helpfull is used.
  //
  // Inspired by
  // https://github.com/gflags/gflags/issues/43#issuecomment-168280647.
  gflags::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  FLAGS_logtostderr = true;
  const char* progName = argv[0];
  kudu::InitGoogleLoggingSafe(progName);
  bool showHelp = parseCommandLineFlags(progName);

  return kudu::tools::runTool(argc, argv, showHelp);
}

} // namespace tools
} // namespace kudu
