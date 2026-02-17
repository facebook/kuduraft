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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/url-coding.h"

using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace tools {

namespace {

string fakeDescribeOneFlag(const ActionArgsDescriptor::Arg& arg) {
  string res = gflags::DescribeOneFlag({
      arg.name, // name
      "string", // type
      arg.description, // description
      "", // current_value
      "", // default_value
      "", // filename
      false, // has_validator_fn
      true, // is_default
      nullptr // flag_ptr
  });

  // Strip the first dash from the description; this is a positional parameter
  // so let's make sure it looks like one.
  string::size_type firstDashIdx = res.find('-');
  DCHECK_NE(string::npos, firstDashIdx);
  return res.substr(0, firstDashIdx) + res.substr(firstDashIdx + 1);
}

string buildUsageString(const vector<Mode*>& chain) {
  return JoinMapped(chain, [](Mode* a) { return a->name(); }, " ");
}

// Append 'toAppend' to 'dst', but hard-wrapped at 78 columns.
// After any newline, 'continuationIndent' spaces are prepended.
void appendHardWrapped(
    StringPiece toAppend,
    int continuationIndent,
    string* dst) {
  const int kWrapColumns = 78;
  DCHECK_LT(continuationIndent, kWrapColumns);

  // The string we're appending to might not be already at a newline.
  int lastLineLength = 0;
  auto newlinePos = dst->rfind('\n');
  if (newlinePos != string::npos) {
    lastLineLength = dst->size() - newlinePos;
  }

  // Iterate through the words deciding where to wrap.
  vector<StringPiece> words = strings::Split(toAppend, " ");
  if (words.empty()) {
    return;
  }

  for (const auto& word : words) {
    // If the next word won't fit on this line, break before we append it.
    if (lastLineLength + word.size() > kWrapColumns) {
      dst->push_back('\n');
      for (int i = 0; i < continuationIndent; i++) {
        dst->push_back(' ');
      }
      lastLineLength = continuationIndent;
    }
    word.AppendToString(dst);
    dst->push_back(' ');
    lastLineLength += word.size() + 1;
  }

  // Remove the extra space that we added at the end.
  dst->resize(dst->size() - 1);
}

string spacePad(StringPiece s, int len) {
  if (s.size() >= len) {
    return s.ToString();
  }
  return string(len - s.size(), ' ') + s.ToString();
}

} // anonymous namespace

ModeBuilder::ModeBuilder(string name) : name_(std::move(name)) {}

ModeBuilder& ModeBuilder::Description(const string& description) {
  CHECK(description_.empty());
  description_ = description;
  return *this;
}

ModeBuilder& ModeBuilder::AddMode(unique_ptr<Mode> mode) {
  submodes_.push_back(std::move(mode));
  return *this;
}

ModeBuilder& ModeBuilder::AddAction(unique_ptr<Action> action) {
  actions_.push_back(std::move(action));
  return *this;
}

unique_ptr<Mode> ModeBuilder::Build() {
  CHECK(!description_.empty());
  unique_ptr<Mode> mode(new Mode());
  mode->name_ = name_;
  mode->description_ = description_;
  mode->submodes_ = std::move(submodes_);
  mode->actions_ = std::move(actions_);
  return mode;
}

// Get help for this mode, passing in its parent mode chain.
string Mode::buildHelp(const vector<Mode*>& chain) const {
  string msg;
  msg +=
      fmt::format("Usage: {} <command> [<args>]\n\n", buildUsageString(chain));
  msg += "<command> can be one of the following:\n";

  vector<pair<string, string>> linePairs;
  int maxCommandLen = 0;
  for (const auto& m : modes()) {
    linePairs.emplace_back(m->name(), m->description());
    maxCommandLen = std::max<int>(maxCommandLen, m->name().size());
  }
  for (const auto& a : actions()) {
    linePairs.emplace_back(a->name(), a->description());
    maxCommandLen = std::max<int>(maxCommandLen, a->name().size());
  }

  for (const auto& lp : linePairs) {
    msg += "  " + spacePad(lp.first, maxCommandLen);
    msg += "   ";
    appendHardWrapped(lp.second, maxCommandLen + 5, &msg);
    msg += "\n";
  }

  msg += "\n";
  return msg;
}

string Mode::buildHelpXml(const vector<Mode*>& chain) const {
  string xml;
  xml += "<mode>";
  xml += fmt::format("<name>{}</name>", name());
  xml += fmt::format(
      "<description>{}</description>", escapeForHtmlToString(description()));
  for (const auto& a : actions()) {
    xml += a->buildHelpXml(chain);
  }

  for (const auto& m : modes()) {
    vector<Mode*> mChain(chain);
    mChain.push_back(m.get());
    xml += m->buildHelpXml(mChain);
  }
  xml += "</mode>";
  return xml;
}

ActionBuilder::ActionBuilder(string name, ActionRunner runner)
    : name_(std::move(name)), runner_(std::move(runner)) {}

ActionBuilder& ActionBuilder::Description(const string& description) {
  CHECK(description_.empty());
  description_ = description;
  return *this;
}

ActionBuilder& ActionBuilder::ExtraDescription(
    const string& extra_description) {
  CHECK(!extra_description_.has_value());
  extra_description_ = extra_description;
  return *this;
}

ActionBuilder& ActionBuilder::AddRequiredParameter(
    const ActionArgsDescriptor::Arg& arg) {
  args_.required.push_back(arg);
  return *this;
}

ActionBuilder& ActionBuilder::AddRequiredVariadicParameter(
    const ActionArgsDescriptor::Arg& arg) {
  DCHECK(!args_.variadic);
  args_.variadic = arg;
  return *this;
}

ActionBuilder& ActionBuilder::AddOptionalParameter(
    string param,
    std::optional<std::string> default_value,
    std::optional<std::string> description) {
#ifndef NDEBUG
  // Make sure this gflag exists.
  string option;
  DCHECK(gflags::GetCommandLineOption(param.c_str(), &option))
      << "unknown option: " << param;
#endif
  args_.optional.emplace_back(
      ActionArgsDescriptor::Flag(
          {std::move(param),
           std::move(default_value),
           std::move(description)}));
  return *this;
}

unique_ptr<Action> ActionBuilder::Build() {
  CHECK(!description_.empty());
  unique_ptr<Action> action(new Action());
  action->name_ = name_;
  action->description_ = description_;
  action->extra_description_ = extra_description_;
  action->runner_ = runner_;
  action->args_ = args_;
  return action;
}

Status Action::Run(
    const vector<Mode*>& chain,
    const unordered_map<string, string>& requiredArgs,
    const vector<string>& variadicArgs) const {
  setOptionalParameterDefaultValues();
  return runner_({chain, this, requiredArgs, variadicArgs});
}

string Action::buildHelp(const vector<Mode*>& chain, Action::HelpMode mode)
    const {
  setOptionalParameterDefaultValues();
  string usageMsg =
      fmt::format("Usage: {} {}", buildUsageString(chain), name());
  string descMsg;
  for (const auto& param : args_.required) {
    usageMsg += fmt::format(" <{}>", param.name);
    descMsg += fakeDescribeOneFlag(param);
    descMsg += "\n";
  }
  if (args_.variadic) {
    const ActionArgsDescriptor::Arg& param = args_.variadic.value();
    usageMsg += fmt::format(" <{}>...", param.name);
    descMsg += fakeDescribeOneFlag(param);
    descMsg += "\n";
  }
  for (const auto& param : args_.optional) {
    gflags::CommandLineFlagInfo gflagInfo =
        gflags::GetCommandLineFlagInfoOrDie(param.name.c_str());

    if (param.description) {
      gflagInfo.description = *param.description;
    }

    if (gflagInfo.type == "bool") {
      if (gflagInfo.default_value == "false") {
        usageMsg += fmt::format(" [-{}]", param.name);
      } else {
        usageMsg += fmt::format(" [-no{}]", param.name);
      }
    } else {
      string noun;
      string::size_type lastUnderscoreIdx = param.name.rfind('_');
      if (lastUnderscoreIdx != string::npos &&
          lastUnderscoreIdx != param.name.size() - 1) {
        noun = param.name.substr(lastUnderscoreIdx + 1);
      } else {
        noun = param.name;
      }
      usageMsg += fmt::format(" [-{}=<{}>]", param.name, noun);
    }
    descMsg += gflags::DescribeOneFlag(gflagInfo);
    descMsg += "\n";
  }
  if (mode == kUsageOnly) {
    return usageMsg;
  }
  string msg;
  appendHardWrapped(usageMsg, 8, &msg);
  msg += "\n\n";
  appendHardWrapped(description_, 0, &msg);
  if (extra_description_) {
    msg += "\n\n";
    appendHardWrapped(extra_description_.value(), 0, &msg);
  }
  msg += "\n\n";
  msg += descMsg;
  return msg;
}

string Action::buildHelpXml(const vector<Mode*>& chain) const {
  setOptionalParameterDefaultValues();
  string usage = fmt::format("{} {}", buildUsageString(chain), name());
  string xml;
  xml += "<action>";
  xml += fmt::format("<name>{}</name>", name());
  xml += fmt::format(
      "<description>{}</description>", escapeForHtmlToString(description()));
  xml += fmt::format(
      "<extra_description>{}</extra_description>",
      escapeForHtmlToString(extra_description().value_or("")));
  for (const auto& r : args().required) {
    usage += fmt::format(" &lt;{}&gt;", r.name);
    xml += "<argument>";
    xml += "<kind>required</kind>";
    xml += fmt::format("<name>{}</name>", r.name);
    xml += fmt::format(
        "<description>{}</description>", escapeForHtmlToString(r.description));
    xml += "<type>string</type>";
    xml += "</argument>";
  }

  if (args().variadic) {
    const ActionArgsDescriptor::Arg& v = *args().variadic;
    usage += fmt::format(" &lt;{}&gt;...", v.name);
    xml += "<argument>";
    xml += "<kind>variadic</kind>";
    xml += fmt::format("<name>{}</name>", v.name);
    xml += fmt::format(
        "<description>{}</description>", escapeForHtmlToString(v.description));
    xml += "<type>string</type>";
    xml += "</argument>";
  }

  for (const auto& o : args().optional) {
    gflags::CommandLineFlagInfo gflagInfo =
        gflags::GetCommandLineFlagInfoOrDie(o.name.c_str());

    if (o.description) {
      gflagInfo.description = *o.description;
    }

    if (gflagInfo.type == "bool") {
      if (gflagInfo.default_value == "false") {
        usage += fmt::format(" [-{}]", o.name);
      } else {
        usage += fmt::format(" [-no{}]", o.name);
      }
    } else {
      string noun;
      string::size_type lastUnderscoreIdx = o.name.rfind('_');
      if (lastUnderscoreIdx != string::npos &&
          lastUnderscoreIdx != o.name.size() - 1) {
        noun = o.name.substr(lastUnderscoreIdx + 1);
      } else {
        noun = o.name;
      }
      usage += fmt::format(" [-{}=&lt;{}&gt;]", o.name, noun);
    }

    xml += "<argument>";
    xml += "<kind>optional</kind>";
    xml += fmt::format("<name>{}</name>", gflagInfo.name);
    xml += fmt::format("<description>{}</description>", gflagInfo.description);
    xml += fmt::format("<type>{}</type>", gflagInfo.type);
    xml += fmt::format(
        "<default_value>{}</default_value>", gflagInfo.default_value);
    xml += "</argument>";
  }
  xml += fmt::format("<usage>{}</usage>", escapeForHtmlToString(usage));
  xml += "</action>";
  return xml;
}

void Action::setOptionalParameterDefaultValues() const {
  for (const auto& param : args_.optional) {
    if (param.defaultValue) {
      gflags::SetCommandLineOptionWithMode(
          param.name.c_str(),
          param.defaultValue->c_str(),
          gflags::FlagSettingMode::SET_FLAGS_DEFAULT);
    }
  }
}

} // namespace tools
} // namespace kudu
