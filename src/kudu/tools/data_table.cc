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

#include "kudu/tools/data_table.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/util/jsonwriter.h"
#include "kudu/util/status.h"

DEFINE_string(
    columns,
    "",
    "Comma-separated list of column fields to include in output tables");
DEFINE_string(
    format,
    "pretty",
    "Format to use for printing list output tables.\n"
    "Possible values: pretty, space, tsv, csv, and json");

namespace kudu::tools {

using std::endl;
using std::ostream;
using std::setfill;
using std::setw;
using std::string;
using std::vector;

namespace {

void prettyPrintTable(
    const vector<string>& headers,
    const vector<vector<string>>& columns,
    ostream& out) {
  CHECK_EQ(headers.size(), columns.size());
  if (headers.empty()) {
    return;
  }
  size_t numColumns = headers.size();

  vector<size_t> widths;
  for (int col = 0; col < numColumns; col++) {
    size_t width = std::accumulate(
        columns[col].begin(),
        columns[col].end(),
        headers[col].size(),
        [](size_t acc, const string& cell) {
          return std::max(acc, cell.size());
        });
    widths.push_back(width);
  }

  for (int col = 0; col < numColumns; col++) {
    int padding = widths[col] - headers[col].size();
    out << setw(padding / 2) << "" << " " << headers[col];
    if (col != numColumns - 1) {
      out << setw((padding + 1) / 2) << "" << " |";
    }
  }
  out << endl;

  out << setfill('-');
  for (int col = 0; col < numColumns; col++) {
    out << setw(widths[col] + 2) << "";
    if (col != numColumns - 1) {
      out << "+";
    }
  }
  out << endl;

  out << setfill(' ');
  int numRows = columns.empty() ? 0 : columns[0].size();
  for (int row = 0; row < numRows; row++) {
    for (int col = 0; col < numColumns; col++) {
      const auto& value = columns[col][row];
      out << " " << value;
      if (col != numColumns - 1) {
        size_t padding = widths[col] - value.size();
        out << setw(padding) << "" << " |";
      }
    }
    out << endl;
  }
}

void jsonPrintTable(
    const vector<string>& headers,
    const vector<vector<string>>& columns,
    ostream& out) {
  std::ostringstream stream;
  JsonWriter writer(&stream, JsonWriter::kCompact);

  int numColumns = columns.size();
  int numRows = columns.empty() ? 0 : columns[0].size();

  writer.startArray();
  for (int row = 0; row < numRows; row++) {
    writer.startObject();
    for (int col = 0; col < numColumns; col++) {
      writer.String(headers[col]);
      writer.String(columns[col][row]);
    }
    writer.endObject();
  }
  writer.endArray();

  out << stream.str() << endl;
}

void printTable(
    const vector<vector<string>>& columns,
    const string& separator,
    ostream& out) {
  int numColumns = columns.size();
  int numRows = columns.empty() ? 0 : columns[0].size();
  for (int row = 0; row < numRows; row++) {
    for (int col = 0; col < numColumns; col++) {
      out << columns[col][row];
      if (col != numColumns - 1) {
        out << separator;
      }
    }
    out << endl;
  }
}

} // anonymous namespace

DataTable::DataTable(std::vector<string> colNames)
    : column_names_(std::move(colNames)), columns_(column_names_.size()) {}

void DataTable::addRow(std::vector<string> row) {
  CHECK_EQ(row.size(), columns_.size());
  int i = 0;
  for (auto& v : row) {
    columns_[i++].emplace_back(std::move(v));
  }
}

void DataTable::addColumn(string name, vector<string> column) {
  if (!columns_.empty()) {
    CHECK_EQ(column.size(), columns_[0].size());
  }
  column_names_.emplace_back(std::move(name));
  columns_.emplace_back(std::move(column));
}

Status DataTable::printTo(ostream& out) const {
  if (boost::iequals(FLAGS_format, "pretty")) {
    prettyPrintTable(column_names_, columns_, out);
  } else if (boost::iequals(FLAGS_format, "space")) {
    printTable(columns_, " ", out);
  } else if (boost::iequals(FLAGS_format, "tsv")) {
    printTable(columns_, "	", out);
  } else if (boost::iequals(FLAGS_format, "csv")) {
    printTable(columns_, ",", out);
  } else if (boost::iequals(FLAGS_format, "json")) {
    jsonPrintTable(column_names_, columns_, out);
  } else {
    return Status::InvalidArgument("unknown format (--format)", FLAGS_format);
  }
  return Status::OK();
}

} // namespace kudu::tools
