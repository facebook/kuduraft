# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# **************   NOTICE  *******************************************
# Facebook 2019 - Notice of Changes
# This file has been modified to extract only the Raft implementation
# out of Kudu into a fork known as kuduraft.
# ********************************************************************

PROTOBUF_GENERATE_CPP(
  COMMON_PROTO_SRCS COMMON_PROTO_HDRS COMMON_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES common.proto)
ADD_EXPORTABLE_LIBRARY(kudu_common_proto
  SRCS ${COMMON_PROTO_SRCS}
  DEPS pb_util_proto protobuf util_compression_proto
  NONLINK_DEPS ${COMMON_PROTO_TGTS})

PROTOBUF_GENERATE_CPP(
  WIRE_PROTOCOL_PROTO_SRCS WIRE_PROTOCOL_PROTO_HDRS WIRE_PROTOCOL_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES wire_protocol.proto)
set(WIRE_PROTOCOL_PROTO_LIBS
  kudu_common_proto
  protobuf)
ADD_EXPORTABLE_LIBRARY(wire_protocol_proto
  SRCS ${WIRE_PROTOCOL_PROTO_SRCS}
  DEPS ${WIRE_PROTOCOL_PROTO_LIBS}
  NONLINK_DEPS ${WIRE_PROTOCOL_PROTO_TGTS})

set(COMMON_SRCS
   timestamp.cc
   wire_protocol.cc)

# Workaround for clang bug https://llvm.org/bugs/show_bug.cgi?id=23757
# in which it incorrectly optimizes key_util.cc and causes incorrect results.
if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_source_files_properties(key_util.cc PROPERTIES COMPILE_FLAGS -fwrapv)
endif()

set(COMMON_LIBS
  kudu_common_proto
  wire_protocol_proto
  kudu_util
  gutil)

ADD_EXPORTABLE_LIBRARY(kudu_common
  SRCS ${COMMON_SRCS}
  DEPS ${COMMON_LIBS})

SET_KUDU_TEST_LINK_LIBS(kudu_common)
