// Copyright 2012 Google Inc.
//
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
//
// All rights reserved.
//
// Additional constants from logging.h and its dependencies which are
// not exported by glog.

#pragma once

// DFATAL is FATAL in debug mode, ERROR in normal mode
#ifdef NDEBUG
#ifdef DFATAL_LEVEL
#undef DFATAL_LEVEL
#endif
#define DFATAL_LEVEL ERROR
#else
#ifdef DFATAL_LEVEL
#undef DFATAL_LEVEL
#endif
#define DFATAL_LEVEL FATAL
#endif
