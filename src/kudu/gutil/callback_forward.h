// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

namespace kudu {

template <typename Sig>
class Callback;

using Closure = Callback<void(void)>;

} // namespace kudu
