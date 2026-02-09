// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <string>

namespace base {

// Query information about the processor.
class Cpu {
 public:
  // Constructor
  Cpu();

  enum IntelMicroArchitecture {
    kPentium,
    kSse,
    kSse2,
    kSse3,
    kSsse3,
    kSse41,
    kSse42,
    kAvx,
    kAvx2,
    kMaxIntelMicroArchitecture
  };

  // Accessors for CPU information.
  const std::string& vendor_name() const {
    return cpu_vendor_;
  }
  int signature() const {
    return signature_;
  }
  int stepping() const {
    return stepping_;
  }
  int model() const {
    return model_;
  }
  int family() const {
    return family_;
  }
  int type() const {
    return type_;
  }
  int extended_model() const {
    return ext_model_;
  }
  int extended_family() const {
    return ext_family_;
  }
  bool has_mmx() const {
    return has_mmx_;
  }
  bool has_sse() const {
    return has_sse_;
  }
  bool has_sse2() const {
    return has_sse2_;
  }
  bool has_sse3() const {
    return has_sse3_;
  }
  bool has_pclmulqdq() const {
    return has_pclmulqdq_;
  }
  bool has_ssse3() const {
    return has_ssse3_;
  }
  bool has_sse41() const {
    return has_sse41_;
  }
  bool has_sse42() const {
    return has_sse42_;
  }
  bool has_popcnt() const {
    return has_popcnt_;
  }
  bool has_avx() const {
    return has_avx_;
  }
  bool has_avx2() const {
    return has_avx2_;
  }
  bool has_aesni() const {
    return has_aesni_;
  }
  bool has_bmi() const {
    return has_bmi_;
  }
  bool has_bmi2() const {
    return has_bmi2_;
  }
  bool has_non_stop_time_stamp_counter() const {
    return has_non_stop_time_stamp_counter_;
  }
  // has_broken_neon is only valid on ARM chips. If true, it indicates that we
  // believe that the NEON unit on the current CPU is flawed and cannot execute
  // some code. See https://code.google.com/p/chromium/issues/detail?id=341598
  bool has_broken_neon() const {
    return has_broken_neon_;
  }

  IntelMicroArchitecture getIntelMicroArchitecture() const;
  const std::string& cpu_brand() const {
    return cpu_brand_;
  }

 private:
  // Query the processor for CPUID information.
  void initialize();

  int signature_; // raw form of type, family, model, and stepping
  int type_; // process type
  int family_; // family of the processor
  int model_; // model of processor
  int stepping_; // processor revision number
  int ext_model_;
  int ext_family_;
  bool has_mmx_;
  bool has_sse_;
  bool has_sse2_;
  bool has_sse3_;
  bool has_pclmulqdq_;
  bool has_ssse3_;
  bool has_sse41_;
  bool has_sse42_;
  bool has_popcnt_;
  bool has_avx_;
  bool has_avx2_;
  bool has_aesni_;
  bool has_bmi_;
  bool has_bmi2_;
  bool has_non_stop_time_stamp_counter_;
  bool has_broken_neon_;
  std::string cpu_vendor_;
  std::string cpu_brand_;
};

} // namespace base
