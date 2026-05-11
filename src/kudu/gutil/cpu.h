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
  const std::string& vendorName() const {
    return cpuVendor_;
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
  int extendedModel() const {
    return extModel_;
  }
  int extendedFamily() const {
    return extFamily_;
  }
  bool hasMmx() const {
    return hasMmx_;
  }
  bool hasSse() const {
    return hasSse_;
  }
  bool hasSse2() const {
    return hasSse2_;
  }
  bool hasSse3() const {
    return hasSse3_;
  }
  bool hasPclmulqdq() const {
    return hasPclmulqdq_;
  }
  bool hasSsse3() const {
    return hasSsse3_;
  }
  bool hasSse41() const {
    return hasSse41_;
  }
  bool hasSse42() const {
    return hasSse42_;
  }
  bool hasPopcnt() const {
    return hasPopcnt_;
  }
  bool hasAvx() const {
    return hasAvx_;
  }
  bool hasAvx2() const {
    return hasAvx2_;
  }
  bool hasAesni() const {
    return hasAesni_;
  }
  bool hasBmi() const {
    return hasBmi_;
  }
  bool hasBmi2() const {
    return hasBmi2_;
  }
  bool hasNonStopTimeStampCounter() const {
    return hasNonStopTimeStampCounter_;
  }
  // hasBrokenNeon is only valid on ARM chips. If true, it indicates that we
  // believe that the NEON unit on the current CPU is flawed and cannot execute
  // some code. See https://code.google.com/p/chromium/issues/detail?id=341598
  bool hasBrokenNeon() const {
    return hasBrokenNeon_;
  }

  IntelMicroArchitecture getIntelMicroArchitecture() const;
  const std::string& cpuBrand() const {
    return cpuBrand_;
  }

 private:
  // Query the processor for CPUID information.
  void initialize();

  int signature_; // raw form of type, family, model, and stepping
  int type_; // process type
  int family_; // family of the processor
  int model_; // model of processor
  int stepping_; // processor revision number
  int extModel_;
  int extFamily_;
  bool hasMmx_;
  bool hasSse_;
  bool hasSse2_;
  bool hasSse3_;
  bool hasPclmulqdq_;
  bool hasSsse3_;
  bool hasSse41_;
  bool hasSse42_;
  bool hasPopcnt_;
  bool hasAvx_;
  bool hasAvx2_;
  bool hasAesni_;
  bool hasBmi_;
  bool hasBmi2_;
  bool hasNonStopTimeStampCounter_;
  bool hasBrokenNeon_;
  std::string cpuVendor_;
  std::string cpuBrand_;
};

} // namespace base
