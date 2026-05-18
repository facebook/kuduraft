// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/gutil/cpu.h"

#ifndef __aarch64__
#include <cstdint>
#include <cstring>
#include <utility>
#endif //__aarch64__

namespace base {

Cpu::Cpu()
    : signature_(0),
      type_(0),
      family_(0),
      model_(0),
      stepping_(0),
      extModel_(0),
      extFamily_(0),
      hasMmx_(false),
      hasSse_(false),
      hasSse2_(false),
      hasSse3_(false),
      hasPclmulqdq_(false),
      hasSsse3_(false),
      hasSse41_(false),
      hasSse42_(false),
      hasPopcnt_(false),
      hasAvx_(false),
      hasAvx2_(false),
      hasAesni_(false),
      hasBmi_(false),
      hasBmi2_(false),
      hasNonStopTimeStampCounter_(false),
      hasBrokenNeon_(false),
      cpuVendor_("unknown") {
  initialize();
}

namespace {

#if defined(__x86_64__)

#if defined(__pic__) && defined(__i386__)

void __cpuid(int cpuInfo[4], int infoType) {
  __asm__ volatile(
      "mov %%ebx, %%edi\n"
      "cpuid\n"
      "xchg %%edi, %%ebx\n"
      : "=a"(cpuInfo[0]), "=D"(cpuInfo[1]), "=c"(cpuInfo[2]), "=d"(cpuInfo[3])
      : "a"(infoType));
}

#else

void __cpuid(int cpuInfo[4], int infoType) {
  __asm__ volatile(
      "cpuid\n"
      : "=a"(cpuInfo[0]), "=b"(cpuInfo[1]), "=c"(cpuInfo[2]), "=d"(cpuInfo[3])
      : "a"(infoType), "c"(0));
}

#endif

// _xgetbv returns the value of an Intel Extended Control Register (XCR).
// Currently only XCR0 is defined by Intel so |xcr| should always be zero.
uint64_t _xgetbv(uint32_t xcr) {
  uint32_t eax, edx;

  __asm__ volatile("xgetbv" : "=a"(eax), "=d"(edx) : "c"(xcr));
  return (static_cast<uint64_t>(edx) << 32) | eax;
}

#endif // __x86_64__

#if defined(ARCH_CPU_ARM_FAMILY) && (defined(OS_ANDROID) || defined(OS_LINUX))
class LazyCpuInfoValue {
 public:
  LazyCpuInfoValue() : hasBrokenNeon_(false) {
    // This function finds the value from /proc/cpuinfo under the key "model
    // name" or "Processor". "model name" is used in Linux 3.8 and later (3.7
    // and later for arm64) and is shown once per CPU. "Processor" is used in
    // earler versions and is shown only once at the top of /proc/cpuinfo
    // regardless of the number CPUs.
    const char kModelNamePrefix[] = "model name\t: ";
    const char kProcessorPrefix[] = "Processor\t: ";

    // This function also calculates whether we believe that this CPU has a
    // broken NEON unit based on these fields from cpuinfo:
    unsigned implementer = 0, architecture = 0, variant = 0, part = 0,
             revision = 0;
    const struct {
      const char key[17];
      unsigned int* result;
    } kUnsignedValues[] = {
        {"CPU implementer", &implementer},
        {"CPU architecture", &architecture},
        {"CPU variant", &variant},
        {"CPU part", &part},
        {"CPU revision", &revision},
    };

    std::string contents;
    ReadFileToString(FilePath("/proc/cpuinfo"), &contents);
    DCHECK(!contents.empty());
    if (contents.empty()) {
      return;
    }

    std::istringstream iss(contents);
    std::string line;
    while (std::getline(iss, line)) {
      if (brand_.empty() &&
          (line.compare(0, strlen(kModelNamePrefix), kModelNamePrefix) == 0 ||
           line.compare(0, strlen(kProcessorPrefix), kProcessorPrefix) == 0)) {
        brand_.assign(line.substr(strlen(kModelNamePrefix)));
      }

      for (size_t i = 0; i < arraysize(kUnsignedValues); i++) {
        const char* key = kUnsignedValues[i].key;
        const size_t len = strlen(key);

        if (line.compare(0, len, key) == 0 && line.size() >= len + 1 &&
            (line[len] == '\t' || line[len] == ' ' || line[len] == ':')) {
          size_t colonPos = line.find(':', len);
          if (colonPos == std::string::npos) {
            continue;
          }

          const StringPiece lineSp(line);
          StringPiece valueSp = lineSp.substr(colonPos + 1);
          while (!valueSp.empty() &&
                 (valueSp[0] == ' ' || valueSp[0] == '\t')) {
            valueSp = valueSp.substr(1);
          }

          // The string may have leading "0x" or not, so we use strtoul to
          // handle that.
          char* endptr;
          std::string value(valueSp.asString());
          unsigned long int result = strtoul(value.c_str(), &endptr, 0);
          if (*endptr == 0 && result <= UINT_MAX) {
            *kUnsignedValues[i].result = result;
          }
        }
      }
    }

    hasBrokenNeon_ = implementer == 0x51 && architecture == 7 && variant == 1 &&
        part == 0x4d && revision == 0;
  }

  const std::string& brand() const {
    return brand_;
  }
  bool hasBrokenNeon() const {
    return hasBrokenNeon_;
  }

 private:
  std::string brand_;
  bool hasBrokenNeon_;
  DISALLOW_COPY_AND_ASSIGN(LazyCpuInfoValue);
};

base::LazyInstance<LazyCpuInfoValue>::Leaky gLazyCpuinfo =
    LAZY_INSTANCE_INITIALIZER;

#endif // defined(ARCH_CPU_ARM_FAMILY) && (defined(OS_ANDROID) ||
       // defined(OS_LINUX))

} // anonymous namespace

void Cpu::initialize() {
#if defined(__x86_64__)
  int cpuInfo[4] = {-1};
  char cpuString[48];

  // __cpuid with an InfoType argument of 0 returns the number of
  // valid Ids in CPUInfo[0] and the CPU identification string in
  // the other three array elements. The CPU identification string is
  // not in linear order. The code below arranges the information
  // in a human readable form. The human readable order is CPUInfo[1] |
  // CPUInfo[3] | CPUInfo[2]. CPUInfo[2] and CPUInfo[3] are swapped
  // before using memcpy to copy these three array elements to cpuString.
  __cpuid(cpuInfo, 0);
  int numIds = cpuInfo[0];
  std::swap(cpuInfo[2], cpuInfo[3]);
  memcpy(cpuString, &cpuInfo[1], 3 * sizeof(cpuInfo[1]));
  cpuVendor_.assign(cpuString, 3 * sizeof(cpuInfo[1]));

  // Interpret CPU feature information.
  if (numIds > 0) {
    int cpuInfo7[4] = {0};
    __cpuid(cpuInfo, 1);
    if (numIds >= 7) {
      __cpuid(cpuInfo7, 7);
    }
    signature_ = cpuInfo[0];
    stepping_ = cpuInfo[0] & 0xf;
    model_ = ((cpuInfo[0] >> 4) & 0xf) + ((cpuInfo[0] >> 12) & 0xf0);
    family_ = (cpuInfo[0] >> 8) & 0xf;
    type_ = (cpuInfo[0] >> 12) & 0x3;
    extModel_ = (cpuInfo[0] >> 16) & 0xf;
    extFamily_ = (cpuInfo[0] >> 20) & 0xff;
    hasMmx_ = (cpuInfo[3] & 0x00800000) != 0;
    hasSse_ = (cpuInfo[3] & 0x02000000) != 0;
    hasSse2_ = (cpuInfo[3] & 0x04000000) != 0;
    hasSse3_ = (cpuInfo[2] & 0x00000001) != 0;
    hasPclmulqdq_ = (cpuInfo[2] & 0x00000002) != 0;
    hasSsse3_ = (cpuInfo[2] & 0x00000200) != 0;
    hasSse41_ = (cpuInfo[2] & 0x00080000) != 0;
    hasSse42_ = (cpuInfo[2] & 0x00100000) != 0;
    hasPopcnt_ = (cpuInfo[2] & 0x00800000) != 0;
    // AVX instructions will generate an illegal instruction exception unless
    //   a) they are supported by the CPU,
    //   b) XSAVE is supported by the CPU and
    //   c) XSAVE is enabled by the kernel.
    // See http://software.intel.com/en-us/blogs/2011/04/14/is-avx-enabled
    //
    // In addition, we have observed some crashes with the xgetbv instruction
    // even after following Intel's example code. (See crbug.com/375968.)
    // Because of that, we also test the XSAVE bit because its description in
    // the CPUID documentation suggests that it signals xgetbv support.
    hasAvx_ = (cpuInfo[2] & 0x10000000) != 0 &&
        (cpuInfo[2] & 0x04000000) != 0 /* XSAVE */ &&
        (cpuInfo[2] & 0x08000000) != 0 /* OSXSAVE */ &&
        (_xgetbv(0) & 6) == 6 /* XSAVE enabled by kernel */;
    hasAesni_ = (cpuInfo[2] & 0x02000000) != 0;
    hasAvx2_ = hasAvx_ && (cpuInfo7[1] & 0x00000020) != 0;
    hasBmi_ = cpuInfo7[1] & (1 << 3);
    hasBmi2_ = cpuInfo7[1] & (1 << 8);
  }

  // Get the brand string of the cpu.
  __cpuid(cpuInfo, 0x80000000);
  const int kParameterEnd = 0x80000004;
  int maxParameter = cpuInfo[0];

  if (cpuInfo[0] >= kParameterEnd) {
    char* cpuStringPtr = cpuString;

    for (int parameter = 0x80000002; parameter <= kParameterEnd &&
         cpuStringPtr < &cpuString[sizeof(cpuString)];
         parameter++) {
      __cpuid(cpuInfo, parameter);
      memcpy(cpuStringPtr, cpuInfo, sizeof(cpuInfo));
      cpuStringPtr += sizeof(cpuInfo);
    }
    cpuBrand_.assign(cpuString, cpuStringPtr - cpuString);
  }

  const int kParameterContainingNonStopTimeStampCounter = 0x80000007;
  if (maxParameter >= kParameterContainingNonStopTimeStampCounter) {
    __cpuid(cpuInfo, kParameterContainingNonStopTimeStampCounter);
    hasNonStopTimeStampCounter_ = (cpuInfo[3] & (1 << 8)) != 0;
  }
#elif defined(ARCH_CPU_ARM_FAMILY) && (defined(OS_ANDROID) || defined(OS_LINUX))
  cpuBrand_.assign(gLazyCpuinfo.Get().brand());
  hasBrokenNeon_ = gLazyCpuinfo.Get().hasBrokenNeon();
#elif defined(__aarch64__)
  cpuBrand_.assign("ARM64");
  hasBrokenNeon_ = false;
#else
#error unknown architecture
#endif
}

Cpu::IntelMicroArchitecture Cpu::getIntelMicroArchitecture() const {
  if (hasAvx2()) {
    return kAvx2;
  }
  if (hasAvx()) {
    return kAvx;
  }
  if (hasSse42()) {
    return kSse42;
  }
  if (hasSse41()) {
    return kSse41;
  }
  if (hasSsse3()) {
    return kSsse3;
  }
  if (hasSse3()) {
    return kSse3;
  }
  if (hasSse2()) {
    return kSse2;
  }
  if (hasSse()) {
    return kSse;
  }
  return kPentium;
}

} // namespace base
