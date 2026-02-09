// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.
//
// Test to verify that std::bit_cast and static_cast produce the same results
// as the legacy custom bit_cast and implicit_cast implementations.

#include <gtest/gtest.h>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>

#include "kudu/gutil/macros.h"

namespace kudu {

// Legacy bit_cast implementation (for comparison)
template <class Dest, class Source>
inline Dest legacyBitCast(const Source& source) {
  KUDU_COMPILE_ASSERT(sizeof(Dest) == sizeof(Source), VerifySizesAreEqual);
  Dest dest;
  memcpy(&dest, &source, sizeof(dest));
  return dest;
}

// Legacy implicit_cast implementation (for comparison)
template <typename To>
inline To legacyImplicitCast(To to) {
  return to;
}

class CastMigrationTest : public ::testing::Test {};

// Test bit_cast with float to uint32_t (from hash32NumWithSeed)
TEST_F(CastMigrationTest, BitCastFloatToUint32) {
  const float testValues[] = {
      0.0f, 1.0f, -1.0f, 3.14159f, -2.718f, 1e10f, -1e10f, 1e-10f, -1e-10f};

  for (float val : testValues) {
    uint32_t stdResult = std::bit_cast<uint32_t>(val);
    uint32_t legacyResult = legacyBitCast<uint32_t>(val);
    EXPECT_EQ(stdResult, legacyResult)
        << "std::bit_cast and legacy bit_cast differ for float value: " << val;
  }
}

// Test bit_cast with double to uint64_t (from hash64NumWithSeed and
// KeyFromDouble)
TEST_F(CastMigrationTest, BitCastDoubleToUint64) {
  const double testValues[] = {
      0.0,
      1.0,
      -1.0,
      3.141592653589793,
      -2.718281828459045,
      1e100,
      -1e100,
      1e-100,
      -1e-100};

  for (double val : testValues) {
    uint64_t stdResult = std::bit_cast<uint64_t>(val);
    uint64_t legacyResult = legacyBitCast<uint64_t>(val);
    EXPECT_EQ(stdResult, legacyResult)
        << "std::bit_cast and legacy bit_cast differ for double value: " << val;
  }
}

// Test bit_cast with uint64_t to double (from DoubleFromKey)
TEST_F(CastMigrationTest, BitCastUint64ToDouble) {
  const uint64_t testValues[] = {
      0ULL,
      1ULL,
      0xFFFFFFFFFFFFFFFFULL,
      0x3FF0000000000000ULL, // 1.0
      0x4000000000000000ULL, // 2.0
      0xBFF0000000000000ULL, // -1.0
  };

  for (uint64_t val : testValues) {
    double stdResult = std::bit_cast<double>(val);
    double legacyResult = legacyBitCast<double>(val);
    // For bit-exact comparison of doubles, compare their bit patterns
    uint64_t stdBits = std::bit_cast<uint64_t>(stdResult);
    uint64_t legacyBits = std::bit_cast<uint64_t>(legacyResult);
    EXPECT_EQ(stdBits, legacyBits)
        << "std::bit_cast and legacy bit_cast differ for uint64_t value: "
        << val;
  }
}

// Test bit_cast with uint32_t to float (reverse of float to uint32_t)
TEST_F(CastMigrationTest, BitCastUint32ToFloat) {
  const uint32_t testValues[] = {
      0U,
      1U,
      0xFFFFFFFFU,
      0x3F800000U, // 1.0f
      0x40000000U, // 2.0f
      0xBF800000U, // -1.0f
      0x7F800000U, // +infinity
      0xFF800000U, // -infinity
  };

  for (uint32_t val : testValues) {
    float stdResult = std::bit_cast<float>(val);
    float legacyResult = legacyBitCast<float>(val);
    // For bit-exact comparison of floats, compare their bit patterns
    uint32_t stdBits = std::bit_cast<uint32_t>(stdResult);
    uint32_t legacyBits = std::bit_cast<uint32_t>(legacyResult);
    EXPECT_EQ(stdBits, legacyBits)
        << "std::bit_cast and legacy bit_cast differ for uint32_t value: "
        << val;
  }
}

// Test bit_cast with uintptr_t (from InlineSlice)
TEST_F(CastMigrationTest, BitCastPointerToUintptr) {
  // Create actual pointers instead of casting from integers
  int dummyVars[5] = {0, 1, 2, 3, 4};
  void* testPointers[] = {
      nullptr, &dummyVars[0], &dummyVars[1], &dummyVars[2], &dummyVars[3]};

  for (void* ptr : testPointers) {
    if (ptr != nullptr) {
      uintptr_t stdResult = std::bit_cast<uintptr_t>(ptr);
      uintptr_t legacyResult = legacyBitCast<uintptr_t>(ptr);
      EXPECT_EQ(stdResult, legacyResult)
          << "std::bit_cast and legacy bit_cast differ for pointer value";
    }
  }
}

// Test implicit_cast with uint64_t (from Random::Next64)
TEST_F(CastMigrationTest, ImplicitCastUint32ToUint64) {
  const uint32_t testValues[] = {0U, 1U, 0xFFFFFFFFU, 0x12345678U, 0x80000000U};

  for (uint32_t val : testValues) {
    uint64_t stdResult = static_cast<uint64_t>(val);
    uint64_t legacyResult = legacyImplicitCast<uint64_t>(val);
    EXPECT_EQ(stdResult, legacyResult)
        << "static_cast and legacy implicit_cast differ for uint32_t value: "
        << val;
  }
}

// Test implicit_cast with double to int64_t (from GetSpinLockContentionMicros)
TEST_F(CastMigrationTest, ImplicitCastDoubleToInt64) {
  const double testValues[] = {0.0, 1.0, 100.5, 1e6, 1e9, -100.5};

  for (double val : testValues) {
    int64_t stdResult = static_cast<int64_t>(val);
    int64_t legacyResult = legacyImplicitCast<int64_t>(val);
    EXPECT_EQ(stdResult, legacyResult)
        << "static_cast and legacy implicit_cast differ for double value: "
        << val;
  }
}

// Comprehensive test: verify hash function behavior is preserved
TEST_F(CastMigrationTest, HashFunctionConsistency) {
  // Simulate hash32NumWithSeed behavior
  const float testFloat = 3.14159f;
  const uint32_t seed = 12345;
  const uint64_t kMul = 0xc6a4a7935bd1e995ULL;

  // Using std::bit_cast
  uint64_t aStd = (std::bit_cast<uint32_t>(testFloat) + seed) * kMul;
  aStd ^= (aStd >> 47);
  aStd *= kMul;

  // Using legacy bit_cast
  uint64_t aLegacy = (legacyBitCast<uint32_t>(testFloat) + seed) * kMul;
  aLegacy ^= (aLegacy >> 47);
  aLegacy *= kMul;

  EXPECT_EQ(aStd, aLegacy)
      << "Hash computation differs between std::bit_cast and legacy bit_cast";
}

// Test edge cases for bit_cast
TEST_F(CastMigrationTest, BitCastEdgeCases) {
  // NaN values
  const float nanF = std::numeric_limits<float>::quiet_NaN();
  EXPECT_EQ(std::bit_cast<uint32_t>(nanF), legacyBitCast<uint32_t>(nanF));

  const double nanD = std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(std::bit_cast<uint64_t>(nanD), legacyBitCast<uint64_t>(nanD));

  // Infinity values
  const float infF = std::numeric_limits<float>::infinity();
  EXPECT_EQ(std::bit_cast<uint32_t>(infF), legacyBitCast<uint32_t>(infF));

  const double infD = std::numeric_limits<double>::infinity();
  EXPECT_EQ(std::bit_cast<uint64_t>(infD), legacyBitCast<uint64_t>(infD));

  // Zero values (positive and negative)
  const float zeroF = 0.0f;
  const float negZeroF = -0.0f;
  EXPECT_EQ(std::bit_cast<uint32_t>(zeroF), legacyBitCast<uint32_t>(zeroF));
  EXPECT_EQ(
      std::bit_cast<uint32_t>(negZeroF), legacyBitCast<uint32_t>(negZeroF));
}

} // namespace kudu
