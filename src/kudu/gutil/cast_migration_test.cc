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
inline Dest legacy_bit_cast(const Source& source) {
  KUDU_COMPILE_ASSERT(sizeof(Dest) == sizeof(Source), VerifySizesAreEqual);
  Dest dest;
  memcpy(&dest, &source, sizeof(dest));
  return dest;
}

// Legacy implicit_cast implementation (for comparison)
template <typename To>
inline To legacy_implicit_cast(To to) {
  return to;
}

class CastMigrationTest : public ::testing::Test {};

// Test bit_cast with float to uint32_t (from Hash32NumWithSeed)
TEST_F(CastMigrationTest, BitCastFloatToUint32) {
  const float test_values[] = {
      0.0f, 1.0f, -1.0f, 3.14159f, -2.718f, 1e10f, -1e10f, 1e-10f, -1e-10f};

  for (float val : test_values) {
    uint32_t std_result = std::bit_cast<uint32_t>(val);
    uint32_t legacy_result = legacy_bit_cast<uint32_t>(val);
    EXPECT_EQ(std_result, legacy_result)
        << "std::bit_cast and legacy bit_cast differ for float value: " << val;
  }
}

// Test bit_cast with double to uint64_t (from Hash64NumWithSeed and
// KeyFromDouble)
TEST_F(CastMigrationTest, BitCastDoubleToUint64) {
  const double test_values[] = {
      0.0,
      1.0,
      -1.0,
      3.141592653589793,
      -2.718281828459045,
      1e100,
      -1e100,
      1e-100,
      -1e-100};

  for (double val : test_values) {
    uint64_t std_result = std::bit_cast<uint64_t>(val);
    uint64_t legacy_result = legacy_bit_cast<uint64_t>(val);
    EXPECT_EQ(std_result, legacy_result)
        << "std::bit_cast and legacy bit_cast differ for double value: " << val;
  }
}

// Test bit_cast with uint64_t to double (from DoubleFromKey)
TEST_F(CastMigrationTest, BitCastUint64ToDouble) {
  const uint64_t test_values[] = {
      0ULL,
      1ULL,
      0xFFFFFFFFFFFFFFFFULL,
      0x3FF0000000000000ULL, // 1.0
      0x4000000000000000ULL, // 2.0
      0xBFF0000000000000ULL, // -1.0
  };

  for (uint64_t val : test_values) {
    double std_result = std::bit_cast<double>(val);
    double legacy_result = legacy_bit_cast<double>(val);
    // For bit-exact comparison of doubles, compare their bit patterns
    uint64_t std_bits = std::bit_cast<uint64_t>(std_result);
    uint64_t legacy_bits = std::bit_cast<uint64_t>(legacy_result);
    EXPECT_EQ(std_bits, legacy_bits)
        << "std::bit_cast and legacy bit_cast differ for uint64_t value: "
        << val;
  }
}

// Test bit_cast with uint32_t to float (reverse of float to uint32_t)
TEST_F(CastMigrationTest, BitCastUint32ToFloat) {
  const uint32_t test_values[] = {
      0U,
      1U,
      0xFFFFFFFFU,
      0x3F800000U, // 1.0f
      0x40000000U, // 2.0f
      0xBF800000U, // -1.0f
      0x7F800000U, // +infinity
      0xFF800000U, // -infinity
  };

  for (uint32_t val : test_values) {
    float std_result = std::bit_cast<float>(val);
    float legacy_result = legacy_bit_cast<float>(val);
    // For bit-exact comparison of floats, compare their bit patterns
    uint32_t std_bits = std::bit_cast<uint32_t>(std_result);
    uint32_t legacy_bits = std::bit_cast<uint32_t>(legacy_result);
    EXPECT_EQ(std_bits, legacy_bits)
        << "std::bit_cast and legacy bit_cast differ for uint32_t value: "
        << val;
  }
}

// Test bit_cast with uintptr_t (from InlineSlice)
TEST_F(CastMigrationTest, BitCastPointerToUintptr) {
  // Create actual pointers instead of casting from integers
  int dummy_vars[5] = {0, 1, 2, 3, 4};
  void* test_pointers[] = {
      nullptr, &dummy_vars[0], &dummy_vars[1], &dummy_vars[2], &dummy_vars[3]};

  for (void* ptr : test_pointers) {
    if (ptr != nullptr) {
      uintptr_t std_result = std::bit_cast<uintptr_t>(ptr);
      uintptr_t legacy_result = legacy_bit_cast<uintptr_t>(ptr);
      EXPECT_EQ(std_result, legacy_result)
          << "std::bit_cast and legacy bit_cast differ for pointer value";
    }
  }
}

// Test implicit_cast with uint64_t (from Random::Next64)
TEST_F(CastMigrationTest, ImplicitCastUint32ToUint64) {
  const uint32_t test_values[] = {
      0U, 1U, 0xFFFFFFFFU, 0x12345678U, 0x80000000U};

  for (uint32_t val : test_values) {
    uint64_t std_result = static_cast<uint64_t>(val);
    uint64_t legacy_result = legacy_implicit_cast<uint64_t>(val);
    EXPECT_EQ(std_result, legacy_result)
        << "static_cast and legacy implicit_cast differ for uint32_t value: "
        << val;
  }
}

// Test implicit_cast with double to int64_t (from GetSpinLockContentionMicros)
TEST_F(CastMigrationTest, ImplicitCastDoubleToInt64) {
  const double test_values[] = {0.0, 1.0, 100.5, 1e6, 1e9, -100.5};

  for (double val : test_values) {
    int64_t std_result = static_cast<int64_t>(val);
    int64_t legacy_result = legacy_implicit_cast<int64_t>(val);
    EXPECT_EQ(std_result, legacy_result)
        << "static_cast and legacy implicit_cast differ for double value: "
        << val;
  }
}

// Comprehensive test: verify hash function behavior is preserved
TEST_F(CastMigrationTest, HashFunctionConsistency) {
  // Simulate Hash32NumWithSeed behavior
  const float test_float = 3.14159f;
  const uint32_t seed = 12345;
  const uint64_t kMul = 0xc6a4a7935bd1e995ULL;

  // Using std::bit_cast
  uint64_t a_std = (std::bit_cast<uint32_t>(test_float) + seed) * kMul;
  a_std ^= (a_std >> 47);
  a_std *= kMul;

  // Using legacy bit_cast
  uint64_t a_legacy = (legacy_bit_cast<uint32_t>(test_float) + seed) * kMul;
  a_legacy ^= (a_legacy >> 47);
  a_legacy *= kMul;

  EXPECT_EQ(a_std, a_legacy)
      << "Hash computation differs between std::bit_cast and legacy bit_cast";
}

// Test edge cases for bit_cast
TEST_F(CastMigrationTest, BitCastEdgeCases) {
  // NaN values
  const float nan_f = std::numeric_limits<float>::quiet_NaN();
  EXPECT_EQ(std::bit_cast<uint32_t>(nan_f), legacy_bit_cast<uint32_t>(nan_f));

  const double nan_d = std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(std::bit_cast<uint64_t>(nan_d), legacy_bit_cast<uint64_t>(nan_d));

  // Infinity values
  const float inf_f = std::numeric_limits<float>::infinity();
  EXPECT_EQ(std::bit_cast<uint32_t>(inf_f), legacy_bit_cast<uint32_t>(inf_f));

  const double inf_d = std::numeric_limits<double>::infinity();
  EXPECT_EQ(std::bit_cast<uint64_t>(inf_d), legacy_bit_cast<uint64_t>(inf_d));

  // Zero values (positive and negative)
  const float zero_f = 0.0f;
  const float neg_zero_f = -0.0f;
  EXPECT_EQ(std::bit_cast<uint32_t>(zero_f), legacy_bit_cast<uint32_t>(zero_f));
  EXPECT_EQ(
      std::bit_cast<uint32_t>(neg_zero_f),
      legacy_bit_cast<uint32_t>(neg_zero_f));
}

} // namespace kudu
