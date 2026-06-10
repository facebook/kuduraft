//
// Copyright (C) 1999 and onwards Google, Inc.
//
//
// These are weird things we need to do to get this compiling on
// random systems (and on SWIG).

#pragma once

#include <limits.h> // So we can set the bounds of our types
#include <stdlib.h> // for free()
#include <string.h> // for memcpy()

#include <type_traits>

#include <cstdint>

// _BIG_ENDIAN
#include <endian.h>

// The uint mess:
// mysql.h sets _GNU_SOURCE which sets __USE_MISC in <features.h>
// sys/types.h typedefs uint if __USE_MISC
// mysql typedefs uint if HAVE_UINT not set
// The following typedef is carefully considered, and should not cause
//  any clashes
#if !defined(__USE_MISC)
#if !defined(HAVE_UINT)
#define HAVE_UINT 1
typedef unsigned int uint;
#endif
#if !defined(HAVE_USHORT)
#define HAVE_USHORT 1
typedef unsigned short ushort;
#endif
#if !defined(HAVE_ULONG)
#define HAVE_ULONG 1
typedef unsigned long ulong;
#endif
#endif

#if defined(__cplusplus)
#include <cstddef> // For _GLIBCXX macros
#endif

#if !defined(HAVE_TLS) && defined(_GLIBCXX_HAVE_TLS) && defined(__x86_64__)
#define HAVE_TLS 1
#endif

// The following guarenty declaration of the byte swap functions
#include <byteswap.h> // IWYU pragma: export

// define the macros IS_LITTLE_ENDIAN or IS_BIG_ENDIAN
// using the above endian defintions from endian.h
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
#define IS_BIG_ENDIAN
#endif

// Define the OS's path separator
#ifdef __cplusplus // C won't merge duplicate const variables at link time
// Some headers provide a macro for this (GCC's system.h), remove it so that we
// can use our own.
#undef PATH_SEPARATOR
const char PATH_SEPARATOR = '/';
#endif

// Windows has O_BINARY as a flag to open() (like "b" for fopen).
// Linux doesn't need make this distinction.
#ifndef O_BINARY
#define O_BINARY 0
#endif

// Klocwork static analysis tool's C/C++ complier kwcc
#if defined(__KLOCWORK__)
#define STATIC_ANALYSIS
#endif // __KLOCWORK__

// Annotate a function indicating the caller must examine the return value.
// Use like:
//   int foo() WARN_UNUSED_RESULT;
// To explicitly ignore a result, see |ignoreResult()| in <base/basictypes.h>.
#if defined(__GNUC__)
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define WARN_UNUSED_RESULT
#endif

// GCC-specific features

#if defined(__GNUC__) && !defined(SWIG)

//
// Tell the compiler to do printf format string checking if the
// compiler supports it; see the 'format' attribute in
// <http://gcc.gnu.org/onlinedocs/gcc-4.3.0/gcc/Function-Attributes.html>.
//
// N.B.: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
#define PRINTF_ATTRIBUTE(string_index, first_to_check) \
  __attribute__((__format__(__printf__, string_index, first_to_check)))
//
// Prevent the compiler from padding a structure to natural alignment
//
#define PACKED __attribute__((packed))

// Cache line alignment
#if defined(__i386__) || defined(__x86_64__)
#define CACHELINE_SIZE 64
#elif defined(__powerpc64__)
// TODO(user) This is the L1 D-cache line size of our Power7 machines.
// Need to check if this is appropriate for other PowerPC64 systems.
#define CACHELINE_SIZE 128
#elif defined(__aarch64__)
#define CACHELINE_SIZE 64
#elif defined(__arm__)
// Cache line sizes for ARM: These values are not strictly correct since
// cache line sizes depend on implementations, not architectures.  There
// are even implementations with cache line sizes configurable at boot
// time.
#if defined(__ARM_ARCH_5T__)
#define CACHELINE_SIZE 32
#elif defined(__ARM_ARCH_7A__)
#define CACHELINE_SIZE 64
#endif
#endif

// This is a NOP if CACHELINE_SIZE is not defined.
#ifdef CACHELINE_SIZE
#define CACHELINE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#else
#define CACHELINE_ALIGNED
#endif

//
// Prevent the compiler from complaining about or optimizing away variables
// that appear unused
// (careful, others e.g. third_party/libxml/xmlversion.h also define this)
#undef ATTRIBUTE_UNUSED
#define ATTRIBUTE_UNUSED __attribute__((unused))

//
// For functions we want to force inline or not inline.
// Introduced in gcc 3.1.
#define ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))

// For weak functions
#undef ATTRIBUTE_WEAK
#define ATTRIBUTE_WEAK __attribute__((weak))
#define HAVE_ATTRIBUTE_WEAK 1

// For deprecated functions or variables, generate a warning at usage sites.
// Verified to work as early as GCC 3.1.1 and clang 3.2 (so we'll assume any
// clang is new enough).
#if defined(__clang__) ||     \
    (defined(COMPILER_GCC) && \
     (__GNUC__ * 10000 + __GNUC_MINOR__ * 100) >= 30200)
#define ATTRIBUTE_DEPRECATED(msg) __attribute__((deprecated(msg)))
#else
#define ATTRIBUTE_DEPRECATED(msg)
#endif

//
// Tell the compiler that a given function never returns
//
#define ATTRIBUTE_NORETURN __attribute__((noreturn))

// Tell ThreadSanitizer to ignore a given function. This can dramatically reduce
// the running time and memory requirements for racy code when TSAN is active.
// GCC does not support this attribute at the time of this writing (GCC 4.8).
#if defined(__llvm__)
#define ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread))
#else
#define ATTRIBUTE_NO_SANITIZE_THREAD
#endif

// Tell UBSAN to ignore a given function completely. There is no
// __has_feature(undefined_sanitizer) or equivalent, so ASAN support is used as
// a proxy.
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define ATTRIBUTE_NO_SANITIZE_UNDEFINED \
  __attribute__((no_sanitize("undefined")))
#endif
#endif
#ifndef ATTRIBUTE_NO_SANITIZE_UNDEFINED
#define ATTRIBUTE_NO_SANITIZE_UNDEFINED
#endif

// Tell UBSAN to ignore integer overflows in a given function. There is no
// __has_feature(undefined_sanitizer) or equivalent, so ASAN support is used as
// a proxy.
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define ATTRIBUTE_NO_SANITIZE_INTEGER __attribute__((no_sanitize("integer")))
#endif
#endif
#ifndef ATTRIBUTE_NO_SANITIZE_INTEGER
#define ATTRIBUTE_NO_SANITIZE_INTEGER
#endif

// Detect ThreadSanitizer using standard compiler macros.
// This provides compatibility with both:
// - Upstream Kudu's explicit -DTHREAD_SANITIZER flag (from CMake)
// - Buck/compiler's automatic __SANITIZE_THREAD__ / __has_feature detection
#ifndef KUDU_SANITIZE_THREAD
#if defined(THREAD_SANITIZER) || defined(__SANITIZE_THREAD__) || \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
#define KUDU_SANITIZE_THREAD 1
#endif
#endif

//
// Tell the compiler to warn about unused return values for functions declared
// with this macro.  The macro should be used on function declarations
// following the argument list:
//
//   Sprocket* AllocateSprocket() MUST_USE_RESULT;
//
#if defined(SWIG)
#define MUST_USE_RESULT
#elif __GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4)
#define MUST_USE_RESULT __attribute__((warn_unused_result))
#else
#define MUST_USE_RESULT
#endif

#if defined(__GNUC__)
// Defined behavior on some of the uarchs:
// PREFETCH_HINT_T0:
//   prefetch to all levels of the hierarchy (except on p4: prefetch to L2)
// PREFETCH_HINT_NTA:
//   p4: fetch to L2, but limit to 1 way (out of the 8 ways)
//   core: skip L2, go directly to L1
//   k8 rev E and later: skip L2, can go to either of the 2-ways in L1
enum PrefetchHint {
  PREFETCH_HINT_T0 = 3, // More temporal locality
  PREFETCH_HINT_T1 = 2,
  PREFETCH_HINT_T2 = 1, // Less temporal locality
  PREFETCH_HINT_NTA = 0 // No temporal locality
};
#else
// prefetch is a no-op for this target. Feel free to add more sections above.
#endif

extern inline void prefetch(const char* x, int hint) {
#if defined(__llvm__)
  // In the gcc version of prefetch(), hint is only a constant _after_ inlining
  // (assumed to have been successful).  llvm views things differently, and
  // checks constant-ness _before_ inlining.  This leads to compilation errors
  // with using the other version of this code with llvm.
  //
  // One way round this is to use a switch statement to explicitly match
  // prefetch hint enumerations, and invoke __builtin_prefetch for each valid
  // value.  llvm's optimization removes the switch and unused case statements
  // after inlining, so that this boils down in the end to the same as for gcc;
  // that is, a single inlined prefetchX instruction.
  //
  // Note that this version of prefetch() cannot verify constant-ness of hint.
  // If client code calls prefetch() with a variable value for hint, it will
  // receive the full expansion of the switch below, perhaps also not inlined.
  // This should however not be a problem in the general case of well behaved
  // caller code that uses the supplied prefetch hint enumerations.
  switch (hint) {
    case PREFETCH_HINT_T0:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T0);
      break;
    case PREFETCH_HINT_T1:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T1);
      break;
    case PREFETCH_HINT_T2:
      __builtin_prefetch(x, 0, PREFETCH_HINT_T2);
      break;
    case PREFETCH_HINT_NTA:
      __builtin_prefetch(x, 0, PREFETCH_HINT_NTA);
      break;
    default:
      __builtin_prefetch(x);
      break;
  }
#elif defined(__GNUC__)
#if !defined(__i386) || defined(__SSE__)
  if (__builtin_constant_p(hint)) {
    __builtin_prefetch(x, 0, hint);
  } else {
    // Defaults to PREFETCH_HINT_T0
    __builtin_prefetch(x);
  }
#else
  // We want a __builtin_prefetch, but we build with the default -march=i386
  // where __builtin_prefetch quietly turns into nothing.
  // Once we crank up to -march=pentium3 or higher the __SSE__
  // clause above will kick in with the builtin.
  // -- mec 2006-06-06
  if (hint == PREFETCH_HINT_NTA)
    __asm__ __volatile__("prefetchnta (%0)" : : "r"(x));
#endif
#else
  // You get no effect.  Feel free to add more sections above.
#endif
}

#ifdef __cplusplus
// prefetch intrinsic (bring data to L1 without polluting L2 cache)
extern inline void prefetch(const char* x) {
  return prefetch(x, 0);
}
#endif // ifdef __cplusplus

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x
#endif

#if !defined(__cplusplus)
// stdlib.h only declares this in C++, not in C, so we declare it here.
// Also make sure to avoid declaring it on platforms which don't support it.
extern int posix_memalign(void** memptr, size_t alignment, size_t size);
#endif

inline void* alignedMalloc(size_t size, int minimumAlignment) {
  void* ptr = nullptr;
  if (posix_memalign(&ptr, minimumAlignment, size) != 0) {
    return nullptr;
  } else {
    return ptr;
  }
}

inline void alignedFree(void* alignedMemory) {
  free(alignedMemory);
}

#else // not GCC

#define PRINTF_ATTRIBUTE(string_index, first_to_check)
#define PACKED
#define CACHELINE_ALIGNED
#define ATTRIBUTE_UNUSED
#define ATTRIBUTE_ALWAYS_INLINE
#define ATTRIBUTE_WEAK
#define HAVE_ATTRIBUTE_WEAK 0
#define ATTRIBUTE_NORETURN
#define MUST_USE_RESULT
extern inline void prefetch(const char* x) {}
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x

#endif // GCC

//
// Provides a char array with the exact same alignment as another type. The
// first parameter must be a complete type, the second parameter is how many
// of that type to provide space for.
//
//   ALIGNED_CHAR_ARRAY(struct stat, 16) storage_;
//
#if defined(__cplusplus)
#undef ALIGNED_CHAR_ARRAY
// Because MSVC and older GCCs require that the argument to their alignment
// construct to be a literal constant integer, we use a template instantiated
// at all the possible powers of two.
#ifndef SWIG
template <int alignment, int size>
struct AlignType {};
template <int size>
struct AlignType<0, size> {
  using result = char[size];
};
#define BASE_PORT_H_ALIGN_ATTRIBUTE(X) __attribute__((aligned(X)))
#define BASE_PORT_H_ALIGN_OF(T) __alignof__(T)

#if defined(BASE_PORT_H_ALIGN_ATTRIBUTE)

#define BASE_PORT_H_ALIGNTYPE_TEMPLATE(X)                     \
  template <int size>                                         \
  struct AlignType<X, size> {                                 \
    typedef BASE_PORT_H_ALIGN_ATTRIBUTE(X) char result[size]; \
  }

BASE_PORT_H_ALIGNTYPE_TEMPLATE(1);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(2);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(4);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(8);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(16);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(32);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(64);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(128);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(256);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(512);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(1024);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(2048);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(4096);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(8192);
// Any larger and MSVC++ will complain.

#define ALIGNED_CHAR_ARRAY(T, Size) \
  typename AlignType<BASE_PORT_H_ALIGN_OF(T), sizeof(T) * Size>::result

#undef BASE_PORT_H_ALIGNTYPE_TEMPLATE
#undef BASE_PORT_H_ALIGN_ATTRIBUTE

#else // defined(BASE_PORT_H_ALIGN_ATTRIBUTE)
#define ALIGNED_CHAR_ARRAY \
  you_must_define_ALIGNED_CHAR_ARRAY_for_your_compiler_in_base_port_h
#endif // defined(BASE_PORT_H_ALIGN_ATTRIBUTE)

#else // !SWIG

// SWIG can't represent alignment and doesn't care about alignment on data
// members (it works fine without it).
template <typename Size>
struct AlignType {
  typedef char result[Size];
};
#define ALIGNED_CHAR_ARRAY(T, Size) AlignType<Size * sizeof(T)>::result

#endif // !SWIG
#else // __cpluscplus
#define ALIGNED_CHAR_ARRAY ALIGNED_CHAR_ARRAY_is_not_available_without_Cplusplus
#endif // __cplusplus

// gethostbyname() is not thread-safe.  So disallow its use.  People
// should either use the HostLookup::Lookup*() methods, or gethostbyname_r()
#define gethostbyname gethostbyname_is_not_thread_safe_DO_NOT_USE

// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.

#if defined(__i386) || defined(ARCH_ATHLON) || defined(__x86_64__) || \
    defined(_ARCH_PPC)

// x86 and x86-64 can perform unaligned loads/stores directly;
// modern PowerPC hardware can also do unaligned integer loads and stores;
// but note: the FPU still sends unaligned loads and stores to a trap handler!

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16_t*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32_t*>(_p))
#define UNALIGNED_LOAD64(_p) (*reinterpret_cast<const uint64_t*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16_t*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32_t*>(_p) = (_val))
#define UNALIGNED_STORE64(_p, _val) (*reinterpret_cast<uint64_t*>(_p) = (_val))

#elif defined(__arm__) && !defined(__ARM_ARCH_5__) &&          \
    !defined(__ARM_ARCH_5T__) && !defined(__ARM_ARCH_5TE__) && \
    !defined(__ARM_ARCH_5TEJ__) && !defined(__ARM_ARCH_6__) && \
    !defined(__ARM_ARCH_6J__) && !defined(__ARM_ARCH_6K__) &&  \
    !defined(__ARM_ARCH_6Z__) && !defined(__ARM_ARCH_6ZK__) && \
    !defined(__ARM_ARCH_6T2__)

// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// “ARMv7 or higher”, so we have to filter away all ARMv5 and ARMv6
// sub-architectures. Newer gcc (>= 4.6) set an __ARM_FEATURE_ALIGNED #define,
// so in time, maybe we can move on to that.
//
// This is a mess, but there's not much we can do about it.

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32*>(_p) = (_val))

// TODO(user): NEON supports unaligned 64-bit loads and stores.
// See if that would be more efficient on platforms supporting it,
// at least for copies.

inline uint64_t UNALIGNED_LOAD64(const void* p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UNALIGNED_STORE64(void* p, uint64_t v) {
  memcpy(p, &v, sizeof v);
}

#else

#define NEED_ALIGNED_LOADS

// These functions are provided for architectures that don't support
// unaligned loads and stores.

inline uint16_t UNALIGNED_LOAD16(const void* p) {
  uint16_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint32_t UNALIGNED_LOAD32(const void* p) {
  uint32_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline uint64_t UNALIGNED_LOAD64(const void* p) {
  uint64_t t;
  memcpy(&t, p, sizeof t);
  return t;
}

inline void UNALIGNED_STORE16(void* p, uint16_t v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE32(void* p, uint32_t v) {
  memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE64(void* p, uint64_t v) {
  memcpy(p, &v, sizeof v);
}

#endif

// NOTE(user): These are only exported to C++ because the macros they depend on
// use C++-only syntax. This #ifdef can be removed if/when the macros are fixed.

#if defined(__cplusplus)

namespace port_internal {

template <class T>
constexpr bool loadByReinterpretCast() {
#ifndef NEED_ALIGNED_LOADS
  // Per above, it's safe to use reinterpret_cast on x86 for types int64 and
  // smaller.
  return sizeof(T) <= 8;
#else
  return false;
#endif
}

// Enable UnalignedLoad and UnalignedStore for numeric types (floats and ints)
// including int128. We don't allow these functions for other types, even if
// they are POD and <= 16 bits.
template <class T>
using EnableIfNumeric = std::enable_if<
    std::is_arithmetic<T>::value || std::is_same<T, __int128>::value,
    T>;

} // namespace port_internal

// Load an integer from pointer 'src'.
//
// This is a safer equivalent of *reinterpret_cast<const T*>(src) that properly
// handles the case of larger types such as int128 which require alignment.
//
// Usage:
//   int32_t x = UnalignedLoad<int32_t>(void_ptr);
//
template <
    typename T,
    typename port_internal::EnableIfNumeric<T>::type* = nullptr,
    bool USE_REINTERPRET = port_internal::loadByReinterpretCast<T>()>
inline T UnalignedLoad(const void* src) {
  if (USE_REINTERPRET) {
    return *reinterpret_cast<const T*>(src);
  }
  T ret;
  memcpy(&ret, src, sizeof(T));
  return ret;
}

// Store the integer 'src' in the pointer 'dst'.
//
// Usage:
//   int32_t foo = 123;
//   UnalignedStore(my_void_ptr, foo);
//
// NOTE: this reverses the usual style-guide-suggested order of arguments
// to match the more natural "*p = v;" ordering of a normal store.
template <
    typename T,
    typename port_internal::EnableIfNumeric<T>::type* = nullptr,
    bool USE_REINTERPRET = port_internal::loadByReinterpretCast<T>()>
inline void UnalignedStore(void* dst, const T& src) {
  if (USE_REINTERPRET) {
    *reinterpret_cast<T*>(dst) = src;
  } else {
    memcpy(dst, &src, sizeof(T));
  }
}

#endif // defined(__cpluscplus)

// GXX_EXPERIMENTAL_CXX0X is defined by gcc and clang up to at least
// gcc-4.7 and clang-3.1 (2011-12-13).  __cplusplus was defined to 1
// in gcc before 4.7 (Crosstool 16) and clang before 3.1, but is
// defined according to the language version in effect thereafter.  I
// believe MSVC will also define __cplusplus according to the language
// version, but haven't checked that.
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L
// Define this to 1 if the code is compiled in C++11 mode; leave it
// undefined otherwise.  Do NOT define it to 0 -- that causes
// '#ifdef LANG_CXX11' to behave differently from '#if LANG_CXX11'.
#define LANG_CXX11 1
#endif
