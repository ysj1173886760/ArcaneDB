/**
 * @file encoding.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief port from rocksdb
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <cstdint>
#include <string>

// little endian encoding

namespace arcanedb {
namespace util {

extern void PutBoolAsFixed8(std::string *dst, bool value);
extern void PutFixed8(std::string *dst, uint8_t value);
extern void PutFixed16(std::string *dst, uint16_t value);
extern void PutFixed32(std::string *dst, uint32_t value);
extern void PutFixed64(std::string *dst, uint64_t value);
extern void PutFixedF32(std::string *dst, float value);
extern void PutFixedF64(std::string *dst, double value);

// Standard Get... routines parse a value from the beginning of a
// Slice and advance the slice past the parsed value.
extern bool GetFixed8AsBool(std::string_view *input, bool *value);
extern bool GetFixed8(std::string_view *input, uint8_t *value);
extern bool GetFixed16(std::string_view *input, uint16_t *value);
extern bool GetFixed64(std::string_view *input, uint64_t *value);
extern bool GetFixed32(std::string_view *input, uint32_t *value);
extern bool GetFixedF32(std::string_view *input, float *value);
extern bool GetFixedF64(std::string_view *input, double *value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

/// we will generate functions like:
///
/// ```
/// inline uint32_t DecodeFixed32(const char* ptr) {
///  // Load the raw bytes
///  uint32_t result;
///  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain
///  load return result;
/// }
/// ```

#define DECLARE_DECODE_FIXED_FN(DECODE_FIXED_FN, pod_type)                     \
  inline pod_type DECODE_FIXED_FN(const char *ptr) {                           \
    static_assert(std::is_pod<pod_type>::value);                               \
    pod_type result;                                                           \
    memcpy(&result, ptr, sizeof(result));                                      \
    return result;                                                             \
  }

DECLARE_DECODE_FIXED_FN(DecodeFixed8, uint8_t);
DECLARE_DECODE_FIXED_FN(DecodeFixed16, uint16_t);
DECLARE_DECODE_FIXED_FN(DecodeFixed32, uint32_t);
DECLARE_DECODE_FIXED_FN(DecodeFixed64, uint64_t);
DECLARE_DECODE_FIXED_FN(DecodeFixedF32, float);
DECLARE_DECODE_FIXED_FN(DecodeFixedF64, double);

#undef DECLARE_DECODE_FIXED_FN

/// we will generate functions like:
///
/// ```
/// inline void PutFixed16(std::string* dst, uint16_t value) {
///  dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
///              sizeof(value));
/// }
/// ```

#define DECLARE_PUT_FIXED_FN(PUT_FIXED_FN, pod_type)                           \
  inline void PUT_FIXED_FN(std::string *dst, pod_type value) {                 \
    dst->append(const_cast<const char *>(reinterpret_cast<char *>(&value)),    \
                sizeof(value));                                                \
  }

DECLARE_PUT_FIXED_FN(PutFixed8, uint8_t);
DECLARE_PUT_FIXED_FN(PutFixed16, uint16_t);
DECLARE_PUT_FIXED_FN(PutFixed32, uint32_t);
DECLARE_PUT_FIXED_FN(PutFixed64, uint64_t);
DECLARE_PUT_FIXED_FN(PutFixedF32, float);
DECLARE_PUT_FIXED_FN(PutFixedF64, double);

#undef DECLARE_PUT_FIXED_FN

inline void PutBoolAsFixed8(std::string *dst, bool value) {
  uint8_t u8 = 0;
  if (value) {
    u8 = 1;
  }
  PutFixed8(dst, u8);
}

#define DECLARE_GET_FIXED_FN(GET_FIXED_FN, DECODE_FN_NAME, pod_type)           \
  inline bool GET_FIXED_FN(std::string_view *input, pod_type *value) {         \
    if (input->size() < sizeof(pod_type)) {                                    \
      return false;                                                            \
    }                                                                          \
    *value = DECODE_FN_NAME(input->data());                                    \
    input->remove_prefix(sizeof(pod_type));                                    \
    return true;                                                               \
  }

DECLARE_GET_FIXED_FN(GetFixed8, DecodeFixed8, uint8_t);
DECLARE_GET_FIXED_FN(GetFixed16, DecodeFixed16, uint16_t);
DECLARE_GET_FIXED_FN(GetFixed32, DecodeFixed32, uint32_t);
DECLARE_GET_FIXED_FN(GetFixed64, DecodeFixed64, uint64_t);
DECLARE_GET_FIXED_FN(GetFixedF32, DecodeFixedF32, float);
DECLARE_GET_FIXED_FN(GetFixedF64, DecodeFixedF64, double);

#undef DECLARE_GET_FIXED_FN

inline bool GetFixed8AsBool(std::string_view *input, bool *value) {
  uint8_t u8;
  bool get_succ = GetFixed8(input, &u8);
  if (!get_succ) {
    return false;
  }
  if (u8 == 0) {
    *value = false;
  } else {
    *value = true;
  }
  return true;
}

} // namespace util
} // namespace arcanedb