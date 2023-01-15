/**
 * @file sort_key_encoding.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/sort_key/sort_key_encoding.h"
#include "absl/base/internal/endian.h"
#include "util/codec/encoding.h"
#include <cstring>

namespace arcanedb {
namespace property {

constexpr uint32_t Sign32Mask = 0x80000000;
constexpr uint64_t Sign64Mask = 0x8000000000000000;
constexpr size_t EncGroupBytes = 8;

void GetComparableFixedU16(std::string_view *sp, uint16_t *value) noexcept {
  uint16_t tmp;
  auto res = util::GetFixed16(sp, &tmp);
  assert(res);
  *value = absl::big_endian::ToHost16(tmp);
}

void GetComparableFixedU32(std::string_view *sp, uint32_t *value) noexcept {
  uint32_t tmp;
  auto res = util::GetFixed32(sp, &tmp);
  assert(res);
  *value = absl::big_endian::ToHost32(tmp);
}

void GetComparableFixedU64(std::string_view *sp, uint64_t *value) noexcept {
  uint64_t tmp;
  auto res = util::GetFixed64(sp, &tmp);
  assert(res);
  *value = absl::big_endian::ToHost64(tmp);
}

void GetComparableFixed32(std::string_view *sp, int32_t *value) noexcept {
  uint32_t tmp;
  GetComparableFixedU32(sp, &tmp);
  *value = static_cast<int32_t>(tmp ^ Sign32Mask);
}

void GetComparableFixed64(std::string_view *sp, int64_t *value) noexcept {
  uint64_t tmp;
  GetComparableFixedU64(sp, &tmp);
  *value = static_cast<int64_t>(tmp ^ Sign64Mask);
}

void GetComparableFixedF32(std::string_view *sp, float *value) noexcept {
  uint32_t tmp;
  GetComparableFixedU32(sp, &tmp);
  if ((tmp & Sign32Mask) > 0) {
    tmp &= ~Sign32Mask;
  } else {
    tmp = ~tmp;
  }
  memcpy(value, &tmp, sizeof(float));
}

void GetComparableFixedF64(std::string_view *sp, double *value) noexcept {
  uint64_t tmp;
  GetComparableFixedU64(sp, &tmp);
  if ((tmp & Sign64Mask) > 0) {
    tmp &= ~Sign64Mask;
  } else {
    tmp = ~tmp;
  }
  memcpy(value, &tmp, sizeof(double));
}

void GetComparableString(std::string_view *sp, std::string *s) noexcept {
  bool has_remaining = true;
  while (has_remaining) {
    assert(sp->size() >= EncGroupBytes + 1);
    size_t remaining;
    if ((*sp)[EncGroupBytes] == EncGroupBytes + 1) {
      has_remaining = true;
      remaining = EncGroupBytes;
    } else {
      has_remaining = false;
      remaining = (*sp)[EncGroupBytes];
    }
    if (remaining != 0) {
      s->append(sp->data(), remaining);
    }
    *sp = sp->substr(EncGroupBytes + 1);
  }
}

void GetComparableFixedU8(std::string_view *sp, uint8_t *value) noexcept {
  auto res = util::GetFixed8(sp, value);
  assert(res);
}

void GetComparableFixedU8AsBool(std::string_view *sp, bool *value) noexcept {
  auto res = util::GetFixed8AsBool(sp, value);
  assert(res);
}

void PutComparableFixedU16(std::string *s, uint16_t value) noexcept {
  util::PutFixed16(s, absl::big_endian::FromHost16(value));
}

void PutComparableFixedU32(std::string *s, uint32_t value) noexcept {
  util::PutFixed32(s, absl::big_endian::FromHost32(value));
}

void PutComparableFixedU64(std::string *s, uint64_t value) noexcept {
  util::PutFixed64(s, absl::big_endian::FromHost64(value));
}

void PutComparableFixed32(std::string *s, int32_t value) noexcept {
  uint32_t tmp = static_cast<uint32_t>(value) ^ Sign32Mask;
  PutComparableFixedU32(s, tmp);
}

void PutComparableFixed64(std::string *s, int64_t value) noexcept {
  uint64_t tmp = static_cast<uint64_t>(value) ^ Sign64Mask;
  PutComparableFixedU64(s, tmp);
}

void PutComparableFixedF32(std::string *s, float value) noexcept {
  uint32_t tmp;
  memcpy(&tmp, &value, sizeof(float));
  if (tmp >= 0) {
    tmp |= Sign32Mask;
  } else {
    tmp = ~tmp;
  }
  PutComparableFixedU32(s, tmp);
}

void PutComparableFixedF64(std::string *s, double value) noexcept {
  uint64_t tmp;
  memcpy(&tmp, &value, sizeof(double));
  if (tmp >= 0) {
    tmp |= Sign64Mask;
  } else {
    tmp = ~tmp;
  }
  PutComparableFixedU64(s, tmp);
}

void PutComparableString(std::string *s, std::string_view sp) noexcept {
  size_t length = sp.size();
  size_t current_idx = 0;
  while (length >= EncGroupBytes) {
    s->append(sp.data() + current_idx, EncGroupBytes);
    s->push_back(static_cast<unsigned char>(EncGroupBytes + 1));
    length -= 8;
    current_idx += 8;
  }
  if (length != 0) {
    s->append(sp.data() + current_idx, length);
  }
  s->append(EncGroupBytes - length, static_cast<unsigned char>(0));
  s->push_back(static_cast<unsigned char>(length));
}

void PutComparableFixedU8(std::string *s, uint8_t value) noexcept {
  util::PutFixed8(s, value);
}

void PutComparableBoolAsFixedU8(std::string *s, bool value) noexcept {
  util::PutBoolAsFixed8(s, value);
}

} // namespace property
} // namespace arcanedb