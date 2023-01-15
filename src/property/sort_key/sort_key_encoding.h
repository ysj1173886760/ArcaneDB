/**
 * @file sort_key_encoding.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <string>

namespace arcanedb {
namespace property {

void GetComparableFixedU16(std::string_view *sp, uint16_t *value) noexcept;
void GetComparableFixedU32(std::string_view *sp, uint32_t *value) noexcept;
void GetComparableFixedU64(std::string_view *sp, uint64_t *value) noexcept;
void GetComparableFixed32(std::string_view *sp, int32_t *value) noexcept;
void GetComparableFixed64(std::string_view *sp, int64_t *value) noexcept;
void GetComparableFixedF32(std::string_view *sp, float *value) noexcept;
void GetComparableFixedF64(std::string_view *sp, double *value) noexcept;
void GetComparableString(std::string_view *sp, std::string *s) noexcept;
void GetComparableFixedU8(std::string_view *sp, uint8_t *value) noexcept;
void GetComparableFixedU8AsBool(std::string_view *sp, bool *value) noexcept;

void PutComparableFixedU16(std::string *s, uint16_t value) noexcept;
void PutComparableFixedU32(std::string *s, uint32_t value) noexcept;
void PutComparableFixedU64(std::string *s, uint64_t value) noexcept;
void PutComparableFixed32(std::string *s, int32_t value) noexcept;
void PutComparableFixed64(std::string *s, int64_t value) noexcept;
void PutComparableFixedF32(std::string *s, float value) noexcept;
void PutComparableFixedF64(std::string *s, double value) noexcept;
void PutComparableString(std::string *s, std::string_view sp) noexcept;
void PutComparableFixedU8(std::string *s, uint8_t value) noexcept;
void PutComparableBoolAsFixedU8(std::string *s, bool value) noexcept;

} // namespace property
} // namespace arcanedb