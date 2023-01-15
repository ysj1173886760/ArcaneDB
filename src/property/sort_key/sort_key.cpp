/**
 * @file sort_key.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/sort_key/sort_key.h"

namespace arcanedb {
namespace property {

SortKeysRef SortKeys::as_ref() const noexcept { return SortKeysRef(bytes_); }

} // namespace property
} // namespace arcanedb