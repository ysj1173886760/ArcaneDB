/**
 * @file uuid.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <cstdint>
#include <limits>
#include <random>

namespace arcanedb {
namespace util {

// hack the uuid by using random generator
inline int64_t GenerateUUID() noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(
      std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
  return distribution(generator);
}

} // namespace util
} // namespace arcanedb