/**
 * @file config.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include <cstddef>

namespace arcanedb {
namespace common {

class Config {
public:
  static constexpr size_t kThreadPoolDefaultNum = 2;
  static constexpr size_t kLogSegmentDefaultNum = 16;
  static constexpr size_t kLogSegmentDefaultSize = 4 << 10;
};

} // namespace common
} // namespace arcanedb