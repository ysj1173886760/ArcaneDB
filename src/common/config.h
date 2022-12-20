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
  static constexpr size_t ThreadPoolDefaultNum = 2;
};

} // namespace common
} // namespace arcanedb