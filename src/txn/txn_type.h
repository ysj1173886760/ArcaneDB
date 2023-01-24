/**
 * @file txn_type.h
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

namespace arcanedb {
namespace txn {

enum class LockType : int8_t {
  RLock,
  WLock,
};

}
} // namespace arcanedb