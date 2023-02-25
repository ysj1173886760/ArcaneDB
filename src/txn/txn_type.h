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

enum class TxnType : int8_t {
  ReadOnlyTxn,
  ReadWriteTxn,
};

enum class LockManagerType : uint8_t {
  kCentralized,
  kDecentralized,
  kInlined,
};

} // namespace txn
} // namespace arcanedb