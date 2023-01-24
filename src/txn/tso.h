/**
 * @file tso.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include <atomic>

namespace arcanedb {
namespace txn {

class Tso {
public:
  static TxnTs RequestTs() noexcept {
    static Tso tso;
    return tso.ts_.fetch_add(1, std::memory_order_relaxed);
  }

private:
  std::atomic<TxnTs> ts_{0};
};

} // namespace txn
} // namespace arcanedb