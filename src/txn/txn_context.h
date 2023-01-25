/**
 * @file txn_context.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/status.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"

namespace arcanedb {
namespace txn {

class TxnContext {
public:
  TxnContext() = default;

  // Status SetKKV(const std::string_view &k1, property::SortKeysRef sk, const
  // property::Row &row) noexcept;

  // Status DeleteKKV(const std::string_view &)

private:
};

} // namespace txn
} // namespace arcanedb