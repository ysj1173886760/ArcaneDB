/**
 * @file txn_manager.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "txn/tso.h"
#include "txn/txn_context.h"
#include "txn/txn_type.h"
#include "util/uuid.h"

namespace arcanedb {
namespace txn {

/**
 * @brief
 * Currently just a dummy wrapper.
 * could used to implement read view to support mvcc in the future.
 */
class TxnManager {
public:
  std::unique_ptr<TxnContext> BeginRoTxn() noexcept {}

  std::unique_ptr<TxnContext> BeginRwTxn() noexcept {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = Tso::RequestTs();
    return std::make_unique<TxnContext>(txn_id, txn_ts, TxnType::ReadWriteTxn);
  }
};

} // namespace txn
} // namespace arcanedb