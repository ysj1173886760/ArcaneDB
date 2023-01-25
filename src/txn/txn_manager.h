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

#include "common/config.h"
#include "txn/lock_table.h"
#include "txn/snapshot_manager.h"
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
  TxnManager() noexcept
      : snapshot_manager_{}, lock_table_(common::Config::kLockTableShardNum) {}

  std::unique_ptr<TxnContext> BeginRoTxn() const noexcept {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = snapshot_manager_.GetSnapshotTs();
    return std::make_unique<TxnContext>(txn_id, txn_ts, TxnType::ReadOnlyTxn,
                                        &snapshot_manager_, &lock_table_);
  }

  std::unique_ptr<TxnContext> BeginRoTxnWithTs(TxnTs ts) const noexcept {
    auto txn_id = util::GenerateUUID();
    return std::make_unique<TxnContext>(txn_id, ts, TxnType::ReadOnlyTxn,
                                        &snapshot_manager_, &lock_table_);
  }

  std::unique_ptr<TxnContext> BeginRwTxn() const noexcept {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = Tso::RequestTs();
    return std::make_unique<TxnContext>(txn_id, txn_ts, TxnType::ReadWriteTxn,
                                        &snapshot_manager_, &lock_table_);
  }

  LinkBufSnapshotManager *GetSnapshotManager() noexcept {
    return &snapshot_manager_;
  }

private:
  FRIEND_TEST(TxnContextTest, ConcurrentTest);

  mutable LinkBufSnapshotManager snapshot_manager_;
  mutable ShardedLockTable lock_table_;
};

} // namespace txn
} // namespace arcanedb