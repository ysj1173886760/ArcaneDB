/**
 * @file txn_manager_2pl.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-26
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
#include "txn/txn_context_2pl.h"
#include "txn/txn_manager.h"
#include "txn/txn_type.h"
#include "util/uuid.h"

namespace arcanedb {
namespace txn {

/**
 * @brief
 * Currently just a dummy wrapper.
 * could used to implement read view to support mvcc in the future.
 */
class TxnManager2PL : public TxnManager {
public:
  TxnManager2PL() noexcept
      : snapshot_manager_{}, lock_table_(common::Config::kLockTableShardNum) {}

  std::unique_ptr<TxnContext> BeginRoTxn() const noexcept override {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = snapshot_manager_.GetSnapshotTs();
    return std::make_unique<TxnContext2PL>(txn_id, txn_ts, TxnType::ReadOnlyTxn,
                                           &snapshot_manager_, &lock_table_);
  }

  std::unique_ptr<TxnContext> BeginRoTxnWithTs(TxnTs ts) const noexcept {
    auto txn_id = util::GenerateUUID();
    return std::make_unique<TxnContext2PL>(txn_id, ts, TxnType::ReadOnlyTxn,
                                           &snapshot_manager_, &lock_table_);
  }

  std::unique_ptr<TxnContext> BeginRwTxn() const noexcept override {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = tso_.RequestTs();
    return std::make_unique<TxnContext2PL>(txn_id, txn_ts,
                                           TxnType::ReadWriteTxn,
                                           &snapshot_manager_, &lock_table_);
  }

  LinkBufSnapshotManager *GetSnapshotManager() noexcept {
    return &snapshot_manager_;
  }

private:
  mutable LinkBufSnapshotManager snapshot_manager_;
  mutable ShardedLockTable lock_table_;
  mutable Tso tso_;
};

} // namespace txn
} // namespace arcanedb