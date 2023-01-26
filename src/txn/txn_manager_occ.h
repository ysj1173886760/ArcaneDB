/**
 * @file txn_manager_occ.h
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
#include "txn/tso.h"
#include "txn/txn_context_occ.h"
#include "util/uuid.h"

namespace arcanedb {
namespace txn {

class TxnManagerOCC {
public:
  TxnManagerOCC() noexcept
      : snapshot_manager_{}, lock_table_(common::Config::kLockTableShardNum) {}

  std::unique_ptr<TxnContext> BeginRoTxn() const noexcept {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = snapshot_manager_.GetSnapshotTs();
    return std::make_unique<TxnContextOCC>(txn_id, txn_ts, TxnType::ReadOnlyTxn,
                                           &lock_table_, this);
  }

  std::unique_ptr<TxnContext> BeginRoTxnWithTs(TxnTs ts) const noexcept {
    auto txn_id = util::GenerateUUID();
    return std::make_unique<TxnContextOCC>(txn_id, ts, TxnType::ReadOnlyTxn,
                                           &lock_table_, this);
  }

  std::unique_ptr<TxnContext> BeginRwTxn() const noexcept {
    auto txn_id = util::GenerateUUID();
    auto txn_ts = tso_.RequestTs();
    return std::make_unique<TxnContextOCC>(
        txn_id, txn_ts, TxnType::ReadWriteTxn, &lock_table_, this);
  }

  TxnTs RequestTs() const noexcept { return tso_.RequestTs(); }

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