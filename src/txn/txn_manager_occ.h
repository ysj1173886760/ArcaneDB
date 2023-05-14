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
#include "common/lock_table.h"
#include "txn/snapshot_manager.h"
#include "txn/tso.h"
#include "txn/txn_context_occ.h"
#include "txn/txn_manager.h"
#include "util/uuid.h"

namespace arcanedb {
namespace txn {

class TxnManagerOCC : public TxnManager {
public:
  TxnManagerOCC(LockManagerType type) noexcept
      : snapshot_manager_{}, lock_table_(common::Config::kLockTableShardNum),
        lock_manager_type_(type) {}

  std::unique_ptr<TxnContext>
  BeginRoTxn(const Options &opts) const noexcept override {
    auto txn_id = util::GenerateUUID();
    auto read_ts = snapshot_manager_.GetSnapshotTs();
    return std::make_unique<TxnContextOCC>(opts, txn_id, read_ts,
                                           TxnType::ReadOnlyTxn, &lock_table_,
                                           this, lock_manager_type_);
  }

  std::unique_ptr<TxnContext> BeginRoTxnWithTs(const Options &opts,
                                               TxnTs ts) const noexcept {
    auto txn_id = util::GenerateUUID();
    return std::make_unique<TxnContextOCC>(opts, txn_id, ts,
                                           TxnType::ReadOnlyTxn, &lock_table_,
                                           this, lock_manager_type_);
  }

  std::unique_ptr<TxnContext>
  BeginRwTxn(const Options &opts) const noexcept override {
    auto txn_id = util::GenerateUUID();
    auto read_ts = tso_.RequestTs();
    // commit this read ts immediately.
    snapshot_manager_.CommitTs(read_ts);
    return std::make_unique<TxnContextOCC>(opts, txn_id, read_ts,
                                           TxnType::ReadWriteTxn, &lock_table_,
                                           this, lock_manager_type_);
  }

  TxnTs RequestTs() const noexcept { return tso_.RequestTs(); }

  void Commit(TxnContext *txn_context) const noexcept {
    snapshot_manager_.CommitTs(txn_context->GetWriteTs());
  }

  LinkBufSnapshotManager *GetSnapshotManager() noexcept {
    return &snapshot_manager_;
  }

private:
  mutable LinkBufSnapshotManager snapshot_manager_;
  mutable common::ShardedLockTable lock_table_;
  mutable Tso tso_;
  const LockManagerType lock_manager_type_;
};

} // namespace txn
} // namespace arcanedb