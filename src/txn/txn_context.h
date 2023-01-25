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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "btree/btree_type.h"
#include "btree/sub_table.h"
#include "btree/versioned_btree.h"
#include "common/options.h"
#include "common/status.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include "txn/lock_table.h"
#include "txn/snapshot_manager.h"
#include "txn/txn_type.h"

namespace arcanedb {
namespace txn {

/**
 * @brief
 * Transactional KKV
 */
class TxnContext {
public:
  TxnContext(TxnId txn_id, TxnTs txn_ts, TxnType txn_type,
             LinkBufSnapshotManager *snapshot_manager,
             ShardedLockTable *lock_table) noexcept
      : txn_id_(txn_id), txn_ts_(txn_ts), txn_type_(txn_type),
        snapshot_manager_(snapshot_manager), lock_table_(lock_table) {}

  /**
   * @brief
   *
   * @param sub_table_key
   * @param row
   * @param opts
   * @return Status
   */
  Status SetRow(const std::string &sub_table_key, const property::Row &row,
                const Options &opts) noexcept;

  /**
   * @brief
   *
   * @param sub_table_key
   * @param sort_key
   * @param opts
   * @return Status
   */
  Status DeleteRow(const std::string &sub_table_key,
                   property::SortKeysRef sort_key,
                   const Options &opts) noexcept;

  /**
   * @brief
   *
   * @param sub_table_key
   * @param sort_key
   * @param opts
   * @param view
   * @return Status
   */
  Status GetRow(const std::string &sub_table_key,
                property::SortKeysRef sort_key, const Options &opts,
                btree::RowView *view) noexcept;

  TxnTs GetTxnTs() const noexcept { return txn_ts_; }

  // TODO(sheep): handle abort
  void Commit() noexcept;

private:
  btree::SubTable *GetSubTable_(const std::string &sub_table_key,
                                const Options &opts) noexcept;

  Status AcquireLock_(const std::string &sub_table_key,
                      std::string_view sort_key) noexcept;

  TxnId txn_id_;
  /**
   * @brief
   * txn_ts is read ts when txn type is read only txn.
   * and txn_ts is write ts when txn type is read write txn.
   */
  TxnTs txn_ts_;
  TxnType txn_type_;
  LinkBufSnapshotManager *snapshot_manager_;
  ShardedLockTable *lock_table_;
  absl::flat_hash_set<std::string> lock_set_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<btree::SubTable>>
      tables_;
};

} // namespace txn
} // namespace arcanedb