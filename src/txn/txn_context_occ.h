/**
 * @file txn_context_occ.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/sub_table.h"
#include "common/lock_table.h"
#include "txn/txn_context.h"

namespace arcanedb {
namespace txn {

class TxnManagerOCC;

/**
 * @brief
 * Hekaton style occ
 */
class TxnContextOCC : public TxnContext {
public:
  TxnContextOCC(const Options &opts, TxnId txn_id, TxnTs txn_ts,
                TxnType txn_type, common::ShardedLockTable *lock_table,
                const TxnManagerOCC *txn_manager,
                LockManagerType lock_manager_type) noexcept
      : txn_id_(txn_id), read_ts_(txn_ts), txn_type_(txn_type),
        lock_table_(lock_table), txn_manager_(txn_manager),
        lock_manager_type_(lock_manager_type) {}

  ~TxnContextOCC() noexcept override = default;

  /**
   * @brief
   *
   * @param sub_table_key
   * @param row
   * @param opts
   * @return Status
   */
  Status SetRow(const std::string &sub_table_key, const property::Row &row,
                const Options &opts) noexcept override;

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
                   const Options &opts) noexcept override;

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
                btree::RowView *view) noexcept override;

  /**
   * @brief
   * Commit or abort current txn.
   * return the txn state.
   * @return Status
   */
  Status CommitOrAbort(const Options &opts) noexcept override;

  TxnTs GetReadTs() const noexcept override { return read_ts_; }

  TxnTs GetWriteTs() const noexcept override { return commit_ts_; }

  TxnType GetTxnType() const noexcept override { return txn_type_; }

  void RangeFilter(const std::string &sub_table_key, const Options &opts,
                   const Filter &filter, const BtreeScanOpts &scan_opts,
                   btree::RangeScanRowView *rows_view) noexcept override;

  btree::RowIterator GetRowIterator(const std::string &sub_table_key,
                                    const Options &opts) noexcept override;

private:
  btree::SubTable *GetSubTable_(const std::string_view &sub_table_key,
                                const Options &opts) noexcept;

  Status AcquireLock_(const std::string &sub_table_key,
                      std::string_view sort_key, const Options &opts) noexcept;

  Status WriteIntents_(const Options &opts) noexcept;

  bool ValidateRead_(const Options &opts) noexcept;

  void CommitIntents_(const Options &opts) noexcept;

  void AbortIntents_(const Options &opts) noexcept;

  void ReleaseLock_(const Options &opts) noexcept;

  void Begin_(log_store::LogStore *log_store) noexcept;

  void Commit_(log_store::LogStore *log_store) noexcept;

  void Abort_(log_store::LogStore *log_store) noexcept;

  void WaitForCommit_(log_store::LogStore *log_store) noexcept;

  void UndoWriteIntents_(
      const std::vector<std::pair<std::string_view, property::SortKeysRef>>
          &undo_list,
      const Options &opts) noexcept;

  struct WriteSetHash {
    size_t
    operator()(const std::pair<std::string, property::SortKeysRef> &value) const
        noexcept {
      // using xor to simplicity.
      // Might need to use absl::combine
      return absl::Hash<std::string>()(value.first) ^
             property::SortKeysRefHash()(value.second);
    }
  };

  struct ReadSetHash {
    size_t
    operator()(const std::pair<std::string, property::SortKeys> &value) const
        noexcept {
      return absl::Hash<std::string>()(value.first) ^
             property::SortKeysHash()(value.second);
    }
  };

  // note that we are relying on the fact that
  // every rw txn has different read ts.
  TxnId txn_id_;
  TxnTs read_ts_;
  TxnTs commit_ts_;
  TxnType txn_type_;
  common::ShardedLockTable *lock_table_;
  const TxnManagerOCC *txn_manager_;
  absl::flat_hash_set<std::string> lock_set_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<btree::SubTable>>
      tables_;
  // TODO(sheep): use arena to optimize memory allocation
  absl::flat_hash_map<std::pair<std::string, property::SortKeysRef>,
                      std::optional<property::Row>, WriteSetHash>
      write_set_;
  // sort_key -> TxnTs
  absl::flat_hash_map<std::pair<std::string, property::SortKeys>,
                      std::optional<TxnTs>, ReadSetHash>
      read_set_;
  absl::flat_hash_set<std::unique_ptr<std::string>> row_owners_;

  LockManagerType lock_manager_type_;

  log_store::LsnType lsn_{};
};

} // namespace txn
} // namespace arcanedb