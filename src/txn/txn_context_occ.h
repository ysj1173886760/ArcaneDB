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
  TxnContextOCC(TxnId txn_id, TxnTs txn_ts, TxnType txn_type,
                ShardedLockTable *lock_table,
                const TxnManagerOCC *txn_manager) noexcept
      : txn_id_(txn_id), read_ts_(txn_ts), txn_type_(txn_type),
        lock_table_(lock_table), txn_manager_(txn_manager) {}

  ~TxnContextOCC() noexcept override {
    write_set_.clear();
    read_set_.clear();
    row_owners_.clear();
  }

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

private:
  btree::SubTable *GetSubTable_(const std::string &sub_table_key,
                                const Options &opts) noexcept;

  Status AcquireLock_(const std::string &sub_table_key,
                      std::string_view sort_key) noexcept;

  Status WriteIntents_(const Options &opts) noexcept;

  bool ValidateRead_(const Options &opts) noexcept;

  Status CommitIntents_(const Options &opts) noexcept;

  void ReleaseLock_() noexcept;

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
  ShardedLockTable *lock_table_;
  const TxnManagerOCC *txn_manager_;
  absl::flat_hash_set<std::string> lock_set_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<btree::SubTable>>
      tables_;
  // TODO(sheep): use arena to optimize memory allocation
  absl::flat_hash_map<std::pair<std::string, property::SortKeysRef>,
                      std::optional<property::Row>, WriteSetHash>
      write_set_;
  absl::flat_hash_set<std::unique_ptr<std::string>> row_owners_;
  // sort_key -> TxnTs
  absl::flat_hash_map<std::pair<std::string, property::SortKeys>,
                      std::optional<TxnTs>, ReadSetHash>
      read_set_;
};

} // namespace txn
} // namespace arcanedb