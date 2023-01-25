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
#include "btree/btree_type.h"
#include "btree/versioned_btree.h"
#include "common/options.h"
#include "common/status.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include "txn/txn_type.h"

namespace arcanedb {
namespace txn {

/**
 * @brief
 * Transactional KKV
 */
class TxnContext {
public:
  TxnContext(TxnId txn_id, TxnTs txn_ts, TxnType txn_type) noexcept;

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

  void Commit() noexcept;

private:
  TxnId txn_id_;
  /**
   * @brief
   * txn_ts is read ts when txn type is read only txn.
   * and txn_ts is write ts when txn type is read write txn.
   */
  TxnTs txn_ts_;
  TxnType txn_type_;
  std::unordered_set<std::string> lock_set_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<btree::VersionedBtree>>
      tables_;
};

} // namespace txn
} // namespace arcanedb