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
  /**
   * @brief
   *
   * @param sub_table_key
   * @param row
   * @param opts
   * @return Status
   */
  virtual Status SetRow(const std::string &sub_table_key,
                        const property::Row &row,
                        const Options &opts) noexcept = 0;

  /**
   * @brief
   *
   * @param sub_table_key
   * @param sort_key
   * @param opts
   * @return Status
   */
  virtual Status DeleteRow(const std::string &sub_table_key,
                           property::SortKeysRef sort_key,
                           const Options &opts) noexcept = 0;

  /**
   * @brief
   *
   * @param sub_table_key
   * @param sort_key
   * @param opts
   * @param view
   * @return Status
   */
  virtual Status GetRow(const std::string &sub_table_key,
                        property::SortKeysRef sort_key, const Options &opts,
                        btree::RowView *view) noexcept = 0;

  /**
   * @brief
   * Commit or abort current txn.
   * return the txn state.
   * @return Status
   */
  virtual Status CommitOrAbort() noexcept = 0;

  virtual TxnTs GetTxnTs() const noexcept = 0;

  virtual ~TxnContext() noexcept {}
};

} // namespace txn
} // namespace arcanedb