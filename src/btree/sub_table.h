/**
 * @file sub_table.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/versioned_btree.h"
#include "btree/write_info.h"
#include "cache/buffer_pool.h"
#include "page/versioned_btree_page.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Currently just a wrapper for btree.
 * For supporting secondary-index in the future.
 */
class SubTable {
public:
  SubTable(cache::BufferPool::PageHolder root_page) noexcept
      : cluster_index_(root_page) {}

  std::string_view GetTableKey() noexcept {
    return cluster_index_.GetRootPageKey();
  }

  /**
   * @brief
   *
   * @param table_key
   * @param opts
   * @param sub_table
   * @return Status
   */
  static Status OpenSubTable(const std::string_view &table_key,
                             const Options &opts,
                             std::unique_ptr<SubTable> *sub_table) noexcept;

  /**
   * @brief
   * Insert a row into page.
   * if row with same sortkey is existed, then we will overwrite that row.
   * i.e. semantic is to always perform upsert.
   * @param row
   * @param write_ts
   * @param opts
   * @param info
   * @return Status
   */
  Status SetRow(const property::Row &row, TxnTs write_ts, const Options &opts,
                WriteInfo *info) noexcept {
    return cluster_index_.SetRow(row, write_ts, opts, info);
  }

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param write_ts
   * @param opts
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key, TxnTs write_ts,
                   const Options &opts, WriteInfo *info) noexcept {
    return cluster_index_.DeleteRow(sort_key, write_ts, opts, info);
  }

  /**
   * @brief
   * Get a row from page
   * @param tuple SortKey.
   * @param read_ts
   * @param opts
   * @param view
   * @return Status: Ok when row has been found
   *                 NotFound.
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                const Options &opts, RowView *view) const noexcept {
    return cluster_index_.GetRow(sort_key, read_ts, opts, view);
  }

  /**
   * @brief
   * Set ts of the newest version with "sort_key" to "target_ts"
   * @param sort_key
   * @param target_ts
   * @param opts
   * @return Status
   */
  void SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
             const Options &opts, WriteInfo *info) noexcept {
    cluster_index_.SetTs(sort_key, target_ts, opts, info);
  }

  /**
   * @brief
   * Range scan
   * @param opts
   * @param filter
   * @param scan_opts
   * @param views
   */
  void RangeFilter(const Options &opts, const Filter &filter,
                   const BtreeScanOpts &scan_opts,
                   RangeScanRowView *views) const noexcept {
    cluster_index_.RangeFilter(opts, filter, scan_opts, views);
  }

  /**
   * @brief
   * Range scan without order
   * @return RowIterator
   */
  RowIterator GetRowIterator() const noexcept {
    return cluster_index_.GetRowIterator();
  }

  common::LockTable &GetLockTable() noexcept {
    return cluster_index_.GetLockTable();
  }

private:
  VersionedBtree cluster_index_;
};

} // namespace btree
} // namespace arcanedb