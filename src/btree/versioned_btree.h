/**
 * @file versioned_btree.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/page/versioned_btree_page.h"
#include "btree/write_info.h"
#include "cache/buffer_pool.h"
#include "common/btree_scan_opts.h"
#include "common/filter.h"

namespace arcanedb {
namespace btree {

class VersionedBtree {
public:
  explicit VersionedBtree(cache::BufferPool::PageHolder root_page) noexcept
      : root_page_(std::move(root_page)) {}

  ~VersionedBtree() = default;

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
                WriteInfo *info) noexcept;

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param write_ts
   * @param opts
   * @param info
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key, TxnTs write_ts,
                   const Options &opts, WriteInfo *info) noexcept;

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
                const Options &opts, RowView *view) const noexcept;

  /**
   * @brief
   * Set ts of the newest version with "sort_key" to "target_ts"
   * @param sort_key
   * @param target_ts
   * @param opts
   * @param info
   */
  void SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
             const Options &opts, WriteInfo *info) noexcept;

  void RangeFilter(const Options &opts, const Filter &filter,
                   const BtreeScanOpts &scan_opts,
                   RangeScanRowView *views) const noexcept {
    root_page_->RangeFilter(opts, filter, scan_opts, views);
  }

  std::string_view GetRootPageKey() const noexcept {
    return root_page_->GetPageKey();
  }

  // TODO(sheep): support SMO interface

  common::LockTable &GetLockTable() noexcept {
    return root_page_->GetLockTable();
  }

private:
  Status GetRowMultilevel_(property::SortKeysRef sort_key, TxnTs read_ts,
                           const Options &opts, RowView *view) const noexcept;

  cache::BufferPool::PageHolder root_page_;
};

} // namespace btree
} // namespace arcanedb