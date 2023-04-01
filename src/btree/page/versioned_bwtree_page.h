/**
 * @file versioned_bwtree_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-23
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree_type.h"
#include "btree/page/page_snapshot.h"
#include "btree/page/versioned_delta_node.h"
#include "btree/write_info.h"
#include "butil/containers/doubly_buffered_data.h"
#include "common/btree_scan_opts.h"
#include "common/filter.h"
#include "common/lock_table.h"
#include "common/options.h"
#include "common/status.h"
#include "property/row/row.h"
#include <atomic>

namespace arcanedb {
namespace btree {

class VersionedBwTreePage {
public:
  VersionedBwTreePage(const std::string_view &page_id) noexcept
      : page_id_(page_id) {}

  std::string_view GetPageKey() const noexcept { return page_id_; }

  const std::string GetPageKeyRef() const noexcept { return page_id_; }

  /**
   * @brief
   * Insert a row into page
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
   * @return Status
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

  common::LockTable &GetLockTable() noexcept { return lock_table_; }

  /**
   * @brief Get page snapshot which is used to flush page
   * to persistent storage
   * @return std::unique_ptr<PageSnapshot>
   */
  std::unique_ptr<PageSnapshot> GetPageSnapshot() noexcept;

  /**
   * @brief
   * Update flushed lsn.
   * @param s Flush status
   * @param lsn flushed lsn
   * @return true when page still need to flush.
   * @return false when page doesn't need to flush.
   */
  bool FinishFlush(const Status &s, log_store::LsnType lsn) noexcept;

  /**
   * @brief
   * Deserialize page from data.
   * @param data
   * @return Status
   */
  Status Deserialize(std::string_view data) noexcept;

  size_t GetTotalCharge() noexcept {
    return total_charge_.load(std::memory_order_relaxed);
  }

  void RangeFilter(const Options &opts, const Filter &filter,
                   const BtreeScanOpts &scan_opts,
                   RangeScanRowView *views) const noexcept;

  size_t TEST_GetDeltaLength() const noexcept {
    auto ptr = GetPtr_();
    return ptr->GetTotalLength();
  }

  std::string TEST_DumpPage() const noexcept;

  bool TEST_TsDesending() const noexcept;

  bool TEST_Equal(const VersionedBwTreePage &rhs) const noexcept;

private:
  using DoublyBufferedData =
      butil::DoublyBufferedData<std::shared_ptr<VersionedDeltaNode>>;

  std::shared_ptr<VersionedDeltaNode>
  Compaction_(VersionedDeltaNode *current_ptr, bool force_compaction) noexcept;

  void MaybePerformCompaction_(const Options &opts,
                               VersionedDeltaNode *current_ptr) noexcept;

  Status GetRowOnce_(property::SortKeysRef sort_key, TxnTs read_ts,
                     const Options &opts, RowView *view) const noexcept;

  bool CheckRowLocked_(property::SortKeysRef sort_key,
                       const Options &opts) const noexcept;

  std::shared_ptr<VersionedDeltaNode> GetPtr_() const noexcept {
    DoublyBufferedData::ScopedPtr scoped_ptr;
    CHECK(ptr_.Read(&scoped_ptr) == 0);
    return *scoped_ptr;
  }

  void UpdatePtr_(std::shared_ptr<VersionedDeltaNode> new_node) const noexcept {
    ptr_.Modify(UpdateFn_, new_node);
  }

  void DummyUpdate_() const noexcept { ptr_.Modify(DummyFn_); }

  static bool DummyFn_(std::shared_ptr<VersionedDeltaNode> &old_node) noexcept {
    return true;
  }

  static bool UpdateFn_(std::shared_ptr<VersionedDeltaNode> &old_node,
                        std::shared_ptr<VersionedDeltaNode> new_node) noexcept {
    old_node = new_node;
    return true;
  }

  // TODO(sheep) use group commit to optimize write performance
  // mutable ArcanedbLock write_mu_{"VersionedBwTreePageWriteMutex"};
  mutable ArcanedbLock write_mu_;
  mutable DoublyBufferedData ptr_;
  common::LockTable lock_table_;
  const std::string page_id_;
  std::atomic<size_t> total_charge_{sizeof(VersionedBwTreePage)};
};

class VersionedBwTreePageSnapshot : public PageSnapshot {
public:
  VersionedBwTreePageSnapshot(std::string bytes,
                              log_store::LsnType lsn) noexcept
      : lsn_(lsn), bytes_(std::move(bytes)) {}

  ~VersionedBwTreePageSnapshot() noexcept override {}

  std::string Serialize() noexcept override { return std::move(bytes_); }

  log_store::LsnType GetLSN() noexcept override { return lsn_; }

private:
  log_store::LsnType lsn_{};
  std::string bytes_;
};

} // namespace btree
} // namespace arcanedb