/**
 * @file versioned_btree_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree_type.h"
#include "btree/page/internal_page.h"
#include "btree/page/page_snapshot.h"
#include "btree/page/versioned_bwtree_page.h"
#include "btree/write_info.h"
#include "common/btree_scan_opts.h"
#include "common/filter.h"
#include <mutex>

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Real page type.
 * IndexPage could either be leaf page or internal page.
 */
class VersionedBtreePage {

public:
  /**
   * @brief
   * Default ctor, construct leaf page.
   */
  VersionedBtreePage(const std::string_view &page_id) noexcept {
    leaf_page_ = std::make_unique<VersionedBwTreePage>(page_id);
    ModifyPageType(PageType::LeafPage);
  }

  /**
   * @brief
   * Meta interfaces
   */

  /**
   * @brief
   * Get page type
   * @return PageType
   */
  PageType GetPageType() const noexcept {
    return page_type_.load(std::memory_order_acquire);
  }

  /**
   * @brief
   * Modify page type
   * @param type
   */
  void ModifyPageType(PageType type) noexcept {
    page_type_.store(type, std::memory_order_release);
  }

  std::string_view GetPageKey() const noexcept {
    assert(leaf_page_);
    return leaf_page_->GetPageKey();
  }

  const std::string GetPageKeyRef() const noexcept {
    assert(leaf_page_);
    return leaf_page_->GetPageKeyRef();
  }

  /**
   * @brief
   * Interfaces for leaf page type
   */

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
    assert(leaf_page_);
    auto s = leaf_page_->SetRow(row, write_ts, opts, info);
    if (!s.ok()) {
      return s;
    }
    if (info->is_dirty) {
      std::lock_guard<decltype(mu_)> guard(mu_);
      TryMarkDirtyInLock_();
      UpdateAppliedLSN_(info->lsn);
    }
    return Status::Ok();
  }

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
                   const Options &opts, WriteInfo *info) noexcept {
    assert(leaf_page_);
    auto s = leaf_page_->DeleteRow(sort_key, write_ts, opts, info);
    if (!s.ok()) {
      return s;
    }
    if (info->is_dirty) {
      std::lock_guard<decltype(mu_)> guard(mu_);
      TryMarkDirtyInLock_();
      UpdateAppliedLSN_(info->lsn);
    }
    return Status::Ok();
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
    assert(leaf_page_);
    return leaf_page_->GetRow(sort_key, read_ts, opts, view);
  }

  /**
   * @brief
   * Set ts of the newest version with "sort_key" to "target_ts"
   * @param sort_key
   * @param target_ts
   * @param opts
   * @param info
   */
  void SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
             const Options &opts, WriteInfo *info) noexcept {
    assert(leaf_page_);
    leaf_page_->SetTs(sort_key, target_ts, opts, info);
    if (info->is_dirty) {
      std::lock_guard<decltype(mu_)> guard(mu_);
      TryMarkDirtyInLock_();
      UpdateAppliedLSN_(info->lsn);
    }
  }

  /**
   * @brief
   * Interfaces for internal page type
   */

  /**
   * @brief
   * Get page id though sort_key
   * @param opts
   * @param sort_key
   * @param view
   * @return Status
   */
  Status GetPageId(const Options &opts, property::SortKeysRef sort_key,
                   InternalRowView *view) const noexcept {
    assert(internal_page_);
    return internal_page_->GetPageId(opts, sort_key, view);
  }

  /**
   * @brief
   * Inserting internal rows into internal page, corresponding to btree split
   * operation. If old_sort_key is not empty, we will use new_internal_rows to
   * replace the entry which has same sort_key with old_sort_key. otherwise, we
   * will use new_internal_rows to overwrite the entire internal page.
   * @param opts
   * @param old_sort_key
   * @param new_internal_rows
   * @return Status
   */
  Status Split(const Options &opts, property::SortKeysRef old_sort_key,
               std::vector<InternalRow> new_internal_rows) noexcept {
    assert(internal_page_);
    return internal_page_->Split(opts, old_sort_key,
                                 std::move(new_internal_rows));
  }

  common::LockTable &GetLockTable() noexcept {
    assert(leaf_page_);
    return leaf_page_->GetLockTable();
  }

  /**
   * @brief Get page snapshot which is used to flush page
   * to persistent storage
   * @return std::unique_ptr<PageSnapshot>
   */
  std::unique_ptr<PageSnapshot> GetPageSnapshot() noexcept {
    assert(leaf_page_);
    return leaf_page_->GetPageSnapshot();
  }

  /**
   * @brief
   * Update flushed lsn.
   * @param s Flush status
   * @param lsn flushed lsn
   * @return true when page still need to flush.
   * @return false when page doesn't need to flush.
   */
  bool FinishFlush(const Status &s, log_store::LsnType lsn) noexcept {
    std::lock_guard<decltype(mu_)> guard(mu_);
    if (s.ok()) {
      flushed_lsn_ = std::max(lsn, flushed_lsn_);
    }
    return NeedFlush_();
  }

  /**
   * @brief
   * Deserialize page from data.
   * @param data
   * @return Status
   */
  Status Deserialize(std::string_view data) noexcept {
    assert(leaf_page_);
    return leaf_page_->Deserialize(data);
  }

  size_t GetTotalCharge() noexcept {
    assert(leaf_page_);
    return leaf_page_->GetTotalCharge();
  }

  void RangeFilter(const Options &opts, const Filter &filter,
                   const BtreeScanOpts &scan_opts,
                   RangeScanRowView *views) const noexcept {
    assert(leaf_page_);
    return leaf_page_->RangeFilter(opts, filter, scan_opts, views);
  }

  /**
   * @brief
   * Test code below
   */

  std::string TEST_DumpPage() noexcept {
    assert(leaf_page_);
    return leaf_page_->TEST_DumpPage();
  }

  bool TEST_TsDesending() noexcept {
    assert(leaf_page_);
    return leaf_page_->TEST_TsDesending();
  }

  bool TryMarkInFlusher() noexcept {
    std::lock_guard<decltype(mu_)> guard(mu_);
    if (page_state_ == PageState::kDirty) {
      page_state_ = PageState::kInFlusher;
      return true;
    }
    return false;
  }

private:
  enum class PageState : uint8_t {
    kUnDirty,
    kDirty,
    kInFlusher,
  };

  // require guarded by mu
  void TryMarkDirtyInLock_() noexcept {
    if (page_state_ == PageState::kUnDirty) {
      page_state_ = PageState::kDirty;
    }
  }

  // require guarded by mu
  void UpdateAppliedLSN_(log_store::LsnType lsn) noexcept {
    applied_lsn_ = std::max(lsn, applied_lsn_);
  }

  // require guarded by mu
  bool NeedFlush_() noexcept { return applied_lsn_ > flushed_lsn_; }

  // TODO(sheep): introduce Page interface
  // for different page type
  std::unique_ptr<VersionedBwTreePage> leaf_page_;
  std::unique_ptr<InternalPage> internal_page_;
  std::atomic<PageType> page_type_;

  bthread::Mutex mu_;
  PageState page_state_{PageState::kUnDirty};              // guarded by mu_
  log_store::LsnType flushed_lsn_{log_store::kInvalidLsn}; // guarded by mu_
  log_store::LsnType applied_lsn_{log_store::kInvalidLsn}; // guarded by mu_
};

} // namespace btree
} // namespace arcanedb