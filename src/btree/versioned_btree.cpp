/**
 * @file versioned_btree.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/versioned_btree.h"
#include "common/macros.h"

namespace arcanedb {
namespace btree {

Status VersionedBtree::SetRow(const property::Row &row, TxnTs write_ts,
                              const Options &opts) noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->SetRow(row, write_ts, opts);
    break;
  }
  case PageType::InternalPage: {
    NOTIMPLEMENTED();
    break;
  }
  }
  return s;
}

Status VersionedBtree::DeleteRow(property::SortKeysRef sort_key, TxnTs write_ts,
                                 const Options &opts) noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->DeleteRow(sort_key, write_ts, opts);
    break;
  }
  case PageType::InternalPage: {
    NOTIMPLEMENTED();
    break;
  }
  }
  return s;
}

Status VersionedBtree::SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
                             const Options &opts) noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->SetTs(sort_key, target_ts, opts);
    break;
  }
  case PageType::InternalPage: {
    NOTIMPLEMENTED();
    break;
  }
  }
  return s;
}

Status VersionedBtree::GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                              const Options &opts, RowView *view) const
    noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->GetRow(sort_key, read_ts, opts, view);
    break;
  }
  case PageType::InternalPage: {
    s = GetRowMultilevel_(sort_key, read_ts, opts, view);
    break;
  }
  default:
    UNREACHABLE();
  }
  return s;
}

Status VersionedBtree::GetRowMultilevel_(property::SortKeysRef sort_key,
                                         TxnTs read_ts, const Options &opts,
                                         RowView *view) const noexcept {
  InternalRowView internal_view;
  auto s = root_page_->GetPageId(opts, sort_key, &internal_view);
  if (unlikely(!s.ok())) {
    return s;
  }
  NOTIMPLEMENTED();
}

} // namespace btree
} // namespace arcanedb