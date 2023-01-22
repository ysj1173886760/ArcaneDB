/**
 * @file btree.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/btree.h"
#include "common/macros.h"

namespace arcanedb {
namespace btree {

Status Btree::SetRow(const property::Row &row, const Options &opts) noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->SetRow(row, opts);
    break;
  }
  case PageType::InternalPage: {
    NOTIMPLEMENTED();
    break;
  }
  }
  return s;
}

Status Btree::DeleteRow(property::SortKeysRef sort_key,
                        const Options &opts) noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->DeleteRow(sort_key, opts);
    break;
  }
  case PageType::InternalPage: {
    NOTIMPLEMENTED();
    break;
  }
  }
  return s;
}

Status Btree::GetRow(property::SortKeysRef sort_key, const Options &opts,
                     RowView *view) const noexcept {
  auto page_type = root_page_->GetPageType();
  Status s;
  switch (page_type) {
  case PageType::LeafPage: {
    s = root_page_->GetRow(sort_key, opts, view);
    break;
  }
  case PageType::InternalPage: {
    s = GetRowMultilevel_(sort_key, opts, view);
    break;
  }
  default:
    UNREACHABLE();
  }
  return s;
}

Status Btree::GetRowMultilevel_(property::SortKeysRef sort_key,
                                const Options &opts, RowView *view) const
    noexcept {
  InternalRowView internal_view;
  auto s = root_page_->GetPageId(opts, sort_key, &internal_view);
  if (unlikely(!s.ok())) {
    return s;
  }
  NOTIMPLEMENTED();
}

} // namespace btree
} // namespace arcanedb