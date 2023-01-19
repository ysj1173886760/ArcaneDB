/**
 * @file bwtree_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/bwtree_page.h"
#include "butil/logging.h"

namespace arcanedb {
namespace btree {

Status BwTreePage::InsertRow(const property::Row &row,
                             const Options &opts) noexcept {
  auto delta = std::make_shared<DeltaNode>(row);
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
  delta->SetPrevious(std::move(ptr_));
  ptr_ = std::move(delta);
  return Status::Ok();
}

Status BwTreePage::UpdateRow(const property::Row &row,
                             const Options &opts) noexcept {
  auto delta = std::make_shared<DeltaNode>(row);
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
  delta->SetPrevious(std::move(ptr_));
  ptr_ = std::move(delta);
  return Status::Ok();
}

Status BwTreePage::DeleteRow(property::SortKeysRef sort_key,
                             const Options &opts) noexcept {
  auto delta = std::make_shared<DeltaNode>(sort_key);
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
  delta->SetPrevious(std::move(ptr_));
  ptr_ = std::move(delta);
  return Status::Ok();
}

Status BwTreePage::GetRow(property::SortKeysRef sort_key, const Options &opts,
                          RowRef *row_ref) const noexcept {
  NOTIMPLEMENTED();
}

} // namespace btree
} // namespace arcanedb