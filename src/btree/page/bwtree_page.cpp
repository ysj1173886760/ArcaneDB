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
                          property::Row *res) const noexcept {
  std::shared_ptr<DeltaNode> current_ptr;
  {
    util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
    current_ptr = ptr_;
  }
  // traverse the delta node
  while (current_ptr != nullptr) {
    auto s = current_ptr->GetRow(sort_key, res);
    if (s.ok()) {
      return Status::Ok();
    }
    if (s.IsDeleted()) {
      return Status::NotFound();
    }
    current_ptr = current_ptr->GetPrevious();
  }
  return Status::NotFound();
}

} // namespace btree
} // namespace arcanedb