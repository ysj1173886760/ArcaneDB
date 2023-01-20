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
#include "common/config.h"

namespace arcanedb {
namespace btree {

std::shared_ptr<DeltaNode> BwTreePage::Compaction_() noexcept {
  ptr_mu_.AssertHeld();
  auto current = ptr_->GetPrevious();
  DeltaNodeBuilder builder;
  builder.AddDeltaNode(ptr_.get());
  // simple stragty
  while (current != nullptr) {
    builder.AddDeltaNode(current.get());
    current = current->GetPrevious();
  }
  return builder.GenerateDeltaNode();
}

void BwTreePage::MaybePerformCompaction_(const Options &opts) noexcept {
  ptr_mu_.AssertHeld();
  auto total_size = ptr_->GetTotalLength();
  if (!opts.disable_compaction &&
      total_size > common::Config::kBwTreeDeltaChainLength) {
    auto new_ptr = Compaction_();
    ptr_ = std::move(new_ptr);
  }
}

Status BwTreePage::SetRow(const property::Row &row,
                          const Options &opts) noexcept {
  {
    auto delta = std::make_shared<DeltaNode>(row);
    util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
    delta->SetPrevious(std::move(ptr_));
    ptr_ = std::move(delta);
    MaybePerformCompaction_(opts);
  }
  return Status::Ok();
}

Status BwTreePage::DeleteRow(property::SortKeysRef sort_key,
                             const Options &opts) noexcept {
  {
    auto delta = std::make_shared<DeltaNode>(sort_key);
    util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
    delta->SetPrevious(std::move(ptr_));
    ptr_ = std::move(delta);
    MaybePerformCompaction_(opts);
  }
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