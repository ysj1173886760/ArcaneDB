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

std::shared_ptr<DeltaNode>
BwTreePage::Compaction_(DeltaNode *current_ptr) noexcept {
  write_mu_.AssertHeld();
  auto current = current_ptr->GetPrevious();
  DeltaNodeBuilder builder;
  builder.AddDeltaNode(current_ptr);
  // simple stragty
  while (current != nullptr) {
    builder.AddDeltaNode(current.get());
    current = current->GetPrevious();
  }
  auto new_node = builder.GenerateDeltaNode();
  new_node->SetPrevious(current);
  return new_node;
}

void BwTreePage::MaybePerformCompaction_(const Options &opts,
                                         DeltaNode *current_ptr) noexcept {
  write_mu_.AssertHeld();
  auto total_size = current_ptr->GetTotalLength();
  if (!opts.disable_compaction &&
      total_size > common::Config::kBwTreeDeltaChainLength) {
    auto new_ptr = Compaction_(current_ptr);
    UpdatePtr_(new_ptr);
  }
}

Status BwTreePage::SetRow(const property::Row &row,
                          const Options &opts) noexcept {
  {
    auto delta = std::make_shared<DeltaNode>(row);
    ArcanedbLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  return Status::Ok();
}

Status BwTreePage::DeleteRow(property::SortKeysRef sort_key,
                             const Options &opts) noexcept {
  {
    auto delta = std::make_shared<DeltaNode>(sort_key);
    ArcanedbLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  return Status::Ok();
}

Status BwTreePage::GetRow(property::SortKeysRef sort_key, const Options &opts,
                          RowView *view) const noexcept {
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  while (current_ptr != nullptr) {
    auto s = current_ptr->GetRow(sort_key, view);
    if (s.ok()) {
      return Status::Ok();
    }
    if (s.IsDeleted()) {
      return Status::NotFound();
    }
    current_ptr = current_ptr->GetPrevious().get();
  }
  return Status::NotFound();
}

} // namespace btree
} // namespace arcanedb