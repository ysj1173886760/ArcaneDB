/**
 * @file versioned_bwtree_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/versioned_bwtree_page.h"
#include "common/config.h"

namespace arcanedb {
namespace btree {

std::shared_ptr<VersionedDeltaNode>
VersionedBwTreePage::Compaction_(VersionedDeltaNode *current_ptr) noexcept {
  write_mu_.AssertHeld();
  auto current = current_ptr->GetPrevious();
  VersionedDeltaNodeBuilder builder;
  builder.AddDeltaNode(current_ptr);
  // simple stragty
  // TODO(sheep): optimize compaction stragty
  // currently write amplification is too much.
  while (current != nullptr) {
    builder.AddDeltaNode(current.get());
    current = current->GetPrevious();
  }
  auto new_node = builder.GenerateDeltaNode();
  new_node->SetPrevious(current);
  return new_node;
}

void VersionedBwTreePage::MaybePerformCompaction_(
    const Options &opts, VersionedDeltaNode *current_ptr) noexcept {
  write_mu_.AssertHeld();
  auto total_size = current_ptr->GetTotalLength();
  if (!opts.disable_compaction &&
      total_size > common::Config::kBwTreeDeltaChainLength) {
    util::Timer timer;
    auto new_ptr = Compaction_(current_ptr);
    compaction_ << timer.GetElapsed();
    UpdatePtr_(new_ptr);
  }
}

Status VersionedBwTreePage::SetRow(const property::Row &row, TxnTs write_ts,
                                   const Options &opts) noexcept {
  util::Timer timer;
  {
    auto delta = std::make_shared<VersionedDeltaNode>(row, write_ts);
    util::InstrumentedLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  write_ << timer.GetElapsed();
  return Status::Ok();
}

Status VersionedBwTreePage::DeleteRow(property::SortKeysRef sort_key,
                                      TxnTs write_ts,
                                      const Options &opts) noexcept {
  util::Timer timer;
  {
    auto delta = std::make_shared<VersionedDeltaNode>(sort_key, write_ts);
    util::InstrumentedLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  write_ << timer.GetElapsed();
  return Status::Ok();
}

Status VersionedBwTreePage::GetRow(property::SortKeysRef sort_key,
                                   TxnTs read_ts, const Options &opts,
                                   RowView *view) const noexcept {
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  while (current_ptr != nullptr) {
    auto s = current_ptr->GetRow(sort_key, read_ts, view);
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