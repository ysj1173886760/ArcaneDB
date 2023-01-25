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
    auto new_ptr = Compaction_(current_ptr);
    UpdatePtr_(new_ptr);
  }
}

Status VersionedBwTreePage::SetRow(const property::Row &row, TxnTs write_ts,
                                   const Options &opts) noexcept {
  {
    auto delta = std::make_shared<VersionedDeltaNode>(row, write_ts);
    util::InstrumentedLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  return Status::Ok();
}

Status VersionedBwTreePage::DeleteRow(property::SortKeysRef sort_key,
                                      TxnTs write_ts,
                                      const Options &opts) noexcept {
  {
    auto delta = std::make_shared<VersionedDeltaNode>(sort_key, write_ts);
    util::InstrumentedLockGuard<ArcanedbLock> guard(write_mu_);
    auto current_ptr = GetPtr_();
    delta->SetPrevious(std::move(current_ptr));
    UpdatePtr_(delta);
    MaybePerformCompaction_(opts, delta.get());
  }
  return Status::Ok();
}

Status VersionedBwTreePage::GetRow(property::SortKeysRef sort_key,
                                   TxnTs read_ts, const Options &opts,
                                   RowView *view) const noexcept {
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  view->AddOwnerPointer(shared_ptr);
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

std::string VersionedBwTreePage::TEST_DumpPage() const noexcept {
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
    TxnTs write_ts;
  };
  std::map<property::SortKeysRef, std::vector<BuildEntry>> map;
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  while (current_ptr != nullptr) {
    current_ptr->Traverse(
        [&](const property::Row &row, bool is_deleted, TxnTs write_ts) {
          map[row.GetSortKeys()].emplace_back(BuildEntry{
              .row = row, .is_deleted = is_deleted, .write_ts = write_ts});
        });
    current_ptr = current_ptr->GetPrevious().get();
  }
  std::string result = "BwTreePageDump:\n";
  for (const auto &[sk, vec] : map) {
    result += sk.ToString() + ", ";
    for (const auto &entry : vec) {
      result += fmt::format("{} {}, ", entry.write_ts, entry.is_deleted);
    }
    result += "\n";
  }
  return result;
}

bool VersionedBwTreePage::TEST_TsDesending() const noexcept {
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
    TxnTs write_ts;
  };
  std::map<property::SortKeysRef, std::vector<BuildEntry>> map;
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  while (current_ptr != nullptr) {
    current_ptr->Traverse(
        [&](const property::Row &row, bool is_deleted, TxnTs write_ts) {
          map[row.GetSortKeys()].emplace_back(BuildEntry{
              .row = row, .is_deleted = is_deleted, .write_ts = write_ts});
        });
    current_ptr = current_ptr->GetPrevious().get();
  }
  for (const auto &[sk, vec] : map) {
    for (int i = 1; i < vec.size(); i++) {
      if (vec[i].write_ts >= vec[i - 1].write_ts) {
        return false;
      }
    }
  }
  return true;
}

} // namespace btree
} // namespace arcanedb