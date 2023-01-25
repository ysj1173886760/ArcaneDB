/**
 * @file txn_context.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "txn/txn_context.h"
#include "txn_type.h"

namespace arcanedb {
namespace txn {

Status TxnContext::SetRow(const std::string &sub_table_key,
                          const property::Row &row,
                          const Options &opts) noexcept {
  auto sub_table = GetSubTable_(sub_table_key, opts);
  auto s = AcquireLock_(sub_table_key, row.GetSortKeys().ToString());
  if (unlikely(!s.ok())) {
    return s;
  }
  return sub_table->SetRow(row, txn_ts_, opts);
}

Status TxnContext::DeleteRow(const std::string &sub_table_key,
                             property::SortKeysRef sort_key,
                             const Options &opts) noexcept {
  auto sub_table = GetSubTable_(sub_table_key, opts);
  auto s = AcquireLock_(sub_table_key, sort_key.as_slice());
  if (unlikely(!s.ok())) {
    return s;
  }
  return sub_table->DeleteRow(sort_key, txn_ts_, opts);
}

Status TxnContext::GetRow(const std::string &sub_table_key,
                          property::SortKeysRef sort_key, const Options &opts,
                          btree::RowView *view) noexcept {
  auto sub_table = GetSubTable_(sub_table_key, opts);
  if (txn_type_ == TxnType::ReadWriteTxn) {
    // acquire lock
    auto s = AcquireLock_(sub_table_key, sort_key.as_slice());
    if (unlikely(!s.ok())) {
      return s;
    }
  }
  return sub_table->GetRow(sort_key, txn_ts_, opts, view);
}

Status TxnContext::AcquireLock_(const std::string &sub_table_key,
                                std::string_view sort_key) noexcept {
  // concat the subtable key and sortkey here.
  // user's subtable key and sortkey couldn't contains #
  // since it is used as delimiter here.
  std::string lock_key = sub_table_key;
  lock_key.append("#");
  lock_key.append(sort_key);
  if (!lock_set_.count(lock_key)) {
    auto s = lock_table_->Lock(lock_key, txn_id_);
    lock_set_.insert(std::move(lock_key));
    return s;
  }
  return Status::Ok();
}

btree::SubTable *TxnContext::GetSubTable_(const std::string &sub_table_key,
                                          const Options &opts) noexcept {
  auto it = tables_.find(sub_table_key);
  if (it != tables_.end()) {
    return it->second.get();
  }
  std::unique_ptr<btree::SubTable> table;
  auto s = btree::SubTable::OpenSubTable(sub_table_key, opts, &table);
  CHECK(s.ok());
  auto [new_it, succeed] =
      tables_.emplace(table->GetTableKey(), std::move(table));
  CHECK(succeed);
  return new_it->second.get();
}

void TxnContext::Commit() noexcept {
  if (txn_type_ == TxnType::ReadOnlyTxn) {
    return;
  }
  // TODO(sheep): wait WAL
  // release all lock
  for (const auto &lock : lock_set_) {
    lock_table_->Unlock(lock, txn_id_);
  }
  snapshot_manager_->CommitTs(txn_ts_);
}

} // namespace txn
} // namespace arcanedb
