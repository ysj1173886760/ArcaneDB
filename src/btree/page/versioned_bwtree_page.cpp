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
#include "bthread/bthread.h"
#include "btree/page/page_snapshot.h"
#include "btree/page/versioned_delta_node.h"
#include "butil/object_pool.h"
#include "bwtree_page.h"
#include "common/config.h"
#include "util/monitor.h"
#include "wal/bwtree_log_writer.h"
#include <cmath>
#include <optional>

namespace arcanedb {
namespace btree {

std::shared_ptr<VersionedDeltaNode>
VersionedBwTreePage::Compaction_(VersionedDeltaNode *current_ptr,
                                 bool force_compaction) noexcept {
  // write_mu_.AssertHeld();
  auto current = current_ptr->GetPrevious();
  VersionedDeltaNodeBuilder builder;
  builder.AddDeltaNode(current_ptr);
  // simple stragty
  // TODO(sheep): optimize compaction stragty
  // currently write amplification is too much.
  while (current != nullptr &&
         (current->GetSize() == 1 ||
          builder.GetRowSize() * common::Config::kBwTreeCompactionFactor >
              current->GetSize() ||
          force_compaction)) {
    builder.AddDeltaNode(current.get());
    current = current->GetPrevious();
  }
  auto new_node = builder.GenerateDeltaNode();
  new_node->SetPrevious(current);
  return new_node;
}

void VersionedBwTreePage::MaybePerformCompaction_(
    const Options &opts, VersionedDeltaNode *current_ptr) noexcept {
  // write_mu_.AssertHeld();
  auto total_size = current_ptr->GetTotalLength();
  if ((!opts.disable_compaction &&
       total_size > common::Config::kBwTreeDeltaChainLength) ||
      opts.force_compaction) {
    auto new_ptr = Compaction_(current_ptr, opts.force_compaction);
    UpdatePtr_(new_ptr);
  }
}

void AppendLogAndSetLsn_(log_store::LogStore *log_store, WriteInfo *info,
                         const wal::BwTreeLogWriter &log_writer) noexcept {
  log_store::LogStore::LogResultContainer result;
  log_store->AppendLogRecord(log_writer.GetLogRecords(), &result);
  info->lsn = result[0].end_lsn;
}

Status VersionedBwTreePage::SetRow(const property::Row &row, TxnTs write_ts,
                                   const Options &opts,
                                   WriteInfo *info) noexcept {
  // make delta
  auto delta = std::make_shared<VersionedDeltaNode>(row, write_ts);

  // write log
  wal::BwTreeLogWriter log_writer;
  if (opts.log_store != nullptr) {
    log_writer.SetRow(page_id_, opts.txn_id, write_ts, row);
  }

  ArcanedbLockGuard<ArcanedbLock> guard(write_mu_);
  // check intent locked
  if (opts.check_intent_locked && CheckRowLocked_(row.GetSortKeys(), opts)) {
    return Status::TxnConflict();
  }

  // append log
  if (opts.log_store != nullptr) {
    AppendLogAndSetLsn_(opts.log_store, info, log_writer);
    delta->SetLSN(info->lsn);
  }
  info->is_dirty = true;
  total_charge_ += delta->GetTotalCharge();

  // prepend delta
  auto current_ptr = GetPtr_();
  delta->SetPrevious(std::move(current_ptr));
  UpdatePtr_(delta);

  // perform compaction
  MaybePerformCompaction_(opts, delta.get());
  return Status::Ok();
}

Status VersionedBwTreePage::DeleteRow(property::SortKeysRef sort_key,
                                      TxnTs write_ts, const Options &opts,
                                      WriteInfo *info) noexcept {
  // make delta
  auto delta = std::make_shared<VersionedDeltaNode>(sort_key, write_ts);

  // write log
  wal::BwTreeLogWriter log_writer;
  if (opts.log_store != nullptr) {
    log_writer.DeleteRow(page_id_, opts.txn_id, write_ts, sort_key);
  }

  ArcanedbLockGuard<ArcanedbLock> guard(write_mu_);
  // check intent locked
  if (opts.check_intent_locked && CheckRowLocked_(sort_key, opts)) {
    return Status::TxnConflict();
  }

  // append log
  if (opts.log_store != nullptr) {
    AppendLogAndSetLsn_(opts.log_store, info, log_writer);
    delta->SetLSN(info->lsn);
  }
  info->is_dirty = true;
  total_charge_ += delta->GetTotalCharge();

  // prepend delta
  auto current_ptr = GetPtr_();
  delta->SetPrevious(std::move(current_ptr));
  UpdatePtr_(delta);

  // compaction
  MaybePerformCompaction_(opts, delta.get());
  return Status::Ok();
}

Status VersionedBwTreePage::GetRow(property::SortKeysRef sort_key,
                                   TxnTs read_ts, const Options &opts,
                                   RowView *view) const noexcept {
  while (true) {
    auto s = GetRowOnce_(sort_key, read_ts, opts, view);
    if (!s.IsRetry()) {
      return s;
    }
    // sleep 20 microseconds
    bthread_usleep(20);
  }
  UNREACHABLE();
}

Status VersionedBwTreePage::GetRowOnce_(property::SortKeysRef sort_key,
                                        TxnTs read_ts, const Options &opts,
                                        RowView *view) const noexcept {
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  while (current_ptr != nullptr) {
    auto s = current_ptr->GetRow(sort_key, read_ts, opts, view);
    if (s.ok()) {
      return Status::Ok();
    } else if (s.IsDeleted()) {
      return Status::NotFound();
    } else if (s.IsRowLocked()) {
      return Status::Retry();
    }
    current_ptr = current_ptr->GetPrevious().get();
  }
  return Status::NotFound();
}

bool VersionedBwTreePage::CheckRowLocked_(property::SortKeysRef sort_key,
                                          const Options &opts) const noexcept {
  RowView view;
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // read by using max ts.
  TxnTs read_ts = kMaxTxnTs;
  // traverse the delta node
  while (current_ptr != nullptr) {
    auto s = current_ptr->GetRow(sort_key, read_ts, opts, &view);
    if (s.IsRowLocked()) {
      return true;
    } else if (s.IsDeleted() || s.ok()) {
      return false;
    }
    current_ptr = current_ptr->GetPrevious().get();
  }
  return false;
}

void VersionedBwTreePage::SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
                                const Options &opts, WriteInfo *info) noexcept {
  // write log
  wal::BwTreeLogWriter log_writer;
  if (opts.log_store != nullptr) {
    log_writer.SetTs(page_id_, opts.txn_id, target_ts, sort_key);
  }

  // acquire write lock
  ArcanedbLockGuard<ArcanedbLock> guard(write_mu_);
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();

  // append log
  if (opts.log_store != nullptr) {
    AppendLogAndSetLsn_(opts.log_store, info, log_writer);
  }
  info->is_dirty = true;

  while (current_ptr != nullptr) {
    auto s = current_ptr->SetTs(sort_key, target_ts, info->lsn);
    if (likely(s.ok())) {
      // need to perform dummy update,
      // so that readers afterward will see our update.
      DummyUpdate_();
      return;
    }
    DCHECK(s.IsNotFound());
    current_ptr = current_ptr->GetPrevious().get();
  }
  UNREACHABLE();
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
      result += fmt::format("ts:{} del:{}, ", entry.write_ts, entry.is_deleted);
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
    std::optional<TxnTs> last_valid_ts{std::nullopt};
    for (int i = 0; i < vec.size(); i++) {
      if (vec[i].write_ts != kAbortedTxnTs) {
        if (last_valid_ts.has_value() && vec[i].write_ts >= *last_valid_ts) {
          return false;
        }
        last_valid_ts = vec[i].write_ts;
      }
    }
  }
  return true;
}

bool VersionedBwTreePage::TEST_Equal(const VersionedBwTreePage &rhs) const
    noexcept {
  return TEST_DumpPage() == rhs.TEST_DumpPage();
}

/**
 * @brief
 * Format:
 * | lsn 8byte | row1 v0 | row1 v1 | ... | rowN v0 | ...
 * Row format:
 * | delete bit 1byte | write_ts 4byte | row varlen |
 */
std::unique_ptr<PageSnapshot> VersionedBwTreePage::GetPageSnapshot() noexcept {
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
    TxnTs write_ts;
  };
  std::map<property::SortKeysRef, std::vector<BuildEntry>> map;
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  // traverse the delta node
  log_store::LsnType lsn{};
  while (current_ptr != nullptr) {
    constexpr bool should_lock = true;
    auto tmp_lsn = current_ptr->Traverse(
        [&](const property::Row &row, bool is_deleted, TxnTs write_ts) {
          map[row.GetSortKeys()].emplace_back(BuildEntry{
              .row = row, .is_deleted = is_deleted, .write_ts = write_ts});
        },
        should_lock);
    current_ptr = current_ptr->GetPrevious().get();
    lsn = std::max(lsn, tmp_lsn);
  }

  util::BufWriter writer;
  writer.WriteBytes(lsn);
  auto serialize_row = [](util::BufWriter *writer, const property::Row &row,
                          bool is_deleted, TxnTs write_ts) {
    writer->WriteBytes(static_cast<uint8_t>(is_deleted));
    writer->WriteBytes(write_ts);
    writer->WriteBytes(row.as_slice());
  };
  for (const auto &[k, vec] : map) {
    for (const auto &entry : vec) {
      serialize_row(&writer, entry.row, entry.is_deleted, entry.write_ts);
    }
  }

  return std::make_unique<VersionedBwTreePageSnapshot>(writer.Detach(), lsn);
}

Status VersionedBwTreePage::Deserialize(std::string_view data) noexcept {
  std::map<property::SortKeysRef,
           std::vector<VersionedDeltaNodeBuilder::BuildEntry>>
      map;

  util::BufReader reader(data);
  log_store::LsnType lsn;
  reader.ReadBytes(&lsn);

  auto deserialize_entry = [&]() {
    uint8_t is_deleted;
    TxnTs write_ts;
    reader.ReadBytes(&is_deleted);
    reader.ReadBytes(&write_ts);
    auto row = property::Row(reader.CurrentPtr());
    reader.Skip(row.as_slice().size());
    return VersionedDeltaNodeBuilder::BuildEntry{
        .row = row, .is_deleted = (is_deleted != 0), .write_ts = write_ts};
  };

  util::BufWriter writer;
  util::BufWriter version_writer;
  std::vector<VersionedDeltaNode::Entry> rows;
  VersionedDeltaNode::VersionContainer versions;
  bool has_version = false;

  property::SortKeysRef sk;
  while (reader.Remaining() != 0) {
    auto entry = deserialize_entry();
    if (!sk.empty() && entry.row.GetSortKeys() == sk) {
      // old version
      VersionedDeltaNodeBuilder::WriteRow_(versions.back(), &version_writer,
                                           entry);
      has_version = true;
    } else {
      // newest version
      VersionedDeltaNodeBuilder::WriteRow_(rows, &writer, entry);
      versions.push_back({});
    }
  }
  if (!has_version) {
    versions.clear();
  }

  auto delta = std::make_shared<VersionedDeltaNode>(
      writer.Detach(), version_writer.Detach(), std::move(rows),
      std::move(versions));
  UpdatePtr_(delta);
  return Status::Ok();
}

bool VersionedBwTreePage::FinishFlush(const Status &s,
                                      log_store::LsnType lsn) noexcept {}

void VersionedBwTreePage::RangeFilter(const Options &opts, const Filter &filter,
                                      const BtreeScanOpts &scan_opts,
                                      RangeScanRowView *views) const noexcept {
  auto shared_ptr = GetPtr_();
  auto current_ptr = shared_ptr.get();
  int cnt = 0;
  while (current_ptr) {
    current_ptr->Traverse(
        [&](const property::Row &row, bool is_deleted, TxnTs write_ts) {
          if (!is_deleted) {
            views->PushBackRef(RowRef(row));
          }
        });
    current_ptr = current_ptr->GetPrevious().get();
    cnt += 1;
  }

  if (cnt > 1) {
    std::sort(views->begin(), views->end(),
              [](const RowRef &lhs, const RowRef &rhs) {
                return lhs.GetSortKeys() < rhs.GetSortKeys();
              });
  }
  if (scan_opts.remove_duplicate) {
    // TODO(sheep): impl me.
  }
}

} // namespace btree
} // namespace arcanedb