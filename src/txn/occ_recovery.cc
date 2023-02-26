/**
 * @file occ_recovery.cc
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-02-26
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include "txn/occ_recovery.h"
#include "wal/log_type.h"
#include "wal/bwtree_log_reader.h"
#include "wal/occ_log_reader.h"
#include "btree/page/versioned_btree_page.h"

namespace arcanedb {
namespace txn {

void OccRecovery::Recover() noexcept {
  while (log_reader_->HasNext()) {
    std::string log_record;
    log_reader_->GetNextLogRecord(&log_record);
    std::string_view log_data = log_record;
    auto type = wal::ParseLogRecord(&log_data);
    switch (type) {
      case wal::LogType::kBwtreeSetTs: {
        BwTreeSetTs_(log_data);
        break;
      }
      case wal::LogType::kBwtreeSetRow: {
        BwTreeSetRow_(log_data);
        break;
      }
      case wal::LogType::kBwtreeDeleteRow: {
        BwTreeDeleteRow_(log_data);
        break;
      }
      case wal::LogType::kOccBegin: {
        OccBegin_(log_data);
        break;
      }
      case wal::LogType::kOccAbort: {
        OccAbort_(log_data);
        break;
      }
      case wal::LogType::kOccCommit: {
        OccCommit_(log_data);
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  ARCANEDB_INFO("Recover done. txn map size: {}", txn_map_.size());
  for (const auto &[txn_id, cnt]: txn_map_) {
    ARCANEDB_INFO("TxnId: {}, Cnt: {}", txn_id, cnt);
  }
}

btree::VersionedBtreePage* GetPage_(cache::BufferPool *buffer_pool, const std::string_view &page_id) noexcept {
  btree::VersionedBtreePage *page;
  auto s = buffer_pool->GetPage<btree::VersionedBtreePage>(page_id, &page);
  CHECK(s.ok());
  return page;
}

void OccRecovery::BwTreeSetRow_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeSetRowLog(data);

  auto page = GetPage_(buffer_pool_, log.page_id);

  Options opts;
  btree::WriteInfo info;
  auto s = page->SetRow(log.row, log.write_ts, opts, &info);
  CHECK(s.ok());

  AddPrepare_(log.write_ts);
}

void OccRecovery::BwTreeDeleteRow_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeDeleteRowLog(data);

  auto page = GetPage_(buffer_pool_, log.page_id);

  Options opts;
  btree::WriteInfo info;
  auto s = page->DeleteRow(log.sort_key, log.write_ts, opts, &info);
  CHECK(s.ok());

  AddPrepare_(log.write_ts);
}

void OccRecovery::BwTreeSetTs_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeSetTsLog(data);

  auto page = GetPage_(buffer_pool_, log.page_id);

  Options opts;
  btree::WriteInfo info;
  page->SetTs(log.sort_key, log.commit_ts, opts, &info);

  AddCommit_(log.txn_id);
}

void OccRecovery::OccBegin_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeBeginLog(data);
  auto it = txn_map_.find(log.txn_id);
  CHECK(it == txn_map_.end());
  txn_map_.emplace(log.txn_id, 0);
}

void OccRecovery::OccAbort_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeAbortLog(data);
  auto it = txn_map_.find(log.txn_id);
  CHECK(it != txn_map_.end());
}

void OccRecovery::OccCommit_(const std::string_view &data) noexcept {
  auto log = wal::DeserializeCommitLog(data);
  auto it = txn_map_.find(log.txn_id);
  CHECK(it != txn_map_.end());
}

void OccRecovery::AddPrepare_(TxnId txn_id) noexcept {
  auto it = txn_map_.find(txn_id);
  CHECK(it != txn_map_.end());
  it->second += 1;
}

void OccRecovery::AddCommit_(TxnId txn_id) noexcept {
  auto it = txn_map_.find(txn_id);
  CHECK(it != txn_map_.end());
  it->second -= 1;
  if (it->second == 0) {
    txn_map_.erase(it);
  }
}

}
}