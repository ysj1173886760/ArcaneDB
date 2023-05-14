/**
 * @file bwtree_log_writer.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-28
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include "log_store/log_store.h"
#include "property/row/row.h"
#include "util/codec/buf_writer.h"
#include "wal/log_type.h"

namespace arcanedb {
namespace wal {

/**
 * @brief
 * Format:
 * | type 1 byte | txn_id 8 byte | page id varlen | write ts 4 byte | row |
 *
 * PageId & SortKey format:
 * | total length 2 byte | string varlen |
 */

class BwTreeLogWriter {
public:
  BwTreeLogWriter() noexcept : writer_(block_, kDefaultBlockSize) {}

  void SetRow(const std::string_view &page_id, TxnId txn_id, TxnTs write_ts,
              const property::Row &row) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kBwtreeSetRow);

    writer_.WriteBytes(txn_id);

    SerializeString_(page_id);

    writer_.WriteBytes(write_ts);

    writer_.WriteBytes(row.as_slice());

    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void DeleteRow(const std::string_view &page_id, TxnId txn_id, TxnTs write_ts,
                 property::SortKeysRef sort_key) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kBwtreeDeleteRow);

    writer_.WriteBytes(txn_id);

    SerializeString_(page_id);

    writer_.WriteBytes(write_ts);

    SerializeString_(sort_key.as_slice());

    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void SetTs(const std::string_view &page_id, TxnId txn_id, TxnTs commit_ts,
             property::SortKeysRef sort_key) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kBwtreeSetTs);

    writer_.WriteBytes(txn_id);

    SerializeString_(page_id);

    writer_.WriteBytes(commit_ts);

    SerializeString_(sort_key.as_slice());

    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  const log_store::LogStore::LogRecordContainer &
  GetLogRecords() const noexcept {
    return container_;
  }

private:
  void SerializeString_(const std::string_view &str) noexcept {
    writer_.WriteBytes(static_cast<uint16_t>(str.size()));
    writer_.WriteBytes(str);
  }

  static constexpr size_t kDefaultBlockSize = 64;
  char block_[kDefaultBlockSize];
  util::NonOwnershipBufWriter writer_;
  // TODO(sheep): optimize memory allocation
  log_store::LogStore::LogRecordContainer container_;
};

} // namespace wal
} // namespace arcanedb