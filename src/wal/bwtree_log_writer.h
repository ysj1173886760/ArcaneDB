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

namespace arcanedb {
namespace wal {

/**
 * @brief
 * Format:
 * | type 1 byte | write ts 4 byte | row |
 */

class BwTreeLogWriter {
public:
  enum class LogType : uint8_t {
    SetRow = 0,
    DeleteRow = 1,
    SetTs = 2,
  };

  BwTreeLogWriter() noexcept : writer_(block_, kDefaultBlockSize) {}

  void WriteSetRow(TxnTs write_ts, const property::Row &row) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::SetRow);
    writer_.WriteBytes(write_ts);
    writer_.WriteBytes(row.as_slice());
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void WriteDeleteRow(TxnTs write_ts, property::SortKeysRef sort_key) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::DeleteRow);
    writer_.WriteBytes(write_ts);
    writer_.WriteBytes(static_cast<uint16_t>(sort_key.as_slice().size()));
    writer_.WriteBytes(sort_key.as_slice());
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void WriteSetTs(TxnTs commit_ts, property::SortKeysRef sort_key) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::SetTs);
    writer_.WriteBytes(commit_ts);
    writer_.WriteBytes(static_cast<uint16_t>(sort_key.as_slice().size()));
    writer_.WriteBytes(sort_key.as_slice());
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  const log_store::LogStore::LogRecordContainer &GetLogRecords() const
      noexcept {
    return container_;
  }

private:
  static constexpr size_t kDefaultBlockSize = 64;
  char block_[kDefaultBlockSize];
  util::NonOwnershipBufWriter writer_;
  // TODO(sheep): optimize memory allocation
  log_store::LogStore::LogRecordContainer container_;
};

} // namespace wal
} // namespace arcanedb