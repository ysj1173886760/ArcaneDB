/**
 * @file occ_log.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-02-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include "log_store/log_store.h"
#include "util/codec/buf_writer.h"
#include "wal/log_type.h"

namespace arcanedb {
namespace wal {

/**
 * @brief
 * Format:
 * | type 1byte | read ts 4 byte | commit ts 4 byte |
 */

class OccLogWriter {
public:
  OccLogWriter() noexcept : writer_(block_, kDefaultBlockSize) {}

  const log_store::LogStore::LogRecordContainer &GetLogRecords() const
      noexcept {
    return container_;
  }

  void Begin(TxnTs read_ts) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kOccBegin);
    writer_.WriteBytes(read_ts);
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void Commit(TxnTs read_ts, TxnTs commit_ts) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kOccCommit);
    writer_.WriteBytes(read_ts);
    writer_.WriteBytes(commit_ts);
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

  void Abort(TxnTs read_ts) noexcept {
    auto offset = writer_.Offset();
    writer_.WriteBytes(LogType::kOccAbort);
    writer_.WriteBytes(read_ts);
    container_.push_back(
        std::string_view(block_ + offset, writer_.Offset() - offset));
  }

private:
  static constexpr size_t kDefaultBlockSize = 64;
  char block_[kDefaultBlockSize];
  util::NonOwnershipBufWriter writer_;
  log_store::LogStore::LogRecordContainer container_;
};

} // namespace wal
} // namespace arcanedb