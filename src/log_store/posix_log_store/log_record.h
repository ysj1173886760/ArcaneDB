/**
 * @file log_record.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "log_store/log_store.h"
#include "util/codec/buf_writer.h"
#include <limits>
#include <string>

namespace arcanedb {
namespace log_store {

/**
 * @brief
 * Format:
 * | lsn 8byte | data len 2byte | data varlen |
 */
class LogRecord {
public:
  LogRecord(LsnType lsn, const std::string &data) : lsn_(lsn), data_(data) {}

  void SerializeTo(util::NonOwnershipBufWriter *writer) noexcept {
    writer->WriteBytes(static_cast<uint64_t>(lsn_));
    CHECK(data_.size() < std::numeric_limits<uint16_t>::max());
    writer->WriteBytes(static_cast<uint16_t>(data_.size()));
    writer->WriteBytes(data_);
  }

  size_t GetSerializeSize() noexcept { return kHeaderSize + data_.size(); }

  static constexpr size_t kHeaderSize = 10;

private:
  LsnType lsn_;
  const std::string &data_;
};

} // namespace log_store
} // namespace arcanedb