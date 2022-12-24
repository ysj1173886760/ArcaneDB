/**
 * @file log_store.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/status.h"
#include "log_store/options.h"
#include <limits>
#include <vector>

namespace arcanedb {
namespace log_store {

/**
 * @brief
 * Lsn is byte offset of log records.
 */
using LsnType = uint64_t;

static constexpr LsnType kInvalidLsn = std::numeric_limits<LsnType>::max();

struct LsnRange {
  LsnType start_lsn;
  LsnType end_lsn;
};

// TODO: impl checkpoint & truncate logs
class LogStore {
public:
  /**
   * @brief
   * Append a batch of logs.
   * @param log_records
   * @return Status
   */
  virtual Status AppendLogRecord(std::vector<std::string> log_records,
                                 std::vector<LsnRange> *result) noexcept = 0;

  /**
   * @brief persistent lsn indicates that up to which lsn, data has been
   * persisted.
   *
   * @return LsnType
   */
  virtual LsnType GetPersistentLsn() noexcept = 0;

  static Status Open(const std::string &name, const Options &options,
                     std::shared_ptr<LogStore> *log_store) noexcept {
    return Status::Ok();
  }
};

} // namespace log_store
} // namespace arcanedb