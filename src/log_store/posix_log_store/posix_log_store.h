/**
 * @file posix_log_store.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "log_store/log_store.h"

namespace arcanedb {
namespace log_store {

/**
 * @brief
 * LogStore based on posix env
 */
class PosixLogStore : public LogStore {
public:
  static Status Open(const std::string &name, const Options &options,
                     std::shared_ptr<LogStore> *log_store) noexcept;

  Status AppendLogRecord(std::vector<std::string> log_records,
                         std::vector<LsnRange> *result) noexcept override;

  LsnType GetPersistentLsn() noexcept override;

private:
};

} // namespace log_store
} // namespace arcanedb