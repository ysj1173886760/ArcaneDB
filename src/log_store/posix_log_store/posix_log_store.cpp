/**
 * @file posix_log_store.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "log_store/posix_log_store/posix_log_store.h"
#include "log_store/log_store.h"
#include <string>

namespace arcanedb {
namespace log_store {

Status PosixLogStore::Open(const std::string &name, const Options &options,
                           std::shared_ptr<LogStore> *log_store) noexcept {
  return Status::Ok();
}

Status PosixLogStore::AppendLogRecord(std::vector<std::string> log_records,
                                      std::vector<LsnRange> *result) noexcept {
  return Status::Ok();
}

LsnType GetPersistentLsn() noexcept { return kInvalidLsn; }

} // namespace log_store
} // namespace arcanedb