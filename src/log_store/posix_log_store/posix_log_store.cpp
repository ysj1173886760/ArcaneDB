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
  auto store = std::make_shared<PosixLogStore>();
  store->env_ = leveldb::Env::Default();
  store->name_ = name;
  // create directory
  auto s = store->env_->CreateDir(name);
  if (!s.ok()) {
    LOG_WARN("Failed to create dir, error: %s", s.ToString().c_str());
    return Status::Err();
  }

  // create log file
  s = store->env_->NewWritableFile(MakeLogFileName(name), &store->log_file_);
  if (!s.ok()) {
    LOG_WARN("Failed to create writable file, error: %s", s.ToString().c_str());
    return Status::Err();
  }

  // initialize thread pool
  auto thread_pool = options.thread_pool;
  if (thread_pool == nullptr) {
    // create if missing
    thread_pool = std::make_shared<util::ThreadPool>(
        common::Config::kThreadPoolDefaultNum);
  }
  store->thread_pool_ = std::move(thread_pool);

  // initialize log segment
  store->segments_.reserve(options.segment_num);
  for (int i = 0; i < options.segment_num; i++) {
    store->segments_.emplace_back(
        LogSegment(options.segment_size, store->thread_pool_));
  }

  *log_store = store;
  return Status::Ok();
}

Status PosixLogStore::AppendLogRecord(std::vector<std::string> log_records,
                                      std::vector<LsnRange> *result) noexcept {
  return Status::Ok();
}

LsnType GetPersistentLsn() noexcept { return kInvalidLsn; }

} // namespace log_store
} // namespace arcanedb