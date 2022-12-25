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
#include "log_store/posix_log_store/log_segment.h"
#include "util/thread_pool.h"
#include <atomic>
#include <leveldb/env.h>

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

  ~PosixLogStore() noexcept {
    stopped_.store(std::memory_order_relaxed);
    if (background_thread_ != nullptr) {
      background_thread_->join();
    }
  }

  Status AppendLogRecord(std::vector<std::string> log_records,
                         std::vector<LsnRange> *result) noexcept override;

  LsnType GetPersistentLsn() noexcept override;

private:
  void StartBackgroundThread_() noexcept {
    background_thread_ =
        std::make_unique<std::thread>(&PosixLogStore::ThreadJob_, this);
  }

  void ThreadJob_() noexcept;

  LogSegment *GetCurrentLogSegment_() noexcept {
    return &segments_[current_log_segment_.load(std::memory_order_relaxed)];
  }

  static std::string MakeLogFileName_(const std::string name) noexcept {
    return name + "/LOG";
  }

  leveldb::Env *env_{nullptr};
  leveldb::WritableFile *log_file_{nullptr};
  std::vector<LogSegment> segments_{};
  std::atomic_size_t current_log_segment_{0};
  std::string name_;
  std::unique_ptr<std::thread> background_thread_{nullptr};
  std::atomic_bool stopped_{false};
};

} // namespace log_store
} // namespace arcanedb