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
#include "util/backoff.h"
#include "util/simple_waiter.h"
#include "util/thread_pool.h"
#include "util/time.h"
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

  static Status Destory(const std::string &name) noexcept;

  ~PosixLogStore() noexcept {
    stopped_.store(true, std::memory_order_relaxed);
    if (background_thread_ != nullptr) {
      background_thread_->join();
    }
    delete log_file_;
  }

  Status AppendLogRecord(std::vector<std::string> log_records,
                         std::vector<LsnRange> *result) noexcept override;

  LsnType GetPersistentLsn() noexcept override { return kInvalidLsn; }

private:
  void StartBackgroundThread_() noexcept {
    background_thread_ =
        std::make_unique<std::thread>(&PosixLogStore::ThreadJob_, this);
  }

  void ThreadJob_() noexcept;

  LogSegment *GetCurrentLogSegment_() noexcept {
    return &segments_[current_log_segment_.load(std::memory_order_relaxed)];
  }

  LogSegment *GetLogSegment_(size_t index) noexcept {
    return &segments_[index];
  }

  static std::string MakeLogFileName_(const std::string name) noexcept {
    return name + "/LOG";
  }

  /**
   * @brief
   * Open a new log segment, spin when there is no freed segments.
   * @param start_lsn start lsn of new log segment
   */
  void OpenNewLogSegment_(LsnType start_lsn) noexcept {
    constexpr int64_t sleep_time = 1 * util::MillSec;
    AdvanceSegmentIndex_();
    util::BackOff bo;
    while (GetCurrentLogSegment_()->state_.load(std::memory_order_acquire) !=
           LogSegment::LogSegmentState::kFree) {
      bo.Sleep(sleep_time);
    }
    GetCurrentLogSegment_()->OpenLogSegment(start_lsn);
  }

  void AdvanceSegmentIndex_() noexcept {
    size_t current = current_log_segment_.load(std::memory_order_acquire);
    size_t expect;
    do {
      expect = (current + 1) % segment_num_;
    } while (current_log_segment_.compare_exchange_weak(
        current, expect, std::memory_order_acq_rel));
  }

  /**
   * @brief
   * try to seal the old log segment and open new one
   * @param log_segment old log segment
   * @return true Seal has succeed
   * @return false others has sealed the segment
   */
  bool SealAndOpen(LogSegment *log_segment) noexcept;

  leveldb::Env *env_{nullptr};
  leveldb::WritableFile *log_file_{nullptr};
  std::unique_ptr<LogSegment[]> segments_{nullptr};
  size_t segment_num_{};
  std::atomic_size_t current_log_segment_{0};
  std::string name_;
  std::unique_ptr<std::thread> background_thread_{nullptr};
  std::atomic_bool stopped_{false};
};

} // namespace log_store
} // namespace arcanedb