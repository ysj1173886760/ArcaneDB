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
#include "util/thread_pool.h"
#include <leveldb/env.h>

namespace arcanedb {
namespace log_store {

class LogSegment {
public:
  LogSegment(size_t size) : size_(size) {}

private:
  /**
   * @brief
   * State change circle:
   * Initial state is kFree.
   * First segment is kOpen.
   * When segment is not capable of writing new logs, one of the foreground
   * threads is responsible to seal the segment. (the one who has complete the
   * CAS operation). The one who sealed the previous segment, is responsible to
   * open next log segment. note that background worker could also seal the
   * segment, preventing the latency of persisting log record being to high. The
   * last writer of sealed segment is responsible to change segment state from
   * kSeal to kSerialize. Then background worker will observe this change, and
   * serializing the log segments. After serialization has been done, background
   * worker will change state from kSerialize to kIo, then schedule an io job.
   * At last, after io worker has finish it's job, it will change kIo to kFree,
   * indicating this segment is reusable again.
   */
  enum class LogSegmentState {
    kFree,
    kOpen,
    kSeal,
    kSerialize,
    kIo,
  };

  LogSegmentState state_;
  size_t size_{};
  LsnType start_lsn_{};
};

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
  static std::string MakeLogFileName(const std::string name) noexcept {
    return name + "/LOG";
  }

  leveldb::Env *env_{nullptr};
  leveldb::WritableFile *log_file_{nullptr};
  std::vector<LogSegment> segments_{};
  std::shared_ptr<util::ThreadPool> thread_pool_{nullptr};
  std::atomic_size_t current_log_segment_{0};
  std::string name_;
};

} // namespace log_store
} // namespace arcanedb