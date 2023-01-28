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

#include "absl/container/inlined_vector.h"
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

class LogReader {
public:
  virtual bool HasNext() noexcept = 0;
  virtual LsnType GetNextLogRecord(std::string *bytes) noexcept = 0;
  virtual ~LogReader() noexcept {};
};

// TODO: impl checkpoint & truncate logs
class LogStore {
public:
  static constexpr size_t kDefaultLogNum = 1;
  using LogRecordContainer =
      absl::InlinedVector<std::string_view, kDefaultLogNum>;
  using LogResultContainer = absl::InlinedVector<LsnRange, kDefaultLogNum>;

  /**
   * @brief
   * Append a batch of logs.
   * @param log_records
   * @param result
   */
  virtual void AppendLogRecord(const LogRecordContainer &log_records,
                               LogResultContainer *result) noexcept = 0;

  /**
   * @brief persistent lsn indicates that up to which lsn, data has been
   * persisted.
   *
   * @return LsnType
   */
  virtual LsnType GetPersistentLsn() noexcept = 0;

  /**
   * @brief Get the LogReader
   * log reader is used to read existing log to perform recovery.
   * note that the behaviour of concurrent reading the log and appending the log
   * record is undefined. So user should guarantee that there is at most one
   * reader/writer
   * @param log_reader
   * @return Status
   */
  virtual Status
  GetLogReader(std::unique_ptr<LogReader> *log_reader) noexcept = 0;

  static Status Open(const std::string &name, const Options &options,
                     std::shared_ptr<LogStore> *log_store) noexcept {
    return Status::Ok();
  }

  virtual ~LogStore() noexcept {};
};

} // namespace log_store
} // namespace arcanedb