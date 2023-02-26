/**
 * @file log_type.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-02-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <cstdint>
#include <string>

namespace arcanedb {
namespace wal {

enum class LogType : uint8_t {
  // Page redo log
  kBwtreeSetRow = 0,
  kBwtreeDeleteRow = 1,
  kBwtreeSetTs = 2,

  // log for OCC
  kOccBegin = 3,
  kOccCommit = 4,
  kOccAbort = 5,
};

inline LogType ParseLogRecord(std::string_view *data) noexcept {
  auto type = static_cast<LogType>(data->at(0));
  *data = data->substr(1);
  return type;
}

} // namespace wal
} // namespace arcanedb