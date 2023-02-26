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

}
} // namespace arcanedb