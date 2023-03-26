/**
 * @file config.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "util/time.h"
#include <cstddef>

namespace arcanedb {
namespace common {

class Config {
public:
  static constexpr size_t kThreadPoolDefaultNum = 16;

  static constexpr size_t kLogSegmentDefaultNum = 32;
  static constexpr size_t kLogSegmentDefaultSize = 4 << 20;
  static constexpr size_t kLogStoreFlushInterval = 50 * util::MicroSec;

  static constexpr size_t kBwTreeDeltaChainLength = 16;
  static constexpr size_t kBwTreeCompactionFactor = 2;

  // 6 bit indicates 32 shard
  static constexpr size_t kCacheShardNumBits = 6;
  // 1G capacity
  static constexpr size_t kCacheCapacity = 1 << 30;

  static constexpr int64_t kLockTimeoutUs = 150 * util::MillSec;

  static constexpr size_t kSnapshotManagerShardNum = 64;

  static constexpr size_t kLockTableShardNum = 64;

  // txn ts is 4 byte
  // 4 mb link buf
  static constexpr size_t kLinkBufSnapshotManagerSize = 1 << 20;

  static constexpr size_t kTxnWaitLogInterval = 50 * util::MicroSec;

  // 32 shard
  static constexpr size_t kFlusherShardNum = 32;
};

} // namespace common
} // namespace arcanedb