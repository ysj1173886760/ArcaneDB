/**
 * @file type.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief type alias
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "bthread/mutex.h"
#include "util/lock.h"
#include <limits>
#include <string>

namespace arcanedb {

using PageIdType = std::string;
using PageIdView = std::string_view;

// using ArcanedbLock = util::ArcaneMutex<bthread::Mutex>;
// template <typename Mutex>
// using ArcanedbLockGuard = util::InstrumentedLockGuard<Mutex>;
using ArcanedbLock = bthread::Mutex;
template <typename Mutex>
using ArcanedbLockGuard = std::lock_guard<Mutex>;

using TxnId = int64_t;
using TxnTs = uint32_t;

static constexpr uint32_t kMaxTxnTs = std::numeric_limits<int32_t>::max();
static constexpr uint32_t kAbortedTxnTs = 0;
static constexpr size_t kLockBitOffset = 31;

inline bool IsLocked(TxnTs ts) { return (ts >> 31) & 1; }

inline TxnTs MarkLocked(TxnTs ts) { return ts | (1 << kLockBitOffset); }

inline TxnTs GetTs(TxnTs ts) { return ts & kMaxTxnTs; }

} // namespace arcanedb