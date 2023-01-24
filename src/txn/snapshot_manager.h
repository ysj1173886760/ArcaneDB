/**
 * @file snapshot_manager.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/status.h"
#include "common/type.h"
#include <set>

namespace arcanedb {
namespace txn {

/**
 * @brief
 * TxnManager is used to manage txn ts.
 * i.e. tract the snapshot ts for read-only txn.
 */
class SnapshotManager {
public:
  SnapshotManager() = default;

  void RegisterTs(TxnTs ts) noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    ts_set_.insert(ts);
  }

  void CommitTs(TxnTs ts) noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    ts_set_.erase(ts);
  }

  TxnTs GetSnapshotTs() noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    return ts_set_.empty() ? kMaxTxnTs : *ts_set_.begin();
  }

private:
  ArcanedbLock mu_;
  std::set<TxnTs> ts_set_;
};

} // namespace txn
} // namespace arcanedb