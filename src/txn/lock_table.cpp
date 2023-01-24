/**
 * @file lock_table.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "txn/lock_table.h"
#include "common/config.h"

namespace arcanedb {
namespace txn {

Status LockTable::Lock(property::SortKeysRef sort_key, TxnId txn_id) noexcept {
  std::unique_lock<bthread::Mutex> guard(mu_);
  // try emplace the lock
  auto [it, lock_succeed] = map_.emplace(std::string(sort_key.as_slice()),
                                         std::make_unique<LockEntry>(txn_id));
  if (lock_succeed) {
    return Status::Ok();
  }
  LockEntry *lock_entry = it->second.get();
  // add waiter count
  lock_entry->AddWaiter();
  // if failed, wait on the cv
  // don't handle suspicious wakeup since it will increase code complexity
  bool succeed =
      lock_entry->cv.wait_for(guard, common::Config::kLockTimeoutUs) == 0;
  // remove waiter
  lock_entry->DecWaiter();
  // suspicious wakeup might reverse the priority.
  if (!lock_entry->is_locked) {
    lock_entry->txn_id = txn_id;
    lock_entry->is_locked = true;
    return Status::Ok();
  }
  return Status::Timeout();
}

Status LockTable::Unlock(property::SortKeysRef sort_key,
                         TxnId txn_id) noexcept {
  std::unique_lock<bthread::Mutex> guard(mu_);
  auto it = map_.find(sort_key.as_slice());
  CHECK(it != map_.end());
  CHECK(it->second->txn_id == txn_id);
  // unlock
  it->second->is_locked = false;
  if (it->second->ShouldGC()) {
    // no concurrent waiter, erase the lock entry
    map_.erase(it);
  }
  return Status::Ok();
}

} // namespace txn
} // namespace arcanedb