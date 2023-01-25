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
#include <asm-generic/errno.h>

namespace arcanedb {
namespace txn {

Status LockTable::Lock(std::string_view sort_key, TxnId txn_id) noexcept {
  std::unique_lock<bthread::Mutex> guard(mu_);
  // try emplace the lock
  auto [it, lock_succeed] =
      map_.emplace(std::string(sort_key), std::make_unique<LockEntry>(txn_id));
  if (lock_succeed) {
    return Status::Ok();
  }
  LockEntry *lock_entry = it->second.get();
  // duplicated lock should be handled outside.
  CHECK(lock_entry->txn_id != txn_id);
  // add waiter count
  lock_entry->AddWaiter();
  // if failed, wait on the cv
  // don't handle suspicious wakeup since it will increase code complexity
  bool timeout = lock_entry->cv.wait_for(
                     guard, common::Config::kLockTimeoutUs) == ETIMEDOUT;
  // remove waiter
  lock_entry->DecWaiter();
  // suspicious wakeup might reverse the priority.
  if (!timeout && !lock_entry->is_locked) {
    lock_entry->txn_id = txn_id;
    lock_entry->is_locked = true;
    return Status::Ok();
  }
  return Status::Timeout();
}

Status LockTable::Unlock(std::string_view sort_key, TxnId txn_id) noexcept {
  std::unique_lock<bthread::Mutex> guard(mu_);
  auto it = map_.find(sort_key);
  CHECK(it != map_.end());
  CHECK(it->second->txn_id == txn_id);
  // unlock
  it->second->is_locked = false;
  if (it->second->ShouldGC()) {
    // no concurrent waiter, erase the lock entry
    map_.erase(it);
  } else {
    it->second->cv.notify_one();
  }
  return Status::Ok();
}

} // namespace txn
} // namespace arcanedb