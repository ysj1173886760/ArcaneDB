/**
 * @file simple_waiter.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

namespace arcanedb {
namespace util {

// TODO: implement more efficient version by using butex
class SimpleWaiter {
public:
  SimpleWaiter() = default;

  void Wait() noexcept {
    std::unique_lock<bthread::Mutex> lock(mu_);
    cv_.wait(lock);
  }

  void Wait(int64_t timeout_us) noexcept {
    std::unique_lock<bthread::Mutex> lock(mu_);
    cv_.wait_for(lock, timeout_us);
  }

  void NotifyOne() noexcept { cv_.notify_one(); }

  void NotifyAll() noexcept { cv_.notify_all(); }

private:
  bthread::Mutex mu_;
  bthread::ConditionVariable cv_;
};

} // namespace util
} // namespace arcanedb