/**
 * @file wait_group.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include <cassert>

namespace arcanedb {
namespace util {

class WaitGroup {
public:
  explicit WaitGroup(size_t size = 0) : size_(size) {}

  void Add(size_t n) noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    size_ += n;
  }

  void Done() noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    assert(size_ > 0);
    size_--;
    if (size_ == 0) {
      cv_.notify_all();
    }
  }

  void Wait() noexcept {
    std::unique_lock<bthread::Mutex> guard(mu_);
    while (size_ > 0) {
      cv_.wait(guard);
    }
  }

private:
  bthread::Mutex mu_;
  bthread::ConditionVariable cv_;
  size_t size_;
};

} // namespace util
} // namespace arcanedb
