/**
 * @file backoff.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "bthread/bthread.h"
#include "common/logger.h"

namespace arcanedb {
namespace util {

class BackOff {
public:
  void Sleep(int64_t wait_us) noexcept {
    UnhealthyWaitCntChecker_();
    bthread_usleep(wait_us * cnt_);
    cnt_ *= 2;
  }

  void Sleep(int64_t wait_us, int64_t max_wait_us) noexcept {
    int64_t us = std::min(cnt_ * wait_us, max_wait_us);
    bthread_usleep(us);
    cnt_ *= 2;
  }

  static constexpr int32_t kUnhealthyWaitCnt = 128;

private:
  void UnhealthyWaitCntChecker_() noexcept {
    if (cnt_ > kUnhealthyWaitCnt) {
      ARCANEDB_WARN("Unhealthy sleep times {}", cnt_);
    }
  }
  int32_t cnt_{1};
};

} // namespace util
} // namespace arcanedb