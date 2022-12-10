/**
 * @file time.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once

#include <chrono>
#include <stdlib.h>
#include <sys/time.h>

namespace arcanedb {
namespace util {

constexpr int64_t MicroSec = 1ll;
constexpr int64_t MillSec = 1000ll * MicroSec;
constexpr int64_t Second = 1000ll * MillSec;

class Timer {
public:
  Timer() { gettimeofday(&start_time_, NULL); }

  /**
   * @brief Get time elapsed.
   * unit: microsecond.
   * @return int64_t
   */
  int64_t GetElapsed() const noexcept {
    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    return (end_time.tv_sec - start_time_.tv_sec) * 1000 * 1000 +
           (end_time.tv_usec - start_time_.tv_usec);
  }

  /**
   * @brief
   * Reset the timer
   */
  void Reset() { gettimeofday(&start_time_, NULL); }

private:
  struct timeval start_time_;
};

} // namespace util
} // namespace arcanedb