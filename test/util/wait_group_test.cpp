/**
 * @file wait_group_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "util/bthread_util.h"
#include "util/wait_group.h"
#include <atomic>
#include <gtest/gtest.h>

namespace arcanedb {
namespace util {

TEST(WaitGroupTest, BasicTest) {
  const int worker_cnt = 1000;
  WaitGroup wg(worker_cnt);
  std::atomic_int cnt{0};
  for (int i = 0; i < worker_cnt; i++) {
    LaunchAsync([&]() {
      cnt.fetch_add(1, std::memory_order_relaxed);
      wg.Done();
    });
  }
  wg.Wait();
  EXPECT_EQ(cnt.load(std::memory_order_relaxed), worker_cnt);
}

} // namespace util
} // namespace arcanedb