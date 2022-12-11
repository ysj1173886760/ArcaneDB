/**
 * @file bthread_util_test.cpp
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
#include "util/time.h"
#include <gtest/gtest.h>
#include <memory>
#include <thread>

namespace arcanedb {
namespace util {

TEST(BthreadUtilTest, FutureTest) {
  {
    // pod
    BthreadFuture<int> future;
    future.Set(10);
    EXPECT_EQ(future.Get(), 10);
  }
  {
    // string
    BthreadFuture<std::string> future;
    future.Set("hello world");
    EXPECT_EQ(future.Get(), "hello world");
  }
  {
    // void
    BthreadFuture<void> future;
    future.Set();
    future.Get();
  }
}

TEST(BthreadUtilTest, LaunchAsyncBasicTest) {
  {
    // pod
    auto future = LaunchAsync([]() { return 10; });
    EXPECT_EQ(future->Get(), 10);
  }
  {
    // string
    auto future = LaunchAsync([]() { return "hello world"; });
    EXPECT_EQ(future->Get(), "hello world");
  }
  {
    // void
    auto future = LaunchAsync([]() { return "hello world"; });
    future->Wait();
  }
}

TEST(BthreadUtilTest, LaunchAsyncInThreadPoolTest) {
  using namespace std::chrono_literals;
  auto thread_pool = std::make_shared<ThreadPool>(2);
  Timer timer;
  auto future1 =
      LaunchAsync([]() { std::this_thread::sleep_for(1s); }, thread_pool);
  auto future2 =
      LaunchAsync([]() { std::this_thread::sleep_for(1s); }, thread_pool);
  auto future3 =
      LaunchAsync([]() { std::this_thread::sleep_for(1s); }, thread_pool);
  future1->Wait();
  future2->Wait();
  future3->Wait();
  EXPECT_GT(timer.GetElapsed(), 1.5 * Second);
}

} // namespace util
} // namespace arcanedb
