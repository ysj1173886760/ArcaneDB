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
#include <gtest/gtest.h>

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

} // namespace util
} // namespace arcanedb
