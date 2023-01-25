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
#include "util/bthread_util.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace txn {

TEST(LockTableTest, BasicTest) {
  // basic lock and unlock
  LockTable table;
  auto sk1 = property::SortKeys({1, 2, 3});
  auto sk2 = property::SortKeys({4, 5, 6});
  TxnId txn1 = 0;
  EXPECT_TRUE(table.Lock(sk1.as_ref().as_slice(), txn1).ok());
  EXPECT_TRUE(table.Lock(sk2.as_ref().as_slice(), txn1).ok());
  TxnId txn2 = 1;
  auto future = util::LaunchAsync([&]() {
    util::Timer timer;
    EXPECT_TRUE(table.Lock(sk1.as_ref().as_slice(), txn2).ok());
    EXPECT_GT(timer.GetElapsed(), 5 * util::MillSec);
  });
  bthread_usleep(10 * util::MillSec);
  EXPECT_TRUE(table.Unlock(sk1.as_ref().as_slice(), txn1).ok());
  EXPECT_TRUE(table.Unlock(sk2.as_ref().as_slice(), txn1).ok());
  future->Wait();
}

} // namespace txn
} // namespace arcanedb