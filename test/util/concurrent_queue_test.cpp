/**
 * @file concurrent_queue_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "util/concurrent_queue/blockingconcurrentqueue.h"
#include <gtest/gtest.h>

namespace arcanedb {

TEST(ConcurrentQueueTest, BasicTest) {
  moodycamel::BlockingConcurrentQueue<int> queue;
  queue.enqueue(10);
  int item;
  queue.wait_dequeue(item);
  EXPECT_EQ(item, 10);
}

} // namespace arcanedb