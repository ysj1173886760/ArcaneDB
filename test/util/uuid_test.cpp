/**
 * @file uuid_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "util/bthread_util.h"
#include "util/uuid.h"
#include "util/wait_group.h"
#include <gtest/gtest.h>
#include <unordered_set>

namespace arcanedb {
namespace util {

TEST(UuidTest, BasicTest) {
  int worker_num = 10;
  util::WaitGroup wg(worker_num);
  int epoch_num = 1000;
  std::vector<std::unordered_set<int64_t>> sets(10);
  for (int i = 0; i < worker_num; i++) {
    util::LaunchAsync([&, index = i]() {
      for (int j = 0; j < epoch_num; j++) {
        sets[index].insert(GenerateUUID());
      }
      wg.Done();
    });
  }
  wg.Wait();
  std::unordered_set<int64_t> all;
  for (const auto &set : sets) {
    all.insert(set.begin(), set.end());
  }
  EXPECT_EQ(all.size(), worker_num * epoch_num);
}

} // namespace util
} // namespace arcanedb