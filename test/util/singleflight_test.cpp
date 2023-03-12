/**
 * @file singleflight_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-02
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "util/bthread_util.h"
#include "util/singleflight.h"
#include "util/wait_group.h"
#include <atomic>
#include <gtest/gtest.h>
#include <string>

namespace arcanedb {
namespace util {

TEST(SingleFlightTest, BasicTest) {
  auto single_flight = SingleFlight<std::shared_ptr<int>>();
  bthread::Mutex mu;
  int counter = 0;
  std::unordered_set<std::string> flag;
  auto loader = [&](const std::string_view &key, std::shared_ptr<int> *entry) {
    std::lock_guard<bthread::Mutex> guard(mu);
    if (!flag.count(std::string(key))) {
      flag.emplace(std::string(key));
      counter += 1;
    }
    return Status::Ok();
  };
  WaitGroup wg(10 * 100);
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 100; j++) {
      LaunchAsync([&, index = i]() {
        std::shared_ptr<int> entry;
        auto s = single_flight.Do(std::to_string(index), &entry, loader);
        EXPECT_EQ(s, Status::Ok());
        wg.Done();
      });
    }
  }
  wg.Wait();
  EXPECT_EQ(counter, 10);
}

} // namespace util
} // namespace arcanedb