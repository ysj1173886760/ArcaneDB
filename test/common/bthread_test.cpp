#include "bthread/bthread.h"
#include "util/bthread_util.h"
#include "util/time.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

namespace arcanedb {
namespace util {

TEST(BthreadTest, BasicTest) {
  bthread_usleep(100);
  ASSERT_TRUE(true);
}

bool TestConcurrency(int thread_num) {
  using namespace std::chrono_literals;
  std::vector<std::shared_ptr<BthreadFuture<void>>> futures(thread_num);
  Timer timer;
  for (int i = 0; i < thread_num; i++) {
    futures[i] = LaunchAsync([]() { std::this_thread::sleep_for(1s); });
  }
  for (int i = 0; i < thread_num; i++) {
    futures[i]->Wait();
  }
  return timer.GetElapsed() < 1.2 * Second;
}

TEST(BthreadTest, MaximumConcurrencyTest) {
  // skip this test since it's only used for testing maximum concurrency
  GTEST_SKIP();
  auto concurrency = bthread_getconcurrency();
  EXPECT_TRUE(TestConcurrency(concurrency));
  bthread_setconcurrency(concurrency * 2);
  EXPECT_TRUE(TestConcurrency(concurrency * 2));
}

} // namespace util
} // namespace arcanedb