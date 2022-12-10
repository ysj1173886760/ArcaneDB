#include <gtest/gtest.h>
#include "bthread/bthread.h"
#include "util/bthread_util.h"

TEST(BthreadTest, BasicTest) {
    bthread_usleep(100);
    ASSERT_TRUE(true);
}

void TestConcurrency(int thread_num) {
}

TEST(BthreadTest, MaximumConcurrencyTest) {
    int lb = 0;
    int ub = 16;
    // while (ub - lb > 1) {
    //     int mid = (ub + lb) / 2;

    // }
}