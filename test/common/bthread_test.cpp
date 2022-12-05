#include <gtest/gtest.h>
#include "bthread/bthread.h"

TEST(BthreadTest, BasicTest) {
    bthread_usleep(100);
    ASSERT_TRUE(true);
}