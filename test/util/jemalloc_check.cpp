/**
 * @file jemalloc_check.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-28
 *
 * @copyright Copyright (c) 2023
 *
 */

#include <gtest/gtest.h>

extern "C" {
// weak symbol: resolved at runtime by the linker if we are using jemalloc,
// nullptr otherwise
int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp,
            size_t newlen) __attribute__((weak));
}

bool check_jemalloc() { return (mallctl != nullptr); }

TEST(JemallocCheck, CheckTest) { EXPECT_TRUE(check_jemalloc()); }