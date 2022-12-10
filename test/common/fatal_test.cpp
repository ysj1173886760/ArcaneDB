/**
 * @file fatal_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief test macro CHECK and FATAL
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "common/macros.h"
#include "gtest/gtest.h"
#include <gtest/gtest.h>

namespace arcanedb {

void FatalFunc1() { FATAL("here %d", 1); }
void FatalFunc2() { FATAL("here"); }
TEST(FatalTest, TestFatal) {
  GTEST_SKIP_("skip death test due to unsafe");
  ASSERT_DEATH(FatalFunc1(), "");
  ASSERT_DEATH(FatalFunc2(), "");
}

void CheckFunc() { CHECK(1 == 2); }
TEST(FatalTest, TestCheck) {
  GTEST_SKIP_("skip death test due to unsafe");
  ASSERT_DEATH(CheckFunc(), "");
}

} // namespace arcanedb