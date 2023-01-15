/**
 * @file log_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "common/logger.h"
#include <gtest/gtest.h>

namespace arcanedb {

TEST(LogTest, BasicTest) {
  ARCANEDB_DEBUG("hello {}", 1);
  ARCANEDB_INFO("hello {}", "world");
  ARCANEDB_WARN("0.0 {} {}", 1.1, 2.2);
  int a = 10;
  ARCANEDB_ERROR("error {}", a);
}

} // namespace arcanedb