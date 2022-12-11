/**
 * @file status_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "common/status.h"
#include <gtest/gtest.h>

namespace arcanedb {

TEST(StatusTest, BasicTest) {
  {
    auto s = Status::Ok();
    EXPECT_TRUE(s.IsOk());
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.ToString(), "Ok: ");
  }
  {
    auto s = Status::Ok("hello");
    EXPECT_EQ(s.GetMsg(), "hello");
    EXPECT_EQ(s.ToString(), "Ok: hello");
  }
}

} // namespace arcanedb