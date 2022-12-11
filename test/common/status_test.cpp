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

TEST(ResultTest, BasicTest) {
  {
    // pod error
    auto s = Result<int>::Err();
    EXPECT_TRUE(s.IsErr());
    EXPECT_EQ(s.ValueOr(10), 10);
  }
  {
    // pod ok
    auto s = Result<int>(20);
    EXPECT_TRUE(s.IsOk());
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.GetValue(), 20);
    EXPECT_EQ(s.ValueOr(10), 20);
  }
  {
    // string error
    auto s = Result<std::string>::Err();
    EXPECT_TRUE(s.IsErr());
    EXPECT_EQ(s.ValueOr("Hello"), "Hello");
  }
  {
    // string ok
    auto s = Result<std::string>("World");
    EXPECT_TRUE(s.IsOk());
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(s.GetValue(), "World");
    EXPECT_EQ(s.ValueOr("Hello"), "World");
  }
  {
    // rvalue string
    EXPECT_EQ(Result<std::string>("Arcane").GetValue(), "Arcane");
    EXPECT_EQ(Result<std::string>("Arcane").ValueOr("DB"), "Arcane");
    EXPECT_EQ(Result<std::string>::Err().ValueOr("DB"), "DB");
  }
}

} // namespace arcanedb