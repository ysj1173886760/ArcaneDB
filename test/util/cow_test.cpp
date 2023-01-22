/**
 * @file cow_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "util/cow.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace util {

TEST(CowTest, BasicTest) {
  auto origin = std::make_shared<std::string>("hello");
  Cow<std::string> cow(origin);
  EXPECT_EQ(*cow.GetImmutablePtr(), *origin);
  auto immutable_ptr = cow.GetImmutablePtr();
  auto mutable_ptr = cow.GetMutablePtr();
  mutable_ptr->append(" world");
  EXPECT_EQ(*immutable_ptr, *origin);
  EXPECT_NE(*mutable_ptr, *origin);
  cow.Promote();
  immutable_ptr = cow.GetImmutablePtr();
  EXPECT_EQ(*immutable_ptr, "hello world");
}

} // namespace util
} // namespace arcanedb