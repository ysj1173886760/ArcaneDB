/**
 * @file sort_key_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "butil/logging.h"
#include "property/sort_key/sort_key.h"
#include <gtest/gtest.h>
#include <limits>
#include <string_view>

namespace arcanedb {
namespace property {

TEST(SortKeyTest, BasicTest) {
  // generate two sort key, then compare
  auto sk1 = SortKeys({10, 20, std::string_view("123")});
  auto sk2 = SortKeys({10, 20, std::string_view("456")});
  EXPECT_LE(sk1, sk2);
  EXPECT_LE(sk1.as_ref(), sk2.as_ref());
  EXPECT_LE(sk1.as_slice(), sk2.as_slice());
  ARCANEDB_DEBUG("{} {}", sk1.ToString(), sk2.ToString());
}

TEST(SortKeyTest,
     GetMinTest){{auto sk = SortKeys({10, 20, std::string_view("123")});
auto min_sk1 = sk.GetMinSortKeys();
auto min_sk2 = sk.GetMinSortKeys();
EXPECT_EQ(min_sk1, min_sk2);
EXPECT_LE(min_sk1, sk);
} // namespace property
{
  auto sk = SortKeys(std::vector<Value>{std::numeric_limits<int32_t>::min()});
  auto min = sk.GetMinSortKeys();
  EXPECT_EQ(min, sk);
}
{
  auto sk =
      SortKeys({std::numeric_limits<int32_t>::min(), std::string_view("123")});
  auto min = sk.GetMinSortKeys();
  EXPECT_LE(min, sk);
}
}; // namespace arcanedb

} // namespace property
} // namespace arcanedb