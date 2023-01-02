#include "absl/container/flat_hash_map.h"
#include <gtest/gtest.h>

namespace arcanedb {

TEST(AbslTest, BasicTest) {
  auto hash_map = absl::flat_hash_map<int, int>();
  for (int i = 0; i < 10; i++) {
    EXPECT_TRUE(hash_map.find(i) == hash_map.end());
    hash_map[i] = 10 - i;
    EXPECT_FALSE(hash_map.find(i) == hash_map.end());
  }
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(hash_map[i], 10 - i);
  }
}

} // namespace arcanedb