#include "butil/logging.h"
#include "property/sort_key/sort_key.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace property {

TEST(SortKeyTest, BasicTest) {
  // generate two sort key, then compare
  auto sk1 = SortKeys({10, 20, "123"});
  auto sk2 = SortKeys({10, 20, "456"});
  EXPECT_LE(sk1, sk2);
  EXPECT_LE(sk1.as_ref(), sk2.as_ref());
  EXPECT_LE(sk1.as_slice(), sk2.as_slice());
  ARCANEDB_DEBUG("{} {}", sk1.ToString(), sk2.ToString());
}

} // namespace property
} // namespace arcanedb