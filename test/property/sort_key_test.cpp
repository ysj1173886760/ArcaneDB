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
  LOG_DEBUG("%s %s", sk1.ToString().c_str(), sk2.ToString().c_str());
}

} // namespace property
} // namespace arcanedb