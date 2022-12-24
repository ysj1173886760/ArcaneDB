/**
 * @file kv_page_store_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "page_store/kv_page_store/index_page.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace page_store {

TEST(KvPageStoreTest, IndexPageSerializationTest) {
  IndexPage index_page;
  index_page.UpdateReplacement("0");
  index_page.UpdateDelta("123");
  index_page.UpdateDelta("456");
  util::BufWriter writer;
  index_page.SerializationTo(&writer);

  auto bytes = writer.Detach();
  util::BufReader reader(bytes);
  IndexPage new_page;
  EXPECT_TRUE(new_page.DeserializationFrom(&reader).ok());
  EXPECT_EQ(new_page, index_page);
}

TEST(KvPageStoreTest, IndexPageUpdateTest) {
  IndexPage index_page;
  index_page.UpdateDelta("123");
  index_page.UpdateDelta("456");
  index_page.UpdateDelta("789");
  EXPECT_EQ(index_page.pages_.size(), 3);
  index_page.UpdateReplacement("10");
  EXPECT_EQ(index_page.pages_.size(), 1);
  EXPECT_EQ(index_page.pages_[0].page_id, "10");
  EXPECT_EQ(index_page.pages_[0].type, PageStore::PageType::BasePage);
}

} // namespace page_store
} // namespace arcanedb
